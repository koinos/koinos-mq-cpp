#include <koinos/mq/client.hpp>

#include <map>
#include <mutex>

namespace koinos::mq {

namespace detail {

std::string random_string( int32_t len )
{
   std::string s;
   s.resize( len );

   for( int32_t i = 0; i < len; i++ )
   {
      s[i] = rand() / 26 + 65;
   }

   return s;
}

struct client_impl
{
   error_code connect( const std::string& amqp_url );

   std::future< std::string > rpc( const std::string& content_type, const std::string& rpc_type, const std::string& payload, int64_t timeout_ms );
   void broadcast( const std::string& content_type, const std::string& routing_key, const std::string& payload );

   std::map< std::string, std::promise< std::string > > _promise_map;
   std::mutex                                           _promise_map_mutex;

   std::shared_ptr< message_broker >                    _writer_broker;

   std::unique_ptr< std::thread >                       _reader_thread;
   std::shared_ptr< message_broker >                    _reader_broker;
};

error_code client_impl::connect( const std::string& amqp_url )
{
   error_code ec;

   ec = _writer_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   ec = _reader_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   return error_code::success;
}

std::future< std::string > client_impl::rpc( const std::string& content_type, const std::string& rpc_type, const std::string& payload, int64_t timeout_ms )
{
   auto promise = std::promise< std::string >();
   message msg;
   msg.exchange = "koinos_rpc";
   msg.routing_key = "koinos_rpc_" + rpc_type;
   msg.content_type = content_type;
   msg.data = payload;
   // TODO: Get reply_to from reply queue name
   // msg.reply_to =
   msg.correlation_id = random_string( 32 );

   auto err = _writer_broker->publish( msg );
   if ( err != error_code::success )
   {
      promise.set_exception( std::make_exception_ptr( amqp_publish_error( "Error sending rpc message" ) ) );
      return promise.get_future();
   }


   std::lock_guard< std::mutex > guard( _promise_map_mutex );
   auto empl_res = _promise_map.emplace( *msg.correlation_id, std::move(promise) );

   if ( !empl_res.second )
   {
      promise = std::promise< std::string >();
      promise.set_exception( std::make_exception_ptr( correlation_id_collision( "Error recording correlation id" ) ) );
      return promise.get_future();
   }

   if ( timeout_ms > 0 )
   {
      std::async(
         std::launch::async,
         [&](std::future< std::string > future)
         {
            auto status = future.wait_for( std::chrono::milliseconds( timeout_ms ) );
            if ( status != std::future_status::ready )
            {
               std::lock_guard< std::mutex > guard( _promise_map_mutex );
               auto itr = _promise_map.find( *msg.correlation_id );
               if ( itr != _promise_map.end() )
               {
                  itr->second.set_exception( std::make_exception_ptr( timeout_error( "Request timeout" ) ) );
                  _promise_map.erase( itr );
               }
            }
         },
         empl_res.first->second.get_future()
      );
   }


   return empl_res.first->second.get_future();
}

void client_impl::broadcast( const std::string& content_type, const std::string& routing_key, const std::string& payload )
{
   auto err = _writer_broker->publish( message {
      .exchange     = "koinos_event",
      .routing_key  = routing_key,
      .content_type = content_type,
      .data         = payload
   } );

   KOINOS_ASSERT( err == error_code::success, amqp_publish_error, "Error broadcasting message" );
}

} // detail

error_code client::connect( const std::string& amqp_url )
{
   return _my->connect( amqp_url );
}

std::future< std::string > client::rpc( const std::string& content_type, const std::string& rpc_type, const std::string& payload, int64_t timeout_ms )
{
   return _my->rpc( content_type, rpc_type, payload, timeout_ms );
}

void client::broadcast( const std::string& content_type, const std::string& routing_key, const std::string& payload )
{
   _my->broadcast( content_type, routing_key, payload );
}

} // koinos::mq
