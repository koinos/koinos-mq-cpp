#include <koinos/mq/client.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/log.hpp>

#include <atomic>
#include <map>
#include <mutex>
#include <random>

namespace koinos::mq {

namespace detail {

class client_impl final
{
public:
   client_impl();
   ~client_impl();

   error_code connect( const std::string& amqp_url );
   void disconnect();

   bool is_connected() const;

   std::shared_future< std::string > rpc( const std::string& service, const std::string& payload, const std::string& content_type, int64_t timeout_ms );
   void broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type );

private:
   error_code prepare();
   void consumer( std::shared_ptr< message_broker > broker );
   std::string random_alphanumeric( std::size_t len );

   std::map< std::string, std::promise< std::string > > _promise_map;
   std::mutex                                           _promise_map_mutex;

   std::shared_ptr< message_broker >                    _writer_broker;

   std::unique_ptr< std::thread >                       _reader_thread;
   std::shared_ptr< message_broker >                    _reader_broker;
   std::string                                          _queue_name;
   std::atomic< bool >                                  _running   = true;
   bool                                                 _connected = false;

   std::mt19937                                         _random_generator;
};

client_impl::client_impl() :
   _writer_broker( std::make_unique< message_broker >() ),
   _reader_broker( std::make_unique< message_broker >() ),
   _random_generator( std::random_device()() ) {}

client_impl::~client_impl()
{
   disconnect();
}

std::string client_impl::random_alphanumeric( std::size_t len )
{
   auto random_char = [this]() -> char
   {
      constexpr char charset[] =
         "0123456789"
         "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
         "abcdefghijklmnopqrstuvwxyz";
      constexpr std::size_t max_index = sizeof( charset ) - 1;
      std::uniform_int_distribution<> distribution( 0, max_index );
      return charset[ distribution( this->_random_generator ) % max_index ];
   };
   std::string str( len, 0 );
   std::generate_n( str.begin(), len, random_char );
   return str;
}

error_code client_impl::connect( const std::string& amqp_url )
{
   error_code ec;

   ec = _writer_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   ec = _reader_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   ec = prepare();
   if ( ec != error_code::success )
   {
      disconnect();
      return ec;
   }

   _reader_thread = std::make_unique< std::thread >( [&]()
   {
      consumer( _reader_broker );
   } );

   _connected = true;

   return error_code::success;
}

void client_impl::disconnect()
{
   _running = false;

   if ( _reader_thread )
      _reader_thread->join();

   if ( _writer_broker->is_connected() )
      _writer_broker->disconnect();

   if ( _reader_broker->is_connected() )
      _reader_broker->disconnect();

   _connected = false;
}

bool client_impl::is_connected() const
{
   return _connected;
}

error_code client_impl::prepare()
{
   error_code ec;

   ec = _reader_broker->declare_exchange(
      exchange::event,      // Name
      exchange_type::topic, // Type
      false,                // Passive
      true,                 // Durable
      false,                // Auto-deleted
      false                 // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "error while declaring broadcast exchange";
      return ec;
   }

   ec = _reader_broker->declare_exchange(
      exchange::rpc,         // Name
      exchange_type::direct, // Type
      false,                 // Passive
      true,                  // Durable
      false,                 // Auto-deleted
      false                  // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "error while declaring rpc exchange";
      return ec;
   }

   auto queue_res = _reader_broker->declare_queue(
      "",
      false, // Passive
      false, // Durable
      true,  // Exclusive
      true   // Auto-deleted
   );

   if ( queue_res.first != error_code::success )
   {
      LOG(error) << "error while declaring temporary queue";
      return queue_res.first;
   }

   _queue_name = queue_res.second;

   ec = _reader_broker->bind_queue( queue_res.second, exchange::rpc, queue_res.second );
   if ( ec != error_code::success )
   {
      LOG(error) << "error while binding temporary queue";
      return ec;
   }

   return ec;
}

void client_impl::consumer( std::shared_ptr< message_broker > broker )
{
   while ( _running )
   {
      auto result = _reader_broker->consume();

      if ( result.first == error_code::time_out )
      {
         continue;
      }

      if ( result.first != error_code::success )
      {
         LOG(error) << "failed to consume message";
         continue;
      }

      if ( !result.second )
      {
         LOG(error) << "consumption succeeded but resulted in an empty message";
         continue;
      }

      auto& msg = result.second;
      if ( !msg->correlation_id.has_value() )
      {
         LOG(error) << "received message without a correlation id";
         continue;
      }

      {
         std::lock_guard< std::mutex > lock( _promise_map_mutex );
         auto it = _promise_map.find( *msg->correlation_id );
         if ( it != _promise_map.end() )
         {
            it->second.set_value( std::move( msg->data ) );
            _promise_map.erase( it );
         }
      }
   }
}

std::shared_future< std::string > client_impl::rpc( const std::string& service, const std::string& payload, const std::string& content_type, int64_t timeout_ms )
{
   KOINOS_ASSERT( _running, client_not_running, "Client is not running" );

   auto promise = std::promise< std::string >();
   message msg;
   msg.exchange = exchange::rpc;
   msg.routing_key = service_routing_key( service );
   msg.content_type = content_type;
   msg.data = payload;
   msg.reply_to = _queue_name;
   msg.correlation_id = random_alphanumeric( 32 );

   auto err = _writer_broker->publish( msg );
   if ( err != error_code::success )
   {
      promise.set_exception( std::make_exception_ptr( amqp_publish_error( "Error sending rpc message" ) ) );
      return promise.get_future();
   }

   std::shared_future< std::string > future_val;
   {
      std::lock_guard< std::mutex > guard( _promise_map_mutex );
      auto empl_res = _promise_map.emplace( *msg.correlation_id, std::move(promise) );

      if ( !empl_res.second )
      {
         promise = std::promise< std::string >();
         promise.set_exception( std::make_exception_ptr( correlation_id_collision( "Error recording correlation id" ) ) );
         return promise.get_future();
      }

      future_val = empl_res.first->second.get_future();
   }

   if ( timeout_ms > 0 )
   {
      std::async(
         std::launch::async,
         [&]( std::shared_future< std::string > future )
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
         future_val
      );
   }


   return future_val;
}

void client_impl::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type )
{
   KOINOS_ASSERT( _running, client_not_running, "Client is not running" );

   auto err = _writer_broker->publish( message {
      .exchange     = exchange::event,
      .routing_key  = routing_key,
      .content_type = content_type,
      .data         = payload
   } );

   KOINOS_ASSERT( err == error_code::success, amqp_publish_error, "Error broadcasting message" );
}

} // detail

client::client() : _my( std::make_unique< detail::client_impl >() ) {}
client::~client() = default;

error_code client::connect( const std::string& amqp_url )
{
   return _my->connect( amqp_url );
}

std::shared_future< std::string > client::rpc( const std::string& service, const std::string& payload, const std::string& content_type, int64_t timeout_ms )
{
   return _my->rpc( service, payload, content_type, timeout_ms );
}

void client::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type )
{
   _my->broadcast( routing_key, payload, content_type );
}

bool client::is_connected() const
{
   return _my->is_connected();
}

void client::disconnect()
{
   _my->disconnect();
}

} // koinos::mq
