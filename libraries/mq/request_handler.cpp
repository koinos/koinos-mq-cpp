#include <koinos/mq/request_handler.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/util/hex.hpp>
#include <koinos/util/overloaded.hpp>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>

#include <chrono>
#include <cstdlib>

namespace koinos::mq {

void request_handler::handle_message()
{
   if ( _stopped )
      return;

   std::shared_ptr< message > msg;

   _input_queue.pull_front( msg );

   auto reply = std::make_shared< message >();
   auto routing_itr = _handler_map.find( std::make_pair( msg->exchange, msg->routing_key ) );

   if ( routing_itr == _handler_map.end() )
   {
      LOG(error) << "Did not find route: " << msg->exchange << ":" << msg->routing_key;
   }
   else
   {
      for ( const auto& h_pair : routing_itr->second )
      {
         if ( !h_pair.first( msg->content_type ) )
            continue;

         std::visit(
            util::overloaded {
               [&]( const msg_handler_string_func& f )
               {
                  auto resp = f( msg->data );
                  if ( msg->reply_to.has_value() && msg->correlation_id.has_value() )
                  {
                     reply->exchange       = exchange::rpc;
                     reply->routing_key    = *msg->reply_to;
                     reply->content_type   = msg->content_type;
                     reply->correlation_id = *msg->correlation_id;
                     reply->data           = resp;
                     reply->delivery_tag   = msg->delivery_tag;

                     _output_queue.push_back( reply );

                     boost::asio::post( _io_context, std::bind( &request_handler::publish, this ) );
                  }
               },
               [&]( const msg_handler_void_func& f )
               {
                  f( msg->data );
               },
               [&]( const auto& ) {}
            },h_pair.second );
         break;
      }
   }
}

request_handler::request_handler( boost::asio::io_context& io_context ) :
   _publisher_broker( std::make_unique< message_broker >() ),
   _consumer_broker( std::make_unique< message_broker >() ),
   _io_context( io_context ),
   _signals( io_context ),
   _retryer( io_context, std::chrono::milliseconds( 30000 ) )
{
   _signals.add( SIGINT );
   _signals.add( SIGTERM );
#if defined(SIGQUIT)
   _signals.add( SIGQUIT );
#endif // defined(SIGQUIT)
   static_assert( std::atomic_bool::is_always_lock_free );

   _signals.async_wait( [&]( const boost::system::error_code& err, int num )
   {
      _stopped = true;
   } );
}

request_handler::~request_handler()
{
   _retryer.cancel();
   disconnect();
}

void request_handler::disconnect()
{
   if ( _consumer_broker->connected() )
      _consumer_broker->disconnect();

   if ( _publisher_broker->connected() )
      _publisher_broker->disconnect();
}

void request_handler::connect( const std::string& amqp_url, retry_policy policy )
{
   KOINOS_ASSERT( !connected(), request_handler_already_connected, "request handler is already connected" );
   KOINOS_ASSERT( running(), request_handler_not_running, "request handler is not running" );

   _amqp_url = amqp_url;

   error_code code = _retryer.with_policy(
      policy,
      [&]() -> error_code
      {
         error_code e;

         e = _publisher_broker->connect( _amqp_url );

         if ( e != error_code::success )
            return e;

         e = _consumer_broker->connect(
            _amqp_url,
            [this]( message_broker& m ) -> error_code
            {
               return this->on_connect( m );
            }
         );

         if ( e != error_code::success )
            _publisher_broker->disconnect();

         return e;
      },
      "request handler connection to AMQP"
   );

   if ( code != error_code::success )
   {
      _publisher_broker->disconnect();
      KOINOS_THROW( mq_connection_failure, "could not connect consumer to amqp server ${a}", ("a", amqp_url) );
   }

   boost::asio::post( _io_context, std::bind( &request_handler::consume, this ) );
}

error_code request_handler::on_connect( message_broker& m )
{
   error_code ec;

   _queue_bindings.clear();
   _handler_map.clear();

   for ( const auto& msg_handler : _message_handlers )
   {
      std::string queue_name;
      auto binding = std::make_pair( msg_handler.exchange, msg_handler.routing_key );
      auto binding_itr = _queue_bindings.find( binding );
      error_code ec = error_code::success;

      if ( binding_itr == _queue_bindings.end() )
      {
         ec = _consumer_broker->declare_exchange(
            msg_handler.exchange,
            msg_handler.competing_consumer ? exchange_type::direct : exchange_type::topic,
            false, // Passive
            true,  // Durable
            false, // Auto Delete
            false  // Internal
         );
         if ( ec != error_code::success )
            return ec;

         auto queue_res = _consumer_broker->declare_queue(
            msg_handler.competing_consumer ? msg_handler.routing_key : "",
            false,               // Passive
            msg_handler.competing_consumer,  // Durable
            !msg_handler.competing_consumer, // Exclusive
            false                // Internal
         );
         if ( queue_res.first != error_code::success )
            return queue_res.first;

         ec = _consumer_broker->bind_queue( queue_res.second, msg_handler.exchange, msg_handler.routing_key, true );
         if ( ec != error_code::success )
            return ec;

         queue_name = queue_res.second;
         _queue_bindings.emplace( binding, queue_name );
      }
      else
      {
         queue_name = binding_itr->second;
      }

      // Valid routes are:
      //    exchange, routing_key
      //    "", queue_name
      auto default_binding = std::make_pair( "", queue_name );
      auto handler_itr = _handler_map.find( binding );

      if ( handler_itr == _handler_map.end() )
      {
         _handler_map.emplace( binding,         std::vector< handler_pair >{ std::make_pair( msg_handler.verify, msg_handler.handler ) } );
         _handler_map.emplace( default_binding, std::vector< handler_pair >{ std::make_pair( msg_handler.verify, msg_handler.handler ) } );
      }
      else
      {
         handler_itr->second.emplace_back( std::make_pair( msg_handler.verify, msg_handler.handler ) );

         handler_itr = _handler_map.find( default_binding );
         if ( handler_itr == _handler_map.end() )
         {
            _handler_map[ binding ].pop_back();
            LOG(error) << "Default binding route not found in handler map";
            ec = error_code::failure;
         }
         else
         {
            handler_itr->second.emplace_back( std::make_pair( msg_handler.verify, msg_handler.handler ) );
         }
      }
   }

   return ec;
}

void request_handler::add_broadcast_handler(
   const std::string& routing_key,
   msg_handler_void_func func,
   handler_verify_func vfunc )
{
   add_msg_handler( exchange::event, routing_key, false, vfunc, func );
}

void request_handler::add_rpc_handler(
   const std::string& service,
   msg_handler_string_func func,
   handler_verify_func vfunc )
{
   add_msg_handler( exchange::rpc, service_routing_key( service ), true, vfunc, func );
}

void request_handler::add_msg_handler(
   const std::string& exchange,
   const std::string& routing_key,
   bool competing_consumer,
   handler_verify_func verify,
   msg_handler_func handler )
{
   KOINOS_ASSERT( !connected(), request_handler_already_connected, "message handlers should be added to the request handler prior to amqp connection" );

   _message_handlers.push_back(
      {
         exchange,
         routing_key,
         competing_consumer,
         verify,
         handler
      }
   );
}

void request_handler::add_msg_handler(
   const std::string& exchange,
   const std::string& routing_key,
   bool exclusive,
   handler_verify_func verify,
   msg_handler_string_func handler )
{
   add_msg_handler( exchange, routing_key, exclusive, verify, msg_handler_func( handler ) );
}

void request_handler::publish()
{
   if ( _stopped )
      return;

   std::shared_ptr< message > m;

   _output_queue.pull_front( m );

   auto r = _retryer.with_policy(
      retry_policy::exponential_backoff,
      [&]() -> error_code
      {
         error_code e = _publisher_broker->publish( *m );

         if ( e == error_code::success )
            return e;

         _publisher_broker->disconnect();
         e = _publisher_broker->connect( _amqp_url );

         if ( e == error_code::success )
            e = _publisher_broker->publish( *m );

         return e;
      },
      "request handler publication"
   );

   if ( r != error_code::success )
   {
      LOG(error) << "An error has occurred while publishing message on the request handler";
   }
}

void request_handler::consume()
{
   if ( _stopped )
      return;

   error_code code;
   std::shared_ptr< message > msg;

   code = _retryer.with_policy(
      retry_policy::exponential_backoff,
      [&]() -> error_code
      {
         auto [ e, m ] = _consumer_broker->consume();

         if ( e != error_code::failure )
         {
            msg = m;
            return e;
         }

         _consumer_broker->disconnect();

         e = _consumer_broker->connect(
            _amqp_url,
            [this]( message_broker& m ) -> error_code
            {
               return this->on_connect( m );
            }
         );

         if ( e == error_code::success )
         {
            std::tie( e, m ) = _consumer_broker->consume();

            if ( e != error_code::failure )
               msg = m;
         }

         return e;
      },
      "request handler message consumption"
   );

   boost::asio::post( _io_context, std::bind( &request_handler::consume, this ) );

   if ( code == error_code::time_out ) {}
   else if ( code != error_code::success )
   {
      LOG(warning) << "Request handler failed to consume message";
   }
   else if ( !msg )
   {
      LOG(debug) << "Request handler message consumption succeeded but resulted in an empty message";
   }
   else
   {
      LOG(debug) << "Request handler received message: " << to_string( *msg );

      _input_queue.push_back( msg );

      boost::asio::dispatch( _io_context, std::bind( &request_handler::handle_message, this ) );
   }
}

bool request_handler::running() const
{
   return !_io_context.stopped();
}

bool request_handler::connected() const
{
   return _publisher_broker->connected() && _consumer_broker->connected();
}

bool request_handler::ready() const
{
   return connected() && running();
}

} // koinos::mq
