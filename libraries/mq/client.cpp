#include <koinos/mq/client.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/log.hpp>
#include <koinos/util.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <random>
#include <thread>

namespace koinos::mq {

namespace detail {

class client_impl final
{
public:
   client_impl();
   ~client_impl();

   error_code connect( const std::string& amqp_url, retry_policy policy );
   void disconnect();

   bool is_connected() const;

   std::shared_future< std::string > rpc(
      const std::string& service,
      const std::string& payload,
      uint64_t timeout_ms,
      retry_policy policy,
      const std::string& content_type );

   void broadcast(
      const std::string& routing_key,
      const std::string& payload,
      const std::string& content_type );

private:
   error_code on_connect( message_broker& m );
   void consumer( std::shared_ptr< message_broker > broker );
   void policy_handler( std::shared_future< std::string > future, std::shared_ptr< message > msg, retry_policy retry );

   std::map< std::string, std::promise< std::string > > _promise_map;
   std::mutex                                           _promise_map_mutex;

   std::string                                          _amqp_url;
   std::shared_ptr< message_broker >                    _broker;
   std::unique_ptr< std::thread >                       _reader_thread;
   std::string                                          _queue_name;
   std::atomic< bool >                                  _running   = true;
   bool                                                 _connected = false;
   static constexpr uint64_t                            _max_expiration = 30000;
   static constexpr std::size_t                         _correlation_id_len = 32;
};

client_impl::client_impl() : _broker( std::make_unique< message_broker >() ) {}

client_impl::~client_impl()
{
   disconnect();
}

error_code client_impl::connect( const std::string& amqp_url, retry_policy policy )
{
   error_code ec;

   _amqp_url = amqp_url;

   ec = _broker->connect(
      _amqp_url,
      policy,
      [this]( message_broker& m ) -> error_code
      {
         return this->on_connect( m );
      }
   );

   if ( ec != error_code::success )
   {
      return ec;
   }

   _reader_thread = std::make_unique< std::thread >( [&]()
   {
      consumer( _broker );
   } );

   _connected = true;

   return error_code::success;
}

void client_impl::disconnect()
{
   _running = false;

   if ( _reader_thread )
      _reader_thread->join();

   if ( _broker->is_connected() )
      _broker->disconnect();

   _connected = false;
}

bool client_impl::is_connected() const
{
   return _connected;
}

error_code client_impl::on_connect( message_broker& m )
{
   error_code ec;

   ec = m.declare_exchange(
      exchange::event,      // Name
      exchange_type::topic, // Type
      false,                // Passive
      true,                 // Durable
      false,                // Auto-deleted
      false                 // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "Error while declaring broadcast exchange";
      return ec;
   }

   ec = m.declare_exchange(
      exchange::rpc,         // Name
      exchange_type::direct, // Type
      false,                 // Passive
      true,                  // Durable
      false,                 // Auto-deleted
      false                  // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "Error while declaring rpc exchange";
      return ec;
   }

   auto queue_res = m.declare_queue(
      "",
      false, // Passive
      false, // Durable
      true,  // Exclusive
      true   // Auto-deleted
   );

   if ( queue_res.first != error_code::success )
   {
      LOG(error) << "Error while declaring temporary queue";
      return queue_res.first;
   }

   _queue_name = queue_res.second;

   ec = m.bind_queue( queue_res.second, exchange::rpc, queue_res.second );
   if ( ec != error_code::success )
   {
      LOG(error) << "Error while binding temporary queue";
      return ec;
   }

   return ec;
}

void client_impl::consumer( std::shared_ptr< message_broker > broker )
{
   while ( _running )
   {
      auto result = _broker->consume();

      if ( result.first == error_code::time_out )
      {
         continue;
      }

      if ( result.first != error_code::success )
      {
         LOG(error) << "Failed to consume message";
         continue;
      }

      if ( !result.second )
      {
         LOG(error) << "Consumption succeeded but resulted in an empty message";
         continue;
      }

      auto& msg = result.second;
      if ( !msg->correlation_id.has_value() )
      {
         LOG(error) << "Received message without a correlation id";
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

void client_impl::policy_handler( std::shared_future< std::string > future, std::shared_ptr< message > msg, retry_policy policy  )
{
   while ( future.wait_for( std::chrono::milliseconds( msg->expiration.value() ) ) != std::future_status::ready )
   {
      std::lock_guard< std::mutex > guard( _promise_map_mutex );
      auto node_handle = _promise_map.extract( msg->correlation_id.value() );
      if ( !node_handle.empty() )
      {
         switch ( policy )
         {
         case retry_policy::none:
            node_handle.mapped().set_exception( std::make_exception_ptr( timeout_error( "Request timeout: " + msg->correlation_id.value() ) ) );
            return;
         case retry_policy::exponential_backoff:
            LOG(warning) << "No response to message with correlation ID: "
               << msg->correlation_id.value() << ", within " << msg->expiration.value() << "ms";

            LOG(debug) << " -> correlation_id: " << msg->correlation_id.value();
            LOG(debug) << " -> exchange:       " << msg->exchange;
            LOG(debug) << " -> routing_key:    " << msg->routing_key;
            LOG(debug) << " -> content_type:   " << msg->content_type;
            LOG(debug) << " -> reply_to:       " << msg->reply_to.value();
            LOG(debug) << " -> expiration:     " << msg->expiration.value();
            LOG(debug) << " -> data:           " << msg->data;

            // Adjust our message for another attempt
            auto old_correlation_id = msg->correlation_id.value();

            msg->correlation_id = random_alphanumeric( _correlation_id_len );
            msg->expiration     = std::min( msg->expiration.value() * 2, _max_expiration );

            LOG(debug) << "Resending message (correlation ID: " << old_correlation_id << ") with new correlation ID: "
               << msg->correlation_id.value() << ", expiration: " << msg->expiration.value();

            // Publish another attempt
            if ( _broker->publish( *msg ) != error_code::success )
            {
               node_handle.mapped().set_exception( std::make_exception_ptr( amqp_publish_error( "Error sending RPC message" ) ) );
               return;
            }

            // Update and reinsert our node handle
            node_handle.key()              = msg->correlation_id.value();
            const auto [ it, success, nh ] = _promise_map.insert( std::move( node_handle ) );

            if ( !success )
            {
               nh.mapped().set_exception( std::make_exception_ptr( correlation_id_collision( "Error recording correlation id" ) ) );
               return;
            }
            break;
         }
      }
      else
      {
         LOG(warning) << "Correlation ID " << msg->correlation_id.value() << " expected but not found in the promise map";
         return;
      }
   }
}

std::shared_future< std::string > client_impl::rpc(
   const std::string& service,
   const std::string& payload,
   uint64_t timeout_ms,
   retry_policy policy,
   const std::string& content_type )
{
   KOINOS_ASSERT( _running, client_not_running, "Client is not running" );

   auto promise = std::promise< std::string >();

   auto msg = std::make_shared< message >();
   msg->exchange = exchange::rpc;
   msg->routing_key = service_routing_key( service );
   msg->content_type = content_type;
   msg->data = payload;
   msg->reply_to = _queue_name;
   msg->correlation_id = random_alphanumeric( _correlation_id_len );

   if ( timeout_ms > 0 )
      msg->expiration = timeout_ms;

   LOG(debug) << "Sending RPC";
   LOG(debug) << " -> correlation_id: " << *msg->correlation_id;
   LOG(debug) << " -> exchange:       " << msg->exchange;
   LOG(debug) << " -> routing_key:    " << msg->routing_key;
   LOG(debug) << " -> content_type:   " << msg->content_type;
   LOG(debug) << " -> reply_to:       " << *msg->reply_to;

   if ( msg->expiration.has_value() )
      LOG(debug) << " -> expiration:     " << *msg->expiration;

   LOG(debug) << " -> data:           " << msg->data;

   auto err = _broker->publish( *msg );
   if ( err != error_code::success )
   {
      promise.set_exception( std::make_exception_ptr( amqp_publish_error( "Error sending RPC message" ) ) );
      return promise.get_future();
   }

   std::shared_future< std::string > future_val;
   {
      std::lock_guard< std::mutex > guard( _promise_map_mutex );
      auto empl_res = _promise_map.emplace( *msg->correlation_id, std::move( promise ) );

      if ( !empl_res.second )
      {
         promise = std::promise< std::string >();
         promise.set_exception( std::make_exception_ptr( correlation_id_collision( "Error recording correlation id" ) ) );
         return promise.get_future();
      }

      future_val = empl_res.first->second.get_future();
   }

   if ( msg->expiration.has_value() )
   {
      std::thread( &client_impl::policy_handler, this, future_val, msg, policy ).detach();
   }

   return future_val;
}

void client_impl::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type )
{
   KOINOS_ASSERT( _running, client_not_running, "Client is not running" );

   auto err = _broker->publish( message {
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

error_code client::connect( const std::string& amqp_url, retry_policy policy )
{
   return _my->connect( amqp_url, policy );
}

std::shared_future< std::string > client::rpc(
   const std::string& service,
   const std::string& payload,
   uint64_t timeout_ms,
   retry_policy policy,
   const std::string& content_type )
{
   return _my->rpc( service, payload, timeout_ms, policy, content_type );
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
