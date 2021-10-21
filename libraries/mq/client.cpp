#include <koinos/mq/client.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/log.hpp>
#include <koinos/util/hex.hpp>
#include <koinos/util/random.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <random>
#include <thread>

using namespace std::chrono_literals;

namespace koinos::mq {

namespace detail {

class client_impl final
{
public:
   client_impl();
   ~client_impl();

   void connect( const std::string& amqp_url, retry_policy policy );
   void disconnect();

   bool is_running() const;

   std::shared_future< std::string > rpc(
      const std::string& service,
      const std::string& payload,
      std::chrono::milliseconds timeout,
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

   void set_queue_name( const std::string& s );
   std::string get_queue_name();

   std::map< std::string, std::promise< std::string > > _promise_map;
   std::mutex                                           _promise_map_mutex;

   std::string                                          _queue_name;
   std::mutex                                           _queue_name_mutex;

   std::shared_ptr< message_broker >                    _writer_broker;
   std::shared_ptr< message_broker >                    _reader_broker;
   std::unique_ptr< std::thread >                       _reader_thread;
   std::atomic< bool >                                  _running = false;
   static constexpr uint64_t                            _max_expiration = 30000;
   static constexpr std::size_t                         _correlation_id_len = 32;
};

client_impl::client_impl() :
   _writer_broker( std::make_shared< message_broker >() ),
   _reader_broker( std::make_shared< message_broker >() ) {}

client_impl::~client_impl()
{
   try
   {
      if ( is_running() )
         disconnect();
   }
   catch( ... ) {}
}

void client_impl::set_queue_name( const std::string& s )
{
   std::lock_guard< std::mutex > lock( _queue_name_mutex );
   _queue_name = s;
}

std::string client_impl::get_queue_name()
{
   std::lock_guard< std::mutex > lock( _queue_name_mutex );
   return _queue_name;
}

void client_impl::connect( const std::string& amqp_url, retry_policy policy )
{
   KOINOS_ASSERT( !is_running(), broker_already_running, "client is already running" );

   error_code ec;

   ec = _writer_broker->connect( amqp_url, policy );

   if ( ec != error_code::success )
   {
      KOINOS_THROW( unable_to_connect, "could not connect to endpoint: ${e}", ("e", amqp_url.c_str()) );
   }

   ec = _reader_broker->connect(
      amqp_url,
      policy,
      [this]( message_broker& m ) -> error_code
      {
         return this->on_connect( m );
      }
   );

   if ( ec != error_code::success )
   {
      _writer_broker->disconnect();
      KOINOS_THROW( unable_to_connect, "could not connect to endpoint: ${e}", ("e", amqp_url.c_str()) );
   }

   _running = true;

   _reader_thread = std::make_unique< std::thread >( [&]()
   {
      consumer( _reader_broker );
   } );
}

void client_impl::disconnect()
{
   KOINOS_ASSERT( is_running(), client_not_running, "client is already disconnected" );

   _running = false;

   _writer_broker->disconnect();
   _reader_broker->disconnect();

   if ( _reader_thread && _reader_thread->joinable() )
      _reader_thread->join();

   {
      std::lock_guard< std::mutex > lock( _promise_map_mutex );
      for ( auto it = _promise_map.begin(); it != _promise_map.end(); ++it )
      {
         it->second.set_exception( std::make_exception_ptr( client_not_running( "client has disconnected" ) ) );
         _promise_map.erase( it );
      }
   }
}

bool client_impl::is_running() const
{
   return _running;
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

   set_queue_name( queue_res.second );

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
      auto result = _reader_broker->consume();

      if ( result.first == error_code::time_out )
      {
         continue;
      }

      if ( result.first != error_code::success )
      {
         LOG(warning) << "Failed to consume message";
         continue;
      }

      if ( !result.second )
      {
         LOG(warning) << "Consumption succeeded but resulted in an empty message";
         continue;
      }

      auto& msg = result.second;
      if ( !msg->correlation_id.has_value() )
      {
         LOG(warning) << "Received message without a correlation id";
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
            node_handle.mapped().set_exception( std::make_exception_ptr( timeout_error( "request timeout: " + msg->correlation_id.value() ) ) );
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
            LOG(debug) << " -> data:           " << util::to_hex( msg->data );

            // Adjust our message for another attempt
            auto old_correlation_id = msg->correlation_id.value();

            msg->correlation_id = util::random_alphanumeric( _correlation_id_len );
            msg->expiration     = std::min( msg->expiration.value() * 2, _max_expiration );
            msg->reply_to       = get_queue_name();

            LOG(debug) << "Resending message (correlation ID: " << old_correlation_id << ") with new correlation ID: "
               << msg->correlation_id.value() << ", expiration: " << msg->expiration.value();

            // Publish another attempt
            if ( _writer_broker->publish( *msg ) != error_code::success )
            {
               node_handle.mapped().set_exception( std::make_exception_ptr( broker_publish_error( "error sending RPC message" ) ) );
               return;
            }

            // Update and reinsert our node handle
            node_handle.key()              = msg->correlation_id.value();
            const auto [ it, success, nh ] = _promise_map.insert( std::move( node_handle ) );

            if ( !success )
            {
               nh.mapped().set_exception( std::make_exception_ptr( correlation_id_collision( "error recording correlation id" ) ) );
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
   std::chrono::milliseconds timeout,
   retry_policy policy,
   const std::string& content_type )
{
   KOINOS_ASSERT( _running, client_not_running, "client is not running" );

   auto promise = std::promise< std::string >();

   auto msg = std::make_shared< message >();
   msg->exchange = exchange::rpc;
   msg->routing_key = service_routing_key( service );
   msg->content_type = content_type;
   msg->data = payload;
   msg->reply_to = get_queue_name();
   msg->correlation_id = util::random_alphanumeric( _correlation_id_len );

   if ( timeout.count() > 0 )
      msg->expiration = timeout.count();

   LOG(debug) << "Sending RPC";
   LOG(debug) << " -> correlation_id: " << *msg->correlation_id;
   LOG(debug) << " -> exchange:       " << msg->exchange;
   LOG(debug) << " -> routing_key:    " << msg->routing_key;
   LOG(debug) << " -> content_type:   " << msg->content_type;
   LOG(debug) << " -> reply_to:       " << *msg->reply_to;

   if ( msg->expiration.has_value() )
      LOG(debug) << " -> expiration:     " << *msg->expiration;

   LOG(debug) << " -> data:           " << util::to_hex( msg->data );

   auto err = _writer_broker->publish( *msg );
   if ( err != error_code::success )
   {
      promise.set_exception( std::make_exception_ptr( broker_publish_error( "error sending RPC message" ) ) );
      return promise.get_future();
   }

   std::shared_future< std::string > future_val;
   {
      std::lock_guard< std::mutex > guard( _promise_map_mutex );
      auto empl_res = _promise_map.emplace( *msg->correlation_id, std::move( promise ) );

      if ( !empl_res.second )
      {
         promise = std::promise< std::string >();
         promise.set_exception( std::make_exception_ptr( correlation_id_collision( "error recording correlation id" ) ) );
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
   KOINOS_ASSERT( _running, client_not_running, "client is not running" );

   auto err = _writer_broker->publish( message {
      .exchange     = exchange::event,
      .routing_key  = routing_key,
      .content_type = content_type,
      .data         = payload
   } );

   KOINOS_ASSERT( err == error_code::success, broker_publish_error, "error broadcasting message" );
}

} // detail

client::client() : _my( std::make_unique< detail::client_impl >() ) {}
client::~client() = default;

void client::connect( const std::string& amqp_url, retry_policy policy )
{
   _my->connect( amqp_url, policy );
}

std::shared_future< std::string > client::rpc(
   const std::string& service,
   const std::string& payload,
   std::chrono::milliseconds timeout,
   retry_policy policy,
   const std::string& content_type )
{
   return _my->rpc( service, payload, timeout, policy, content_type );
}

void client::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type )
{
   _my->broadcast( routing_key, payload, content_type );
}

bool client::is_running() const
{
   return _my->is_running();
}

void client::disconnect()
{
   _my->disconnect();
}

} // koinos::mq
