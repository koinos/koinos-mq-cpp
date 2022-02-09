#include <koinos/mq/client.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/log.hpp>
#include <koinos/util/hex.hpp>
#include <koinos/util/random.hpp>

#include <boost/asio/high_resolution_timer.hpp>
#include <boost/asio/post.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <random>
#include <thread>

using namespace std::chrono_literals;

namespace koinos::mq {

namespace detail {

struct request
{
   std::string                         correlation_id;
   std::chrono::milliseconds           expiration;
   retry_policy                        policy;
   mutable std::promise< std::string > response;
   mutable std::shared_ptr< message >  msg;
};

struct by_correlation_id{};
struct by_expiration{};

typedef boost::multi_index::multi_index_container<
  request,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag< by_correlation_id >, BOOST_MULTI_INDEX_MEMBER( request, std::string, correlation_id )>,
    boost::multi_index::ordered_non_unique<
      boost::multi_index::tag< by_expiration >, BOOST_MULTI_INDEX_MEMBER( request, std::chrono::milliseconds, expiration )>
   >
> request_set;

class client_impl final
{
public:
   client_impl( boost::asio::io_context& io_context );
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
   void consume( const boost::system::error_code& ec );
   void policy_handler( const boost::system::error_code& ec );
   void abort();

   void set_queue_name( const std::string& s );
   std::string get_queue_name();

   request_set                                          _requests;
   std::mutex                                           _requests_mutex;

   std::string                                          _queue_name;
   std::mutex                                           _queue_name_mutex;

   std::shared_ptr< message_broker >                    _writer_broker;
   std::shared_ptr< message_broker >                    _reader_broker;
   static constexpr uint64_t                            _max_expiration = 30000;
   static constexpr std::size_t                         _correlation_id_len = 32;
   boost::asio::io_context&                             _io_context;
   boost::asio::high_resolution_timer                   _timer;
};

client_impl::client_impl( boost::asio::io_context& io_context ) :
   _io_context( io_context ),
   _writer_broker( std::make_shared< message_broker >() ),
   _reader_broker( std::make_shared< message_broker >() ),
   _timer( io_context ) {}

client_impl::~client_impl()
{
   disconnect();
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
//   KOINOS_ASSERT( !is_running(), broker_already_running, "client is already running" );

   error_code ec;

   ec = _writer_broker->connect( amqp_url );

   if ( ec != error_code::success )
   {
      KOINOS_THROW( unable_to_connect, "could not connect to endpoint: ${e}", ("e", amqp_url.c_str()) );
   }

   ec = _reader_broker->connect(
      amqp_url,
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

   boost::asio::post( _io_context, std::bind( &client_impl::consume, this, boost::system::error_code{} ) );
   _timer.async_wait( std::bind( &client_impl::policy_handler, this, boost::system::error_code{} ) );
   _timer.expires_from_now( 1s );
}

void client_impl::abort()
{
   LOG(info) << "abort() called";
   std::lock_guard< std::mutex > lock( _requests_mutex );
   for ( auto it = _requests.begin(); it != _requests.end(); ++it )
   {
      LOG(info) << "set_exception";
      it->response.set_exception( std::make_exception_ptr( client_not_running( "client has disconnected" ) ) );
      LOG(info) << "erase";
      _requests.erase( it );
   }
}

void client_impl::disconnect()
{
   if ( _writer_broker->connected() )
      _writer_broker->disconnect();

   if ( _reader_broker->connected() )
      _reader_broker->disconnect();
}

bool client_impl::is_running() const
{
   return !_io_context.stopped();
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

void client_impl::consume( const boost::system::error_code& ec )
{
   LOG(info) << "consume()";
   if ( ec )
   {
      LOG(info) << "operation_aborted in consume()";
      return abort();
   }

   auto result = _reader_broker->consume();

   if ( result.first == error_code::time_out ) {}
   else if ( result.first != error_code::success )
   {
      LOG(warning) << "Failed to consume message";
   }
   else if ( !result.second )
   {
      LOG(warning) << "Consumption succeeded but resulted in an empty message";
   }
   else
   {
      auto& msg = result.second;
      if ( !msg->correlation_id.has_value() )
      {
         LOG(warning) << "Received message without a correlation id";
      }
      else
      {
         std::lock_guard< std::mutex > lock( _requests_mutex );
         const auto& idx = boost::multi_index::get< by_correlation_id >( _requests );
         auto it = idx.find( *msg->correlation_id );
         if ( it != idx.end() )
         {
            it->response.set_value( std::move( msg->data ) );
            _requests.erase( it );
         }
      }
   }

   boost::asio::post( _io_context, std::bind( &client_impl::consume, this, boost::system::error_code{} ) );
}

void client_impl::policy_handler( const boost::system::error_code& ec )
{
   LOG(info) << "policy_handler()";
   if ( ec )
   {
      LOG(info) << "operation_aborted in policy_handler()";
      return abort();
   }

   std::lock_guard< std::mutex > guard( _requests_mutex );
   auto& idx = boost::multi_index::get< by_expiration >( _requests );

   for ( auto it = idx.begin(); it != idx.end(); ++it )
   {
      if ( it->expiration >= std::chrono::duration_cast< std::chrono::milliseconds >( std::chrono::system_clock::now().time_since_epoch() ) )
         break;

      switch ( it->policy )
      {
      case retry_policy::none:
         it->response.set_exception( std::make_exception_ptr( timeout_error( "request timeout: " + it->correlation_id ) ) );
         idx.erase( it );
         return;
      case retry_policy::exponential_backoff:
         LOG(warning) << "No response to message with correlation ID: " << it->msg->correlation_id.value() << ", within " << it->msg->expiration.value() << "ms";
         LOG(debug) << " -> correlation_id: " << it->msg->correlation_id.value();
         LOG(debug) << " -> exchange:       " << it->msg->exchange;
         LOG(debug) << " -> routing_key:    " << it->msg->routing_key;
         LOG(debug) << " -> content_type:   " << it->msg->content_type;
         LOG(debug) << " -> reply_to:       " << it->msg->reply_to.value();
         LOG(debug) << " -> expiration:     " << it->msg->expiration.value();
         LOG(debug) << " -> data:           " << util::to_hex( it->msg->data );

         // Adjust our message for another attempt
         auto old_correlation_id = it->msg->correlation_id.value();

         it->msg->correlation_id = util::random_alphanumeric( _correlation_id_len );
         it->msg->expiration     = std::min( it->msg->expiration.value() * 2, _max_expiration );
         it->msg->reply_to       = get_queue_name();

         LOG(debug) << "Resending message (correlation ID: " << old_correlation_id << ") with new correlation ID: "
            << it->msg->correlation_id.value() << ", expiration: " << it->msg->expiration.value();

         // Publish another attempt
         if ( _writer_broker->publish( *it->msg ) != error_code::success )
         {
            it->response.set_exception( std::make_exception_ptr( broker_publish_error( "error sending rpc message" ) ) );
            idx.erase( it );
            return;
         }

         request r;
         r.correlation_id = it->msg->correlation_id.value();
         r.expiration     = std::chrono::duration_cast< std::chrono::milliseconds >( std::chrono::system_clock::now().time_since_epoch() + std::chrono::milliseconds( *it->msg->expiration ) );
         r.policy         = it->policy;
         r.response       = std::move( it->response );
         r.msg            = it->msg;

         idx.erase( it );
         const auto& [ iter, success ] = _requests.insert( std::move( r ) );
         KOINOS_ASSERT( success, request_insertion_error, "failed to insert request" );
         break;
      }
   }

   _timer.async_wait( std::bind( &client_impl::policy_handler, this, boost::system::error_code{} ) );
   _timer.expires_from_now( 1s );
}

std::shared_future< std::string > client_impl::rpc(
   const std::string& service,
   const std::string& payload,
   std::chrono::milliseconds timeout,
   retry_policy policy,
   const std::string& content_type )
{
   KOINOS_ASSERT( is_running(), client_not_running, "client is not running" );

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
      promise.set_exception( std::make_exception_ptr( broker_publish_error( "error sending rpc message" ) ) );
      return promise.get_future();
   }

   std::shared_future< std::string > fut = promise.get_future();

   std::lock_guard< std::mutex > guard( _requests_mutex );

   request r;
   r.correlation_id = *msg->correlation_id;
   r.expiration     = msg->expiration ? std::chrono::milliseconds( 0 ) : std::chrono::duration_cast< std::chrono::milliseconds >( std::chrono::system_clock::now().time_since_epoch() + std::chrono::milliseconds( *msg->expiration ) );
   r.policy         = policy;
   r.response       = std::move( promise );
   r.msg            = msg;

   const auto& [ iter, success ] = _requests.insert( std::move( r ) );
   KOINOS_ASSERT( success, request_insertion_error, "failed to insert request" );

   return fut;
}

void client_impl::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type )
{
   KOINOS_ASSERT( is_running(), client_not_running, "client is not running" );

   auto err = _writer_broker->publish( message {
      .exchange     = exchange::event,
      .routing_key  = routing_key,
      .content_type = content_type,
      .data         = payload
   } );

   KOINOS_ASSERT( err == error_code::success, broker_publish_error, "error broadcasting message" );
}

} // detail

client::client( boost::asio::io_context& io_context ) : _my( std::make_unique< detail::client_impl >( io_context ) ) {}
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
