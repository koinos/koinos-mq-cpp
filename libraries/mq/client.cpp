#include <koinos/mq/client.hpp>
#include <koinos/mq/retryer.hpp>
#include <koinos/mq/util.hpp>
#include <koinos/log.hpp>
#include <koinos/util/hex.hpp>
#include <koinos/util/random.hpp>

#include <boost/asio/dispatch.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/bind.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/mem_fun.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>

#include <atomic>
#include <chrono>
#include <mutex>

using namespace std::chrono_literals;

namespace koinos::mq {

namespace detail {

struct request
{
   std::chrono::time_point< std::chrono::system_clock >   expiration;
   retry_policy                                           policy;
   mutable std::shared_ptr< std::promise< std::string > > response;
   mutable std::shared_ptr< message >                     msg;

   std::optional< std::string >& correlation_id() const
   {
      return msg->correlation_id;
   }
};

struct by_correlation_id{};
struct by_expiration{};

typedef boost::multi_index::multi_index_container<
  request,
  boost::multi_index::indexed_by<
    boost::multi_index::ordered_unique<
      boost::multi_index::tag< by_correlation_id >, boost::multi_index::const_mem_fun< request, std::optional< std::string >&, &request::correlation_id > >,
    boost::multi_index::ordered_non_unique<
      boost::multi_index::tag< by_expiration >, boost::multi_index::member< request, std::chrono::time_point< std::chrono::system_clock >, &request::expiration > >
   >
> request_set;

class client_impl final
{
public:
   client_impl( boost::asio::io_context& io_context );
   ~client_impl();

   void connect( const std::string& amqp_url, retry_policy policy );
   void disconnect();

   bool running() const;
   bool connected() const;
   bool ready() const;

   std::shared_future< std::string > rpc(
      const std::string& service,
      const std::string& payload,
      std::chrono::milliseconds timeout,
      retry_policy policy,
      const std::string& content_type );

   void broadcast(
      const std::string& routing_key,
      const std::string& payload,
      const std::string& content_type,
      retry_policy policy );

private:
   error_code on_connect( message_broker& m );
   void consume();
   void policy_handler( std::chrono::time_point< std::chrono::system_clock > time );
   void abort();
   error_code publish( const message& m, retry_policy policy, std::optional< std::string > action_log );
   std::vector< request_set::node_type > extract_expired_message( const std::chrono::time_point< std::chrono::system_clock >& time );

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
   boost::asio::io_context&                             _ioc;
   boost::asio::signal_set                              _signals;
   std::atomic< bool >                                  _stopped = false;
   retryer                                              _retryer;
   std::string                                          _amqp_url;
};

client_impl::client_impl( boost::asio::io_context& io_context ) :
   _ioc( io_context ),
   _writer_broker( std::make_shared< message_broker >() ),
   _reader_broker( std::make_shared< message_broker >() ),
   _signals( io_context ),
   _retryer( io_context, std::chrono::milliseconds( _max_expiration ) )
{
   _signals.add( SIGINT );
   _signals.add( SIGTERM );
#if defined(SIGQUIT)
   _signals.add( SIGQUIT );
#endif // defined(SIGQUIT)

   _signals.async_wait( [&]( const boost::system::error_code& err, int num )
   {
      _stopped = true;
      abort();
   } );
}

client_impl::~client_impl()
{
   _retryer.cancel();
   abort();
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
   KOINOS_ASSERT( !connected(), client_already_connected, "client is already connected" );
   KOINOS_ASSERT( running(), client_not_running, "client is not running" );

   _amqp_url = amqp_url;

   error_code code = _retryer.with_policy(
      policy,
      [&]() -> error_code
      {
         error_code e;

         e = _writer_broker->connect( _amqp_url );

         if ( e != error_code::success )
            return e;

         e = _reader_broker->connect(
            _amqp_url,
            [this]( message_broker& m ) -> error_code
            {
               return this->on_connect( m );
            }
         );

         if ( e != error_code::success )
            _writer_broker->disconnect();

         return e;
      },
      "client connection to AMQP"
   );

   if ( code != error_code::success )
      KOINOS_THROW( mq_connection_failure, "could not connect to endpoint: ${e}", ("e", amqp_url) );

   boost::asio::post( _ioc, std::bind( &client_impl::consume, this ) );
}

void client_impl::abort()
{
   std::lock_guard< std::mutex > lock( _requests_mutex );
   for ( auto it = _requests.begin(); it != _requests.end(); ++it )
   {
      it->response->set_exception( std::make_exception_ptr( client_not_running( "client has disconnected" ) ) );
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

bool client_impl::running() const
{
   return !_ioc.stopped();
}

bool client_impl::connected() const
{
   return _writer_broker->connected() && _reader_broker->connected();
}

bool client_impl::ready() const
{
   return connected() && running();
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
      LOG(error) << "Error while declaring broadcast exchange for client";
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
      LOG(error) << "Error while declaring RPC exchange for client";
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
      LOG(error) << "Error while declaring temporary queue for client";
      return queue_res.first;
   }

   set_queue_name( queue_res.second );

   ec = m.bind_queue( queue_res.second, exchange::rpc, queue_res.second );
   if ( ec != error_code::success )
   {
      LOG(error) << "Error while binding temporary queue for client";
      return ec;
   }

   return ec;
}

void client_impl::consume()
{
   if ( _stopped )
      return;

   error_code code;
   std::shared_ptr< message > msg;

   code = _retryer.with_policy(
      retry_policy::exponential_backoff,
      [&]() -> error_code
      {
         auto [ e, m ] = _reader_broker->consume();

         if ( e != error_code::failure )
         {
            msg = m;
            return e;
         }

         _reader_broker->disconnect();

         e = _reader_broker->connect(
            _amqp_url,
            [this]( message_broker& m ) -> error_code
            {
               return this->on_connect( m );
            }
         );

         if ( e == error_code::success )
         {
            std::tie( e, m ) = _reader_broker->consume();

            if ( e != error_code::failure )
               msg = m;
         }

         return e;
      },
      "client message consumption"
   );

   auto arrival_time = std::chrono::system_clock::now();

   if ( code == error_code::time_out ) {}
   else if ( code != error_code::success )
   {
      LOG(warning) << "Client failed to consume message";
   }
   else if ( !msg )
   {
      LOG(debug) << "Client message consumption succeeded but resulted in an empty message";
   }
   else
   {
      if ( !msg->correlation_id.has_value() )
      {
         LOG(warning) << "Client received message without a correlation id";
      }
      else
      {
         LOG(debug) << "Client received message: " << to_string( *msg );

         std::lock_guard< std::mutex > lock( _requests_mutex );
         const auto& idx = boost::multi_index::get< by_correlation_id >( _requests );
         auto it = idx.find( *msg->correlation_id );
         if ( it != idx.end() )
         {
            it->response->set_value( std::move( msg->data ) );
            _requests.erase( it );
         }
         else
         {
            LOG(warning) << "Client could not find correlation ID in request set: " << *msg->correlation_id;
         }
      }
   }

   boost::asio::post( _ioc, std::bind( &client_impl::policy_handler, this, arrival_time ) );
   boost::asio::post( _ioc, std::bind( &client_impl::consume, this ) );
}

error_code client_impl::publish( const message& m, retry_policy policy, std::optional< std::string > action_log )
{
   return _retryer.with_policy(
      policy,
      [&]() -> error_code
      {
         error_code e = _writer_broker->publish( m );

         if ( e == error_code::success )
            return e;

         _writer_broker->disconnect();
         e = _writer_broker->connect( _amqp_url );

         if ( e == error_code::success )
            e = _writer_broker->publish( m );

         return e;
      },
      action_log
   );
}

std::vector< request_set::node_type > client_impl::extract_expired_message( const std::chrono::time_point< std::chrono::system_clock >& time )
{
   std::vector< request_set::node_type > expired_messages;

   std::lock_guard< std::mutex > guard( _requests_mutex );
   auto& idx = boost::multi_index::get< by_expiration >( _requests );

   for ( auto it = idx.begin(); it != idx.end(); ++it )
   {
      if ( it->expiration > time )
         break;

      expired_messages.push_back( idx.extract( it ) );
   }

   return expired_messages;
}

void client_impl::policy_handler( std::chrono::time_point< std::chrono::system_clock > time )
{
   if ( _stopped )
      return;

   auto expired_messages = extract_expired_message( time );

   for ( auto& req_node : expired_messages )
   {
      switch ( req_node.value().policy )
      {
      case retry_policy::none:
         req_node.value().response->set_exception( std::make_exception_ptr( client_timeout_error( "request timeout: " + *req_node.value().correlation_id() ) ) );
         return;
      case retry_policy::exponential_backoff:
         LOG(warning) << "No response to client request with correlation ID: " << req_node.value().msg->correlation_id.value() << ", within " << req_node.value().msg->expiration.value() << "ms";
         LOG(debug) << "Client applying exponential backoff for message: " << to_string( *req_node.value().msg );

         // Adjust our message for another attempt
         auto old_correlation_id = req_node.value().msg->correlation_id.value();

         req_node.value().msg->correlation_id = util::random_alphanumeric( _correlation_id_len );
         req_node.value().msg->expiration     = std::min( req_node.value().msg->expiration.value() * 2, _max_expiration );
         req_node.value().msg->reply_to       = get_queue_name();

         LOG(debug) << "Resending message from client (correlation ID: " << old_correlation_id << ") with new correlation ID: "
            << req_node.value().msg->correlation_id.value() << ", expiration: " << req_node.value().msg->expiration.value();

         req_node.value().expiration = std::chrono::system_clock::now() + std::chrono::milliseconds( *req_node.value().msg->expiration );

         auto promise = req_node.value().response;

         const auto [ it, inserted ] = [&]() {
            std::lock_guard< std::mutex > guard( _requests_mutex );
            const auto res = _requests.insert( std::move( req_node ) );
            return std::make_pair( res.position, res.inserted );
         }();

         if ( inserted )
         {
            auto err = publish( *it->msg, it->policy, "client rpc republication" );

            if ( err != error_code::success )
            {
               promise->set_exception( std::make_exception_ptr( client_publish_error( "error resending rpc message" ) ) );
               std::lock_guard< std::mutex > guard( _requests_mutex );
               _requests.erase( it );
            }
         }
         else
         {
            promise->set_exception( std::make_exception_ptr( client_request_insertion_error( "failed to insert request, possible a correlation id collision" ) ) );
         }

         break;
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
   KOINOS_ASSERT( connected(), client_not_connected, "client is not connected" );
   KOINOS_ASSERT( running(), client_not_running, "client is not running" );

   auto promise = std::make_shared< std::promise< std::string > >();

   auto msg = std::make_shared< message >();
   msg->exchange = exchange::rpc;
   msg->routing_key = service_routing_key( service );
   msg->content_type = content_type;
   msg->data = payload;
   msg->reply_to = get_queue_name();
   msg->correlation_id = util::random_alphanumeric( _correlation_id_len );

   if ( timeout.count() > 0 )
      msg->expiration = timeout.count();
   else
      KOINOS_ASSERT( policy != retry_policy::exponential_backoff, client_request_invalid, "cannot have an ${p} policy without a timeout", ("p", to_string( policy )) );

   LOG(debug) << "Sending RPC from client: " << to_string( *msg );

   std::shared_future< std::string > fut = promise->get_future();

   auto err = publish( *msg, policy, "client rpc publication" );

   if ( err != error_code::success )
   {
      promise->set_exception( std::make_exception_ptr( client_publish_error( "error sending rpc message" ) ) );
   }
   else
   {
      request r;
      if ( msg->expiration )
         r.expiration  = std::chrono::system_clock::now() + timeout;
      else
         r.expiration  = std::chrono::time_point< std::chrono::system_clock >::max();
      r.policy         = policy;
      r.response       = promise;
      r.msg            = msg;

      std::lock_guard< std::mutex > guard( _requests_mutex );
      const auto& [ iter, success ] = _requests.insert( std::move( r ) );
      KOINOS_ASSERT( success, client_request_insertion_error, "failed to insert request, possibly a correlation id collision" );
   }

   return fut;
}

void client_impl::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type, retry_policy policy )
{
   KOINOS_ASSERT( connected(), client_not_connected, "client is not connected" );
   KOINOS_ASSERT( running(), client_not_running, "client is not running" );

   auto err = publish(
      message {
         .exchange     = exchange::event,
         .routing_key  = routing_key,
         .content_type = content_type,
         .data         = payload
      },
      policy,
      "client broadcast publication"
   );

   KOINOS_ASSERT( err == error_code::success, client_publish_error, "error broadcasting message" );
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

void client::broadcast( const std::string& routing_key, const std::string& payload, const std::string& content_type, retry_policy policy )
{
   _my->broadcast( routing_key, payload, content_type, policy );
}

bool client::running() const
{
   return _my->running();
}

bool client::connected() const
{
   return _my->connected();
}

bool client::ready() const
{
   return _my->ready();
}

void client::disconnect()
{
   _my->disconnect();
}

} // koinos::mq
