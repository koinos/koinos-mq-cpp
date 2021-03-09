#include <koinos/mq/client.hpp>
#include <koinos/log.hpp>

#include <atomic>
#include <map>
#include <mutex>

namespace koinos::mq {

namespace detail {

constexpr const char* broadcast_exchange    = "koinos_event";
constexpr const char* rpc_exchange          = "koinos_rpc";
constexpr const char* rpc_reply_to_exchange = "koinos_rpc_reply";

class client_impl final
{
public:
   client_impl();
   ~client_impl();

   error_code connect( const std::string& amqp_url );

   std::future< std::string > rpc( std::string content_type, std::string rpc_type, std::string payload );
   void broadcast( std::string content_type, std::string rpc_type, std::string payload );

private:
   error_code prepare();
   void consumer( std::shared_ptr< message_broker > broker );

   std::map< std::string, std::promise< std::string > > _promise_map;
   std::mutex                                           _promise_map_mutex;

   std::shared_ptr< message_broker >                    _writer_broker;

   std::unique_ptr< std::thread >                       _reader_thread;
   std::shared_ptr< message_broker >                    _reader_broker;
   std::string                                          _queue_name;
   std::atomic< bool >                                  _running = true;
};

client_impl::client_impl() :
   _writer_broker( std::make_unique< message_broker >() ),
   _reader_broker( std::make_unique< message_broker >() ) {}

client_impl::~client_impl()
{
   _running = false;
   if ( _reader_thread )
      _reader_thread->join();
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
      return ec;

   _reader_thread = std::make_unique< std::thread >( [&]()
   {
      consumer( _reader_broker );
   } );

   return error_code::success;
}

error_code client_impl::prepare()
{
   error_code ec;

   ec = _reader_broker->declare_exchange(
      broadcast_exchange, // Name
      "topic",            // Type
      false,              // Passive
      true,               // Durable
      false,              // Auto-deleted
      false               // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "error while declaring broadcast exchange";
      return ec;
   }

   ec = _reader_broker->declare_exchange(
      rpc_exchange, // Name
      "direct",     // Type
      false,        // Passive
      true,         // Durable
      false,        // Auto-deleted
      false         // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "error while declaring rpc exchange";
      return ec;
   }

   ec = _reader_broker->declare_exchange(
      rpc_reply_to_exchange, // Name
      "direct",              // Type
      false,                 // Passive
      true,                  // Durable
      false,                 // Auto-deleted
      false                  // Internal
   );

   if ( ec != error_code::success )
   {
      LOG(error) << "error while declaring rpc reply-to exchange";
      return ec;
   }

   auto queue_res = _reader_broker->declare_queue(
      "",
      false, // Passive
      false, // Durable
      true,  // Exclusive
      false  // Internal
   );

   if ( queue_res.first != error_code::success )
   {
      LOG(error) << "error while declaring temporary queue";
      return queue_res.first;
   }

   _queue_name = queue_res.second;

   ec = _reader_broker->bind_queue( queue_res.second, rpc_reply_to_exchange, queue_res.second );
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
         }
      }
   }
}

std::future< std::string > client_impl::rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   auto promise = std::promise< std::string >();
   return promise.get_future();
}

void client_impl::broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
}

} // detail

client::client() : _my( std::make_unique< detail::client_impl >() ) {}
client::~client() = default;

error_code client::connect( const std::string& amqp_url )
{
   return _my->connect( amqp_url );
}

std::future< std::string > client::rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   return _my->rpc( content_type, rpc_type, payload );
}

void client::broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
   _my->broadcast( content_type, rpc_type, payload );
}

} // koinos::mq
