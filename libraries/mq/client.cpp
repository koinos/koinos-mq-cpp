#include <koinos/mq/client.hpp>

namespace koinos::mq {

namespace detail {

struct client_impl
{
   error_code connect( const std::string& amqp_url );

   std::future< std::string > send_rpc( std::string content_type, std::string rpc_type, std::string payload );
   std::future< std::string > send_broadcast( std::string content_type, std::string rpc_type, std::string payload );

   std::shared_ptr< message_broker > _reader_broker;
   std::shared_ptr< message_broker > _writer_broker;
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

std::future< std::string > client_impl::send_rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   auto promise = std::promise< std::string >();
   return promise.get_future();
}

std::future< std::string > client_impl::send_broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
   auto promise = std::promise< std::string >();
   return promise.get_future();
}

} // detail

error_code client::connect( const std::string& amqp_url )
{
   return _my->connect( amqp_url );
}

std::future< std::string > client::send_rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   return _my->send_rpc( content_type, rpc_type, payload );
}

std::future< std::string > client::send_broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
   return _my->send_broadcast( content_type, rpc_type, payload );
}

} // koinos::mq
