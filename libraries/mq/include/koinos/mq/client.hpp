#pragma once

#include <koinos/mq/message_broker.hpp>

#include <future>
#include <memory>
#include <string>

namespace koinos::mq {

namespace detail { struct client_impl; }

class client final
{
public:
   error_code connect( const std::string& amqp_url );

   std::future< std::string > send_rpc( std::string content_type, std::string rpc_type, std::string payload );
   std::future< std::string > send_broadcast( std::string content_type, std::string rpc_type, std::string payload );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
