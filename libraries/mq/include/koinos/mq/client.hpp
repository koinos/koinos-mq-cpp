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

   std::future< std::string > rpc( const std::string& content_type, const std::string& rpc_type, const std::string& payload, int64_t timeout_ms = 0 );
   void broadcast( const std::string& content_type, const std::string& routing_key, const std::string& payload );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
