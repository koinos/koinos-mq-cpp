#pragma once

#include <koinos/mq/message_broker.hpp>

#include <future>
#include <memory>
#include <string>

namespace koinos::mq {

namespace detail { class client_impl; }

class client final
{
public:
   client();
   ~client();

   error_code connect( const std::string& amqp_url );

   std::future< std::string > rpc( std::string content_type, std::string rpc_type, std::string payload );
   void broadcast( std::string content_type, std::string rpc_type, std::string payload );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
