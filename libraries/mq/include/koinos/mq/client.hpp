#pragma once

#include <koinos/mq/message_broker.hpp>

#include <koinos/exception.hpp>

#include <future>
#include <memory>
#include <string>

namespace koinos::mq {

namespace detail { class client_impl; }

KOINOS_DECLARE_EXCEPTION( amqp_publish_error );
KOINOS_DECLARE_EXCEPTION( correlation_id_collision );
KOINOS_DECLARE_EXCEPTION( timeout_error );

class client final
{
public:
   client();
   ~client();

   error_code connect( const std::string& amqp_url );

   std::future< std::string > rpc( const std::string& content_type, const std::string& rpc_type, const std::string& payload, int64_t timeout_ms = 0 );
   void broadcast( const std::string& content_type, const std::string& routing_key, const std::string& payload );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
