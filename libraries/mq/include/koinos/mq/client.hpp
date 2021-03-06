#pragma once

#include <koinos/mq/message_broker.hpp>
#include <koinos/exception.hpp>

#include <future>
#include <memory>
#include <string>

namespace koinos::mq {

namespace detail { class client_impl; }

KOINOS_DECLARE_EXCEPTION( client_not_running );
KOINOS_DECLARE_EXCEPTION( amqp_publish_error );
KOINOS_DECLARE_EXCEPTION( correlation_id_collision );
KOINOS_DECLARE_EXCEPTION( timeout_error );

class client final
{
public:
   client();
   ~client();

   error_code connect(
      const std::string& amqp_url,
      retry_policy policy = retry_policy::exponential_backoff
   );

   void disconnect();

   bool is_connected() const;

   std::shared_future< std::string > rpc(
      const std::string& service,
      const std::string& payload,
      uint64_t timeout_ms = 1000,
      retry_policy policy = retry_policy::exponential_backoff,
      const std::string& content_type = "application/json" );

   void broadcast(
      const std::string& routing_key,
      const std::string& payload,
      const std::string& content_type = "application/json" );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
