#pragma once

#include <koinos/mq/message_broker.hpp>
#include <koinos/exception.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <string>

namespace koinos::mq {

namespace detail { class client_impl; }

KOINOS_DECLARE_EXCEPTION( mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( unable_to_connect, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_not_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_already_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( amqp_publish_error, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( correlation_id_collision, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( timeout_error, mq_exception );

class client final
{
public:
   client();
   ~client();

   void connect(
      const std::string& amqp_url,
      retry_policy policy = retry_policy::exponential_backoff
   );

   void disconnect();

   bool is_running() const;

   std::shared_future< std::string > rpc(
      const std::string& service,
      const std::string& payload,
      std::chrono::milliseconds timeout = std::chrono::milliseconds( 1000 ),
      retry_policy policy = retry_policy::exponential_backoff,
      const std::string& content_type = "application/octet-stream" );

   void broadcast(
      const std::string& routing_key,
      const std::string& payload,
      const std::string& content_type = "application/octet-stream" );
private:
   std::unique_ptr< detail::client_impl > _my;
};

} // koinos::mq
