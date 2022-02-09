#pragma once

#include <boost/core/noncopyable.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace koinos::mq {

enum class retry_policy
{
   none,
   exponential_backoff
};

enum class error_code : int64_t
{
   success,
   failure,
   time_out
};

struct message
{
   std::string                  exchange;
   std::string                  routing_key;
   std::string                  content_type;
   std::string                  data;
   uint64_t                     delivery_tag;
   std::optional< std::string > reply_to;
   std::optional< std::string > correlation_id;
   std::optional< uint64_t >    expiration;
};

namespace detail { struct message_broker_impl; }

class message_broker final : private boost::noncopyable
{
private:
   std::unique_ptr< detail::message_broker_impl > _message_broker_impl;

public:
   message_broker();
   ~message_broker();

   using on_connect_func = std::function< error_code( message_broker& m ) >;

   error_code connect(
      const std::string& url,
      on_connect_func f = []( message_broker& m ){ return error_code::success; }
   ) noexcept;

   void disconnect() noexcept;
   bool connected() noexcept;

   error_code publish( const message& msg ) noexcept;

   std::pair< error_code, std::shared_ptr< message > > consume() noexcept;

   error_code declare_exchange(
      const std::string& exchange,
      const std::string& exchange_type,
      bool passive = false,
      bool durable = false,
      bool auto_delete = false,
      bool internal = false
   ) noexcept;

   std::pair< error_code, std::string > declare_queue(
      const std::string& queue,
      bool passive = false,
      bool durable = false,
      bool exclusive = false,
      bool auto_delete = false
   ) noexcept;

   error_code bind_queue(
      const std::string& queue,
      const std::string& exchange,
      const std::string& binding_key,
      bool autoack = true
   ) noexcept;

   error_code ack_message( uint64_t delivery_tag ) noexcept;
};

} // koinos::mq
