#pragma once

#include <koinos/mq/exception.hpp>
#include <koinos/mq/message_broker.hpp>
#include <koinos/log.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/thread/sync_bounded_queue.hpp>
#include <boost/tuple/tuple.hpp>

#include <atomic>
#include <memory>
#include <set>
#include <thread>
#include <unordered_map>
#include <variant>

namespace koinos::mq {

using msg_handler_void_func   = std::function< void( const std::string& ) >;
using msg_handler_string_func = std::function< std::string( const std::string& ) >;
using msg_handler_func        = std::variant< msg_handler_void_func, msg_handler_string_func >;
using handler_verify_func     = std::function< bool( const std::string& ) >;
using handler_pair            = std::pair< handler_verify_func, msg_handler_func >;
using msg_routing_map         = boost::container::flat_map< std::pair< std::string, std::string >, std::vector< handler_pair > >;
using binding_queue_map       = boost::container::flat_map< std::pair< std::string, std::string >, std::string >;
using synced_msg_queue        = boost::concurrent::sync_bounded_queue< std::shared_ptr< message > >;

struct message_handler
{
   const std::string exchange;
   const std::string routing_key;
   bool competing_consumer;
   handler_verify_func verify;
   msg_handler_func handler;
};

namespace constants {
   constexpr std::size_t max_queue_size = 1024;
} // constants

class request_handler : public std::enable_shared_from_this< request_handler >
{
   public:
      request_handler( boost::asio::io_context& io_context );
      virtual ~request_handler();

      void disconnect();
      void connect( const std::string& amqp_url, retry_policy = retry_policy::exponential_backoff );

      void add_broadcast_handler(
         const std::string& routing_key,
         msg_handler_void_func func,
         handler_verify_func = []( const std::string& content_type ) { return content_type == "application/octet-stream"; }
      );

      void add_rpc_handler(
         const std::string& service,
         msg_handler_string_func func,
         handler_verify_func = []( const std::string& content_type ) { return content_type == "application/octet-stream"; }
      );

   private:
      void consume( const boost::system::error_code& ec );
      void publish( const boost::system::error_code& ec );
      void handle_message( const boost::system::error_code& ec );

      error_code on_connect( message_broker& m );

      void add_msg_handler(
         const std::string& exchange,
         const std::string& routing_key,
         bool competing_consumer,
         handler_verify_func,
         msg_handler_func );

      // The compiler should be able to implicitly convert msg_handler_string_func -> msg_handler_func
      // but doesn't. This explicit binding compiles. Very likely to break in the future as an
      // ambiguous function binding. If so, delete this function.
      void add_msg_handler(
         const std::string& exchange,
         const std::string& routing_key,
         bool competing_consumer,
         handler_verify_func,
         msg_handler_string_func );

      std::unique_ptr< message_broker > _consumer_broker;
      std::unique_ptr< message_broker > _publisher_broker;

      binding_queue_map                 _queue_bindings;
      msg_routing_map                   _handler_map;

      synced_msg_queue                  _input_queue{ constants::max_queue_size };
      synced_msg_queue                  _output_queue{ constants::max_queue_size };

      std::vector< message_handler >    _message_handlers;
      boost::asio::io_context&          _io_context;
};

} // koinos::mq
