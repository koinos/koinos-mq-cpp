#pragma once

#include <koinos/log.hpp>
#include <koinos/mq/exception.hpp>
#include <koinos/mq/message_broker.hpp>
#include <koinos/mq/retryer.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/container/flat_map.hpp>
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
using msg_routing_map =
  boost::container::flat_map< std::pair< std::string, std::string >, std::vector< handler_pair > >;
using binding_queue_map = boost::container::flat_map< std::pair< std::string, std::string >, std::string >;

struct message_handler
{
  const std::string exchange;
  const std::string routing_key;
  bool competing_consumer;
  handler_verify_func verify;
  msg_handler_func handler;
};

class request_handler: public std::enable_shared_from_this< request_handler >
{
public:
  request_handler( boost::asio::io_context& io_context );
  virtual ~request_handler();

  void disconnect();
  void connect( const std::string& amqp_url, retry_policy = retry_policy::exponential_backoff );

  bool running() const;
  bool connected() const;
  bool ready() const;

  void add_broadcast_handler(
    const std::string& routing_key,
    msg_handler_void_func func,
    handler_verify_func =
      []( const std::string& content_type )
    {
      return content_type == "application/octet-stream";
    } );

  void add_rpc_handler(
    const std::string& service,
    msg_handler_string_func func,
    handler_verify_func =
      []( const std::string& content_type )
    {
      return content_type == "application/octet-stream";
    } );

private:
  void consume();
  void publish( std::shared_ptr< message > msg );
  void handle_message( std::shared_ptr< message > msg );

  error_code on_connect( message_broker& m );

  void add_msg_handler( const std::string& exchange,
                        const std::string& routing_key,
                        bool competing_consumer,
                        handler_verify_func,
                        msg_handler_func );

  // The compiler should be able to implicitly convert msg_handler_string_func -> msg_handler_func
  // but doesn't. This explicit binding compiles. Very likely to break in the future as an
  // ambiguous function binding. If so, delete this function.
  void add_msg_handler( const std::string& exchange,
                        const std::string& routing_key,
                        bool competing_consumer,
                        handler_verify_func,
                        msg_handler_string_func );

  std::unique_ptr< message_broker > _consumer_broker;
  std::unique_ptr< message_broker > _publisher_broker;

  binding_queue_map _queue_bindings;
  msg_routing_map _handler_map;

  std::string _amqp_url;

  std::vector< message_handler > _message_handlers;
  boost::asio::io_context& _ioc;

  boost::asio::signal_set _signals;
  std::atomic< bool > _stopped = false;
  retryer _retryer;
};

} // namespace koinos::mq
