#pragma once

#include <koinos/mq/exception.hpp>
#include <koinos/mq/message_broker.hpp>
#include <koinos/log.hpp>

#include <boost/asio/high_resolution_timer.hpp>
#include <boost/asio/io_context.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <optional>

using namespace std::chrono_literals;

namespace koinos::mq {

class retryer final
{
public:
   retryer( boost::asio::io_context& ioc, std::atomic_bool& stopped, std::chrono::milliseconds max_timeout );
   ~retryer();

   error_code with_policy(
      retry_policy policy,
      std::function< error_code( void ) > fn,
      std::optional< std::string > message,
      std::chrono::milliseconds timeout = 1000ms
   );
private:
   void retry_logic(
      const boost::system::error_code& ec,
      std::shared_ptr< std::promise< error_code > > p,
      std::function< error_code( void ) > f,
      std::chrono::milliseconds t,
      std::optional< std::string > m
   );

   boost::asio::io_context&           _ioc;
   std::atomic_bool&                  _stopped;
   boost::asio::high_resolution_timer _timer;
   std::chrono::milliseconds          _max_timeout;
};

} // koinos::mq
