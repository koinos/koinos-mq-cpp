#pragma once

#include <koinos/log.hpp>
#include <koinos/mq/exception.hpp>
#include <koinos/mq/message_broker.hpp>

#include <boost/asio/high_resolution_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

using namespace std::chrono_literals;

namespace koinos::mq {

class retryer final
{
public:
  retryer( boost::asio::io_context& ioc, std::chrono::milliseconds max_timeout );

  error_code with_policy( retry_policy policy,
                          std::function< error_code( void ) > fn,
                          std::optional< std::string > message,
                          std::chrono::milliseconds timeout = 1'000ms );

  void cancel();

  using timer_ptr = std::shared_ptr< boost::asio::high_resolution_timer >;

private:
  void retry_logic( const boost::system::error_code& ec,
                    std::shared_ptr< boost::asio::high_resolution_timer > timer,
                    std::shared_ptr< std::promise< error_code > > p,
                    std::function< error_code( void ) > f,
                    std::chrono::milliseconds t,
                    std::optional< std::string > m );

  void add_timer( timer_ptr t );
  void remove_timer( timer_ptr t );

  boost::asio::io_context& _ioc;
  std::chrono::milliseconds _max_timeout;
  std::mutex _timers_mutex;
  std::set< timer_ptr > _timers;
  boost::asio::signal_set _signals;
};

} // namespace koinos::mq
