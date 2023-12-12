#include <koinos/mq/retryer.hpp>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <functional>
#include <future>

using namespace std::placeholders;

namespace koinos::mq {

retryer::retryer( boost::asio::io_context& ioc, std::chrono::milliseconds max_timeout ):
    _ioc( ioc ),
    _max_timeout( max_timeout ),
    _signals( ioc )
{
  _signals.add( SIGINT );
  _signals.add( SIGTERM );
#if defined( SIGQUIT )
  _signals.add( SIGQUIT );
#endif // defined(SIGQUIT)

  _signals.async_wait(
    [ & ]( const boost::system::error_code& err, int num )
    {
      cancel();
    } );
}

void retryer::cancel()
{
  _signals.clear();
  std::lock_guard guard( _timers_mutex );
  for( const auto& timer: _timers )
    timer->cancel();
}

void retryer::add_timer( timer_ptr t )
{
  std::lock_guard guard( _timers_mutex );
  _timers.insert( t );
}

void retryer::remove_timer( timer_ptr t )
{
  std::lock_guard guard( _timers_mutex );
  auto it = _timers.find( t );
  if( it != _timers.end() )
    _timers.erase( it );
}

void retryer::retry_logic( const boost::system::error_code& ec,
                           std::shared_ptr< boost::asio::high_resolution_timer > timer,
                           std::shared_ptr< std::promise< error_code > > p,
                           std::function< error_code( void ) > f,
                           std::chrono::milliseconds t,
                           std::optional< std::string > m )
{
  if( ec == boost::asio::error::operation_aborted )
  {
    p->set_value( error_code::failure );
    remove_timer( timer );
    return;
  }

  error_code e = f();

  if( e == error_code::failure )
  {
    t = std::min( t * 2, _max_timeout );

    if( m )
      LOG( warning ) << "Failure during " << *m << ", retrying in " << t.count() << "ms";

    timer->expires_after( t );
    timer->async_wait(
      boost::bind( &retryer::retry_logic, this, boost::asio::placeholders::error, timer, p, f, t, m ) );
  }
  else
  {
    p->set_value( e );
    remove_timer( timer );
  }
}

error_code retryer::with_policy( retry_policy policy,
                                 std::function< error_code( void ) > fn,
                                 std::optional< std::string > message,
                                 std::chrono::milliseconds timeout )
{
  if( error_code e = fn(); e != error_code::failure )
    return e;

  std::shared_ptr< std::promise< error_code > > promise = std::make_shared< std::promise< error_code > >();
  std::future fut                                       = promise->get_future();

  std::shared_ptr< boost::asio::high_resolution_timer > timer =
    std::make_shared< boost::asio::high_resolution_timer >( _ioc );

  switch( policy )
  {
    case retry_policy::none:
      promise->set_value( error_code::failure );
      break;
    case retry_policy::exponential_backoff:
      if( message )
        LOG( warning ) << "Failure during " << *message << ", retrying in " << timeout.count() << "ms";

      timer->expires_after( timeout );
      timer->async_wait( boost::bind( &retryer::retry_logic,
                                      this,
                                      boost::asio::placeholders::error,
                                      timer,
                                      promise,
                                      fn,
                                      timeout,
                                      message ) );
      add_timer( timer );
      break;
  }

  return fut.get();
}

} // namespace koinos::mq
