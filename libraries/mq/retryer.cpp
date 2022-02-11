#include <koinos/mq/retryer.hpp>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <functional>
#include <future>

using namespace std::placeholders;

namespace koinos::mq {

retryer::retryer( boost::asio::io_context& ioc, std::atomic_bool& stopped, std::chrono::milliseconds max_timeout ) :
   _ioc( ioc ),
   _stopped( stopped ),
   _max_timeout( max_timeout ) {}

void retryer::cancel()
{
   std::lock_guard guard( _timer_set_mutex );
   auto it = _timer_set.begin();
   while ( !_timer_set.empty() )
   {
      it->get()->cancel();
      _timer_set.erase( it );
      it = _timer_set.begin();
   }
}

void retryer::add_timer( timer_ptr t )
{
   std::lock_guard guard( _timer_set_mutex );
   _timer_set.insert( t );
}

void retryer::remove_timer( timer_ptr t )
{
   std::lock_guard guard( _timer_set_mutex );
   _timer_set.erase( t );
}

void retryer::retry_logic(
   const boost::system::error_code& ec,
   std::shared_ptr< boost::asio::high_resolution_timer > timer,
   std::shared_ptr< std::promise< error_code > > p,
   std::function< error_code( void ) > f,
   std::chrono::milliseconds t,
   std::optional< std::string > m )
{
   if ( ec == boost::asio::error::operation_aborted || _stopped )
   {
      p->set_value( error_code::failure );
      remove_timer( timer );
      return;
   }

   error_code e = f();

   if ( e == error_code::failure )
   {
      t = std::min( t * 2, _max_timeout );

      if ( m )
         LOG(warning) << "Failure during " << *m << ", retrying in " << t.count() << "ms";

      timer->expires_after( t );
      timer->async_wait( boost::bind( &retryer::retry_logic, this, boost::asio::placeholders::error, timer, p, f, t, m ) );
   }
   else
   {
      p->set_value( e );
      remove_timer( timer );
   }
}

error_code retryer::with_policy(
   retry_policy policy,
   std::function< error_code( void ) > fn,
   std::optional< std::string > message,
   std::chrono::milliseconds timeout )
{
   if ( error_code e = fn(); e != error_code::failure )
      return e;

   std::shared_ptr< std::promise< error_code > > promise = std::make_shared< std::promise< error_code > >();
   std::future fut = promise->get_future();

   std::shared_ptr< boost::asio::high_resolution_timer > timer = std::make_shared< boost::asio::high_resolution_timer >( _ioc );
   add_timer( timer );

   switch ( policy )
   {
      case retry_policy::none:
         promise->set_value( error_code::failure );
         break;
      case retry_policy::exponential_backoff:
         if ( message )
            LOG(warning) << "Failure during " << *message << ", retrying in " << timeout.count() << "ms";

         timer->expires_after( timeout );
         timer->async_wait( boost::bind( &retryer::retry_logic, this, boost::asio::placeholders::error, timer, promise, fn, timeout, message ) );
         break;
   }

   return fut.get();
}

} // koinos::mq
