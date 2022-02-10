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
   _timer( ioc ),
   _max_timeout( max_timeout ) {}

void retryer::retry_logic(
   const boost::system::error_code& ec,
   std::shared_ptr< std::promise< error_code > > p,
   std::function< error_code( void ) > f,
   std::chrono::milliseconds t,
   std::optional< std::string > m )
{
   if ( ec == boost::asio::error::operation_aborted || _stopped )
      p->set_value( error_code::failure );

   error_code e = f();

   if ( e == error_code::failure )
   {
      if ( m )
         LOG(warning) << "Unable to " << *m << ", retrying in " << t.count() << "ms";

      t = std::min( t * 2, _max_timeout );
      _timer.async_wait( boost::bind( &retryer::retry_logic, this, boost::asio::placeholders::error, p, f, t, m ) );
      _timer.expires_from_now( t );
   }
   else
   {
      p->set_value( e );
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

   switch ( policy )
   {
      case retry_policy::none:
         promise->set_value( error_code::failure );
         break;
      case retry_policy::exponential_backoff:
         if ( message )
            LOG(warning) << "Unable to " << *message << ", retrying in " << timeout.count() << "ms";

         _timer.async_wait( boost::bind( &retryer::retry_logic, this, boost::asio::placeholders::error, promise, fn, timeout, message ) );
         _timer.expires_from_now( timeout );
         break;
   }

   return fut.get();
}

} // koinos::mq
