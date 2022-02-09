#include <koinos/mq/retryer.hpp>

#include <boost/asio/post.hpp>

#include <future>

namespace koinos::mq {

retryer::retryer( boost::asio::io_context& ioc, std::atomic_bool& stopped, std::chrono::milliseconds max_timeout ) :
   _ioc( ioc ),
   _stopped( stopped ),
   _timer( ioc ),
   _max_timeout( max_timeout ) {}

error_code retryer::with_policy(
   retry_policy policy,
   std::function< error_code( void ) > fn,
   std::optional< std::string > message,
   std::chrono::milliseconds timeout )
{
   if ( error_code e = fn(); e != error_code::failure )
      return e;

   std::promise< error_code > promise;
   std::future fut = promise.get_future();

   std::function< void( const boost::system::error_code& ec ) > handler = [&]( const boost::system::error_code& ec )
   {
      if ( boost::asio::error::operation_aborted || _stopped )
         promise.set_value( error_code::failure );

      error_code e = fn();

      if ( e == error_code::failure )
      {
         if ( message )
            LOG(warning) << "Unable to " << *message << ", retrying in " << timeout.count() << "ms";

         timeout = std::min( timeout * 2, _max_timeout );
         _timer.async_wait( handler );
         _timer.expires_from_now( timeout );
      }
      else
      {
         promise.set_value( e );
      }
   };

   switch ( policy )
   {
      case retry_policy::none:
         promise.set_value( error_code::failure );
         break;
      case retry_policy::exponential_backoff:
         if ( message )
            LOG(warning) << "Unable to " << *message << ", retrying in " << timeout.count() << "ms";

         _timer.async_wait( handler );
         _timer.expires_from_now( timeout );
         break;
   }

   return fut.get();
}

} // koinos::mq
