#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>

#include <koinos/log.hpp>
#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>
#include <koinos/util/hex.hpp>

using namespace std::chrono_literals;

using work_guard_type = boost::asio::executor_work_guard< boost::asio::io_context::executor_type >;

int main( int argc, char** argv )
{
  try
  {
    koinos::initialize_logging( "koinos_mq_handler" );
    boost::asio::io_context main_context, work_context;

    auto request_handler = koinos::mq::request_handler( work_context );

    request_handler.add_broadcast_handler( "koinos.block.accept",
                                           [ & ]( const std::string& msg ) -> void
                                           {
                                             LOG( info )
                                               << "Received message: " << koinos::util::to_hex( msg ) << std::endl;
                                           } );

    request_handler.connect( "amqp://guest:guest@localhost:5672/" );

    boost::asio::signal_set signals( main_context, SIGINT, SIGTERM );

    signals.async_wait(
      [ & ]( const boost::system::error_code& err, int num )
      {
        LOG( info ) << "Caught signal, shutting down...";
        main_context.stop();
        work_context.stop();
      } );

    std::vector< std::thread > threads;
    for( std::size_t i = 0; i < std::thread::hardware_concurrency() + 1; i++ )
      threads.emplace_back(
        [ & ]()
        {
          work_context.run();
        } );

    work_guard_type main_work_guard( main_context.get_executor() );
    work_guard_type request_work_guard( work_context.get_executor() );
    main_context.run();
    work_context.run();

    for( auto& t: threads )
      t.join();
  }
  catch( const std::exception& e )
  {
    LOG( error ) << "Error: " << e.what();
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
