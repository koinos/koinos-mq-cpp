#include <cstdlib>
#include <iostream>
#include <thread>
#include <chrono>

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>

#include <koinos/log.hpp>
#include <koinos/mq/client.hpp>
#include <koinos/mq/request_handler.hpp>

using namespace std::chrono_literals;

int main( int argc, char** argv )
{
   auto request_handler = koinos::mq::request_handler();

   koinos::mq::error_code ec = koinos::mq::error_code::success;

   ec = request_handler.add_rpc_handler(
      "my_service",
      [&]( const std::string& msg ) -> std::string
      {
         return msg;
      }
   );

   if ( ec != koinos::mq::error_code::success )
   {
      LOG(error) << "Unable to register MQ RPC handler";
      exit( EXIT_FAILURE );
   }

   LOG(info) << "Connecting AMQP request handler...";
   ec = request_handler.connect( "amqp://guest:guest@localhost:5672/" );
   if ( ec != koinos::mq::error_code::success )
   {
      LOG(error) << "Failed to connect request handler to AMQP server";
      exit( EXIT_FAILURE );
   }
   LOG(info) << "Established request handler connection to the AMQP server";

   request_handler.start();

   auto client = koinos::mq::client();
   client.connect( "amqp://guest:guest@localhost:5672/" );

   std::this_thread::sleep_for(20s);

   request_handler.stop();
   client.disconnect();

   return EXIT_SUCCESS;
}
