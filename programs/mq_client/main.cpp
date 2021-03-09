#include <cstdlib>
#include <iostream>

#include <boost/program_options.hpp>

#include <koinos/mq/client.hpp>

using namespace std;
using namespace boost;
using namespace koinos;

int main( int argc, char** argv )
{
   program_options::options_description desc( "Koinos MQ Client options");
   desc.add_options()
      ("help,h", "print usage message")
      ("amqp,a", program_options::value< string >()->default_value( "amqp://guest:guest@localhost:5672/" ), "amqp url")
      ("broadcast,b", program_options::value< bool >()->default_value( false ), "broadcast mode")
      ("content-type,c", program_options::value< string >()->default_value( "application/json" ), "content type of the message")
      ("routing-key,r", program_options::value< string >()->default_value( "" ), "routing key of the message")
      ("payload,p", program_options::value< string >()->default_value( "" ), "payload of the message")
      ;

   program_options::variables_map vm;
   program_options::store( program_options::parse_command_line( argc, argv, desc ), vm );

   if ( vm.count( "help" ) )
   {
      cout << desc << endl;
      return EXIT_SUCCESS;
   }

   string amqp_url     = vm[ "amqp" ].as< string >();
   bool broadcast_mode = vm[ "broadcast" ].as< bool >();
   string content_type = vm[ "content-type" ].as< string >();
   string routing_key  = vm[ "routing-key" ].as< string >();
   string payload      = vm[ "payload" ].as< string >();

   mq::client c;

   if( c.connect( amqp_url ) != mq::error_code::success )
      return EXIT_FAILURE;

   if ( broadcast_mode )
   {
      c.broadcast( content_type, routing_key, payload );
   }
   else
   {
      auto r = c.rpc( content_type, routing_key, payload );
      cout << r.get() << endl;
   }

   return EXIT_SUCCESS;
}
