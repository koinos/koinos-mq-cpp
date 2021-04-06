#include <cstdlib>
#include <iostream>

#include <boost/program_options.hpp>

#include <koinos/mq/client.hpp>

using namespace boost;
using namespace koinos;

#define HELP_OPTION         "help"
#define AMQP_OPTION         "amqp"
#define BROADCAST_OPTION    "broadcast"
#define CONTENT_TYPE_OPTION "content-type"
#define ROUTING_KEY_OPTION  "routing-key"
#define PAYLOAD_OPTION      "payload"
#define TIMEOUT_OPTION      "timeout"

int main( int argc, char** argv )
{
   program_options::options_description desc( "Koinos MQ Client options" );
   desc.add_options()
      ( HELP_OPTION         ",h", "print usage message" )
      ( AMQP_OPTION         ",a", program_options::value< std::string >()->default_value( "amqp://guest:guest@localhost:5672/" ), "amqp url" )
      ( BROADCAST_OPTION    ",b", program_options::value< bool        >()->default_value( false ), "broadcast mode" )
      ( CONTENT_TYPE_OPTION ",c", program_options::value< std::string >()->default_value( "application/json" ), "content type of the message" )
      ( ROUTING_KEY_OPTION  ",r", program_options::value< std::string >()->default_value( "" ), "routing key of the message" )
      ( TIMEOUT_OPTION      ",t", program_options::value< uint64_t    >()->default_value( 1000 ), "timeout of the message" )
      ( PAYLOAD_OPTION      ",p", program_options::value< std::string >()->default_value( "" ), "payload of the message" )
      ;

   program_options::variables_map vm;
   program_options::store( program_options::parse_command_line( argc, argv, desc ), vm );

   if ( vm.count( HELP_OPTION ) )
   {
      std::cout << desc << std::endl;
      return EXIT_SUCCESS;
   }

   std::string amqp_url     = vm[ AMQP_OPTION ].as< std::string >();
   bool broadcast_mode      = vm[ BROADCAST_OPTION ].as< bool >();
   std::string content_type = vm[ CONTENT_TYPE_OPTION ].as< std::string >();
   std::string routing_key  = vm[ ROUTING_KEY_OPTION ].as< std::string >();
   std::string payload      = vm[ PAYLOAD_OPTION ].as< std::string >();
   uint64_t timeout         = vm[ TIMEOUT_OPTION ].as< uint64_t >();

   mq::client c;

   if( c.connect( amqp_url ) != mq::error_code::success )
      return EXIT_FAILURE;

   try
   {
      if ( broadcast_mode )
      {
         c.broadcast( routing_key, payload, content_type );
      }
      else
      {
         auto r = c.rpc( routing_key, payload, timeout, mq::retry_policy::exponential_backoff, content_type );
         std::cout << r.get() << std::endl;
      }
   }
   catch ( const std::exception& e )
   {
      std::cerr << e.what() << std::endl;
      return EXIT_FAILURE;
   }

   return EXIT_SUCCESS;
}
