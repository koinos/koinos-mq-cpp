#include <koinos/mq/client.hpp>

#include <map>
#include <mutex>

namespace koinos::mq {

namespace detail {

template < class K, class V, class Compare = std::less< K >, class Allocator = std::allocator< std::pair< const K, V > > >
class guarded_map
{
private:
   std::map< K, V, Compare, Allocator > _map;
   std::mutex                           _mutex;

public:
   void set( K key, V&& value )
   {
      std::lock_guard< std::mutex > lock( _mutex );
      _map[ key ] = std::forward< V >( value );
   }

   V& get( K key )
   {
      std::lock_guard< std::mutex > lock( _mutex );
      return _map[ key ];
   }

   void erase( K key )
   {
      std::lock_guard< std::mutex > lock( _mutex );
      _map.erase( key );
   }

   bool empty()
   {
      std::lock_guard< std::mutex > lock( _mutex );
      return _map.empty();
   }
};

using guarded_promise_map = guarded_map< std::string, std::promise< std::string > >;

struct client_impl
{
   error_code connect( const std::string& amqp_url );

   std::future< std::string > rpc( std::string content_type, std::string rpc_type, std::string payload );
   void broadcast( std::string content_type, std::string rpc_type, std::string payload );

   guarded_promise_map                _promise_map;
   std::shared_ptr< message_broker >  _writer_broker;

   std::unique_ptr< std::thread >     _reader_thread;
   std::shared_ptr< message_broker >  _reader_broker;
};

error_code client_impl::connect( const std::string& amqp_url )
{
   error_code ec;

   ec = _writer_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   ec = _reader_broker->connect( amqp_url );
   if ( ec != error_code::success )
      return ec;

   return error_code::success;
}

std::future< std::string > client_impl::rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   auto promise = std::promise< std::string >();
   return promise.get_future();
}

void client_impl::broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
}

} // detail

error_code client::connect( const std::string& amqp_url )
{
   return _my->connect( amqp_url );
}

std::future< std::string > client::rpc( std::string content_type, std::string rpc_type, std::string payload )
{
   return _my->rpc( content_type, rpc_type, payload );
}

void client::broadcast( std::string content_type, std::string rpc_type, std::string payload )
{
   _my->broadcast( content_type, rpc_type, payload );
}

} // koinos::mq
