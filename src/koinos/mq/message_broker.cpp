#include <koinos/mq/message_broker.hpp>

#include <atomic>
#include <cstdio>
#include <mutex>
#include <string>

#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/framing.h>
#include <rabbitmq-c/tcp_socket.h>

#include <koinos/log.hpp>

namespace koinos::mq {

namespace detail {

class message_broker_impl final
{
private:
  std::atomic< bool > _running        = false;
  amqp_connection_state_t _connection = nullptr;
  const amqp_channel_t _channel       = 1;
  std::mutex _amqp_mutex;
  message_broker& _message_broker;

  std::optional< std::string > error_info( amqp_rpc_reply_t r ) noexcept;

  void disconnect_lockfree() noexcept;

  error_code connect_lockfree( const std::string& host,
                               uint16_t port,
                               const std::string& vhost,
                               const std::string& user,
                               const std::string& pass ) noexcept;

public:
  message_broker_impl( message_broker& m );
  ~message_broker_impl();

  error_code connect( const std::string& url, message_broker::on_connect_func fn ) noexcept;

  void disconnect() noexcept;

  bool connected() noexcept;

  error_code publish( const message& msg ) noexcept;

  std::pair< error_code, std::shared_ptr< message > > consume() noexcept;

  error_code declare_exchange( const std::string& exchange,
                               const std::string& exchange_type,
                               bool passive,
                               bool durable,
                               bool auto_delete,
                               bool internal ) noexcept;

  std::pair< error_code, std::string >
  declare_queue( const std::string& queue, bool passive, bool durable, bool exclusive, bool auto_delete ) noexcept;

  error_code bind_queue( const std::string& queue,
                         const std::string& exchange,
                         const std::string& binding_key,
                         bool autoack ) noexcept;

  error_code ack_message( uint64_t delivery_tag ) noexcept;
};

message_broker_impl::message_broker_impl( message_broker& m ):
    _message_broker( m )
{}

message_broker_impl::~message_broker_impl()
{
  if( _running )
    disconnect();
}

void message_broker_impl::disconnect() noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );
  _running = false;
  disconnect_lockfree();
}

void message_broker_impl::disconnect_lockfree() noexcept
{
  if( !_connection )
    return;

  amqp_rpc_reply_t r = amqp_channel_close( _connection, _channel, AMQP_REPLY_SUCCESS );

  if( r.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << "Tried to close channel: " << error_info( r ).value();
  }

  r = amqp_connection_close( _connection, AMQP_REPLY_SUCCESS );

  if( r.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << "Tried to close connection: " << error_info( r ).value();
  }

  int err = amqp_destroy_connection( _connection );

  if( err != AMQP_STATUS_OK )
  {
    LOG( debug ) << "Tried to destroy connection: " << amqp_error_string2( err );
  }

  _connection = nullptr;
}

bool message_broker_impl::connected() noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );
  return _connection != nullptr;
}

error_code message_broker_impl::publish( const message& msg ) noexcept
{
  amqp_basic_properties_t props;

  props._flags        = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.content_type  = amqp_cstring_bytes( msg.content_type.c_str() );
  props.delivery_mode = 2; /* persistent delivery mode */

  if( msg.reply_to.has_value() )
  {
    props._flags   |= AMQP_BASIC_REPLY_TO_FLAG;
    props.reply_to  = amqp_cstring_bytes( msg.reply_to->c_str() );
  }

  if( msg.correlation_id.has_value() )
  {
    props._flags         |= AMQP_BASIC_CORRELATION_ID_FLAG;
    props.correlation_id  = amqp_cstring_bytes( msg.correlation_id->c_str() );
  }

  if( msg.expiration.has_value() )
  {
    props._flags     |= AMQP_BASIC_EXPIRATION_FLAG;
    props.expiration  = amqp_cstring_bytes( std::to_string( msg.expiration.value() ).c_str() );
  }

  amqp_bytes_t data;
  data.bytes = (void*)msg.data.c_str();
  data.len   = msg.data.size();

  {
    std::lock_guard< std::mutex > lock( _amqp_mutex );

    if( _connection == nullptr )
      return error_code::failure;

    int err = amqp_basic_publish( _connection,
                                  _channel,
                                  amqp_cstring_bytes( msg.exchange.c_str() ),
                                  amqp_cstring_bytes( msg.routing_key.c_str() ),
                                  0,
                                  0,
                                  &props,
                                  data );

    if( err != AMQP_STATUS_OK )
    {
      LOG( debug ) << "Failed to publish message";
      disconnect_lockfree();
      return error_code::failure;
    }
  }

  return error_code::success;
}

error_code message_broker_impl::connect_lockfree( const std::string& host,
                                                  uint16_t port,
                                                  const std::string& vhost,
                                                  const std::string& user,
                                                  const std::string& pass ) noexcept
{
  disconnect_lockfree();

  amqp_socket_t* socket = nullptr;

  _connection = amqp_new_connection();
  socket      = amqp_tcp_socket_new( _connection );

  if( !socket )
  {
    LOG( debug ) << "Failed to create socket";
    disconnect_lockfree();
    return error_code::failure;
  }

  int err = amqp_socket_open( socket, host.c_str(), port );

  if( err != AMQP_STATUS_OK )
  {
    LOG( debug ) << "Failed to open socket";
    disconnect_lockfree();
    return error_code::failure;
  }

  amqp_rpc_reply_t r;

  r = amqp_login( _connection,
                  vhost.c_str(),
                  AMQP_DEFAULT_MAX_CHANNELS,
                  AMQP_DEFAULT_FRAME_SIZE,
                  0 /* seconds between heartbeat frames */,
                  AMQP_SASL_METHOD_PLAIN,
                  user.c_str(),
                  pass.c_str() );

  if( r.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( r ).value();
    disconnect_lockfree();
    return error_code::failure;
  }

  amqp_channel_open( _connection, _channel );
  r = amqp_get_rpc_reply( _connection );

  if( r.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( r ).value();
    disconnect_lockfree();
    return error_code::failure;
  }

  return error_code::success;
}

error_code message_broker_impl::connect( const std::string& url, message_broker::on_connect_func fn ) noexcept
{
  _running = true;

  std::vector< char > tmp_url( std::begin( url ), std::end( url ) );
  tmp_url.push_back( '\0' );

  amqp_connection_info cinfo;
  auto result = amqp_parse_url( tmp_url.data(), &cinfo );

  if( result != AMQP_STATUS_OK )
  {
    LOG( debug ) << "Unable to parse provided AMQP url";
    return error_code::failure;
  }

  mq::error_code code = mq::error_code::failure;

  {
    std::lock_guard< std::mutex > lock( _amqp_mutex );

    code = connect_lockfree( std::string( cinfo.host ),
                             uint16_t( cinfo.port ),
                             std::string( *cinfo.vhost ? cinfo.vhost : "/" ),
                             std::string( cinfo.user ),
                             std::string( cinfo.password ) );
  }

  if( code != error_code::success )
    return code;

  if( fn( _message_broker ) == error_code::failure )
  {
    LOG( debug ) << "Failure during connection callback";
    std::lock_guard< std::mutex > lock( _amqp_mutex );
    disconnect_lockfree();
    return error_code::failure;
  }

  return error_code::success;
}

error_code message_broker_impl::declare_exchange( const std::string& exchange,
                                                  const std::string& exchange_type,
                                                  bool passive,
                                                  bool durable,
                                                  bool auto_delete,
                                                  bool internal ) noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );

  amqp_exchange_declare( _connection,
                         _channel,
                         amqp_cstring_bytes( exchange.c_str() ),
                         amqp_cstring_bytes( exchange_type.c_str() ),
                         int( passive ),
                         int( durable ),
                         int( auto_delete ),
                         int( internal ),
                         amqp_empty_table );

  auto reply = amqp_get_rpc_reply( _connection );

  if( reply.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( reply ).value();
    return error_code::failure;
  }

  return error_code::success;
}

std::pair< error_code, std::string > message_broker_impl::declare_queue( const std::string& queue,
                                                                         bool passive,
                                                                         bool durable,
                                                                         bool exclusive,
                                                                         bool auto_delete ) noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );

  amqp_queue_declare_ok_t* r =
    amqp_queue_declare( _connection,
                        _channel,
                        queue.empty() ? amqp_empty_bytes : amqp_cstring_bytes( queue.c_str() ),
                        int( passive ),
                        int( durable ),
                        int( exclusive ),
                        int( auto_delete ),
                        amqp_empty_table );

  auto reply = amqp_get_rpc_reply( _connection );
  if( reply.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( reply ).value();
    return std::make_pair( error_code::failure, "" );
  }

  if( queue.empty() )
  {
    return std::make_pair( error_code::success, std::string( (char*)r->queue.bytes, (std::size_t)r->queue.len ) );
  }

  return std::make_pair( error_code::success, queue );
}

error_code message_broker_impl::bind_queue( const std::string& queue,
                                            const std::string& exchange,
                                            const std::string& binding_key,
                                            bool autoack ) noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );

  auto queue_bytes = amqp_cstring_bytes( queue.c_str() );
  amqp_queue_bind( _connection,
                   _channel,
                   queue_bytes,
                   amqp_cstring_bytes( exchange.c_str() ),
                   amqp_cstring_bytes( binding_key.c_str() ),
                   amqp_empty_table );

  auto reply = amqp_get_rpc_reply( _connection );
  if( reply.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( reply ).value();
    return error_code::failure;
  }

  amqp_basic_consume( _connection,
                      _channel,
                      queue_bytes,
                      amqp_empty_bytes,
                      int( false ),
                      int( autoack ),
                      int( false ),
                      amqp_empty_table );

  reply = amqp_get_rpc_reply( _connection );
  if( reply.reply_type != AMQP_RESPONSE_NORMAL )
  {
    LOG( debug ) << error_info( reply ).value();
    return error_code::failure;
  }

  return error_code::success;
}

std::optional< std::string > message_broker_impl::error_info( amqp_rpc_reply_t r ) noexcept
{
  if( r.reply_type == AMQP_RESPONSE_NONE )
  {
    return "missing RPC reply type";
  }
  else if( r.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION )
  {
    return amqp_error_string2( r.library_error );
  }
  else if( r.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION )
  {
    constexpr std::size_t bufsize = 256;
    char buf[ bufsize ];
    switch( r.reply.id )
    {
      case AMQP_CONNECTION_CLOSE_METHOD:
        {
          amqp_connection_close_t* m = (amqp_connection_close_t*)r.reply.decoded;
          snprintf( buf,
                    bufsize,
                    "server connection error %u, message: %.*s",
                    m->reply_code,
                    (int)m->reply_text.len,
                    (char*)m->reply_text.bytes );
          return buf;
        }
      case AMQP_CHANNEL_CLOSE_METHOD:
        {
          amqp_channel_close_t* m = (amqp_channel_close_t*)r.reply.decoded;
          snprintf( buf,
                    bufsize,
                    "server channel error %u, message: %.*s",
                    m->reply_code,
                    (int)m->reply_text.len,
                    (char*)m->reply_text.bytes );
          return buf;
        }
      default:
        snprintf( buf, bufsize, "unknown server error, method ID 0x%08X", r.reply.id );
        return buf;
    }
  }

  return {};
}

std::pair< error_code, std::shared_ptr< message > > message_broker_impl::consume() noexcept
{
  std::pair< error_code, std::shared_ptr< message > > result;
  amqp_rpc_reply_t reply;
  amqp_envelope_t envelope;

  {
    std::lock_guard< std::mutex > lock( _amqp_mutex );

    if( _connection == nullptr )
      return std::make_pair( error_code::failure, nullptr );

    amqp_maybe_release_buffers( _connection );

    timeval tv;
    tv.tv_sec  = 0;
    tv.tv_usec = 250'000;
    reply      = amqp_consume_message( _connection, &envelope, &tv, 0 );
  }

  if( reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION )
  {
    if( reply.library_error == AMQP_STATUS_TIMEOUT )
    {
      result.first = error_code::time_out;
      return result;
    }
    else
    {
      std::lock_guard< std::mutex > lock( _amqp_mutex );
      LOG( debug ) << "Unable to consume message: " << error_info( reply ).value();
      disconnect_lockfree();
      result.first = error_code::failure;
      return result;
    }
  }
  else if( AMQP_RESPONSE_NORMAL != reply.reply_type )
  {
    std::lock_guard< std::mutex > lock( _amqp_mutex );
    LOG( debug ) << "Unable to consume message: " << error_info( reply ).value();
    disconnect_lockfree();
    result.first = error_code::failure;
    return result;
  }

  result.second = std::make_shared< message >();

  message& msg = *result.second;

  msg.delivery_tag = envelope.delivery_tag;
  msg.exchange     = std::string( (char*)envelope.exchange.bytes, (std::size_t)envelope.exchange.len );
  msg.routing_key  = std::string( (char*)envelope.routing_key.bytes, (std::size_t)envelope.routing_key.len );

  if( envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG )
  {
    msg.content_type = std::string( (char*)envelope.message.properties.content_type.bytes,
                                    (std::size_t)envelope.message.properties.content_type.len );
  }

  if( envelope.message.properties._flags & AMQP_BASIC_REPLY_TO_FLAG )
  {
    msg.reply_to = std::string( (char*)envelope.message.properties.reply_to.bytes,
                                (std::size_t)envelope.message.properties.reply_to.len );
  }

  if( envelope.message.properties._flags & AMQP_BASIC_CORRELATION_ID_FLAG )
  {
    msg.correlation_id = std::string( (char*)envelope.message.properties.correlation_id.bytes,
                                      (std::size_t)envelope.message.properties.correlation_id.len );
  }

  msg.data = std::string( (char*)envelope.message.body.bytes, (std::size_t)envelope.message.body.len );

  amqp_destroy_envelope( &envelope );

  result.first = error_code::success;

  return result;
}

error_code message_broker_impl::ack_message( uint64_t delivery_tag ) noexcept
{
  std::lock_guard< std::mutex > lock( _amqp_mutex );

  int err = amqp_basic_ack( _connection, _channel, delivery_tag, false );

  return err != AMQP_STATUS_OK ? error_code::failure : error_code::success;
}

} // namespace detail

message_broker::message_broker()
{
  _message_broker_impl = std::make_unique< detail::message_broker_impl >( *this );
}

message_broker::~message_broker() = default;

error_code message_broker::connect( const std::string& url, on_connect_func f ) noexcept
{
  return _message_broker_impl->connect( url, f );
}

void message_broker::disconnect() noexcept
{
  _message_broker_impl->disconnect();
}

bool message_broker::connected() noexcept
{
  return _message_broker_impl->connected();
}

error_code message_broker::publish( const message& msg ) noexcept
{
  return _message_broker_impl->publish( msg );
}

error_code message_broker::declare_exchange( const std::string& exchange,
                                             const std::string& exchange_type,
                                             bool passive,
                                             bool durable,
                                             bool auto_delete,
                                             bool internal ) noexcept
{
  return _message_broker_impl->declare_exchange( exchange, exchange_type, passive, durable, auto_delete, internal );
}

std::pair< error_code, std::string > message_broker::declare_queue( const std::string& queue,
                                                                    bool passive,
                                                                    bool durable,
                                                                    bool exclusive,
                                                                    bool auto_delete ) noexcept
{
  return _message_broker_impl->declare_queue( queue, passive, durable, exclusive, auto_delete );
}

error_code message_broker::bind_queue( const std::string& queue,
                                       const std::string& exchange,
                                       const std::string& binding_key,
                                       bool autoack ) noexcept
{
  return _message_broker_impl->bind_queue( queue, exchange, binding_key, autoack );
}

std::pair< error_code, std::shared_ptr< message > > message_broker::consume() noexcept
{
  return _message_broker_impl->consume();
}

error_code message_broker::ack_message( uint64_t delivery_tag ) noexcept
{
  return _message_broker_impl->ack_message( delivery_tag );
}

} // namespace koinos::mq
