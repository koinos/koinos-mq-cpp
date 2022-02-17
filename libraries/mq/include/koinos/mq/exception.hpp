#pragma once

#include <koinos/exception.hpp>

namespace koinos::mq {

KOINOS_DECLARE_EXCEPTION( mq_exception );

// Client
KOINOS_DECLARE_DERIVED_EXCEPTION( client_not_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_not_connected, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_already_connected, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_request_invalid, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_request_insertion_error, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_timeout_error, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_publish_error, mq_exception );

// Request handler
KOINOS_DECLARE_DERIVED_EXCEPTION( request_handler_not_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( request_handler_not_connected, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( request_handler_already_connected, mq_exception );

// Common
KOINOS_DECLARE_DERIVED_EXCEPTION( mq_connection_failure, mq_exception );

} // koinos::mq
