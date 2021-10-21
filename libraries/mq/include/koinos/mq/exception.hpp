#pragma once

#include <koinos/exception.hpp>

namespace koinos::mq {

KOINOS_DECLARE_EXCEPTION( mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( unable_to_connect, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( client_not_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( broker_already_running, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( broker_publish_error, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( correlation_id_collision, mq_exception );
KOINOS_DECLARE_DERIVED_EXCEPTION( timeout_error, mq_exception );

} // koinos::mq
