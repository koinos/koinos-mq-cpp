#pragma once

namespace koinos::mq {

namespace exchange {
   constexpr char event[]     = "koinos_event";
   constexpr char rpc[]       = "koinos_rpc";
   constexpr char rpc_reply[] = "koinos_rpc_reply";
} // exchange

namespace exchange_type {
   constexpr char direct[] = "direct";
   constexpr char topic[]  = "topic";
} // exchange_type

inline std::string service_routing_key( const std::string& service )
{
   constexpr char rpc_prefix[] = "koinos_rpc_";
   return rpc_prefix + service;
}

} // koinos::mq
