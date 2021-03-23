#pragma once

namespace koinos::mq {

namespace exchange {
   constexpr char event[]     = "koinos.event";
   constexpr char rpc[]       = "koinos.rpc";
} // exchange

namespace exchange_type {
   constexpr char direct[] = "direct";
   constexpr char topic[]  = "topic";
} // exchange_type

inline std::string service_routing_key( const std::string& service )
{
   constexpr char rpc_prefix[] = "koinos.rpc.";
   return rpc_prefix + service;
}

} // koinos::mq
