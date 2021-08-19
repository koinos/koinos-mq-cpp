hunter_config(Boost
   VERSION ${HUNTER_Boost_VERSION}
   CMAKE_ARGS
      USE_CONFIG_FROM_BOOST=ON
      Boost_USE_STATIC_LIBS=ON
      Boost_NO_BOOST_CMAKE=ON
)

hunter_config(Protobuf
   VERSION ${HUNTER_Protobuf_VERSION}
   CMAKE_ARGS
      CMAKE_CXX_FLAGS=-fvisibility=hidden
      CMAKE_C_FLAGS=-fvisibility=hidden
)

hunter_config(koinos_util
   URL  "https://github.com/koinos/koinos-util-cpp/archive/22ebcd097395e1e6035fbec7bb993a44e0eab92d.tar.gz"
   SHA1 "8e51b7d1eac85c413c92b4ffe722a848c6a35520"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_log
   URL  "https://github.com/koinos/koinos-log-cpp/archive/8a148b2839116e060b3327fe6358210dd2a55f4d.tar.gz"
   SHA1 "8075e5882ffc5d450b35521792dcf6b29027cebd"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_exception
   URL  "https://github.com/koinos/koinos-exception-cpp/archive/77f5b1cf0877714d4214bab3a7eeab45ad33df54.tar.gz"
   SHA1 "b974a3ef9133c82d144882ad395754c98ee6333c"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_proto
   URL  "https://github.com/koinos/koinos-proto-cpp/archive/4acb3322d25148c66cf423cbe1c77c202439dfa8.tar.gz"
   SHA1 "b8df692d6a535105c7673c883f0b3b8db5732d13"
)

hunter_config(rabbitmq-c
   URL "https://github.com/alanxz/rabbitmq-c/archive/b8e5f43b082c5399bf1ee723c3fd3c19cecd843e.tar.gz"
   SHA1 "35d4ce3e4f0a5348de64bbed25c6e1df72da2594"
   CMAKE_ARGS
      ENABLE_SSL_SUPPORT=OFF
)
