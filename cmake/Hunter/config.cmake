hunter_config(Boost
   VERSION ${HUNTER_Boost_VERSION}
   CMAKE_ARGS
      USE_CONFIG_FROM_BOOST=ON
      Boost_USE_STATIC_LIBS=ON
      Boost_NO_BOOST_CMAKE=ON
)

hunter_config(Protobuf
   URL  "https://github.com/koinos/protobuf/archive/e1b1477875a8b022903b548eb144f2c7bf4d9561.tar.gz"
   SHA1 "5796707a98eec15ffb3ad86ff50e8eec5fa65e68"
   CMAKE_ARGS
      CMAKE_CXX_FLAGS=-fvisibility=hidden
      CMAKE_C_FLAGS=-fvisibility=hidden
)

hunter_config(yaml-cpp
   VERSION "0.6.3"
   CMAKE_ARGS
      CMAKE_CXX_FLAGS=-fvisibility=hidden
      CMAKE_C_FLAGS=-fvisibility=hidden
)

hunter_config(koinos_util
   URL  "https://github.com/koinos/koinos-util-cpp/archive/c42728a1fb312d0246124f3e8a27a4a2b9106606.tar.gz"
   SHA1 "3cbbc0d80c8745e2a9d7e91250428625f427c117"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_log
   URL  "https://github.com/koinos/koinos-log-cpp/archive/9ed0516ee7f010065d5d2de3a866a7e302fc8dae.tar.gz"
   SHA1 "219828a04306f12b64ad4be7cba2ea67bdbbbad7"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_exception
   URL  "https://github.com/koinos/koinos-exception-cpp/archive/b34d82bc7f75297a6500009aa7530f83e1c6142c.tar.gz"
   SHA1 "98eda354dae8ee488285cedae8abe2e5eb94c470"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_proto
   URL  "https://github.com/koinos/koinos-proto-cpp/archive/5e93d9f313ea759eb8b2515e10a25160aa2b1db0.tar.gz"
   SHA1 "11b9724202986f488185fe3d6d232a386c0319b7"
)

hunter_config(rabbitmq-c
   URL "https://github.com/alanxz/rabbitmq-c/archive/b8e5f43b082c5399bf1ee723c3fd3c19cecd843e.tar.gz"
   SHA1 "35d4ce3e4f0a5348de64bbed25c6e1df72da2594"
   CMAKE_ARGS
      ENABLE_SSL_SUPPORT=OFF
)
