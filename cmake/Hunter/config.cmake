hunter_config(Boost
   VERSION ${HUNTER_Boost_VERSION}
   CMAKE_ARGS
      USE_CONFIG_FROM_BOOST=ON
      Boost_USE_STATIC_LIBS=ON
      Boost_NO_BOOST_CMAKE=ON
)

hunter_config(koinos_util
   URL "https://github.com/koinos/koinos-util-cpp/archive/a5f56e6cc68f150f5a126698c42764fbb04c8505.tar.gz"
   SHA1 "deb309751ff754d62b2de7d1c6a726b19d82f372"
)

hunter_config(koinos_log
   URL  "https://github.com/koinos/koinos-log-cpp/archive/4ecb8399d05d1639c52a34845f55aa826f35d484.tar.gz"
   SHA1 "1b11e2acadd4d37a483944096bed916ba579637d"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_types
   URL "https://github.com/koinos/koinos-types/archive/d8a9db91761d8aa84723f0b0b5b12e032fad9fa9.tar.gz"
   SHA1 "3764a668d7e0f6c5876f522f11bdc39cbdbbba8b"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(rabbitmq-c
   URL "https://github.com/alanxz/rabbitmq-c/archive/b8e5f43b082c5399bf1ee723c3fd3c19cecd843e.tar.gz"
   SHA1 "35d4ce3e4f0a5348de64bbed25c6e1df72da2594"
   CMAKE_ARGS
      ENABLE_SSL_SUPPORT=OFF
)

