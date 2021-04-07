hunter_config(Boost
   VERSION ${HUNTER_Boost_VERSION}
   CMAKE_ARGS
      USE_CONFIG_FROM_BOOST=ON
      Boost_USE_STATIC_LIBS=ON
      Boost_NO_BOOST_CMAKE=ON
)

hunter_config(koinos_util
   URL  "https://github.com/koinos/koinos-util-cpp/archive/32f7589037c6ebcb0c55f2dc558df44ed06176a0.tar.gz"
   SHA1 "958a61b9316556382ac6300f7a40798c9e375040"
)

hunter_config(koinos_log
   URL  "https://github.com/koinos/koinos-log-cpp/archive/90b1ea813328a816401e81d8bca68de1e47b7a69.tar.gz"
   SHA1 "d1b8c3eee49e0a584213f1b15131973057b2c6c8"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_types
   URL  "https://github.com/koinos/koinos-types/archive/2cba96077317075e2f53e8ba268abba16a2c5477.tar.gz"
   SHA1 "51dae5f5a65a7f08028b9f8d6277680a4c13e3fa"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(koinos_exception
   URL  "https://github.com/koinos/koinos-exception-cpp/archive/373937ced4b890bc6a8dbdad6452560860a38f5e.tar.gz"
   SHA1 "1dd40d3e733d7a9220adbe64e47e40c0b1079062"
   CMAKE_ARGS
      BUILD_TESTS=OFF
)

hunter_config(rabbitmq-c
   URL "https://github.com/alanxz/rabbitmq-c/archive/b8e5f43b082c5399bf1ee723c3fd3c19cecd843e.tar.gz"
   SHA1 "35d4ce3e4f0a5348de64bbed25c6e1df72da2594"
   CMAKE_ARGS
      ENABLE_SSL_SUPPORT=OFF
)
