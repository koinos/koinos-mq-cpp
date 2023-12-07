# libkoinos_mq
add_library(mq
  client.cpp
  message_broker.cpp
  request_handler.cpp
  retryer.cpp

  ${PROJECT_SOURCE_DIR}/include/koinos/mq/client.hpp
  ${PROJECT_SOURCE_DIR}/include/koinos/mq/exception.hpp
  ${PROJECT_SOURCE_DIR}/include/koinos/mq/message_broker.hpp
  ${PROJECT_SOURCE_DIR}/include/koinos/mq/request_handler.hpp
  ${PROJECT_SOURCE_DIR}/include/koinos/mq/retryer.hpp
  ${PROJECT_SOURCE_DIR}/include/koinos/mq/util.hpp
)

target_link_libraries(
  mq
    PUBLIC
      Koinos::util
      Koinos::exception
      rabbitmq::rabbitmq-static
)

target_include_directories(
  mq
    PUBLIC
      $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>
)

koinos_add_format_target(mq)

add_library(Koinos::mq ALIAS mq)

install(
  DIRECTORY
    ${PROJECT_SOURCE_DIR}/include
  DESTINATION
    include/
)

# koinos_mq_client

add_executable(koinos_mq_client koinos_mq_client.cpp)
target_link_libraries(koinos_mq_client Koinos::mq Koinos::log Boost::program_options)
install(TARGETS
  koinos_mq_client
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

koinos_add_format_target(koinos_mq_client)

# koinos_test_driver

add_executable(koinos_test_driver koinos_test_driver.cpp)
target_link_libraries(koinos_test_driver Koinos::mq Koinos::log Boost::program_options)
install(TARGETS
koinos_test_driver
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

koinos_add_format_target(koinos_test_driver)
