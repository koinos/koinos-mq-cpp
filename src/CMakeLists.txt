# libkoinos_mq

add_library(mq
  koinos/mq/client.cpp
  koinos/mq/message_broker.cpp
  koinos/mq/request_handler.cpp
  koinos/mq/retryer.cpp

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
      Boost::headers
      Koinos::log
      Koinos::proto
      Koinos::util
      Koinos::exception
    PRIVATE
      rabbitmq::rabbitmq-static)

target_include_directories(
  mq
    PUBLIC
      $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:include>)

koinos_add_format(TARGET mq)

install(
  DIRECTORY
    ${PROJECT_SOURCE_DIR}/include
  DESTINATION
    ${CMAKE_INSTALL_PREFIX})

# koinos_mq_client

add_executable(koinos_mq_client koinos_mq_client.cpp)
target_link_libraries(
  koinos_mq_client
    PRIVATE
      mq
      Boost::program_options)

koinos_add_format(TARGET koinos_mq_client)

koinos_install(
  TARGETS
    mq
    koinos_mq_client)
