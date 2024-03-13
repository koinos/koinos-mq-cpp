if (NOT BUILD_EXAMPLES)
  return()
endif()

add_executable(koinos_broadcast_example koinos_broadcast_example.cpp)

target_link_libraries(
  koinos_broadcast_example
    PRIVATE
      mq
      Boost::program_options)

koinos_add_format(TARGET koinos_broadcast_example)

koinos_install(
  TARGETS
    koinos_broadcast_example)
