if (NOT BUILD_EXAMPLES)
  return()
endif()

add_executable(koinos_test_driver koinos_test_driver.cpp)

target_link_libraries(
  koinos_test_driver
    PRIVATE
      mq
      Boost::program_options)

koinos_add_format(TARGET koinos_test_driver)

koinos_install(
  TARGETS
    koinos_test_driver)
