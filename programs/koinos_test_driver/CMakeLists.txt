add_executable(koinos_test_driver main.cpp)
target_link_libraries(koinos_test_driver Koinos::mq Koinos::log Boost::program_options)
install(TARGETS
   koinos_test_driver
   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)

