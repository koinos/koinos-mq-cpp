add_executable(koinos_mq_client main.cpp)
target_link_libraries(koinos_mq_client Koinos::mq Boost::program_options)
install(TARGETS
   koinos_mq_client
   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)

