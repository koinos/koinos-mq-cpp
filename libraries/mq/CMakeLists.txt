file(GLOB HEADERS "include/koinos/mq/*.hpp")
add_library(koinos_mq
            client.cpp
            retryer.cpp
            request_handler.cpp
            message_broker.cpp
            ${HEADERS})

target_link_libraries(koinos_mq PUBLIC Koinos::util Koinos::exception rabbitmq::rabbitmq-static)
target_include_directories(koinos_mq PUBLIC
   $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
   $<INSTALL_INTERFACE:include>
)

add_library(Koinos::mq ALIAS koinos_mq)

install(FILES ${HEADERS} DESTINATION "include/koinos/mq")

export(
   TARGETS ${KOINOS_LIB_TARGET_NAME}
   NAMESPACE Koinos::
   FILE ${KOINOS_LIB_CMAKE_PROJECT_TARGETS_FILE}
)

install(
   TARGETS ${KOINOS_LIB_TARGET_NAME}
   EXPORT ${KOINOS_LIB_TARGETS_EXPORT_NAME}
   INCLUDES DESTINATION ${KOINOS_LIB_INCLUDE_INSTALL_DIR}
   RUNTIME DESTINATION bin
   LIBRARY DESTINATION lib
   ARCHIVE DESTINATION lib
)

install(
   EXPORT ${KOINOS_LIB_TARGETS_EXPORT_NAME}
   NAMESPACE Koinos::
   DESTINATION ${KOINOS_LIB_CONFIG_INSTALL_DIR}
)
