cmake_minimum_required(VERSION 3.24)
project(pookiecpp LANGUAGES CXX)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/pookiepy/message.proto")
set(GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(PROTO_SOURCES "${GENERATED_DIR}/message.pb.cc" "${GENERATED_DIR}/message.grpc.pb.cc")
add_custom_command(
    OUTPUT ${PROTO_SOURCES} "${GENERATED_DIR}/message.pb.h" "${GENERATED_DIR}/message.grpc.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${GENERATED_DIR}"
        COMMAND $<TARGET_FILE:protobuf::protoc>
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/pookiepy
            --cpp_out=${GENERATED_DIR}
            --grpc_out=${GENERATED_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            "${PROTO_FILE}"
    DEPENDS "${PROTO_FILE}"
    VERBATIM
)

add_library(pookiecpp ${PROTO_SOURCES} cpp/src/base_server.cpp)
target_compile_features(pookiecpp PUBLIC cxx_std_17)
target_include_directories(pookiecpp PUBLIC cpp/include "${GENERATED_DIR}")
target_link_libraries(pookiecpp PUBLIC gRPC::grpc++ protobuf::libprotobuf)

install(TARGETS pookiecpp ARCHIVE DESTINATION lib LIBRARY DESTINATION lib RUNTIME DESTINATION bin)
install(DIRECTORY cpp/include/ DESTINATION include)
install(FILES "${GENERATED_DIR}/message.pb.h" "${GENERATED_DIR}/message.grpc.pb.h"
    DESTINATION include)

include(CTest)
if(BUILD_TESTING)
    add_executable(pookiecpp_server_test cpp/tests/server_test.cpp)
    target_link_libraries(pookiecpp_server_test PRIVATE pookiecpp)
    add_test(NAME pookiecpp_server COMMAND pookiecpp_server_test)
endif()