cmake_minimum_required(VERSION 3.24)
project(pookiecpp_consumer LANGUAGES CXX)

find_package(pookiecpp CONFIG REQUIRED)

add_executable(pookiecpp_consumer test_package.cpp)
target_compile_features(pookiecpp_consumer PRIVATE cxx_std_17)
target_link_libraries(pookiecpp_consumer PRIVATE pookiecpp::pookiecpp)

enable_testing()
add_test(NAME package_consumer COMMAND pookiecpp_consumer)
set_tests_properties(package_consumer PROPERTIES TIMEOUT 15)