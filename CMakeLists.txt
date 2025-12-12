cmake_minimum_required(VERSION 3.22)
set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")

project(market-data-collection)

set(CMAKE_CXX_STANDARD 23)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)

# Create symlink to compile_commands.json for IDE to pick it up
execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
)

include(CTest)
enable_testing()

find_package(Catch2 CONFIG REQUIRED)
include(Catch)
find_package(fmt CONFIG REQUIRED)
find_package(glaze CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(Boost CONFIG REQUIRED COMPONENTS
    beast
    cobalt
)
find_package(OpenSSL REQUIRED)

add_compile_options(
    -O3
    -Wall
    -Wextra
    -Wpedantic
    -g
    -fno-omit-frame-pointer
)

include_directories(include)
add_subdirectory(src)
