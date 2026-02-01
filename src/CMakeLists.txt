add_compile_definitions(
    $<$<CONFIG:Debug>:SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG>
    $<$<CONFIG:Release>:SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO>
)

add_library(websocket websocket.cc)
target_link_libraries(websocket PUBLIC
    Boost::beast
    Boost::cobalt
    Boost::system
    OpenSSL::SSL
    OpenSSL::Crypto
    spdlog::spdlog
)


add_executable(test_batch_io batch_io.test.cc)
target_link_libraries(test_batch_io PRIVATE
    Catch2::Catch2WithMain
    spdlog::spdlog
)
catch_discover_tests(test_batch_io)

add_executable(test_spsc spsc.test.cc)
target_link_libraries(test_spsc PRIVATE
    Catch2::Catch2WithMain
    spdlog::spdlog
)
catch_discover_tests(test_spsc)
target_compile_options(test_spsc PRIVATE -fsanitize=thread,undefined)
target_link_options(test_spsc PRIVATE -fsanitize=thread,undefined)


add_executable(benchmark_spsc spsc.benchmark.cc)
target_link_libraries(benchmark_spsc PRIVATE
    Catch2::Catch2WithMain
    spdlog::spdlog
)
catch_discover_tests(benchmark_spsc)

add_executable(benchmark_spsc_throughput spsc.benchmark.throughput.cc)

add_executable(collect_ws collect_ws.cc)
target_link_libraries(collect_ws PRIVATE
    websocket
)

# target_compile_options(collect_ws PRIVATE -fsanitize=thread,undefined)
# target_link_options(collect_ws PRIVATE -fsanitize=thread,undefined)
