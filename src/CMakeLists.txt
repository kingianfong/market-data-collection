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

add_library(batch_io batch_io.cc)
target_link_libraries(batch_io PUBLIC
    spdlog::spdlog
)

add_executable(test_batch_io batch_io.test.cc)
target_link_libraries(test_batch_io PRIVATE
    Catch2::Catch2WithMain
    batch_io
)
catch_discover_tests(test_batch_io)


add_executable(collect_ws collect_ws.cc)
target_link_libraries(collect_ws PRIVATE
    websocket
    batch_io
)

# target_compile_options(collect_ws PRIVATE -fsanitize=thread,undefined)
# target_link_options(collect_ws PRIVATE -fsanitize=thread,undefined)
