#include "tech/batch_io.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <glaze/json/write.hpp>
#include <string>

#include "tech/types.h"

namespace tech {

namespace fs = std::filesystem;

TEST_CASE("JsonWriter - Push", "[JsonWriter]") {
  using namespace std::chrono;  // NOLINT
  const std::string temp_dir = "temp_writer_test";

  fs::create_directories(temp_dir);

  const auto first_msg_time = time_point_cast<nanoseconds>(system_clock::now());
  std::string expected_filename{};
  fmt::format_to(std::back_inserter(expected_filename),
                 "{}/{:%Y%m%d-%H%M%S}.jsonl", temp_dir, first_msg_time);

  std::string str_buf;
  str_buf = R"({"field": "value1"})";
  const StreamMessage msg1{
      .source_index = 1,
      .len = static_cast<uint32_t>(str_buf.size()),
      .recv_seq_num = 100,
      .recv_ts = nanoseconds{first_msg_time.time_since_epoch()}.count(),
      .data = str_buf.data(),
  };

  str_buf = R"({"field": "value2"})";
  const StreamMessage msg2{
      .source_index = 2,
      .len = static_cast<uint32_t>(str_buf.size()),
      .recv_seq_num = 101,
      .recv_ts = 1001,
      .data = str_buf.data(),
  };

  const auto check_outputs = [&] {
    std::this_thread::yield();
    std::ifstream file(expected_filename);
    INFO("expected_filename = " << expected_filename);
    REQUIRE(file.good());

    std::stringstream ss;
    ss << file.rdbuf();
    const auto content = ss.str();

    std::string expected_raw_jsonl;
    expected_raw_jsonl.append(glz::write<glz::opts{}>(msg1).value());
    expected_raw_jsonl.push_back('\n');
    expected_raw_jsonl.append(glz::write<glz::opts{}>(msg2).value());
    expected_raw_jsonl.push_back('\n');

    REQUIRE(content == expected_raw_jsonl);
    fs::remove_all(temp_dir);
  };

  SECTION("reach capacity") {
    static constexpr auto max_buffered_messages = 2;
    JsonWriter writer{temp_dir, max_buffered_messages};
    writer.Push(msg1);
    writer.Push(msg2);
    check_outputs();
  }

  SECTION("destructor") {
    {
      static constexpr auto max_buffered_messages = 32;
      JsonWriter writer{temp_dir, max_buffered_messages};
      writer.Push(msg1);
      writer.Push(msg2);
    }
    check_outputs();
  }
}

}  // namespace tech
