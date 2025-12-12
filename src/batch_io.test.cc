#include "tech/batch_io.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <glaze/json/write.hpp>
#include <string>

#include "tech/types.h"

namespace tech {

namespace fs = std::filesystem;

// =======================================================================================
// SpscBuffers TESTS
// =======================================================================================

TEST_CASE("SpscBuffers - Single Buffer", "[SpscBuffers]") {
  auto buffer = std::make_unique<SpscBuffers>();

  std::string reused_buffer{};  // ensure no bugs from dangling references
  for (int64_t i = 0; i < SpscBuffers::buffer_capacity; ++i) {
    reused_buffer = std::to_string(i);
    REQUIRE(buffer->TryPush(StreamMessage{
        .source_index{},
        .len = static_cast<uint32_t>(reused_buffer.size()),
        .recv_seq_num = i,
        .recv_ts{},
        .data = reused_buffer.data(),
    }));

    REQUIRE_FALSE(buffer->TryConsume([&](auto&&) {
      INFO("i = " << i);
      FAIL("Should not consume yet.");
    }));
  }

  reused_buffer = "hello";
  REQUIRE(buffer->TryPush(StreamMessage{
      .source_index{},
      .len = static_cast<uint32_t>(reused_buffer.size()),
      .recv_seq_num = -1,
      .recv_ts{},
      .data = reused_buffer.data(),
  }));

  std::vector<StreamMessage> consumed_messages;
  CHECK(buffer->TryConsume(
      [&](const StreamMessage& msg) { consumed_messages.push_back(msg); }));
  CHECK(consumed_messages.front().raw_message() == "0");
  CHECK(consumed_messages.back().raw_message() ==
        std::to_string(consumed_messages.size() - 1));

  REQUIRE(consumed_messages.size() == SpscBuffers::buffer_capacity);
  for (int64_t i = 0; i < SpscBuffers::buffer_capacity; ++i) {
    REQUIRE(consumed_messages[i].recv_seq_num == i);
  }

  REQUIRE_FALSE(
      buffer->TryConsume([&](auto&&) { FAIL("Should not consume yet."); }));

  std::vector<StreamMessage> flushed_messages;
  buffer->FlushAll(
      [&](const StreamMessage& msg) { flushed_messages.push_back(msg); });

  CHECK(flushed_messages.size() == 1);
  CHECK(flushed_messages[0].raw_message() == "hello");
}

TEST_CASE("SpscBuffers - Multiple Buffers", "[SpscBuffers]") {
  auto buffer = std::make_unique<SpscBuffers>();
  static constexpr auto n_messages_without_flushing =
      SpscBuffers::buffer_capacity * SpscBuffers::n_buffers;

  int64_t next_recv_seq_num = 0;
  int64_t next_expected_recv_seq_num = next_recv_seq_num;
  std::string str_buf = "x";

  for (size_t i = 0; i < n_messages_without_flushing; ++i) {
    REQUIRE(buffer->TryPush(StreamMessage{
        .source_index{},
        .len = static_cast<uint32_t>(str_buf.size()),
        .recv_seq_num = next_recv_seq_num++,
        .recv_ts{},
        .data = str_buf.data(),
    }));
  }

  str_buf = "should not get pushed";
  REQUIRE_FALSE(buffer->TryPush(StreamMessage{
      .source_index{},
      .len = static_cast<uint32_t>(str_buf.size()),
      .recv_seq_num{},
      .recv_ts{},
      .data = str_buf.data(),
  }));

  const auto consume = [&](const StreamMessage& msg) {
    REQUIRE(msg.recv_seq_num == next_expected_recv_seq_num);
    ++next_expected_recv_seq_num;
  };

  for (size_t i = 0; i < SpscBuffers::n_buffers - 1; ++i) {
    INFO("i = " << i);
    REQUIRE(buffer->TryConsume(consume));
  }
  REQUIRE_FALSE(buffer->TryConsume(consume));

  str_buf = "last pushed";
  REQUIRE(buffer->TryPush(StreamMessage{
      .source_index{},
      .len = static_cast<uint32_t>(str_buf.size()),
      .recv_seq_num = next_recv_seq_num++,
      .recv_ts{},
      .data = str_buf.data(),
  }));

  std::vector<StreamMessage> flushed_messages;
  buffer->FlushAll([&](const StreamMessage& msg) {
    REQUIRE(msg.recv_seq_num == next_expected_recv_seq_num);
    ++next_expected_recv_seq_num;
    flushed_messages.push_back(msg);
  });

  CHECK(flushed_messages.size() == SpscBuffers::buffer_capacity + 1);
  CHECK(flushed_messages.front().raw_message() == "x");
  CHECK(flushed_messages.back().raw_message() == "last pushed");
}

// =======================================================================================
// JsonWriter TESTS
// =======================================================================================

TEST_CASE("JsonWriter - Push and Flush with Compression", "[JsonWriter]") {
  // Create a temporary directory for I/O
  const std::string temp_dir = "temp_writer_test";
  fs::create_directories(temp_dir);

  // Setup Writer
  const auto now = std::chrono::system_clock::now();

  JsonWriter writer(temp_dir, now);

  std::string str_buf;

  // 1. Push messages
  str_buf = R"({"field": "value1"})";
  const StreamMessage msg1{
      .source_index = 1,
      .len = static_cast<uint32_t>(str_buf.size()),
      .recv_seq_num = 100,
      .recv_ts = 1000,
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

  writer.Push(msg1);
  writer.Push(msg2);

  // 2. Flush
  writer.Flush(now);

  // Check file existence and content
  // Filename format: {dir}/{%Y%m%d-%H%M%S}.jsonl
  std::string expected_filename;
  fmt::format_to(std::back_inserter(expected_filename),
                 "{}/{:%Y%m%d-%H%M%S}.jsonl", temp_dir, now);

  // Read file content
  std::ifstream file(expected_filename);
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

  // Cleanup
  fs::remove_all(temp_dir);
}

}  // namespace tech
