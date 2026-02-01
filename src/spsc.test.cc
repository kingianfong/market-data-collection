
#include "tech/spsc.h"

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "tech/types.h"

namespace tech {

TEST_CASE("SpscQueue - Basic Push and Pop", "[SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  std::string msg = "hello";
  const auto to_push = StreamMessage{
      .source_index{},
      .len = static_cast<uint32_t>(msg.size()),
      .recv_seq_num = 42,
      .recv_ts = 1000,
      .data = msg.data(),
  };
  REQUIRE(queue->TryPush(to_push));

  StreamMessage result{};

  const auto fn = [&](const StreamMessage& msg) { result = msg; };

  CHECK(queue->TryConsume(fn));

  CHECK(result.recv_seq_num == 42);
  CHECK(result.recv_ts == 1000);
  CHECK(result.raw_message() == "hello");

  REQUIRE_FALSE(queue->TryConsume(fn));
}

TEST_CASE("SpscQueue - Full Queue and Wraparound", "[SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  // Fill to capacity
  std::vector<StreamMessage> pushed_messages{};
  std::string payload = "x";

  const auto push_next_message = [&] -> bool {
    const auto i = pushed_messages.size();

    const auto msg = StreamMessage{
        .source_index{},
        .len = static_cast<uint32_t>(payload.size()),
        .recv_seq_num = static_cast<int64_t>(i),
        .recv_ts{},
        .data = payload.data(),
    };

    if (!queue->TryPush(msg)) {
      return false;
    }
    pushed_messages.push_back(msg);
    return true;
  };

  while (push_next_message()) {
    ;
  }
  REQUIRE_FALSE(push_next_message());
  REQUIRE(!pushed_messages.empty());

  size_t pops = 0;
  const auto fn = [&](const auto& to_consume) {
    const auto& pushed = pushed_messages.at(pops);
    CHECK(to_consume.source_index == pushed.source_index);
    CHECK(to_consume.len == pushed.len);
    CHECK(to_consume.recv_seq_num == pushed.recv_seq_num);

    const auto consume_sv = std::string_view{to_consume.data, to_consume.len};
    const auto push_sv = std::string_view{pushed.data, pushed.len};
    CHECK(consume_sv == push_sv);
    ++pops;
  };
  REQUIRE(queue->TryConsume(fn));

  // wraps around here

  REQUIRE(push_next_message());
  REQUIRE_FALSE(push_next_message());

  while (queue->TryConsume(fn)) {
    ;
  }
  REQUIRE(pops == pushed_messages.size());
  REQUIRE_FALSE(queue->TryConsume(fn));
}

TEST_CASE("SpscQueue - Variable Length Payloads", "[SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  std::vector<std::string> messages = {
      "a", "short", std::string(100, 'x'), std::string(512, 'y'),
      "",  // empty message
  };

  for (size_t i = 0; i < messages.size(); ++i) {
    INFO("i = " << i);
    REQUIRE(queue->TryPush(StreamMessage{
        .source_index{},
        .len = static_cast<uint32_t>(messages[i].size()),
        .recv_seq_num = static_cast<int64_t>(i),
        .recv_ts{},
        .data = messages[i].data(),
    }));
  }

  size_t n_consumed = 0;
  const auto fn = [&](const auto& msg) {
    const auto i = n_consumed;
    INFO("i = " << i);
    CHECK(msg.recv_seq_num == static_cast<int64_t>(i));
    CHECK(msg.len == messages[i].size());
    CHECK(msg.raw_message() == messages[i]);
    ++n_consumed;
  };

  for (size_t i = 0; i < messages.size(); ++i) {
    REQUIRE(queue->TryConsume(fn));
  }
  REQUIRE_FALSE(queue->TryConsume(fn));
}

TEST_CASE("SpscQueue - No Dangling References", "[SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  std::string reused_buffer;
  for (size_t i = 0; i < 5; ++i) {
    reused_buffer = std::to_string(i);
    REQUIRE(queue->TryPush(StreamMessage{
        .source_index{},
        .len = static_cast<uint32_t>(reused_buffer.size()),
        .recv_seq_num = static_cast<int64_t>(i),
        .recv_ts{},
        .data = reused_buffer.data(),
    }));
  }

  // Overwrite buffer before popping
  reused_buffer = "OVERWRITTEN";

  for (size_t i = 0; i < 5; ++i) {
    INFO("i = " << i);
    REQUIRE(queue->TryConsume([i](const auto& msg) {
      CHECK(msg.raw_message() == std::to_string(i));
    }));
  }
}

TEST_CASE("SpscQueue - Full Queue and Wraparound Multithreaded",
          "[SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  constexpr size_t kNumMessages = 1'000'000;
  constexpr size_t kPayloadSize = 512;

  std::atomic<bool> producer_done{false};
  std::atomic<size_t> messages_pushed{0};
  std::atomic<size_t> messages_consumed{0};

  // Producer thread
  std::thread producer([&]() {
    std::string payload(kPayloadSize, 'x');

    for (size_t i = 0; i < kNumMessages; ++i) {
      StreamMessage msg{
          .source_index{},
          .len = static_cast<uint32_t>(payload.size()),
          .recv_seq_num = static_cast<int64_t>(i),
          .recv_ts = static_cast<int64_t>(i * 1000),
          .data = payload.data(),
      };

      // Spin until we can push
      while (!queue->TryPush(msg)) {
        std::this_thread::yield();
      }

      messages_pushed.fetch_add(1, std::memory_order_release);
    }

    producer_done.store(true, std::memory_order_release);
  });

  // Consumer thread
  std::thread consumer([&]() {
    size_t expected_seq = 0;

    while (true) {
      bool consumed = queue->TryConsume([&](const StreamMessage& msg) {
        CHECK(msg.recv_seq_num == static_cast<int64_t>(expected_seq));
        CHECK(msg.recv_ts == static_cast<int64_t>(expected_seq * 1000));
        CHECK(msg.len == kPayloadSize);

        ++expected_seq;
        messages_consumed.fetch_add(1, std::memory_order_release);
      });

      if (!consumed) {
        // Check if producer is done and queue is empty
        if (producer_done.load(std::memory_order_acquire) &&
            !queue->TryConsume([](const auto&) {})) {
          break;
        }
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  REQUIRE(messages_pushed.load() == kNumMessages);
  REQUIRE(messages_consumed.load() == kNumMessages);
}

}  // namespace tech
