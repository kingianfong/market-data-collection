
#include "tech/spsc.h"

#include <atomic>
#include <catch2/benchmark/catch_benchmark.hpp>
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <thread>

namespace tech {

TEST_CASE("SpscQueue - Single Thread Push/Pop", "[!benchmark][SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();
  std::string payload(100, 'x');

  BENCHMARK("Push and Pop") {
    StreamMessage msg{
        .source_index{},
        .len = static_cast<uint32_t>(payload.size()),
        .recv_seq_num = 0,
        .recv_ts = 0,
        .data = payload.data(),
    };

    std::ignore = queue->TryPush(msg);
    return queue->TryConsume([](const auto&) {});
  };
}

TEST_CASE("SpscQueue - Variable Payload Sizes", "[!benchmark][SpscQueue]") {
  auto queue = std::make_unique<SpscQueue>();

  std::vector<std::string> payloads = {
      std::string(10, 'a'),
      std::string(100, 'b'),
      std::string(1000, 'c'),
  };

  BENCHMARK("Small (10B)") {
    StreamMessage msg{.source_index{},
                      .len = 10,
                      .recv_seq_num = 0,
                      .recv_ts = 0,
                      .data = payloads[0].data()};
    std::ignore = queue->TryPush(msg);
    return queue->TryConsume([](const auto&) {});
  };

  BENCHMARK("Medium (100B)") {
    StreamMessage msg{.source_index{},
                      .len = 100,
                      .recv_seq_num = 0,
                      .recv_ts = 0,
                      .data = payloads[1].data()};
    std::ignore = queue->TryPush(msg);
    return queue->TryConsume([](const auto&) {});
  };

  BENCHMARK("Large (1KB)") {
    StreamMessage msg{.source_index{},
                      .len = 1000,
                      .recv_seq_num = 0,
                      .recv_ts = 0,
                      .data = payloads[2].data()};
    std::ignore = queue->TryPush(msg);
    return queue->TryConsume([](const auto&) {});
  };
}

TEST_CASE("SpscQueue - Throughput", "[!benchmark][SpscQueue]") {
  constexpr size_t kMessages = 100'000;
  std::string payload(100, 'x');

  auto queue = std::make_unique<SpscQueue>();
  std::atomic<bool> done{false};
  std::atomic<size_t> consumed{0};
  std::atomic<bool> is_producing{false};

  // Start consumer thread once, outside benchmark
  std::thread consumer([&]() {
    const auto fn = [&](const auto&) { consumed.fetch_add(1); };
    while (!done.load(std::memory_order_acquire)) {
      while (is_producing.load(std::memory_order_acquire)) {
        while (queue->TryConsume(fn)) {
          ;
        }
      }
    }
    // Final drain
    while (queue->TryConsume(fn)) {
      ;
    }
  });

  BENCHMARK("Producer-Consumer Throughput") {
    consumed.store(0, std::memory_order_release);
    is_producing.store(true, std::memory_order_release);

    for (size_t i = 0; i < kMessages; ++i) {
      StreamMessage msg{
          .source_index{},
          .len = static_cast<uint32_t>(payload.size()),
          .recv_seq_num = static_cast<int64_t>(i),
          .recv_ts = 0,
          .data = payload.data(),
      };

      while (!queue->TryPush(msg)) {
        ;
      }
    }

    // Wait for consumer to catch up
    while (consumed.load(std::memory_order_acquire) < kMessages) {
    }

    is_producing.store(false, std::memory_order_release);
    return consumed.load();
  };

  done.store(true, std::memory_order_release);
  consumer.join();
}

}  // namespace tech