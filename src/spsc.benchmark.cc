#include "tech/spsc.h"

#include <fmt/base.h>

#include <atomic>
#include <chrono>
#include <numeric>
#include <thread>

int main() {
  using namespace tech;  // NOLINT

  constexpr size_t kNumMessages = 10'000'000;
  constexpr size_t kMessageSize = 180;  // from logs
  constexpr int kNumWarmupIterations = 2;
  constexpr int kNumBenchmarkIterations = 10;
  constexpr auto kTotalIterations =
      kNumWarmupIterations + kNumBenchmarkIterations;

  std::array<char, kMessageSize> dummy_data{};
  std::iota(dummy_data.begin(), dummy_data.end(), 0);  // NOLINT

  fmt::print("--- SPSC Queue Throughput Benchmark ---\n");
  fmt::print("Messages to send: {}\n", kNumMessages);
  fmt::print("Message size: {} bytes\n", kMessageSize);
  fmt::print("Queue Capacity: {} MB\n", SpscQueue::kCapacity / 1024 / 1024);

  std::array<double, kNumBenchmarkIterations> latencies_ms{};

  fmt::print("\nStarting...\n");
  for (int i = 0; i < kTotalIterations; ++i) {
    auto queue = std::make_unique<SpscQueue>();

    std::atomic<bool> producer_started = false;
    std::atomic<bool> consumer_started = false;
    std::atomic<bool> consumer_done = false;
    std::jthread consumer{[&] {
      consumer_started.store(true, std::memory_order_release);
      while (!producer_started.load(std::memory_order_acquire)) {
        ;  // busy-wait
      }

      size_t consumed_count = 0;
      while (consumed_count < kNumMessages) {
        std::ignore = queue->TryConsume([&](const StreamMessage& msg) {
          std::ignore = msg;
          consumed_count++;
        });
      }

      consumer_done.store(true, std::memory_order_release);
    }};

    while (!consumer_started.load(std::memory_order_acquire)) {
      ;
    }
    producer_started.store(true, std::memory_order_release);
    const auto start_time = std::chrono::steady_clock::now();
    size_t n_blocked = 0;

    for (size_t i = 0; i < kNumMessages; ++i) {
      const StreamMessage msg{
          .source_index{},
          .len = kMessageSize,
          .recv_seq_num = static_cast<int64_t>(i),
          .recv_ts{},
          .data = dummy_data.data(),
      };

      if (queue->TryPush(msg)) [[likely]] {  // success
        continue;
      }
      ++n_blocked;
      while (!queue->TryPush(msg)) {
        ;  // busy-wait for consumer to pop
      }
    }

    while (!consumer_done.load(std::memory_order_acquire)) {
      ;  // busy-wait
    }

    const auto end_time = std::chrono::steady_clock::now();
    const auto duration =
        std::chrono::duration<double, std::milli>{end_time - start_time};

    if (i < kNumWarmupIterations) {
      fmt::print("Warmup {}: {:.2f} ms\n", i + 1, duration.count());
    } else {
      const auto bm_idx = i - kNumWarmupIterations;
      latencies_ms[bm_idx] = duration.count();
      fmt::print("Benchmark {}: {:.2f} ms\n", bm_idx + 1, duration.count());
    }

    if (n_blocked > 0) {
      fmt::print("Prouducer blocked {:} times\n", n_blocked);
    }
  }

  const auto total_duration_ms =
      std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
  const auto average_duration_ms = total_duration_ms / kNumBenchmarkIterations;
  const auto messages_per_second =
      (static_cast<double>(kNumMessages) / average_duration_ms) * 1000.0;
  const auto gigabytes_per_second =
      (static_cast<double>(kNumMessages) * kMessageSize) /
      (average_duration_ms / 1'000.0) / (1024.0 * 1024.0 * 1024.0);

  fmt::print("\n--- Benchmark Results ---\n");
  fmt::print("Average time per run: {:.2f} ms\n", average_duration_ms);
  fmt::print("Throughput: {:.2f} million msg/sec\n",
             messages_per_second / 1'000'000.0);
  fmt::print("Data Throughput: {:.2f} GB/s\n", gigabytes_per_second);
  return 0;
}
