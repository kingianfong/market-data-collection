#include "tech/spsc.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <numeric>
#include <thread>

int main() {
  using namespace tech;  // NOLINT

  constexpr size_t kNumMessages = 10'000'000;
  constexpr size_t kMessageSize = 256;
  constexpr int kNumWarmupIterations = 2;
  constexpr int kNumBenchmarkIterations = 5;
  constexpr auto kTotalIterations =
      kNumWarmupIterations + kNumBenchmarkIterations;

  std::array<char, kMessageSize> dummy_data{};
  std::ranges::iota(dummy_data, 0);

  std::cout << "--- SPSC Queue Throughput Benchmark ---\n";
  std::cout << "Messages to send: " << kNumMessages << '\n';
  std::cout << "Message size: " << kMessageSize << " bytes\n";
  std::cout << "Queue Capacity: " << SpscQueue::kCapacity / 1024 / 1024
            << " MB\n";

  std::array<double, kTotalIterations> latencies_ms{};

  std::cout << "\nStarting...\n";
  for (int i = 0; i < kTotalIterations; ++i) {
    SpscQueue queue{};

    std::atomic<bool> producer_started = false;
    std::atomic<bool> consumer_started = false;
    std::jthread consumer{[&] {
      consumer_started.store(true, std::memory_order_release);
      while (!producer_started.load(std::memory_order_acquire)) {
        ;
      }

      size_t consumed_count = 0;
      while (consumed_count < kNumMessages) {
        std::ignore = queue.TryConsume([&](const StreamMessage& msg) {
          std::ignore = msg;
          consumed_count++;
        });
      }
    }};

    while (!consumer_started.load(std::memory_order_acquire)) {
      ;
    }
    producer_started.store(true, std::memory_order_release);
    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < kNumMessages; ++i) {
      StreamMessage msg{
          .source_index = 0,
          .len = kMessageSize,
          .recv_seq_num = static_cast<int64_t>(i),
          .recv_ts = std::chrono::high_resolution_clock::now()
                         .time_since_epoch()
                         .count(),
          .data = dummy_data.data(),
      };

      while (!queue.TryPush(msg)) {
        ;  // busy-wait
      }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> duration = end_time - start_time;
    if (i < kNumWarmupIterations) {
      std::cout << "Warmup " << (i + 1) << ": " << duration.count() << " ms\n";
    } else {
      latencies_ms[i] = duration.count();
      std::cout << "Benchmark " << (i + 1) << ": " << duration.count()
                << " ms\n";
    }
  }

  const auto total_duration_ms =
      std::accumulate(latencies_ms.begin(), latencies_ms.end(), 0.0);
  double average_duration_ms = total_duration_ms / kNumBenchmarkIterations;
  double messages_per_second =
      (static_cast<double>(kNumMessages) / average_duration_ms) * 1000.0;
  double gigabytes_per_second =
      (static_cast<double>(kNumMessages) * kMessageSize) /
      (average_duration_ms / 1000.0) / (1024.0 * 1024.0 * 1024.0);

  std::cout << "\n--- Benchmark Results ---\n";
  std::cout << "Average time per run: " << average_duration_ms << " ms\n";
  std::cout << "Throughput: " << messages_per_second / 1000000.0
            << " million messages/second\n";
  std::cout << "Data Throughput: " << gigabytes_per_second << " GB/s\n";

  return 0;
}
