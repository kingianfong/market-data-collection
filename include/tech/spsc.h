#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstring>
#include <new>
#include <type_traits>

#include "tech/types.h"

namespace tech {

class SpscQueue {
 private:
  struct Header {
    uint32_t msg_len = 0;
    bool is_padding_for_wrap = false;
    SourceIndex src_idx = -1;
    int64_t seq = -1;
    int64_t ts = -1;
  };

 public:
  static constexpr size_t kAlign = 8;
  static constexpr size_t kCapacity = 2Z * 1024 * 1024;
  static_assert(std::popcount(kAlign) == 1);
  static_assert(std::popcount(kCapacity) == 1);
  static_assert(std::is_trivially_copyable_v<Header>);

  SpscQueue() = default;

  SpscQueue(const SpscQueue&) = delete;
  SpscQueue(SpscQueue&&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;
  SpscQueue& operator=(SpscQueue&&) = delete;

  ~SpscQueue() = default;

  [[nodiscard]] bool TryPush(const StreamMessage msg) noexcept {
    // need space for padding header if there's a wrap
    static constexpr auto effective_capacity = kCapacity - sizeof(Header);

    const auto msg_len = static_cast<uint32_t>(msg.raw_message().size());
    const auto write_len = RoundToNext<kAlign>(sizeof(Header) + msg_len);

    // check if can write
    auto write_bytes_local = write_bytes_.load(std::memory_order_relaxed);
    if (const auto used = write_bytes_local - cached_read_bytes_;
        used + write_len > effective_capacity) [[unlikely]] {
      cached_read_bytes_ = read_bytes_.load(std::memory_order_acquire);
    }
    if (const auto used = write_bytes_local - cached_read_bytes_;
        used + write_len > effective_capacity) [[unlikely]] {
      return false;
    }

    // check if need to wrap
    if (const auto curr_idx = write_bytes_local % kCapacity;
        curr_idx + write_len > effective_capacity) [[unlikely]] {
      WriteHeader(write_bytes_local, {.is_padding_for_wrap = true});
      write_bytes_local = RoundToNext<kCapacity>(write_bytes_local);
    }
    WriteHeader(write_bytes_local, {.msg_len = msg_len,
                                    .src_idx = msg.source_index,
                                    .seq = msg.recv_seq_num,
                                    .ts = msg.recv_ts});

    auto* data = GetRawMessageStart(write_bytes_local);
    std::memcpy(data, msg.data, msg_len);

    write_bytes_.store(write_bytes_local + write_len,
                       std::memory_order_release);
    return true;
  }

  template <typename Fn>
  [[nodiscard]] bool TryConsume(const Fn& fn) {
    auto read_bytes_local = read_bytes_.load(std::memory_order_relaxed);
    if (read_bytes_local == cached_write_bytes_) [[unlikely]] {
      cached_write_bytes_ = write_bytes_.load(std::memory_order_acquire);
    }
    if (read_bytes_local == cached_write_bytes_) [[unlikely]] {
      return false;
    }

    Header header{};
    ReadHeader(read_bytes_local, header);
    if (header.is_padding_for_wrap) [[unlikely]] {
      read_bytes_local = RoundToNext<kCapacity>(read_bytes_local);
      ReadHeader(read_bytes_local, header);
    }

    const auto* data = GetRawMessageStart(read_bytes_local);

    fn(StreamMessage{
        .source_index = header.src_idx,
        .len = header.msg_len,
        .recv_seq_num = header.seq,
        .recv_ts = header.ts,
        .data = data,
    });

    const auto read_len = RoundToNext<kAlign>(sizeof(header) + header.msg_len);
    const auto next_read_bytes = read_bytes_local + read_len;
    read_bytes_.store(next_read_bytes, std::memory_order_release);
    return true;
  }

 private:
  template <size_t multiple>
    requires(std::popcount(multiple) == 1)
  static constexpr size_t RoundToNext(const size_t len) {
    static constexpr size_t minus_one = multiple - 1;
    const auto result = (len + minus_one) & ~minus_one;
    assert(result % multiple == 0);
    return result;
  }

  // helper to avoid UB from reinterpret cast
  void WriteHeader(const size_t bytes, const Header& header) {
    static_assert(std::is_trivially_copyable_v<Header>);
    static_assert(sizeof(Header) <= 24);
    const auto index = bytes % kCapacity;
    assert(index + sizeof(Header) <= kCapacity);
    auto* ptr = buffer_.data() + index;
    std::memcpy(ptr, &header, sizeof(Header));
  }

  // helper to avoid UB from reinterpret cast
  void ReadHeader(const size_t bytes, Header& header) const {
    static_assert(std::is_trivially_copyable_v<Header>);
    const auto index = bytes % kCapacity;
    assert(index + sizeof(Header) <= kCapacity);
    const auto* ptr = buffer_.data() + index;
    std::memcpy(&header, ptr, sizeof(Header));
  }

  char* GetRawMessageStart(const size_t bytes) {
    const auto index = bytes % kCapacity;
    return buffer_.data() + index + sizeof(Header);
  }

#ifdef __cpp_lib_hardware_interference_size
  static constexpr size_t kCache = std::hardware_destructive_interference_size;
#else
  static constexpr size_t kCache = 128;
#endif

  alignas(kCache) std::atomic<size_t> write_bytes_ = 0;   // both threads
  alignas(kCache) size_t cached_read_bytes_ = 0;          // producer
  alignas(kCache) std::atomic<size_t> read_bytes_ = 0;    // both threads
  alignas(kCache) size_t cached_write_bytes_ = 0;         // consumer
  alignas(kCache) std::array<char, kCapacity> buffer_{};  // both threads
};

}  // namespace tech
