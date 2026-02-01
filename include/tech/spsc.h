#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cassert>
#include <cstring>
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
  static constexpr size_t kAlign = 128;
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
        curr_idx + write_len > effective_capacity) {
      auto& header = GetHeader(write_bytes_local);
      header.is_padding_for_wrap = true;
      write_bytes_local = RoundToNext<kCapacity>(write_bytes_local);
    }

    auto& header = GetHeader(write_bytes_local);
    header = Header{
        .msg_len = msg_len,
        .src_idx = msg.source_index,
        .seq = msg.recv_seq_num,
        .ts = msg.recv_ts,
    };

    auto* data = GetRawMessageStart(write_bytes_local);
    std::memcpy(data, msg.data, msg_len);

    write_bytes_.store(write_bytes_local + write_len,
                       std::memory_order_release);
    return true;
  }

  template <typename Fn>
  bool TryConsume(const Fn& fn) {
    auto read_bytes_local = read_bytes_.load(std::memory_order_relaxed);
    if (read_bytes_local == cached_write_bytes_) [[unlikely]] {
      cached_write_bytes_ = write_bytes_.load(std::memory_order_acquire);
    }
    if (read_bytes_local == cached_write_bytes_) [[unlikely]] {
      return false;
    }
    if (GetHeader(read_bytes_local).is_padding_for_wrap) [[unlikely]] {
      read_bytes_local = RoundToNext<kCapacity>(read_bytes_local);
    }

    const auto& header = GetHeader(read_bytes_local);
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

  Header& GetHeader(const size_t bytes) {
    const auto index = bytes % kCapacity;
    assert(index + sizeof(Header) <= kCapacity &&
           "overflows after including header size");

    // technically UB, use this instead when available:
    // https://en.cppreference.com/w/cpp/memory/start_lifetime_as.html
    auto* header = reinterpret_cast<Header*>(buffer_.data() + index);
    [[maybe_unused]] const auto ptr_val = reinterpret_cast<size_t>(header);
    assert(ptr_val % kAlign == 0);
    return *header;
  }

  char* GetRawMessageStart(const size_t bytes) {
    const auto index = (bytes % kCapacity) + sizeof(Header);
    assert(index + sizeof(Header) <= kCapacity &&
           "overflows after including header size");

    [[maybe_unused]] auto& header = GetHeader(bytes);
    assert(index + header.msg_len <= kCapacity &&
           "overflows after including message size");
    // technically UB, use this instead when available:
    // https://en.cppreference.com/w/cpp/memory/start_lifetime_as.html
    return reinterpret_cast<char*>(buffer_.data() + index);
  }

  alignas(kAlign) std::atomic<size_t> write_bytes_ = 0;  // both threads
  alignas(kAlign) size_t cached_read_bytes_ = 0;         // producer

  alignas(kAlign) std::atomic<size_t> read_bytes_ = 0;  // both threads
  alignas(kAlign) size_t cached_write_bytes_ = 0;       // consumer

  alignas(kAlign) std::array<std::byte, kCapacity> buffer_{};  // both threads
};

}  // namespace tech
