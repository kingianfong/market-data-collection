#pragma once

#include <fcntl.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <unistd.h>

#include <atomic>
#include <bit>
#include <boost/container/static_vector.hpp>
#include <chrono>
#include <glaze/glaze.hpp>
#include <iterator>
#include <string>
#include <type_traits>

#include "tech/logging.h"
#include "tech/types.h"

template <>
struct glz::meta<tech::StreamMessage> {
  using T = tech::StreamMessage;

  static constexpr auto value =
      object("src", &T::source_index,                      //
             "seq", &T::recv_seq_num,                      //
             "ts", &T::recv_ts,                            //
             "raw", glz::custom<nullptr, &T::raw_message>  //
      );
};

namespace tech {

constexpr size_t kAverageMessageLength = 180;  // from logs
constexpr size_t kMaxBufferedMessages = 256ULL * 1024;

// intended to minimise producer push latency,
// consumer write performance is secondary
class SpscBuffers {
 public:
  static constexpr size_t n_buffers = 4;
  static constexpr int64_t buffer_capacity = kMaxBufferedMessages;
  static constexpr int64_t buffer_capacity_bytes =
      buffer_capacity * kAverageMessageLength;

  static_assert(buffer_capacity_bytes <= std::numeric_limits<uint32_t>::max());

 private:
  struct Header {
    uint32_t len;
    SourceIndex src_idx;
    int64_t seq;
    int64_t ts;
  };

  static_assert(std::is_trivial_v<Header>);

  struct Buffer {
    boost::container::static_vector<Header, buffer_capacity> headers;
    boost::container::static_vector<char, buffer_capacity_bytes> bodies;

    void Clear() noexcept {
      headers.clear();
      bodies.clear();
    }

    [[nodiscard]] bool Empty() const noexcept {
      assert(headers.empty() == bodies.empty());
      return headers.empty();
    }
  };

 public:
  SpscBuffers() = default;

  SpscBuffers(const SpscBuffers&) = delete;
  SpscBuffers(SpscBuffers&&) = delete;
  SpscBuffers& operator=(const SpscBuffers&) = delete;
  SpscBuffers& operator=(SpscBuffers&&) = delete;

  ~SpscBuffers() = default;

  [[nodiscard]] bool TryPush(const StreamMessage msg) noexcept {
    const auto front_has_capacity =
        buffers_[producer_idx_].headers.size() < buffer_capacity &&
        buffers_[producer_idx_].bodies.size() + msg.len <=
            buffer_capacity_bytes;

    const auto push_front = [&] {
      auto& [headers, bodies] = buffers_[producer_idx_];
      headers.push_back(Header{
          .len = msg.len,
          .src_idx = msg.source_index,
          .seq = msg.recv_seq_num,
          .ts = msg.recv_ts,
      });
      bodies.insert(bodies.end(), msg.data, msg.data + msg.len);
    };

    if (front_has_capacity) [[likely]] {
      push_front();
      return true;
    }

    // use `n_buffers - 1` instead of `n_buffers` to prevent race condition from
    // current buffer being consumed
    if (n_consumable_buffers_.load(std::memory_order_acquire) == n_buffers - 1)
        [[unlikely]] {
      return false;
    }

    // use *_release instead of *_relax to prevent race condition with consumer
    n_consumable_buffers_.fetch_add(1, std::memory_order_release);
    producer_idx_ = Increment(producer_idx_);
    assert(buffers_[producer_idx_].Empty());
    push_front();
    return true;
  }

  template <typename Fn>
  [[nodiscard]] bool TryConsume(const Fn& fn) {
    if (n_consumable_buffers_.load(std::memory_order_acquire) == 0) {
      return false;
    }

    assert(!buffers_[consumer_idx_].Empty());

    auto& [headers, bodies] = buffers_[consumer_idx_];
    LOG_INFO(logger_,
             "SpscBuffers::TryConsume: n_messages = {}, n_bytes = {}, "
             "avg_msg_len = {:.2f}",
             headers.size(), bodies.size(),
             static_cast<double>(bodies.size()) /
                 static_cast<double>(headers.size()));

    uint32_t offset = 0;
    for (const auto& hdr : headers) {
      fn(StreamMessage{
          .source_index = hdr.src_idx,
          .len = hdr.len,
          .recv_seq_num = hdr.seq,
          .recv_ts = hdr.ts,
          .data = bodies.data() + offset,
      });
      offset += hdr.len;
    }

    buffers_[consumer_idx_].Clear();
    consumer_idx_ = Increment(consumer_idx_);
    n_consumable_buffers_.fetch_sub(1, std::memory_order_release);
    return true;
  }

  // call only after producer stopped
  template <typename Fn>
  void FlushAll(const Fn& fn) {
    const auto flush_buffer = [&fn](Buffer& b) {
      uint32_t offset = 0;
      for (const auto& hdr : b.headers) {
        fn(StreamMessage{
            .source_index = hdr.src_idx,
            .len = hdr.len,
            .recv_seq_num = hdr.seq,
            .recv_ts = hdr.ts,
            .data = b.bodies.data() + offset,
        });
        offset += hdr.len;
      }
      b.Clear();
    };

    while (n_consumable_buffers_ > 0) {
      flush_buffer(buffers_[consumer_idx_]);
      consumer_idx_ = Increment(consumer_idx_);
      --n_consumable_buffers_;
    }

    assert(consumer_idx_ == producer_idx_);
    flush_buffer(buffers_[producer_idx_]);
  }

 private:
  [[nodiscard]] static size_t Increment(const size_t index) noexcept {
    static_assert(std::popcount(n_buffers) == 1);
    return (index + 1) & (n_buffers - 1);
  }

  alignas(128) size_t producer_idx_ = 0;                       // producer only
  alignas(128) std::atomic<size_t> n_consumable_buffers_ = 0;  // both threads
  alignas(128) size_t consumer_idx_ = 0;                       // consumer only
  LoggerPtr logger_ = GetDefaultLogger();                      // consumer only
  std::array<Buffer, n_buffers> buffers_;                      // both threads
};

using TimePoint = std::chrono::system_clock::time_point;

template <typename T>
concept BatchWriter = requires(T writer) {
  { T{std::string{}, TimePoint{}} };
  { writer.Push(StreamMessage{}) };
  { writer.Flush(TimePoint{}) };
};

static constexpr int zstd_compression_level = 19;  // >=20 uses more memory

class JsonWriter {
 public:
  explicit JsonWriter(std::string od, const TimePoint now)
      : out_dir_{std::move(od)} {
    std::filesystem::create_directories(out_dir_);
    line_.reserve(2 * kAverageMessageLength);
    SetNextFilename(now);
    CreateNewFile();
  }

  void Push(const StreamMessage& msg) {
    if (!fd_.has_value()) [[unlikely]] {
      CreateNewFile();
    }

    if (const auto ec = glz::write<glz::opts{}>(msg, line_)) [[unlikely]] {
      LOG_ERROR(logger_, "{}", glz::format_error(ec, line_));
      return;
    }
    line_.push_back('\n');

    const auto written = ::write(*fd_, line_.data(), line_.size());
    if (written != std::ssize(line_)) [[unlikely]] {
      LOG_ERROR(logger_, "written = {}, line_.size(): {}, line_: {}", written,
                line_.size(), line_);
    }
  }

  void Flush(const TimePoint now) {
    assert(fd_.has_value() && "file not created yet");

    ::close(*fd_);
    fd_.reset();
    SetNextFilename(now);
  }

 private:
  void SetNextFilename(const TimePoint now) {
    assert(!fd_.has_value() && "file already created");

    filename_.clear();
    fmt::format_to(std::back_inserter(filename_), "{}/{:%Y%m%d-%H%M%S}.jsonl",
                   out_dir_, now);
  }

  void CreateNewFile() {
    assert(!fd_.has_value() && "file already created");

    // NOLINTNEXTLINE(*-vararg)
    fd_.emplace(::open(filename_.data(), O_WRONLY | O_CREAT | O_TRUNC, 0644));
    if (*fd_ < 0) {
      LOG_CRITICAL(logger_, "filename_ = {}, fd = {}", filename_, *fd_);
    }
  }

  std::string out_dir_{};
  std::string line_{};
  std::string filename_{};
  std::optional<int> fd_{};
  LoggerPtr logger_ = spdlog::default_logger();
};

static_assert(BatchWriter<JsonWriter>);

}  // namespace tech
