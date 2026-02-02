#pragma once

#include <fcntl.h>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <glaze/glaze.hpp>
#include <iterator>
#include <string>

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

class JsonWriter {
 public:
  explicit JsonWriter(std::string od, const size_t max_buffered_messages)
      : out_dir_{std::move(od)}, max_buffered_messages_{max_buffered_messages} {
    std::filesystem::create_directories(out_dir_);
    line_.reserve(2 * kAverageMessageLength);
    file_buffer_.reserve(2 * kAverageMessageLength * max_buffered_messages);
  }

  JsonWriter(const JsonWriter&) = delete;
  JsonWriter(JsonWriter&&) = delete;
  JsonWriter& operator=(const JsonWriter&) = delete;
  JsonWriter& operator=(JsonWriter&&) = delete;

  ~JsonWriter() {
    if (n_buffered_messages_ > 0) {
      Flush();
    }
  }

  void Push(const StreamMessage& msg) {
    if (file_name_.empty()) [[unlikely]] {  // create new file
      assert(file_buffer_.empty());
      assert(n_buffered_messages_ == 0);

      const auto now = std::chrono::system_clock::time_point{} +
                       std::chrono::nanoseconds{msg.recv_ts};
      fmt::format_to(std::back_inserter(file_name_),
                     "{}/{:%Y%m%d-%H%M%S}.jsonl", out_dir_, now);
    }

    line_.clear();
    if (const auto ec = glz::write<glz::opts{}>(msg, line_)) [[unlikely]] {
      LOG_ERROR(logger_, "{}", glz::format_error(ec, line_));
      return;
    }
    file_buffer_.append(line_);
    file_buffer_.push_back('\n');

    if (++n_buffered_messages_ >= max_buffered_messages_) [[unlikely]] {
      const auto total_msg_len =  // subtract newlines
          static_cast<double>(file_buffer_.size() - n_buffered_messages_);
      const auto avg_msg_len =
          total_msg_len / static_cast<double>(n_buffered_messages_);
      LOG_INFO(logger_, "flushing {} msgs, avg_msg_len = {:.2f}",
               n_buffered_messages_, avg_msg_len);
      Flush();
    }
  }

 private:
  void Flush() {
    const auto fd =  // NOLINTNEXTLINE(*-vararg)
        ::open(file_name_.data(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
      const auto e = errno;
      LOG_CRITICAL(logger_, "file_name_ = {}, errno = {}, msg = {}", file_name_,
                   e, strerror(e));  // NOLINT(concurrency-*)
      return;
    }

    const auto written = ::write(fd, file_buffer_.c_str(), file_buffer_.size());
    if (written != std::ssize(file_buffer_)) [[unlikely]] {
      LOG_ERROR(logger_, "file_name_ = {}, written = {}", file_name_, written);
    }

    ::close(fd);
    file_name_.clear();
    file_buffer_.clear();
    n_buffered_messages_ = 0;
  }

  std::string out_dir_{};
  std::string file_name_{};
  std::string line_{};
  std::string file_buffer_{};
  const size_t max_buffered_messages_;
  size_t n_buffered_messages_ = 0;
  LoggerPtr logger_ = spdlog::default_logger();
};

}  // namespace tech
