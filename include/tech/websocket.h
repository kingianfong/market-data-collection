#pragma once

#include <spdlog/spdlog.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/cobalt/promise.hpp>
#include <boost/cobalt/task.hpp>
#include <concepts>
#include <deque>
#include <functional>
#include <memory_resource>
#include <optional>
#include <thread>

#include "tech/defer.h"
#include "tech/logging.h"
#include "tech/types.h"

namespace tech::websocket {

namespace beast = boost::beast;
namespace cobalt = boost::cobalt;
namespace net = boost::asio;
namespace ssl = net::ssl;

using net::ip::tcp;

using WebsocketStream = beast::websocket::stream<ssl::stream<tcp::socket>>;
using ErrorCode = boost::system::error_code;

// unsynchronised
class LoggingMemoryResource final : public std::pmr::memory_resource {
 public:
  explicit LoggingMemoryResource(std::string name,
                                 spdlog::level::level_enum level,
                                 std::pmr::memory_resource* upstream)
      : name_{std::move(name)}, level_{level}, upstream_{upstream} {}

  LoggingMemoryResource(const LoggingMemoryResource&) = delete;
  LoggingMemoryResource(LoggingMemoryResource&&) = delete;
  LoggingMemoryResource& operator=(const LoggingMemoryResource&) = delete;
  LoggingMemoryResource& operator=(LoggingMemoryResource&&) = delete;

  ~LoggingMemoryResource() override {
    LOG_WARNING(logger_, "~LoggingMemoryResource() {}", name_);
  }

 protected:
  void* do_allocate(const size_t bytes, const size_t alignment) override {
    SPDLOG_LOGGER_CALL(logger_, level_,
                       "[{:12}] allocating bytes = {} align = {}", name_, bytes,
                       alignment);
    void* p = upstream_->allocate(bytes, alignment);
    SPDLOG_LOGGER_CALL(logger_, level_,
                       "[{:12}] allocated  ptr = {} bytes = {} align = {}",
                       name_, p, bytes, alignment);
    return p;
  }

  void do_deallocate(void* p, const size_t bytes,
                     const size_t alignment) override {
    SPDLOG_LOGGER_CALL(logger_, level_,
                       "[{:12}] deallocate ptr = {} bytes = {} align = {}",
                       name_, p, bytes, alignment);
    upstream_->deallocate(p, bytes, alignment);
  }

  bool do_is_equal(
      const std::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }

 private:
  std::string name_;
  spdlog::level::level_enum level_;
  std::pmr::memory_resource* upstream_;
  LoggerPtr logger_ = GetDefaultLogger();
};

class PmrContext {
 public:
  static std::unique_ptr<PmrContext> Create(
      std::pmr::memory_resource* upstream = std::pmr::new_delete_resource()) {
    auto* ptr = new PmrContext{upstream};
    return std::unique_ptr<PmrContext>{ptr};
  }

  PmrContext(const PmrContext&) = delete;
  PmrContext(PmrContext&&) = delete;
  PmrContext& operator=(const PmrContext&) = delete;
  PmrContext& operator=(PmrContext&&) = delete;

  ~PmrContext() = default;

  auto* memory_resource() { return &to_pool_; }

 private:
  static constexpr size_t capacity = 32ULL * 1024 * 1024;

  explicit PmrContext(std::pmr::memory_resource* upstream)
      : to_upstream_{"to_upstream", spdlog::level::err, upstream},
        to_mono_{"to_mono", spdlog::level::warn, &mono_},
        to_pool_{"to_pool", spdlog::level::debug, &pool_} {}

  std::array<std::byte, capacity> buf_{};
  LoggingMemoryResource to_upstream_;
  std::pmr::monotonic_buffer_resource mono_{buf_.data(), capacity,
                                            &to_upstream_};
  LoggingMemoryResource to_mono_;
  std::pmr::unsynchronized_pool_resource pool_{&to_mono_};
  LoggingMemoryResource to_pool_;
};

struct Endpoint {
  std::string host;
  std::string port;
  std::string target;
};

ssl::context GetSslContext();

using WsInitResult = std::tuple<ErrorCode, std::optional<WebsocketStream>>;

struct StreamContext {
  SourceIndex source_index = -1;
  MessageTypeIndex msg_type = -1;
  std::optional<std::string> sym{};
  Endpoint endpoint;
  beast::flat_buffer buffer{};
  std::optional<WebsocketStream> ws_stream{std::nullopt};
};

struct WsMessage {
  int64_t seq;
  int64_t ts;
  std::reference_wrapper<const StreamContext> ws_ctx;
  ErrorCode error_code;
  std::string_view raw_message;
};

static_assert(std::is_trivially_copyable_v<WsMessage>);

struct RunContext {
  // main thread only
  std::unique_ptr<PmrContext> pmr_ctx{PmrContext::Create()};
  net::io_context ioc{BOOST_ASIO_CONCURRENCY_HINT_1};
  ssl::context ssl_ctx{GetSslContext()};
  SourceIndex next_source_index = 0;
  int64_t next_recv_seq_num = 0;

  tcp::resolver resolver{ioc};
  tcp::resolver::results_type tcp_eps{};
  tcp::endpoint tcp_ep{};
  // std::deque for reference stability
  std::deque<StreamContext> stream_contexts{};

  const std::thread::id network_thread_id = std::this_thread::get_id();

  // possibly shared across threads
  alignas(128) std::atomic<bool> stop_requested = false;
  alignas(128) std::atomic<int64_t> active_streams = 0;
};

// dynamically allocates on dns resolution
// need std::optional to destroy and reconstruct on restarts
// TODO: consider DNS cache
cobalt::task<ErrorCode> InitWebsocketStream(RunContext& run_ctx,
                                            StreamContext& stream_ctx);

cobalt::task<void> WaitForSignals(RunContext& run_ctx);

template <typename Fn>
  requires std::invocable<Fn, WsMessage>
inline cobalt::task<void> RunWebsocket(const Fn& fn, RunContext& run_ctx,
                                       StreamContext& stream_ctx) {
  assert(std::this_thread::get_id() == run_ctx.network_thread_id);

  auto logger = spdlog::default_logger();
  auto& ws_opt = stream_ctx.ws_stream;
  const auto& target = stream_ctx.endpoint.target;

  const auto cleanup = Defer{[&run_ctx, &target, logger] {
    if (--run_ctx.active_streams == 0) {
      run_ctx.ioc.stop();  // this was the last task
    }
    LOG_INFO(logger, "{} run_ctx.active_streams = {}", target,
             run_ctx.active_streams.load());
  }};

  auto& buffer = stream_ctx.buffer;
  buffer.reserve(4ULL * 1024);

  while (!run_ctx.stop_requested.load(std::memory_order_relaxed)) {
    {
      static constexpr auto max_attempts = 3;
      auto attempt_num = 1;

      for (; attempt_num <= max_attempts; ++attempt_num) {
        if (const auto ec = co_await InitWebsocketStream(run_ctx, stream_ctx)) {
          LOG_ERROR(logger, "{} InitWebsocketStream: {} {}", target,
                    ec.to_string(), ec.message());
        } else {
          break;
        }
      }

      if (attempt_num > max_attempts) {
        LOG_ERROR(logger, "{} InitWebsocketStream failed after {} attempts",
                  target, max_attempts);
        co_return;
      }
    }

    assert(ws_opt.has_value());
    LOG_DEBUG(logger, "{} starting inner loop", target);

    while (!run_ctx.stop_requested.load(std::memory_order_relaxed)) {
      buffer.clear();

      const auto old_cap = buffer.capacity();
      const auto [error_code, bytes] =
          co_await ws_opt->async_read(buffer, net::as_tuple(cobalt::use_op));

      const auto now = std::chrono::system_clock::now();  // TODO: optimal?
      const auto from_epoch = std::chrono::nanoseconds{now.time_since_epoch()};
      const auto recv_ts = from_epoch.count();

      if (const auto new_cap = buffer.capacity(); old_cap != new_cap) {
        LOG_WARNING(logger, "{} capacity {} -> {}", target, old_cap, new_cap);
      }

      fn(WsMessage{
          .seq = run_ctx.next_recv_seq_num++,
          .ts = recv_ts,
          .ws_ctx = stream_ctx,
          .error_code = error_code,
          .raw_message{static_cast<const char*>(buffer.cdata().data()), bytes},
      });

      if (error_code) [[unlikely]] {
        break;
      }
    }

    LOG_WARNING(logger, "{} stopped inner loop", target);
  }

  LOG_WARNING(logger, "{} stopped outer loop", target);
}

}  // namespace tech::websocket
