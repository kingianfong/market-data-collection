#include <fmt/format.h>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <boost/cobalt/spawn.hpp>
#include <chrono>
#include <exception>
#include <fstream>
#include <glaze/glaze.hpp>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tech/batch_io.h"
#include "tech/defer.h"
#include "tech/logging.h"
#include "tech/spsc.h"
#include "tech/websocket.h"

enum MessageType : tech::MessageTypeIndex {
  unset,
  book_ticker,
  trade,
};

template <>
struct glz::meta<MessageType> {
  using enum MessageType;
  static constexpr auto value = enumerate(unset, book_ticker, trade);
};

template <>
struct glz::meta<tech::websocket::StreamContext> {
  using T = tech::websocket::StreamContext;

  static constexpr auto write_msg_type = [](const T& self) {
    return static_cast<MessageType>(self.msg_type);
  };

  static constexpr auto value =
      object("source_index", &T::source_index,                  //
             "msg_type", glz::custom<nullptr, write_msg_type>,  //
             "sym", &T::sym,                                    //
             "endpoint", &T::endpoint                           //
      );
};

namespace tech::websocket {

namespace {

constexpr auto kMaxBufferedMessages = 256Z * 1024;

constexpr std::string_view GetStreamName(const MessageType msg_type) {
  switch (msg_type) {
    case MessageType::unset:
      break;  // should not happen
    case MessageType::trade:
      return std::string_view{"trade"};
    case MessageType::book_ticker:
      return std::string_view{"bookTicker"};
  }
  std::unreachable();
}

void SetupSpdlogDefaultLogger(const std::string& out_dir) {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();

  auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
      fmt::format("{}/collect_ws.log", out_dir),
      /*truncate=*/true);

  auto logger = std::make_shared<spdlog::logger>(
      "collect_ws",
      std::initializer_list<spdlog::sink_ptr>{console_sink, file_sink});
  spdlog::set_default_logger(logger);

#ifndef NDEBUG
  console_sink->set_level(spdlog::level::info);
  file_sink->set_level(spdlog::level::debug);
  logger->set_level(spdlog::level::debug);
#else
  console_sink->set_level(spdlog::level::warn);
  file_sink->set_level(spdlog::level::info);
  logger->set_level(spdlog::level::info);
#endif
}

void WriteMetadata(const std::deque<StreamContext>& stream_contexts,
                   const std::string& out_dir) {
  auto logger = spdlog::default_logger();
  LOG_INFO(logger, "writing metadata");

  const auto out_file = fmt::format("{}/sources.jsonl", out_dir);
  auto os = std::ofstream{out_file};
  auto buffer = std::string{};

  for (const auto& stream_ctx : stream_contexts) {
    if (const auto ec = glz::write_json(stream_ctx, buffer)) {
      LOG_CRITICAL(logger, "{}", glz::format_error(ec, buffer));
      std::terminate();
    }

    buffer.push_back('\n');
    os.write(buffer.data(), std::ssize(buffer));
  }

  os.flush();
  LOG_INFO(logger, "wrote metadata");
}

}  // namespace
}  // namespace tech::websocket

int main() {
  using namespace tech;             // NOLINT(*using-namespace)
  using namespace tech::websocket;  // NOLINT(*using-namespace)

  const auto start_time = std::chrono::system_clock::now();
  const auto out_dir =
      fmt::format("output/collect_ws-{:%Y%m%d-%H%M%S}/", start_time);

  SetupSpdlogDefaultLogger(out_dir);
  auto logger = spdlog::default_logger();

  auto run_ctx = RunContext{};
  cobalt::this_thread::set_executor(run_ctx.ioc.get_executor());
  cobalt::this_thread::set_default_resource(run_ctx.pmr_ctx->memory_resource());

  const auto ex_handler = [logger](const std::exception_ptr& e) {
    try {
      if (e) {
        std::rethrow_exception(e);
      }
    } catch (const std::exception& ex) {
      LOG_ERROR(logger, "ex: {}", ex.what());
    }
  };

  constexpr auto symbols = std::array{
      "btcusdt",
      "ethusdt",
  };
  constexpr auto message_types = std::array{
      MessageType::trade,
      MessageType::book_ticker,
  };

  auto queue = std::make_unique<SpscQueue>();
  const auto handle_ws_message = [&](const WsMessage& msg) {
    const auto& ws_ctx = msg.ws_ctx.get();

    if (const auto ec = msg.error_code) [[unlikely]] {
      if (ec == net::error::operation_aborted) {
        return;  // not noteworthy
      }
      LOG_WARNING(logger, "{} ec: {} {}, skipping raw message: {}",
                  ws_ctx.endpoint.target, ec.to_string(), ec.message(),
                  msg.raw_message);
      return;
    }

    const auto to_push = StreamMessage{
        .source_index = ws_ctx.source_index,
        .len = static_cast<uint32_t>(msg.raw_message.size()),
        .recv_seq_num = msg.seq,
        .recv_ts = msg.ts,
        .data = msg.raw_message.data(),
    };

    if (queue->TryPush(to_push)) [[likely]] {
      return;
    }

    LOG_WARNING(logger, "buffer full");
    while (!queue->TryPush(to_push)) {
      ;  // busy-wait because latency-sensitive
    }
    LOG_WARNING(logger, "buffer ok");
  };

  for (const auto& sym : symbols) {
    for (const auto& msg_type : message_types) {
      auto& ws_ctx = run_ctx.stream_contexts.emplace_back(StreamContext{
          .source_index = run_ctx.next_source_index++,
          .msg_type = msg_type,
          .sym = sym,
          .endpoint =
              Endpoint{
                  .host = "fstream.binance.com",
                  .port = "443",
                  .target =
                      fmt::format("/ws/{}@{}", sym, GetStreamName(msg_type)),
              },
          .ws_stream = std::nullopt,
      });
      ++run_ctx.active_streams;

      cobalt::spawn(run_ctx.ioc,
                    RunWebsocket(handle_ws_message, run_ctx, ws_ctx),
                    ex_handler);
    }
  }

  WriteMetadata(run_ctx.stream_contexts, out_dir);

  cobalt::spawn(run_ctx.ioc, WaitForSignals(run_ctx), ex_handler);

  auto disk_writer = JsonWriter{
      out_dir + "messages",
      kMaxBufferedMessages,
  };
  const auto push_writer = [&disk_writer](const auto& msg) {
    disk_writer.Push(msg);
  };
  const auto flush_messages = Defer{[&] {
    while (queue->TryConsume(push_writer)) {
      ;  // drain queue
    }
  }};
  const auto consumer_thread = std::jthread{[&, logger] {
    assert(std::this_thread::get_id() != run_ctx.network_thread_id);
    LOG_WARNING(logger, "writer thread started");
    while (run_ctx.active_streams.load(std::memory_order_relaxed) > 0) {
      if (!queue->TryConsume(push_writer)) {
        std::this_thread::yield();
      }
    }
    LOG_WARNING(logger, "writer thread stopping");
  }};

  LOG_WARNING(logger, "running");
  run_ctx.ioc.run();
  LOG_WARNING(logger, "exiting main");

  return 0;
}
