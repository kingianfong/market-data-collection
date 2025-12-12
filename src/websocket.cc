#include "tech/websocket.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/beast/websocket/stream.hpp>

#include "tech/logging.h"

namespace tech::websocket {

ssl::context GetSslContext() {
  auto ssl_ctx = ssl::context{ssl::context::tls_client};
  ssl_ctx.set_options(ssl::context::default_workarounds |
                      ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                      ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1);
  ssl_ctx.set_default_verify_paths();
  ssl_ctx.set_verify_mode(ssl::verify_peer);
  return ssl_ctx;
}

cobalt::task<ErrorCode> InitWebsocketStream(RunContext& run_ctx,
                                            StreamContext& stream_ctx) {
  assert(std::this_thread::get_id() == run_ctx.network_thread_id);

  static constexpr auto token = net::as_tuple(cobalt::use_op);

  auto logger = spdlog::default_logger();

  const auto& endpoint = stream_ctx.endpoint;
  const auto& target = endpoint.target;
  LOG_INFO(logger, "{} InitWebsocketStream", target);

  ErrorCode ec{};
  auto& ws = stream_ctx.ws_stream;
  ws.emplace(run_ctx.ioc, run_ctx.ssl_ctx);

  std::tie(ec, run_ctx.tcp_eps) = co_await run_ctx.resolver.async_resolve(
      endpoint.host, endpoint.port, token);
  if (ec) {
    LOG_ERROR(logger, "{} async_resolve: {} {}", target, ec.to_string(),
              ec.message());
    co_return ec;
  }

  auto& tls = ws->next_layer();
  auto& sock = tls.next_layer();

  std::tie(ec, run_ctx.tcp_ep) =
      co_await net::async_connect(sock, run_ctx.tcp_eps, token);

  if (ec) {
    LOG_ERROR(logger, "{} async_connect: {} {}", target, ec.to_string(),
              ec.message());
    co_return ec;
  }

  std::tie(ec) = co_await tls.async_handshake(ssl::stream_base::client, token);
  if (ec) {
    LOG_ERROR(logger, "{} tls.async_handshake: {} {}", target, ec.to_string(),
              ec.message());
    co_return ec;
  }

  std::tie(ec) =
      co_await ws->async_handshake(endpoint.host, endpoint.target, token);
  if (ec) {
    LOG_ERROR(logger, "{} ws.async_handshake: {} {}", target, ec.to_string(),
              ec.message());
    co_return ec;
  }

  ws->binary(false);
  ws->set_option(beast::websocket::stream_base::timeout::suggested(
      beast::role_type::client));

  co_return ec;
}

cobalt::task<void> WaitForSignals(RunContext& run_ctx) {
  assert(std::this_thread::get_id() == run_ctx.network_thread_id);

  static constexpr auto token = net::as_tuple(cobalt::use_op);

  boost::asio::signal_set signals{run_ctx.ioc, SIGINT, SIGTERM};
  const auto [ec, signal_number] = co_await signals.async_wait(token);

  auto logger = spdlog::default_logger();
  LOG_WARNING(logger, "signal {}, error: {} {}", signal_number, ec.to_string(),
              ec.message());
  for (auto& ctx : run_ctx.stream_contexts) {
    if (auto& ws = ctx.ws_stream) {
      auto& lowest = beast::get_lowest_layer(*ws);
      if (lowest.is_open()) {
        LOG_INFO(logger, "cancelling {}", ctx.endpoint.target);
        lowest.cancel();
      }
    }

    run_ctx.stop_requested = true;
  }

  LOG_INFO(logger, "WaitForSignals done");
}

}  // namespace tech::websocket
