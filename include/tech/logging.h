#pragma once

#include <spdlog/logger.h>
#include <spdlog/spdlog.h>

#include <memory>

namespace tech {

#define LOG_TRACE(logger, ...) SPDLOG_LOGGER_TRACE(logger, __VA_ARGS__)
#define LOG_DEBUG(logger, ...) SPDLOG_LOGGER_DEBUG(logger, __VA_ARGS__)
#define LOG_INFO(logger, ...) SPDLOG_LOGGER_INFO(logger, __VA_ARGS__)
#define LOG_WARNING(logger, ...) SPDLOG_LOGGER_WARN(logger, __VA_ARGS__)
#define LOG_ERROR(logger, ...) SPDLOG_LOGGER_ERROR(logger, __VA_ARGS__)
#define LOG_CRITICAL(logger, ...) SPDLOG_LOGGER_CRITICAL(logger, __VA_ARGS__)

using LoggerPtr = std::shared_ptr<spdlog::logger>;

inline auto GetDefaultLogger() { return spdlog::default_logger(); }

}  // namespace tech
