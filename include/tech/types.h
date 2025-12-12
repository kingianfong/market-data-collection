#pragma once

#include <cstdint>
#include <string_view>
#include <type_traits>

namespace tech {

using SourceIndex = uint8_t;
using MessageTypeIndex = uint8_t;

struct StreamMessage {
  SourceIndex source_index;
  uint32_t len;
  int64_t recv_seq_num;
  int64_t recv_ts;
  const char* data;

  std::string_view raw_message() const { return {data, len}; }
};

static_assert(std::is_trivial_v<StreamMessage>);

}  // namespace tech
