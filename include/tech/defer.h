#pragma once

#include <utility>

namespace tech {

// inspired by golang's defer
template <typename Fn>
class Defer {
 public:
  explicit Defer(Fn fn) : fn_{std::move(fn)} {}

  Defer(const Defer&) = delete;
  Defer(Defer&&) = delete;
  Defer& operator=(const Defer&) = delete;
  Defer& operator=(Defer&&) = delete;

  ~Defer() { fn_(); }

 private:
  Fn fn_;
};

}  // namespace tech
