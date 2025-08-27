// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#pragma once

#include <memory>

#include "ee_token_iterator.h"

namespace kwdbts {
class IParser {
 public:
  /// Token iterator augmented with depth information. This allows to control
  /// recursion depth.
  struct Pos : public kwdbts::TokenIterator {
    k_uint32 depth = 0;
    k_uint32 max_depth = 0;
    Pos(std::shared_ptr<Tokens> tokens_, k_uint32 max_depth_)
        : TokenIterator(tokens_), max_depth(max_depth_) {}
    [[maybe_unused]] ALWAYS_INLINE void increaseDepth() const {
      if (depth > max_depth)
        std::throw_with_nested(
            std::runtime_error("Maximum parse depth exceeded"));
    }
    [[maybe_unused]] ALWAYS_INLINE void decreaseDepth() {
      if (depth == 0)
        std::throw_with_nested(std::runtime_error("wrong depth size"));
      --depth;
    }
  };
  /** Get the text of this parser parses. */
  virtual ~IParser() = default;
};

}  // namespace kwdbts
