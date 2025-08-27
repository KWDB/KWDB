// Copyright 2016-2024 ClickHouse, Inc.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.



#pragma once

#include <iostream>
#include <memory>
#include <vector>
#include "ee_lexer.h"

#define ALWAYS_INLINE __attribute__((__always_inline__))

namespace kwdbts {
class Tokens {
 private:
  std::vector<Token> data_;

 public:
  Lexer lexer;
  Tokens(const k_char *begin, const k_char *end, k_size_t max_query_size = 0)
      : lexer(begin, end, max_query_size) {}

  const Token &operator[](k_size_t index) {
    while (true) {
      if (index < data_.size())
        return data_[index];

      if (!data_.empty() && data_.back().isEnd())
        return data_.back();

      Token token = lexer.nextToken();

      if (token.isSignificant())
        data_.emplace_back(token);
    }
  }
};

/// To represent position in a token stream.
class TokenIterator {
 public:
  std::shared_ptr<Tokens> tokens;
  k_size_t index = 0;

 public:
  explicit TokenIterator(std::shared_ptr<Tokens> tokens_) : tokens(tokens_) {}

  ALWAYS_INLINE const Token &get() { return (*tokens)[index]; }
  ALWAYS_INLINE const Token &operator*() { return get(); }
  ALWAYS_INLINE const Token *operator->() { return &get(); }

  ALWAYS_INLINE TokenIterator &operator++() {
    ++index;
    return *this;
  }
  ALWAYS_INLINE TokenIterator &operator--() {
    --index;
    return *this;
  }

  ALWAYS_INLINE bool operator<(const TokenIterator &rhs) const {
    return index < rhs.index;
  }
  ALWAYS_INLINE bool operator<=(const TokenIterator &rhs) const {
    return index <= rhs.index;
  }
  ALWAYS_INLINE bool operator==(const TokenIterator &rhs) const {
    return index == rhs.index;
  }
  ALWAYS_INLINE bool operator!=(const TokenIterator &rhs) const {
    return index != rhs.index;
  }
};
}  // namespace kwdbts
