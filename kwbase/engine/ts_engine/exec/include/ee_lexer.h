// Copyright 2016-2024 ClickHouse, Inc.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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

#include "kwdb_type.h"

namespace kwdbts {
#define APPLY_FOR_TOKENS(M)           \
  M(Whitespace)                       \
  M(Comment)                          \
                                      \
  M(BareWord)                         \
                                      \
  M(Number)                           \
  M(StringLiteral)                    \
                                      \
  M(QuotedIdentifier)                 \
                                      \
  M(OpeningRoundBracket)              \
  M(ClosingRoundBracket)              \
                                      \
  M(OpeningSquareBracket)             \
  M(ClosingSquareBracket)             \
                                      \
  M(OpeningCurlyBrace)                \
  M(ClosingCurlyBrace)                \
                                      \
  M(Comma)                            \
  M(Semicolon)                        \
  M(VerticalDelimiter)                \
  M(Dot)                              \
                                      \
  M(Multiple)                         \
  M(HereDoc)                          \
  M(DollarSign)                       \
  M(Plus)                             \
  M(Minus)                            \
  M(Divide)                           \
  M(Dividez)                          \
  M(Remainder)                        \
  M(Power)                            \
  M(Slash)                            \
  M(Percent)                          \
  M(Arrow)                            \
  M(QuestionMark)                     \
  M(Colon)                            \
  M(DoubleColon)                      \
  M(TypeAnotation)                    \
  M(Equals)                           \
  M(NotEquals)                        \
  M(Less)                             \
  M(Greater)                          \
  M(LessOrEquals)                     \
  M(GreaterOrEquals)                  \
  M(LeftShift)                        \
  M(RightShift)                       \
  M(AND)                              \
  M(ANDCAL)                           \
  M(OR)                               \
  M(ORCAL)                            \
  M(Tilde)                            \
  M(ITilde)                           \
  M(NotRegex)                         \
  M(NotIRegex)                        \
  M(Concatenation)                    \
  M(At)                               \
  M(AbsFunction)                      \
  M(In)                               \
  M(Not)                              \
  M(Like)                             \
  M(ILike)                            \
  M(Is)                               \
  M(Null)                             \
  M(Unknown)                          \
  M(Nan)                              \
  M(Case)                             \
  M(When)                             \
  M(Then)                             \
  M(Else)                             \
  M(End)                              \
  M(COALESCE)                         \
  M(Cast)                             \
  M(Any)                              \
  M(All)                              \
  M(UpperFunction)                    \
  M(LengthFunction)                   \
  M(LowerFunction)                    \
  M(Function)                         \
  M(TimeBucketFunction)               \
  M(DateTrunc)                        \
  M(DoubleAt)                         \
  M(UnknownType)                      \
  M(EndOfStream)                      \
  M(Error)                            \
  M(ErrorMultilineCommentIsNotClosed) \
  M(ErrorSingleQuoteIsNotClosed)      \
  M(ErrorDoubleQuoteIsNotClosed)      \
  M(ErrorBackQuoteIsNotClosed)        \
  M(ErrorSingleExclamationMark)       \
  M(ErrorSinglePipeMark)              \
  M(ErrorWrongNumber)                 \
  M(ErrorMaxQuerySizeExceeded)

enum class TokenType {
#define M(TOKEN) TOKEN,
  APPLY_FOR_TOKENS(M)
#undef M
};

struct Token {
  TokenType type;
  k_int64 current_depth_;
  k_int64 end_depth_;
  const k_char *begin;
  const k_char *end;

  Token() = default;
  Token(TokenType type_, const k_char *begin_, const k_char *end_,
        k_int64 current_depth, k_int64 end_depth)
      : type(type_),
        current_depth_(current_depth),
        end_depth_(end_depth),
        begin(begin_),
        end(end_) {}

  [[nodiscard]] k_bool isSignificant() const {
    return type != TokenType::Whitespace && type != TokenType::Comment;
  }
  [[nodiscard]] k_bool isEnd() const { return type == TokenType::EndOfStream; }
};

class Lexer {
 public:
  k_int64 current_depth = 0;
  k_int64 pos_begin = 0;
  Lexer(const k_char *begin, const k_char *end, size_t max_query_size)
      : current_depth(0),
        begin_(begin),
        pos_(begin),
        end_(end),
        max_query_size_(max_query_size) {}
  Token nextToken();

 private:
  const k_char *const begin_;
  const k_char *pos_;
  const k_char *const end_;
  const k_size_t max_query_size_;

  Token nextTokenImpl();
  TokenType prev_significant_token_type = TokenType::Whitespace;
};

}  // namespace kwdbts
