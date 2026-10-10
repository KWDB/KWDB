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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include "ee_lexer.h"

#include <string>

#include "ee_string_utils.h"

namespace kwdbts {
Token Lexer::nextToken() {
  Token res = nextTokenImpl();
  if (max_query_size_ && res.end > begin_ + max_query_size_)
    res.type = TokenType::ErrorMaxQuerySizeExceeded;
  if (res.isSignificant()) prev_significant_token_type = res.type;
  return res;
}
Token Lexer::nextTokenImpl() {
  if (pos_ >= end_)
    return {TokenType::EndOfStream, end_, end_, this->pos_begin,
            this->current_depth};
  const k_char *const token_begin = pos_;

  switch (*pos_) {
    case ' ':
    case '\t':
    case '\n':
    case '\r':
    case '\f':
    case '\v': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      while (pos_ < end_ && isWhitespaceASCII(*pos_)) {
        ++pos_;
        ++this->current_depth;
      }
      return {TokenType::Whitespace, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }

    case '\'': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      while (*pos_ != '\'' && pos_ < end_) {
        this->current_depth++;
        ++pos_;
      }

      if (*pos_ == '\'') {
        this->current_depth++;
        ++pos_;
        return {TokenType::StringLiteral, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
    }

    case '0':
    case '1':
    case '2':
    case '3':
    case '4':
    case '5':
    case '6':
    case '7':
    case '8':
    case '9': {
      this->pos_begin = this->current_depth;
      if (prev_significant_token_type == TokenType::Dot) {
        ++pos_;
        this->pos_begin = this->current_depth++;
        while (pos_ < end_ && isNumericASCII(*pos_)) {
          ++pos_;
          ++this->current_depth;
        }
      } else {
        /// 0x, 0b
        bool hex = false;
        if (pos_ + 2 < end_ && *pos_ == '0' &&
            (pos_[1] == 'x' || pos_[1] == 'b' || pos_[1] == 'X' ||
             pos_[1] == 'B')) {
          if (pos_[1] == 'x' || pos_[1] == 'X') hex = true;
          pos_ += 2;
          this->current_depth += 2;
        } else {
          ++pos_;
          ++this->current_depth;
        }

        while (pos_ < end_ &&
               (hex ? isHexDigit(*pos_) : isNumericASCII(*pos_))) {
          ++pos_;
          ++this->current_depth;
        }

        /// decimal point
        if (pos_ < end_ && *pos_ == '.') {
          ++pos_;
          ++this->current_depth;
          while (pos_ < end_ &&
                 (hex ? isHexDigit(*pos_) : isNumericASCII(*pos_))) {
            ++pos_;
            ++this->current_depth;
          }
        }

        /// exponentiation (base 10 or base 2)
        if (pos_ + 1 < end_ && (hex ? (*pos_ == 'p' || *pos_ == 'P')
                                    : (*pos_ == 'e' || *pos_ == 'E'))) {
          ++pos_;
          ++this->current_depth;

          /// sign of exponent. It is always decimal.
          if (pos_ + 1 < end_ && (*pos_ == '-' || *pos_ == '+')) {
            ++pos_;
            ++this->current_depth;
          }

          while (pos_ < end_ && isNumericASCII(*pos_)) {
            ++pos_;
            ++this->current_depth;
          }
        }
      }

      /// Try to parse it to a identifier(1identifier_name), otherwise it return
      /// ErrorWrongNumber
      if (pos_ < end_ && isWordCharASCII(*pos_)) {
        ++pos_;
        ++this->current_depth;
        while (pos_ < end_ && isWordCharASCII(*pos_)) {
          ++pos_;
          ++this->current_depth;
        }

        for (const k_char *iterator = token_begin; iterator < pos_;
             ++iterator) {
          if (!isWordCharASCII(*iterator) && *iterator != '$')
            return {TokenType::ErrorWrongNumber, token_begin, pos_,
                    this->pos_begin, this->current_depth};
        }

        return {TokenType::BareWord, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }

      return {TokenType::Number, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }

    case '(': {
      this->pos_begin = this->current_depth++;
      return {TokenType::OpeningRoundBracket, token_begin, ++pos_,
              this->pos_begin, this->current_depth};
    }
    case ')': {
      this->pos_begin = this->current_depth++;
      return {TokenType::ClosingRoundBracket, token_begin, ++pos_,
              this->pos_begin, this->current_depth};
    }
    case ',': {
      this->pos_begin = this->current_depth++;
      return {TokenType::Comma, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }

    case '.': {
      this->pos_begin = this->current_depth++;
      if (pos_ > begin_ &&
          (!(pos_ + 1 < end_ && isNumericASCII(pos_[1])) ||
           prev_significant_token_type == TokenType::ClosingRoundBracket ||
           prev_significant_token_type == TokenType::ClosingSquareBracket ||
           prev_significant_token_type == TokenType::BareWord ||
           prev_significant_token_type == TokenType::QuotedIdentifier ||
           prev_significant_token_type == TokenType::Number))
        return {TokenType::Dot, token_begin, ++pos_, this->pos_begin,
                this->current_depth};

      ++pos_;
      ++this->current_depth;
      while (pos_ < end_ && isNumericASCII(*pos_)) {
        ++pos_;
        ++this->current_depth;
      }

      /// exponentiation
      if (pos_ + 1 < end_ && (*pos_ == 'e' || *pos_ == 'E')) {
        ++pos_;
        ++this->current_depth;

        /// sign of exponent
        if (pos_ + 1 < end_ && (*pos_ == '-' || *pos_ == '+')) ++pos_;
        ++this->current_depth;

        while (pos_ < end_ && isNumericASCII(*pos_)) {
          ++pos_;
          ++this->current_depth;
        }
      }

      return {TokenType::Number, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }

    case '+': {
      this->pos_begin = this->current_depth++;
      return {TokenType::Plus, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }
    case '-':  /// minus (-), arrow (->) or start of comment (--)
    {
      this->pos_begin = this->current_depth++;
      ++pos_;
      if (pos_ < end_ && *pos_ == '>') {
        ++this->current_depth;
        return {TokenType::Arrow, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::Minus, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '*': {
      this->pos_begin = this->current_depth++;
      ++pos_;
      return {TokenType::Multiple, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '/':  /// division (/) or start of comment (//, /*)
    {
      this->pos_begin = this->current_depth++;
      ++pos_;
      if (pos_ < end_ && (*pos_ == '*') || pos_ >= end_) {
        return {TokenType::Error, token_begin, pos_, this->pos_begin,
                this->current_depth};
      } else if (pos_ < end_ && *pos_ == '/') {
        this->pos_begin = this->current_depth++;
        ++pos_;
        return {TokenType::Dividez, token_begin, pos_, this->pos_begin,
                this->current_depth};
      } else {
        return {TokenType::Divide, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
    }
    case '#': {
      this->pos_begin = this->current_depth++;
      return {TokenType::Remainder, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }
    case '%': {
      this->pos_begin = this->current_depth++;
      return {TokenType::Percent, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }
    case '^': {
      this->pos_begin = this->current_depth++;
      return {TokenType::Power, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }
    case '=':  /// =, ==
    {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '=') {
        ++pos_;
        ++this->current_depth;
      }
      return {TokenType::Equals, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '!':  /// !=
    {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '=') {
        this->current_depth++;
        return {TokenType::NotEquals, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      if (pos_ < end_ && *pos_ == '~') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == '*') {
          this->current_depth++;
          return {TokenType::NotIRegex, token_begin, ++pos_, this->pos_begin,
                  this->current_depth};
        }
        return {TokenType::NotRegex, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::Not, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '<':  /// <, <=, <>
    {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '=') {
        this->current_depth++;
        return {TokenType::LessOrEquals, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      if (pos_ < end_ && *pos_ == '>') {
        this->current_depth++;
        return {TokenType::NotEquals, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      if (pos_ < end_ && *pos_ == '<') {
        this->current_depth++;
        return {TokenType::LeftShift, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::Less, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '>':  /// >, >=
    {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '=') {
        this->current_depth++;
        return {TokenType::GreaterOrEquals, token_begin, ++pos_,
                this->pos_begin, this->current_depth};
      } else if (pos_ < end_ && *pos_ == '>') {
        this->current_depth++;
        return {TokenType::RightShift, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::Greater, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '|': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '|') {
        this->current_depth++;
        return {TokenType::OR, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::ORCAL, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '&': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '&') {
        this->current_depth++;
        return {TokenType::AND, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::ANDCAL, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '~': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '*') {
        this->current_depth++;
        return {TokenType::ITilde, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::Tilde, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }
    case '?': {
      this->pos_begin = this->current_depth++;

      return {TokenType::QuestionMark, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
    }
    case ':': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == ':') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == ':') {
          this->current_depth++;
          return {TokenType::TypeAnotation, token_begin, ++pos_,
                  this->pos_begin, this->current_depth};
        } else {
          return {TokenType::DoubleColon, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
      }
      --pos_;
      this->current_depth--;
      return {TokenType::Error, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }

    case '@': {
      ++pos_;
      this->pos_begin = this->current_depth++;
      if (pos_ < end_ && *pos_ == '@') {
        this->current_depth++;
        return {TokenType::DoubleAt, token_begin, ++pos_, this->pos_begin,
                this->current_depth};
      }
      return {TokenType::At, token_begin, pos_, this->pos_begin,
              this->current_depth};
    }

    case 'e': {
      if (*(pos_ + 1) == '\'') {
        ++pos_;
        this->current_depth++;
        ++pos_;
        this->pos_begin = this->current_depth++;
        for (bool done = false; pos_ < end_ && !done;) {
          this->current_depth++;
          ++pos_;
          if (*pos_ == '\'') {
            const k_char *result = pos_ + 1;
            if (std::strncmp(result, ":::STRING", 9) == 0) {
              done = true;
            }
          }
        }
        if (*pos_ == '\'') {
          this->current_depth++;
          ++pos_;
          return {TokenType::StringLiteral, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
        return {TokenType::Error, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
      // fall through
    }

    case 'I':  // 'I' in 'In', 'IS'
    {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'S') {
        ++pos_;
        this->current_depth++;
        return {TokenType::Is, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
      if (pos_ < end_ && *pos_ == 'N') {
        ++pos_;
        this->current_depth++;
        ++pos_;
        this->pos_begin = this->current_depth++;
        int open = 0;
        int close = 0;
        for (int i = 0; pos_ < end_; i++) {
          if (i == 0) {
            if (*pos_ != '(') {
              --pos_;
              --pos_;
              --pos_;
              this->current_depth--;
              this->current_depth--;
              this->current_depth--;
              goto default_case;
            }
          }
          if (*pos_ == '(') {
            open++;
          } else if (*pos_ == ')') {
            close++;
            if (open == close) {
              break;
            }
          }
          this->current_depth++;
          ++pos_;
        }

        if (*pos_ == ')') {
          this->current_depth++;
          ++pos_;
          return {TokenType::In, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
        return {TokenType::Error, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
      if (pos_ < end_ && *pos_ == 'L') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'I') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'K') {
            ++pos_;
            this->current_depth++;
            if (pos_ < end_ && *pos_ == 'E') {
              ++pos_;
              this->current_depth++;
              return {TokenType::ILike, token_begin, pos_, this->pos_begin,
                      this->current_depth};
            }
          }
        }
      }
    }

    case 'A': {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'N') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'D') {
          ++pos_;
          this->current_depth++;
          return {TokenType::AND, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        } else if (pos_ < end_ && *pos_ == 'Y') {
          ++pos_;
          this->current_depth++;
          return {TokenType::Any, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
      } else if (pos_ < end_ && *pos_ == 'S') {
        ++pos_;
        this->current_depth++;
        return {TokenType::DoubleColon, token_begin, pos_, this->pos_begin,
                this->current_depth};
      } else if (pos_ + 1 < end_ && *pos_ == 'L' && *(pos_ + 1) == 'L') {
        pos_ += 2;
        this->current_depth += 2;
        return {TokenType::All, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
    }

    case 'O': {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'R') {
        ++pos_;
        this->current_depth++;
        return {TokenType::OR, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
    }

    case 'U': {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'N') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'K') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'N') {
            ++pos_;
            this->current_depth++;
            if (pos_ < end_ && *pos_ == 'O') {
              ++pos_;
              this->current_depth++;
              if (pos_ < end_ && *pos_ == 'W') {
                ++pos_;
                this->current_depth++;
                if (pos_ < end_ && *pos_ == 'N') {
                  ++pos_;
                  this->current_depth++;
                  return {TokenType::Unknown, token_begin, pos_,
                          this->pos_begin, this->current_depth};
                }
              }
            }
          }
        }
      }
    }

    case 'N': {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'U') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'L') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'L') {
            ++pos_;
            this->current_depth++;
            return {TokenType::Null, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          }
        }
      }
      if (pos_ < end_ && *pos_ == 'O') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'T') {
          ++pos_;
          this->current_depth++;
          return {TokenType::Not, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
      }

      if (pos_ < end_ && *pos_ == 'A') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'N') {
          ++pos_;
          this->current_depth++;
          return {TokenType::Nan, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
      }
      --pos_;
      this->current_depth--;
      goto default_case;  // NCHAR & NVARCHAR
    }

    case 'L':  // 'l' in 'like'
    {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'I') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'K') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'E') {
            ++pos_;
            this->current_depth++;
            return {TokenType::Like, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          }
        }
      }
    }

    case 'F': {
      ++pos_;
      this->current_depth++;
      if (pos_ < end_ && *pos_ == 'u') {
        ++pos_;
        this->current_depth++;
        if (pos_ < end_ && *pos_ == 'n') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'c') {
            ++pos_;
            this->current_depth++;
            if (pos_ < end_ && *pos_ == 't') {
              ++pos_;
              this->current_depth++;
              if (pos_ < end_ && *pos_ == 'i') {
                ++pos_;
                this->current_depth++;
                if (pos_ < end_ && *pos_ == 'o') {
                  ++pos_;
                  this->current_depth++;
                  if (pos_ < end_ && *pos_ == 'n') {
                    ++pos_;
                    this->current_depth++;
                    while (*pos_ != '(' && pos_ < end_) {
                      this->current_depth++;
                      ++pos_;
                    }
                    if (*pos_ == '(') {
                      return {TokenType::Function, token_begin, pos_,
                              this->pos_begin, this->current_depth};
                    }
                  }
                }
              }
            }
          }
        }
      } else {
        // conflict with FLOAT,so get back of "F"
        --pos_;
        this->current_depth--;
        goto default_case;
      }
    }
    case 'C':  // 'C' in "CASE" or "COALESCE"
    {
      if (pos_ + 1 < end_ && *(pos_ + 1) == 'A') {
        pos_ += 2;
        this->current_depth += 2;
        if (pos_ < end_ && *pos_ == 'S') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'E') {
            ++pos_;
            this->current_depth++;
            return {TokenType::Case, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          } else if (pos_ < end_ && *pos_ == 'T') {
            ++pos_;
            this->current_depth++;
            return {TokenType::Cast, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          }
        }
      } else if (pos_ + 1 < end_ && *(pos_ + 1) == 'O') {
        pos_ += 2;
        this->current_depth += 2;
        if (pos_ < end_ && *pos_ == 'A') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'L') {
            ++pos_;
            this->current_depth++;
            if (pos_ < end_ && *pos_ == 'E') {
              ++pos_;
              this->current_depth++;
              if (pos_ < end_ && *pos_ == 'S') {
                ++pos_;
                this->current_depth++;
                if (pos_ < end_ && *pos_ == 'C') {
                  ++pos_;
                  this->current_depth++;
                  if (pos_ < end_ && *pos_ == 'E') {
                    ++pos_;
                    this->current_depth++;
                    return {TokenType::COALESCE, token_begin, pos_,
                            this->pos_begin, this->current_depth};
                  }
                }
              }
            }
          }
        }
      } else {
        goto default_case;
      }
    }
    case 'T':  // 'T' in "THEN"
    {
      if (pos_ + 3 < end_ && *(pos_ + 1) == 'H' && *(pos_ + 2) == 'E' &&
          *(pos_ + 3) == 'N') {
        pos_ += 4;
        this->current_depth += 4;
        return {TokenType::Then, token_begin, pos_, this->pos_begin,
                this->current_depth};
      } else {
        goto default_case;  // TIMESTAMP & TIMESTAMPTZ
      }
    }
    case 'W':  // 'W' in "WHEN"
    {
      if (pos_ + 1 < end_ && *(pos_ + 1) == 'H') {
        pos_ += 2;
        this->current_depth += 2;
        if (pos_ < end_ && *pos_ == 'E') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'N') {
            ++pos_;
            this->current_depth++;
            return {TokenType::When, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          }
        }
      }
    }
    case 'E':  // 'E' in "End" or "Else"
    {
      if (pos_ + 1 < end_ && *(pos_ + 1) == 'L') {
        pos_ += 2;
        this->current_depth += 2;
        if (pos_ < end_ && *pos_ == 'S') {
          ++pos_;
          this->current_depth++;
          if (pos_ < end_ && *pos_ == 'E') {
            ++pos_;
            this->current_depth++;
            return {TokenType::Else, token_begin, pos_, this->pos_begin,
                    this->current_depth};
          }
        }
      } else if (pos_ + 1 < end_ && *(pos_ + 1) == 'N') {
        pos_ += 2;
        this->current_depth += 2;
        if (pos_ < end_ && *pos_ == 'D') {
          ++pos_;
          this->current_depth++;
          return {TokenType::End, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
      }
    }
    case 'S': {
      if (pos_ + 3 < end_ && *(pos_ + 1) == 'O' && *(pos_ + 2) == 'M' &&
          *(pos_ + 3) == 'E') {
        pos_ += 4;
        this->current_depth += 4;
        return {TokenType::Any, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }
    }
    default:
    default_case:
      this->pos_begin = this->current_depth;
      std::string buffer;
      if (isAlphaNumericASCII(*pos_)) {
        buffer.push_back(*pos_);
        this->current_depth++;
        ++pos_;
        buffer.push_back(*pos_);
        while (pos_ < end_ && isAlphaNumericASCII(*pos_)) {
          this->current_depth++;
          ++pos_;
          buffer.push_back(*pos_);
        }
        if (pos_ < end_ && *pos_ == '(') {
          do {
            this->current_depth++;
            ++pos_;
            buffer.push_back(*pos_);
            if (pos_ < end_ && *pos_ == ')') {
              this->current_depth++;
              ++pos_;
              buffer.push_back(*pos_);
              break;
            }
          } while (pos_ < end_ && isAlphaNumericASCII(*pos_));
        }
        if (buffer.find("AND") != std::string::npos ||
            buffer.find("and") != std::string::npos) {
          return {TokenType::AND, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
        if (buffer.find("OR") != std::string::npos ||
            buffer.find("or") != std::string::npos) {
          return {TokenType::OR, token_begin, pos_, this->pos_begin,
                  this->current_depth};
        }
        return {TokenType::BareWord, token_begin, pos_, this->pos_begin,
                this->current_depth};
      }

      return {TokenType::Error, token_begin, ++pos_, this->pos_begin,
              this->current_depth};
  }
}  // NOLINT

}  // namespace kwdbts
