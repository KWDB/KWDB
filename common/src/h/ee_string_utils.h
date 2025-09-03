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


#pragma once
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

namespace kwdbts {
inline bool isASCII(char c) { return static_cast<unsigned char>(c) < 0x80; }

inline bool isAlphaASCII(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

inline bool isNumericASCII(char c) { return (c >= '0' && c <= '9'); }

inline bool isHexDigit(char c) {
  return isNumericASCII(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

inline bool isAlphaNumericASCII(char c) {
  return isAlphaASCII(c) || isNumericASCII(c);
}

inline bool isWordCharASCII(char c) {
  return isAlphaNumericASCII(c) || c == '_';
}

inline bool isWhitespaceASCII(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' ||
         c == '\v';
}

inline const char *skipWhitespacesUTF8(const char *pos, const char *end) {
  // https://en.wikipedia.org/wiki/Whitespace_character
  while (pos < end) {
    if (isWhitespaceASCII(*pos)) {
      ++pos;
    } else {
      const uint8_t *upos = reinterpret_cast<const uint8_t *>(pos);
      if (pos + 1 < end && upos[0] == 0xC2 &&
          (upos[1] == 0x85 || upos[1] == 0xA0)) {
        pos += 2;
      } else if (pos + 2 < end &&
                 ((upos[0] == 0xE1 && upos[1] == 0xA0 && upos[2] == 0x8E) ||
                  (upos[0] == 0xE2 &&
                   ((upos[1] == 0x80 && ((upos[2] >= 0x80 && upos[2] <= 0x8A) ||
                                         (upos[2] >= 0xA8 && upos[2] <= 0xA9) ||
                                         (upos[2] >= 0x8B && upos[2] <= 0x8D) ||
                                         (upos[2] == 0xAF))) ||
                    (upos[1] == 0x81 &&
                     (upos[2] == 0x9F || upos[2] == 0xA0)))) ||
                  (upos[0] == 0xE3 && upos[1] == 0x80 && upos[2] == 0x80) ||
                  (upos[0] == 0xEF && upos[1] == 0xBB && upos[2] == 0xBF))) {
        pos += 3;
      } else {
        break;
      }
    }
  }
  return pos;
}
}  // namespace kwdbts
