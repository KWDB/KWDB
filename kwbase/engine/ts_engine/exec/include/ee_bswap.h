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

#include <string>

#include "kwdb_type.h"

namespace kwdbts {
static inline k_uint8 BSwap(const k_uint8 &x) {
  return x;
}
#define BSWAP64(x)                                                                                                \
  ((k_uint64)((((k_uint64)(x) & 0xff00000000000000ull) >> 56) | (((k_uint64)(x) & 0x00ff000000000000ull) >> 40) | \
              (((k_uint64)(x) & 0x0000ff0000000000ull) >> 24) | (((k_uint64)(x) & 0x000000ff00000000ull) >> 8) |  \
              (((k_uint64)(x) & 0x00000000ff000000ull) << 8) | (((k_uint64)(x) & 0x0000000000ff0000ull) << 24) |  \
              (((k_uint64)(x) & 0x000000000000ff00ull) << 40) | (((k_uint64)(x) & 0x00000000000000ffull) << 56)))
#define BSWAP32(x)                                                                         \
  ((k_uint32)((((k_uint32)(x) & 0xff000000) >> 24) | (((k_uint32)(x) & 0x00ff0000) >> 8) | \
              (((k_uint32)(x) & 0x0000ff00) << 8) | (((k_uint32)(x) & 0x000000ff) << 24)))
#define BSWAP16(x) ((k_uint16)((((k_uint16)(x) & 0xff00) >> 8) | (((k_uint16)(x) & 0x00ff) << 8)))

static inline k_uint64 BSwap(const k_uint64 &x) {
  return BSWAP64(x);
}
static inline k_uint16 BSwap(const k_uint16 &x) {
  return BSWAP16(x);
}

static inline k_uint32 BSwap(const k_uint32 &x) {
  return BSWAP32(x);
}

};  // namespace kwdbts
