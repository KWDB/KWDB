// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

#ifndef _EE_FNV_H
#define _EE_FNV_H

#include "kwdb_type.h"

#define FNV32_PRIME 0x01000193
#define FNV32_OFFSET_BASIS 0x811c9dc5

#define FNV64_PRIME 0x100000001b3
#define FNV64_OFFSET_BASIS 0xcbf29ce484222325

namespace kwdbts {
  k_uint32 fnv1_hash32(const char *data, size_t len);
  k_uint32 fnv1a_hash32(const char *data, size_t len);
  k_uint64 fnv1_hash64(const char *data, size_t len);
  k_uint64 fnv1a_hash64(const char *data, size_t len);
}   // namespace kwdbts
#endif
