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

#include "ee_fnv.h"
namespace kwdbts {
k_uint32 fnv1_hash32(const char *data, size_t len) {
  unsigned char *s = (unsigned char *)data;
  k_uint32 hash = FNV32_OFFSET_BASIS;

  for (k_uint64 i = 0; i < len; i++) {
    k_uint8 value = s[i];
    hash *= FNV32_PRIME;    // 16777619
    hash ^= value;
  }
  return hash;
}

k_uint32 fnv1a_hash32(const char *data, size_t len) {
  unsigned char *s = (unsigned char *)data;
  k_uint32 hash = FNV32_OFFSET_BASIS;

  for (k_uint64 i = 0; i < len; i++) {
    k_uint8 value = s[i];
    hash ^= value;
    hash *= FNV32_PRIME;   // 16777619
  }
  return hash;
}
k_uint64 fnv1_hash64(const char * data, size_t len) {
  unsigned char *s = (unsigned char *) data;
  k_uint64 hash = FNV64_OFFSET_BASIS;
  k_uint64 prime = FNV64_PRIME;

  for (k_uint64 i = 0; i < len; i++) {
    k_uint8 value = s[i];
    hash *= prime;
    hash = hash ^ value;
  }
  return hash;
}

k_uint64 fnv1a_hash64(const char * data, size_t len) {
  unsigned char *s = (unsigned char *) data;
  k_uint64 hash = FNV64_OFFSET_BASIS;
  k_uint64 prime = FNV64_PRIME;

  for (k_uint64 i = 0; i < len; i++) {
    k_uint8 value = s[i];
    hash = hash ^ value;
    hash *= prime;
  }
  return hash;
}
}  // namespace kwdbts
