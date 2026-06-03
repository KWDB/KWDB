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

#include "ee_crc32.h"

#include <cstring>

#include "kwdb_type.h"
namespace kwdbts {

static k_uint32 kwdb_crc32_comp(char ch, k_uint32 crc) {
  k_uint8 data = (k_uint8)(ch);
  k_uint8 tableindex;
#ifdef CRC32_WORDS_BIGENDIAN
  tableindex = (static_cast<int>((crc) >> 24) ^ data) & 0xFF;
  crc = kwdb_crc32i_table[tableindex] ^ ((crc) << 8);
#else
  tableindex = (static_cast<int>(crc) ^ data) & 0xFF;
  crc = kwdb_crc32i_table[tableindex] ^ ((crc) >> 8);
#endif
  return crc;
}

static k_uint32 kwdb_crc32c_comp(k_uint8 ch, k_uint32 crc) {
  k_uint8 data = ch;
  k_uint8 tableindex;
/* Accumulate one input byte */
#ifdef CRC32_WORDS_BIGENDIAN
  tableindex = ((crc >> 24) ^ (data)) & 0xFF;
  crc = kwbd_crc32c_table[0][tableindex] ^ (crc << 8);
#else
  tableindex = (crc ^ (data)) & 0xFF;
  crc = kwbd_crc32c_table[0][tableindex] ^ (crc >> 8);
#endif
  return crc;
}

k_uint32 kwdb_crc32_ieee(const char *buf, int size) {
  k_uint32 crc = 0xFFFFFFFF;
  const char *p = buf;
  while (size > 0) {
    char ch = *p;

    crc = kwdb_crc32_comp(ch, crc);
    size--;
    p++;
  }
  crc ^= 0xFFFFFFFF;
  return crc;
}

k_uint32 kwdb_crc32_castagnoli(const void *data, size_t len) {
  const k_uint8 *p = (const k_uint8 *)data;
  k_uint32 crc = 0xFFFFFFFF;
  while (len > 0 && ((uintptr_t)p & 0x03) != 0) {
    crc = kwdb_crc32c_comp(*p++, crc);
    len--;
  }

  while (len >= KWDB_CRC32_CUTOFF) {
    k_uint32 low = 0;
    k_uint32 high = 0;
    std::memcpy(&low, p, sizeof(low));
    std::memcpy(&high, p + sizeof(low), sizeof(high));
    low ^= crc;
#ifdef CRC32_WORDS_BIGENDIAN
    const k_uint8 c0 = high & 0xff;
    const k_uint8 c1 = ((high >> 8) & 0xff);
    const k_uint8 c2 = ((high >> 16) & 0xff);
    const k_uint8 c3 = ((high >> 24) & 0xff);

    const k_uint8 c4 = low & 0xff;
    const k_uint8 c5 = ((low >> 8) & 0xff);
    const k_uint8 c6 = ((low >> 16) & 0xff);
    const k_uint8 c7 = ((low >> 24) & 0xff);

#else
    const k_uint8 c0 = ((high >> 24) & 0xff);
    const k_uint8 c1 = ((high >> 16) & 0xff);
    const k_uint8 c2 = ((high >> 8) & 0xff);
    const k_uint8 c3 = high & 0xff;

    const k_uint8 c4 = ((low >> 24) & 0xff);
    const k_uint8 c5 = ((low >> 16) & 0xff);
    const k_uint8 c6 = ((low >> 8) & 0xff);
    const k_uint8 c7 = low & 0xff;

#endif

    crc = kwbd_crc32c_table[0][c0] ^ kwbd_crc32c_table[1][c1] ^ kwbd_crc32c_table[2][c2] ^ kwbd_crc32c_table[3][c3] ^
          kwbd_crc32c_table[4][c4] ^ kwbd_crc32c_table[5][c5] ^ kwbd_crc32c_table[6][c6] ^ kwbd_crc32c_table[7][c7];
    p += sizeof(k_uint64);
    len -= sizeof(k_uint64);
  }

  /*
   * Handle any remaining bytes one at a time.
   */
  while (len > 0) {
    crc = kwdb_crc32c_comp(*p++, crc);
    len--;
  }

  crc ^= 0xFFFFFFFF;
  return crc;
}
}  // namespace kwdbts
