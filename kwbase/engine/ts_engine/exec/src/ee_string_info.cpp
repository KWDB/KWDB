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
#include "ee_string_info.h"

#include <netinet/in.h>
#include <cstdlib>
#include <cstring>

#include "mm_kmalloc.h"


namespace kwdbts {
/**
 *
 * @return
 */
EE_StringInfo ee_makeStringInfo(k_int32 size) {
  EE_StringInfo res;
  res = KNEW(EE_StringInfoData);
  if (res == nullptr) {
    return res;
  }
  KStatus ret = ee_initStringInfo(res, size);
  if (ret != SUCCESS) {
    delete res;
    res = nullptr;
  }

  return res;
}

void ee_destroyStringInfo(EE_StringInfo str) {
  if (str == nullptr) {
    return;
  }
  k_free(str->data);
  delete str;
}

/**
 *
 * @param str
 */
KStatus ee_initStringInfo(const EE_StringInfo &str, k_int32 size) {
  str->data = static_cast<char *>(k_malloc(size));
  if (str->data == nullptr) {
    // pusherrN
    return FAIL;
  }
  str->cap = size;
  ee_resetStringInfo(str);
  return SUCCESS;
}

/**
 *
 * @param str
 */
void ee_resetStringInfo(const EE_StringInfo &str) {
  str->len = 0;
  str->cursor = 0;
}

KStatus ee_appendBinaryStringInfoWithoutEnlarge(const EE_StringInfo &str, const char *data,
                                  k_int32 datalen) {
  if (str->len + datalen > str->cap) {
    return FAIL;
  }
  /* copy byte */
  memcpy(str->data + str->len, data, datalen);
  str->len += datalen;
  return SUCCESS;
}

KStatus ee_sendint(EE_StringInfo buf, k_int32 i, k_int32 b) {
  unsigned char n8;
  uint16_t n16;
  uint32_t n32;

  switch (b) {
    case 1:
      n8 = (unsigned char) i;
      return ee_appendBinaryStringInfo(buf, reinterpret_cast<char *>(&n8) , 1);
    case 2:
      n16 = htons((uint16_t) i);
      return ee_appendBinaryStringInfo(buf, reinterpret_cast<char *>(&n16), 2);
    case 4:
      n32 = htonl((uint32_t) i);
      return ee_appendBinaryStringInfo(buf, reinterpret_cast<char *>(&n32), 4);
    default:
      // TODO(SH): log
      // elog(ERROR, "unsupported integer size %d", b);
      break;
  }
  return SUCCESS;
}

}  // namespace kwdbts
