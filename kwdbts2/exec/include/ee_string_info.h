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

#include "kwdb_type.h"
#include "mm_kmalloc.h"
#ifndef EE_MaxAllocSize
#define EE_MaxAllocSize ((Size)0x3fffffff)
#endif

#ifndef likely
#if defined(__GNUC__) || defined(__clang__)
#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x)   (x)
#define unlikely(x) (x)
#endif
#endif
namespace kwdbts {

#define EE_DEFAULT_BUFFER_SIZE 2048

typedef k_uint32 Size;
typedef struct {
  char* data;
  int len;
  int cap;
  int cursor;
} EE_StringInfoData;
typedef EE_StringInfoData* EE_StringInfo;
/*------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern EE_StringInfo ee_makeStringInfo(k_int32 size = EE_DEFAULT_BUFFER_SIZE);
extern void ee_destroyStringInfo(EE_StringInfo str);
/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern KStatus ee_initStringInfo(const EE_StringInfo& str, k_int32 size);

/*------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
extern void ee_resetStringInfo(const EE_StringInfo& str);
/*------------------------
 * enlargeStringInfo
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 */
static inline __attribute__((always_inline)) KStatus ee_enlargeStringInfo(const EE_StringInfo& str, int needed) {
  if (needed < 0) return FAIL;

  Size curr_len = (Size)str->len;
  Size total_needed = curr_len + (Size)needed;

  if (total_needed <= (Size)str->cap) return SUCCESS;

  if (total_needed >= EE_MaxAllocSize) return FAIL;

  Size newcap = (Size)str->cap;
  newcap = (total_needed + newcap - 1) & ~(newcap - 1);

  if (newcap > EE_MaxAllocSize) newcap = EE_MaxAllocSize;

  char* newptr = reinterpret_cast<char*>(k_realloc(str->data, newcap));
  if (!newptr) return FAIL;

  str->data = newptr;
  str->cap = newcap;
  return SUCCESS;
}

static inline __attribute__((always_inline)) KStatus ee_appendBinaryStringInfo(const EE_StringInfo& str,
                                                                               const char* data, int datalen) {
  if (unlikely(ee_enlargeStringInfo(str, datalen) != SUCCESS)) return FAIL;

  char* dest = str->data + str->len;
  memcpy(dest, data, datalen);
  str->len += datalen;
  return SUCCESS;
}
extern KStatus ee_sendint(EE_StringInfo buf, k_int32 i, k_int32 b);
}  // namespace kwdbts

