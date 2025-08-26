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

#ifndef EE_MaxAllocSize
#define EE_MaxAllocSize ((Size)0x3fffffff)
#endif
namespace kwdbts {

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
extern EE_StringInfo ee_makeStringInfo(void);

/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern KStatus ee_initStringInfo(const EE_StringInfo& str);

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
extern KStatus ee_enlargeStringInfo(const EE_StringInfo& str, int needed);
extern KStatus ee_appendBinaryStringInfo(const EE_StringInfo& str, const char* data,
                                   k_int32 datalen);
extern KStatus ee_sendint(EE_StringInfo buf, k_int32 i, k_int32 b);
}  // namespace kwdbts

