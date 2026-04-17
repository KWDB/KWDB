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

#include "data_type.h"
#include <cmath>
using namespace std;

// length of dst = siz + 1; will append NULL at end of string
size_t mmap_strlcpy(char *dst, const char *src, size_t siz);

// length of dst = siz.
size_t CHARcpy(char *dst, const char *src, size_t siz);

int dataTypeMaxTextLen(const AttributeInfo &col);
// true if s is empty or s = 'NULL'
bool isNULL(char *s);

// Return length of binary string.
int unHex(const char *str, unsigned char *hex_data, int max_len);


// the hex result will be reversed. ex: 1234 will be "3930", but actually it should be "3039"
int binaryToHexString(unsigned char *hex_data, char *s, int32_t len);

namespace kwdbts {

class DoubleFormatToString {
protected:
  int precision_;
  bool is_remove_trailing_zeros_;
  double max_v_;
  double min_v_;

  void setMinMax();
public:
  DoubleFormatToString(int precision, bool is_rm_tz = true);
  DoubleFormatToString(const DoubleFormatToString &rhs);
  virtual ~DoubleFormatToString();
  int toString(char *str, double v);
};

} // namespace kwdbts

