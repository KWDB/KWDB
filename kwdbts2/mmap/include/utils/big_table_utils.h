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

#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <algorithm>
#include <sys/resource.h>
#include "ts_object_error.h"
#include "big_table.h"
#include "kwdb_consts.h"
#include "data_type.h"
#include "settings.h"

using namespace kwdbts;


vector<AttributeInfo> & getDummySchema();

/**
 * @brief	obtain the Path
 *
 * @param 	path	the path to process
 * @return	path protocol.
 */
string getTsFilePath(const string &path);

string getTsObjectName(const string &path);

int setInteger(int &n, const string &val_str, int min, int max = std::numeric_limits<int>::max());

bool isInteger(const char *s, int64_t &i);

string nameToEntityBigTablePath(const string &name, const string &ext = kwdbts::s_bt);

int getDataTypeSize(int type);

int getDataTypeSize(AttributeInfo &info);

int setAttributeInfo(vector<AttributeInfo> &info);

// Returns C++ string from char * string.
// If data is NULL, it returns an empty string.
string toString(const char *data);

inline int stringToInt(const string &str)
{ return atoi(str.c_str()); }

inline string intToString(int64_t i) {
  return std::to_string(i);
}

string toString(const char *str, size_t len);

inline off_t getPageOffset(off_t offset, size_t ps =
  kwdbts::EngineOptions::pageSize())
{ return ((offset + ps - 1) & ~(ps - 1)); }

string normalizePath(const string &path);

string makeDirectoryPath(const string &tbl_sub_path);

template <typename T>
void assign(T &right, const T &value) {
  T tmp = value;      // avoid crash in arm
  right = value;
}

typedef vector<std::pair<size_t, size_t>> table_partition;

// set null bitmap
inline void set_null_bitmap(unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  // col = col / 8;
  unsigned char bit_pos = (1 << (col & 7));
  col = col >> 3;
  null_bitmap[col] |= bit_pos;
}

// unset null bitmap
inline void unset_null_bitmap(unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  // col = col / 8;
  unsigned char bit_pos = (1 << (col & 7));
  col = col >> 3;
  null_bitmap[col] &= ~bit_pos;
}

// get null bitmap status bit
inline unsigned char get_null_bitmap (unsigned char *null_bitmap, int col) {
  // unsigned char bit_pos = (1 << (col % 8));
  unsigned char bit_pos = (1 << (col & 7));
  // col = col / 8;
  col = col >> 3;
  return (null_bitmap[col] & bit_pos);
}
