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

#include <sys/stat.h>
#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <netinet/in.h>
#include <parallel/algorithm>
#include "utils/big_table_utils.h"
#include "mmap/mmap_file.h"

vector<AttributeInfo> dummy_schema;

string nameToPath(const string &name, const string &ext) {
  size_t pos = name.rfind(ext);
  if (pos != string::npos)
    return name;
  return name + ext;
}

vector<AttributeInfo> & getDummySchema() { return dummy_schema; }

string getTsFilePath(const string &path) {
  string file_path = path;

  size_t pos = file_path.find_first_not_of('/');
  if (pos != 0 && pos != string::npos)
    file_path = file_path.substr(pos);
  return file_path;
}

string getTsObjectName(const string &path) {
  string fpath = getTsFilePath(path);
  size_t found;
  found = fpath.find_last_of('.');
  return (found == std::string::npos) ? fpath : fpath.substr(0, found);
}

string nameToObjectPath(const string &name, const string &ext) {
    return nameToPath(name, ext);
}

string nameToEntityBigTablePath(const string &name, const string &ext)
{ return nameToObjectPath(name, ext); }


int setInteger(int &n, const string &val_str, int min, int max) {
  int64_t lv;
  if (!isInteger(val_str.c_str(), lv))
    return -1;
  int v = stringToInt(val_str);
  if (v < min || v > max)
    return -1;
  n = v;
  return 0;
}

bool isInteger(const char *s, int64_t &i) {
  if(s == nullptr || ((!isdigit(s[0])) && (s[0] != '-') && (s[0] != '+')))
    return false;

  char *p ;
  i = (int64_t)strtol(s, &p, 10) ;
  return (*p == 0) ;
}


int getDataTypeSize(int type) {
  switch (type) {
    case STRING:
    case ROWID:
      return sizeof(IDTYPE);
    case DATETIME64:
      return sizeof(int64_t);
    case BOOL:
      return sizeof(bool);
    case BYTE:
    case INT8:
      return sizeof(int8_t);
    case INT16:
      return sizeof(int16_t);
    case INT32:
    case DATE32:
    case DATETIME32:
    case DATETIMEDOS:
    case TIMESTAMP:
    case TIME:
      return sizeof(int32_t);
    case INT64:
    case TIME64:
    case TIMESTAMP64:
    case TIMESTAMP64_MICRO:
    case TIMESTAMP64_NANO:
      return sizeof(int64_t);
    case FLOAT:
      return sizeof(float);
    case DOUBLE:
      return sizeof(double);
    case VARSTRING:
    case VARBINARY:
      return sizeof(intptr_t);
  }
  return 0;
}


int getDataTypeSize(AttributeInfo &info) {
  switch (info.type) {
    case STRING:
      if (info.max_len == 0)
        info.max_len = DEFAULT_STRING_MAX_LEN;
    case ROWID:
      return sizeof(IDTYPE);
    case DATE32:
    case DATETIMEDOS:
    case DATETIME32:
      return sizeof(int32_t);
    case DATETIME64:
      return sizeof(int64_t);
    case BOOL:
      return sizeof(bool);
    case BYTE:
    case INT8:
      return sizeof(int8_t);
    case INT16:
      return sizeof(int16_t);
    case INT32:
      return sizeof(int32_t);
    case INT64:
    case TIME64:
    case TIMESTAMP64:
    case TIMESTAMP64_MICRO:
    case TIMESTAMP64_NANO:
      return sizeof(int64_t);
    case FLOAT:
      return sizeof(float);
    case DOUBLE:
      return sizeof(double);
    case CHAR:
    case STRING_CONST:
    case BINARY:
      if (info.max_len == 0)
        info.max_len = DEFAULT_CHAR_MAX_LEN;
      return info.max_len;
    case VARSTRING:
    case VARBINARY:
      if (info.max_len == 0)
        info.max_len = DEFAULT_VARSTRING_MAX_LEN;
      return sizeof(intptr_t);
    case TIMESTAMP:
      return sizeof(int32_t);
    case TIME:          return sizeof(int32_t);
    case NULL_TYPE:     return 0;
  }
  return 0;
}

int setAttributeInfo(vector<AttributeInfo> &info) {
  int offset = 0;

  for (vector<AttributeInfo>::iterator it = info.begin(); it != info.end();
    ++it) {
    if (it->length <= 0)
      it->length = 1;
    if ((it->size = getDataTypeSize(*it)) == -1)
      return -1;
    if (it->max_len == 0)
      it->max_len = it->size;
    it->offset = offset;
    offset += it->size;
    if (it->type == STRING) {
#if defined(USE_SMART_INDEX)
      if ((encoding & SMART_INDEX) && !actual_dim.empty()) {
        it->encoding = SMART_INDEX;
      } else
#endif
      it->encoding = DICTIONARY;
    }
  }
  return offset;
}


string toString(const char *data)
{ return (data == nullptr) ? kwdbts::s_emptyString : string(data); }

string toString(const char *str, size_t len) {
  string s;
  s.resize(len);
  size_t i = 0;
  for (; i < len && *str != 0; ++i) {
    s[i] = *str++;
  }
  s[i] = 0;
  s.resize(i);
  return s;
}

string normalizePath(const string &path) {
  string rpath = path;
  size_t pos = 0;

  ///TODO: optimization
  while (true) {
    /* Locate the substring to replace. */
    size_t pos1 = rpath.find("./", pos);
    if (pos1 != string::npos) {
      rpath.replace(pos1, 2, "/");
    }
    size_t pos2 = rpath.find("//", pos);
    if (pos2 != string::npos) {
      rpath.replace(pos2, 2, "/");
    }
    pos = std::min(pos1, pos2);
    if (pos == string::npos)
      break;
  }

  return makeDirectoryPath(rpath);
}

string makeDirectoryPath(const string &tbl_sub_path) {
  string dir_path = tbl_sub_path;
  int size = dir_path.size();
  if (size > 0
    && (dir_path.at(size - 1) != kwdbts::EngineOptions::directorySeperator()))
    dir_path.push_back(kwdbts::EngineOptions::directorySeperator());
  return dir_path;
}
