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
#include "var_string.h"
#include "mmap/mmap_big_table.h"

vector<AttributeInfo> dummy_schema;

string nameToPath(const string &name, const string &ext) {
  size_t pos = name.rfind(ext);
  if (pos != string::npos)
    return name;
  return name + ext;
}

const string & rmPathSeperator(string &path) {
  if (!path.empty() && path.back() == '/') {
    path.resize(path.size() - 1);
  }
  return path;
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

string nameToTagBigTablePath(const string &name, const string &ext)
{
  return nameToObjectPath(name, ".pt");
}

string nameToEntityBigTablePath(const string &name, const string &ext)
{ return nameToObjectPath(name, ext); }


string genTempObjectPath(const string &src_path) {
  static std::atomic<size_t> ts_inc = 0;
  ostringstream oss;
  int pos = src_path.find('.');
  string src_head = src_path.substr(0, pos);
  int64_t t = ts_inc.fetch_add(1);
  oss << "_t_" << hex << t;

  return oss.str();
}

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

int normalizeString(char *s) {
  if (*s == 0)
    return 0;
  char quote = 0;
  size_t i = 0;
  size_t end = strlen(s);
  size_t back = strlen(s) - 1;

  if (end > 1) {      // at least 2 characters in the string
    if (s[0] == '\'' && s[back] == '\'') {
      quote = '\'';
      i++;
    }
    if (s[0] == '"' && s[back] == '"') {
      quote = '"';
      i++;
    }
    if (s[0] == '`' && s[back] == '`') {
      quote = '`';
      i++;
    }
    if (s[0] == '[' && s[back] == ']') {
      quote = '\\';
      i++;
    }
  }

  size_t j = 0;
  while (i <= back) {
    if (i == back) {
      if (!quote) {
        s[j] = s[i];
        j++;
      }
    } else {
      char si = s[i];
      if (si == '\\' && (i + 1 != back)) {
        char sn = s[i + 1];
        switch (sn) {
        case '0':
          s[j] = '\0';
          break;
        case 'b':
          s[j] = '\b';
          break;
        case 'n':
          s[j] = '\n';
          break;
        case 'r':
          s[j] = '\r';
          break;
        case 't':
          s[j] = '\t';
          break;
        case 'Z':
          s[j] = 0x26;
          break;
        case '\\':
        case '\'':
        case '"':
        case '`':
          s[j] = sn;
          break;
        default:
          s[j++] = '\\';
          s[j] = sn;
        }
        i++;
      } else {
        if (si == quote && (i + 1 != back) && (s[i + 1] == quote)) {
          i++;
        }
        s[j] = si;
      }
      j++;
    }
    i++;
  }
  s[j] = 0;
  return j;
}

void normalizeString(string &s) {
  int len = normalizeString((char *)s.c_str());
  s.resize(len);
}

string normalize(const string &s) {
  string ns = s.substr(0);
  normalizeString(ns);
  return ns;
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

string quoteString(const string &str, char quote) {
  return quote + str + quote;
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

int makeDirectory(const string &dir) {
  int ret_code = 0;
  if (!dir.empty()) {
    struct stat st;
    size_t e_pos = 1;
    char *path = (char *)dir.data();
    while(e_pos < dir.size()) {
      e_pos = dir.find_first_of('/', e_pos);
      if (e_pos != string::npos)
        path[e_pos] = 0;
      if (stat(path, &st) != 0) {
        if (mkdir(path,
          S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0)
          return -1;
        ret_code = 0;
      } else {
        if (!S_ISDIR(st.st_mode)) {
          return -1;
        } else
          ret_code = 1;
      }
      if (e_pos != string::npos)
        path[e_pos] = '/';
      else
        break;      // reach end of directory path
      e_pos++;
    }
  }
  return ret_code;
}

BigTable *CreateTempTable(const vector<AttributeInfo> &schema, const std::string &db_path,
                          int encoding,
                          ErrorInfo &err_info) {
  std::vector<std::string> key;
  int flags = MMAP_CREAT_EXCL;
  std::string bt_actual_path = genTempObjectPath("");
  BigTable *bt = reinterpret_cast<BigTable*>(new MMapBigTable());
  if (unlikely(bt == nullptr)) {
    LOG_ERROR("new MMapBigTable failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_path.c_str());
    return nullptr;
  }
  if (bt->open(bt_actual_path, db_path, "", flags, err_info) < 0) {
    LOG_ERROR("Open temporary table failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_path.c_str());
    delete bt;
    return nullptr;
  }
  if (bt->create(schema, key, "", "", "", "", "", encoding, err_info) < 0 ) {
    LOG_ERROR("Create temporary table failed, db_path: %s name: %s ", db_path.c_str(), bt_actual_path.c_str());
    delete bt;
    return nullptr;
  }
  return bt;
}

void DropTempTable(BigTable* bt, ErrorInfo &err_info) {
  if (bt) {
    bt->remove();
    delete bt;
  }
  return;
}

