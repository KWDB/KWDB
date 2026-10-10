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

#include "ee_field_common.h"

#include <cmath>
#include <codecvt>
#include <iostream>
#include <locale>
#include <regex>
#include <sstream>

#include "ee_field.h"
#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

std::unordered_map<roachpb::DataType, Field_result> reslut_map = {
    {roachpb::DataType::TIMESTAMP, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::TIMESTAMPTZ, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::TIMESTAMP_MICRO, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::TIMESTAMPTZ_MICRO, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::TIMESTAMP_NANO, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::TIMESTAMPTZ_NANO, Field_result::TIMESTAMP_RESULT},
    {roachpb::DataType::DATE, Field_result::INT_RESULT},
    {roachpb::DataType::SMALLINT, Field_result::INT_RESULT},
    {roachpb::DataType::INT, Field_result::INT_RESULT},
    {roachpb::DataType::BIGINT, Field_result::INT_RESULT},
    {roachpb::DataType::FLOAT, Field_result::REAL_RESULT},
    {roachpb::DataType::DOUBLE, Field_result::REAL_RESULT},
    {roachpb::DataType::DECIMAL, Field_result::DECIMAL_RESULT},
    {roachpb::DataType::BOOL, Field_result::INT_RESULT},
    {roachpb::DataType::CHAR, Field_result::STRING_RESULT},
    {roachpb::DataType::BINARY, Field_result::STRING_RESULT},
    {roachpb::DataType::NCHAR, Field_result::STRING_RESULT},
    {roachpb::DataType::VARCHAR, Field_result::STRING_RESULT},
    {roachpb::DataType::NVARCHAR, Field_result::STRING_RESULT},
    {roachpb::DataType::VARBINARY, Field_result::STRING_RESULT},
    {roachpb::DataType::NULLVAL, Field_result::NULL_RESULT}};

Field_result field_cmp_type(roachpb::DataType a, roachpb::DataType b);

bool IsStorageString(roachpb::DataType a) {
  switch (a) {
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARCHAR: {
      return true;
    }
    default: {
      return false;
    }
  }
}

Field_result ResolveResultType(roachpb::DataType type) {
  Field_result result = Field_result::INVALID_RESULT;
  switch (type) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL: {
      result = Field_result::INT_RESULT;
      break;
    }
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      result = Field_result::REAL_RESULT;
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      result = Field_result::STRING_RESULT;
      break;
    }
    case roachpb::DataType::DECIMAL: {
      result = Field_result::DECIMAL_RESULT;
      break;
    }
    default: {
      break;
    }
  }

  return result;
}

static std::string TrimZero(const std::string &str) {
  std::string res = str;
  res.erase(std::remove(res.begin(), res.end(), '\0'),
                res.end());
  return res;
}

Field_result field_cmp_type(roachpb::DataType a, roachpb::DataType b) {
  if (a == b) {
    return reslut_map[a];
  }

  Field_result result_a = reslut_map[a];
  Field_result result_b = reslut_map[b];

  if (result_a == Field_result::INT_RESULT &&
      result_b == Field_result::INT_RESULT) {
    return Field_result::INT_RESULT;
  }

  if (result_a == Field_result::INT_RESULT && result_b == kwdbts::NULL_RESULT) {
    return Field_result::INT_RESULT;
  }

  if (result_a == Field_result::DECIMAL_RESULT ||
      result_b == Field_result::DECIMAL_RESULT) {
    return Field_result::DECIMAL_RESULT;
  }

  if (result_a == Field_result::DECIMAL_RESULT &&
      result_b == kwdbts::NULL_RESULT) {
    return Field_result::DECIMAL_RESULT;
  }

  if (result_a == Field_result::STRING_RESULT ||
      result_b == Field_result::STRING_RESULT) {
    return Field_result::STRING_RESULT;
  }

  if (result_a == Field_result::STRING_RESULT &&
      result_b == kwdbts::NULL_RESULT) {
    return Field_result::STRING_RESULT;
  }

  if (result_a == Field_result::TIMESTAMP_RESULT &&
      result_b == Field_result ::TIMESTAMP_RESULT) {
    return TIMESTAMP_RESULT;
  }

  if (result_a == Field_result::TIMESTAMP_RESULT ||
      result_b == Field_result::TIMESTAMP_RESULT) {
    return INT_RESULT;
  }

  if (b == roachpb::NULLVAL && a == roachpb::NULLVAL) {
    return Field_result::NULL_RESULT;
  }

  return Field_result::REAL_RESULT;
}

bool ArgComparator::set_cmp_func(Field **left, Field **right) {
  left_ = left;
  right_ = right;
  roachpb::DataType sql_type_a = (*left)->get_storage_type();
  roachpb::DataType sql_type_b = (*right)->get_storage_type();
  Field_result type = field_cmp_type(sql_type_a, sql_type_b);
  switch (type) {
    case Field_result::INT_RESULT: {
      func_ = &ArgComparator::compare_int_signed;
      break;
    }
    case Field_result::REAL_RESULT: {
      if (sql_type_a == roachpb::DataType::DOUBLE || sql_type_b == roachpb::DataType::DOUBLE) {
        func_ = &ArgComparator::compare_real_double;
      } else {
        func_ = &ArgComparator::compare_real_float;
      }
      break;
    }
    case Field_result::DECIMAL_RESULT: {
      func_ = &ArgComparator::compare_real_double;
      break;
    }
    case Field_result::STRING_RESULT: {
      if (sql_type_a == roachpb::DataType::CHAR || sql_type_b == roachpb::DataType::CHAR) {
        func_ = &ArgComparator::compare_string;
      } else if (sql_type_a == roachpb::DataType::NCHAR ||
                 sql_type_b == roachpb::DataType::NCHAR) {
        func_ = &ArgComparator::compare_string;
      } else if (sql_type_a == roachpb::DataType::BINARY ||
                 sql_type_b == roachpb::DataType::BINARY) {
        func_ = &ArgComparator::compare_binary_char;
      } else if (sql_type_a == roachpb::DataType::VARBINARY ||
                 sql_type_b == roachpb::DataType::VARBINARY) {
        func_ = &ArgComparator::compare_binary_char;
      } else if (sql_type_a == roachpb::DataType::VARCHAR ||
                 sql_type_b == roachpb::DataType::VARCHAR) {
        func_ = &ArgComparator::compare_binary_string;
      } else if (sql_type_a == roachpb::DataType::NVARCHAR ||
                 sql_type_b == roachpb::DataType::NVARCHAR) {
        func_ = &ArgComparator::compare_binary_string;
      }
      break;
    }
    case Field_result::TIMESTAMP_RESULT: {
      func_ = &ArgComparator::compare_timestamp;
      switch (sql_type_a) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
        switch (sql_type_b) {
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
            timestamp_scale_a_ = 1000;
            timestamp_scale_b_ = 1;
            break;
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
            timestamp_scale_a_ = 1000000;
            timestamp_scale_b_ = 1;
            break;
        default:
          break;
        }
        break;
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
        switch (sql_type_b) {
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
            timestamp_scale_a_ = 1;
            timestamp_scale_b_ = 1000;
            break;
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
            timestamp_scale_a_ = 1000;
            timestamp_scale_b_ = 1;
            break;
        default:
          break;
        }
        break;
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
        switch (sql_type_b) {
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
            timestamp_scale_a_ = 1;
            timestamp_scale_b_ = 1000000;
            break;
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
            timestamp_scale_a_ = 1;
            timestamp_scale_b_ = 1000;
            break;
        default:
          break;
        }
        break;
      default:
        break;
      }
    }
  }

  return false;
}

int ArgComparator::compare_int_signed(char *ptr1, char *ptr2, k_bool is_null1,
                                      k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  k_int64 val1 = ptr1 ? (*left_)->ValInt(ptr1) : (*left_)->ValInt();
  k_int64 val2 = ptr2 ? (*right_)->ValInt(ptr2) : (*right_)->ValInt();

  return compare_int(val1, val2);
}

int ArgComparator::compare_real_float(char *ptr1, char *ptr2, k_bool is_null1,
                                      k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  k_double64 val_a = ptr1 ? (*left_)->ValReal(ptr1) : (*left_)->ValReal();
  k_double64 val_b = ptr2 ? (*right_)->ValReal(ptr2) : (*right_)->ValReal();

  return compare_float(val_a, val_b);
}

int ArgComparator::compare_real_double(char *ptr1, char *ptr2, k_bool is_null1,
                                       k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  k_double64 val_a = ptr1 ? (*left_)->ValReal(ptr1) : (*left_)->ValReal();
  k_double64 val_b = ptr2 ? (*right_)->ValReal(ptr2) : (*right_)->ValReal();

  return compare_float(val_a, val_b);
}

int ArgComparator::compare_string(char *ptr1, char *ptr2, k_bool is_null1,
                                  k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  roachpb::DataType sql_type_b = (*right_)->get_storage_type();
  if (sql_type_b == roachpb::NULLVAL) {
    return 1;
  }

  String s1 = ptr1 ? (*left_)->ValStr(ptr1) : (*left_)->ValStr();
  String s2 = ptr2 ? (*right_)->ValStr(ptr2) : (*right_)->ValStr();
  return compare_string(s1, s2);
}

int ArgComparator::compare_binary_char(char *ptr1, char *ptr2, k_bool is_null1,
                                       k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  String s1 = ptr1 ? (*left_)->ValStr(ptr1) : (*left_)->ValStr();
  String s2 = ptr2 ? (*right_)->ValStr(ptr2) : (*right_)->ValStr();

  return compare_binary_char(s1, s2);
}

k_int32 ArgComparator::compare_binary_string(char *ptr1, char *ptr2,
                                             k_bool is_null1, k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  String s1 = ptr1 ? (*left_)->ValStr(ptr1) : (*left_)->ValStr();
  String s2 = ptr2 ? (*right_)->ValStr(ptr2) : (*right_)->ValStr();
  return compare_binary_string(s1, s2);
}

int ArgComparator::compare_timestamp(char *ptr1, char *ptr2, k_bool is_null1,
                                     k_bool is_null2) {
  if (is_null1 || is_null2) {
    return compare_null(is_null1, is_null2);
  }

  k_int64 val1 = ptr1 ? (*left_)->ValInt(ptr1) : (*left_)->ValInt();
  k_int64 val2 = ptr2 ? (*right_)->ValInt(ptr2) : (*right_)->ValInt();

  return compare_timestamp(val1, val2);
}

int ArgComparator::compare_timestamp(k_int64 val1, k_int64 val2) {
  if (I64_SAFE_MUL_CHECK(val1, timestamp_scale_a_) &&
      I64_SAFE_MUL_CHECK(val2, timestamp_scale_b_)) {
        val1 *= timestamp_scale_a_;
        val2 *= timestamp_scale_b_;
    if (val1 < val2) return -1;
    if (val1 == val2) return 0;
    return 1;
  }
  EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                "Timestamp/TimestampTZ out of range");
  return 0;
}

int ArgComparator::compare_null(k_bool is_null1, k_bool is_null2) {
  if (is_null1 && is_null2) {
    return 0;
  } else if (is_null1) {
    return -1;
  } else {
    return 1;
  }
}

int ArgComparator::compare_int(k_int64 val1, k_int64 val2) {
  if (val1 < val2) return -1;
  if (val1 == val2) return 0;
  return 1;
}

int ArgComparator::compare_float(k_double64 val1, k_double64 val2) {
  if (std::isnan(val1)) {
    return std::isnan(val2) ? 0 : -1;
  }
  if (std::isnan(val2)) {
    return 1;
  }
  const k_double64 diff = val1 - val2;
  if (diff > FLT_COMPAR_TOL_FACTOR * FLT_EPSILON) {
    return 1;
  }
  if (diff < -FLT_COMPAR_TOL_FACTOR * FLT_EPSILON) {
    return -1;
  }
  return 0;
}

int ArgComparator::compare_string(const String &str1, const String &str2) {
  return str1.compare(str2);
}

int ArgComparator::compare_binary_char(const String &str1, const String &str2) {
  return str1.compare(str2);
}

int ArgComparator::compare_binary_string(const String &str1, const String &str2) {
  return str1.compare(str2);
}

bool LikeComparator::set_cmp_func(Field **left, Field **right) {
  left_ = left;
  right_ = right;
  roachpb::DataType sql_type_a = (*left)->get_storage_type();
  roachpb::DataType sql_type_b = (*right)->get_storage_type();
  Field_result type = field_cmp_type(sql_type_a, sql_type_b);
  switch (type) {
    case Field_result::STRING_RESULT: {
      if (sql_type_a == roachpb::DataType::NCHAR &&
          sql_type_b == roachpb::DataType::CHAR) {
        func_ = &LikeComparator::compare_nchar_char;
      } else if (sql_type_a == roachpb::DataType::CHAR || sql_type_b == roachpb::DataType::CHAR) {
        func_ = &LikeComparator::compare_char;
      } else if (sql_type_a == roachpb::DataType::NCHAR ||
                 sql_type_b == roachpb::DataType::NCHAR) {
        func_ = &LikeComparator::compare_string;
      } else if (sql_type_a == roachpb::DataType::BINARY ||
                 sql_type_b == roachpb::DataType::BINARY) {
        func_ = &LikeComparator::compare_binary_char;
      } else if (sql_type_a == roachpb::DataType::VARBINARY ||
                 sql_type_b == roachpb::DataType::VARBINARY) {
        func_ = &LikeComparator::compare_binary_char;
      } else if (sql_type_a == roachpb::DataType::VARCHAR ||
                 sql_type_b == roachpb::DataType::VARCHAR) {
        func_ = &LikeComparator::compare_binary_string;
      } else if (sql_type_a == roachpb::DataType::NVARCHAR ||
                 sql_type_b == roachpb::DataType::NVARCHAR) {
        func_ = &LikeComparator::compare_binary_string;
      }
      break;
    }
  }

  return false;
}

int LikeComparator::compare_string() {
  String s1 = (*left_)->ValStr();
  String s2 = (*right_)->ValStr();
  return compare_string({s1.getptr(), s1.length_}, {s2.getptr(), s2.length_});
}

int LikeComparator::compare_nchar_char() {
  String s1 = (*left_)->ValStr();
  String s2 = (*right_)->ValStr();
  return compare_nchar_char({s1.getptr(), s1.length_},
                            {s2.getptr(), s2.length_});
}

int LikeComparator::compare_char() {
  String s1 = (*left_)->ValStr();
  String s2 = (*right_)->ValStr();
  return compare_char({s1.getptr(), s1.length_}, {s2.getptr(), s2.length_});
}

static int
MatchText(const std::wstring &t, const std::wstring &p, bool is_case) {
  /* Fast path for match-everything pattern */
  if (p.size() == 1 && p[0] == L'%')
    return 1;

  size_t tlen = t.size();
  size_t plen = p.size();
  size_t tpos = 0;
  size_t ppos = 0;

  while (tpos < tlen && ppos < plen) {
    if (p[ppos] == L'\\') {
      /* Next pattern character must match literally, whatever it is */
      ppos++;
      /* ... and there had better be one */
      if (ppos >= plen) {
        // Handle error: LIKE pattern must not end with escape character
        return 0;
      }
      if (p[ppos] != t[tpos])
        return 0;
    } else if (p[ppos] == L'%') {
      wchar_t firstpat;
      ppos++;

      while (ppos < plen) {
        if (p[ppos] == L'%') {
          ppos++;
        } else if (p[ppos] == L'_') {
          /* If not enough text left to match the pattern, ABORT */
          if (tpos >= tlen)
            return 0;
          tpos++;
          ppos++;
        } else {
          break;
        }
      }
      if (ppos >= plen)
        return 1;

      firstpat = p[ppos];

      while (tpos < tlen) {
        if (t[tpos] == firstpat) {
          int matched = MatchText(t.substr(tpos), p.substr(ppos), is_case);

          if (matched != 0) {
            return matched; /* TRUE or ABORT */
          }
        }
        tpos++;
      }
      return 0;
    } else if (p[ppos] == L'_') {
      /* _ matches any single character, and we know there is one */
      tpos++;
      ppos++;
      continue;
    } else if ((p[ppos] != t[tpos]) && !is_case) {
      return 0;
    } else if ((towlower(p[ppos]) != towlower(t[tpos])) && is_case) {
      return 0;
    }

    /* Pattern and text match, so advance */
    tpos++;
    ppos++;
  }

  if (tpos < tlen)
    return 0;

  /*
   * End of text, but perhaps not of pattern.  Match iff the remaining
   * pattern can match a zero-length string, ie, it's zero or more %'s.
   */
  while (ppos < plen && p[ppos] == L'%')
    ppos++;
  if (ppos >= plen) {
    return 1;
  }

  /*
   * End of text with no match, so no point in trying later places to start
   * matching this pattern.
   */
  return 0;
}

int LikeComparator::compare_string(std::string &&str1, std::string &&str2) {
  typedef std::codecvt_utf8<wchar_t> convert_type;
  std::wstring_convert<convert_type, wchar_t> converter;
  // Convert UTF-8 encoded strings to wide character strings
  std::string strn = str1;

  std::wstring widestr = converter.from_bytes(strn);

  std::string strp = str2;

  std::wstring widestrp = converter.from_bytes(strp);
  if (MatchText(widestr, widestrp, is_case_)) {
    return 1;
  }
  return 0;
}

int LikeComparator::compare_nchar_char(std::string &&str1, std::string &&str2) {
    typedef std::codecvt_utf8<wchar_t> convert_type;
  std::wstring_convert<convert_type, wchar_t> converter;
  // Convert UTF-8 encoded strings to wide character strings
  std::string strn = str1;

  std::wstring widestr = converter.from_bytes(strn);

  std::string strp = str2;

  std::wstring widestrp = converter.from_bytes(strp);
  if (MatchText(widestr, widestrp, is_case_)) {
    return 1;
  }
  return 0;
}

int LikeComparator::compare_char(std::string &&str1, std::string &&str2) {
    typedef std::codecvt_utf8<wchar_t> convert_type;
  std::wstring_convert<convert_type, wchar_t> converter;
  // Convert UTF-8 encoded strings to wide character strings
  std::string strn = str1;

  std::wstring widestr = converter.from_bytes(strn);

  std::string strp = str2;

  std::wstring widestrp = converter.from_bytes(strp);
  if (MatchText(widestr, widestrp, is_case_)) {
    return 1;
  }
  return 0;
}

int LikeComparator::compare_binary_char() {
  String s1 = (*left_)->ValStr();
  String s2 = (*right_)->ValStr();
  return compare_binary_char({s1.getptr(), s1.length_},
                             {s2.getptr(), s2.length_});
}

int LikeComparator::compare_binary_char(const std::string &str1,
                                        const std::string &str2) {
    typedef std::codecvt_utf8<wchar_t> convert_type;
  std::wstring_convert<convert_type, wchar_t> converter;
  // Convert UTF-8 encoded strings to wide character strings
  std::string strn = str1;

  std::wstring widestr = converter.from_bytes(strn);

  std::string strp = str2;

  std::wstring widestrp = converter.from_bytes(strp);
  if (MatchText(widestr, widestrp, is_case_)) {
    return 1;
  }
  return 0;
}

k_int32 LikeComparator::compare_binary_string() {
  String s1 = (*left_)->ValStr();
  String s2 = (*right_)->ValStr();
  return compare_binary_string({s1.getptr(), s1.length_},
                               {s2.getptr(), s2.length_});
}

int LikeComparator::compare_binary_string(const std::string &str1,
                                          const std::string &str2) {
    typedef std::codecvt_utf8<wchar_t> convert_type;
  std::wstring_convert<convert_type, wchar_t> converter;
  // Convert UTF-8 encoded strings to wide character strings
  std::string strn = str1;

  std::wstring widestr = converter.from_bytes(strn);

  std::string strp = str2;

  std::wstring widestrp = converter.from_bytes(strp);
  if (MatchText(widestr, widestrp, is_case_)) {
    return 1;
  }
  return 0;
}

void LikeComparator::set_case(k_bool is_case) {
  is_case_ = is_case;
}

FieldInList::FieldInList(const std::list<Field **> &fields, size_t count)
    : count_(count), num_(fields.size()) {
  malloc_args(num_);
  k_int32 i = 0;
  for (auto it : fields) {
    args_[i] = it;
    ++i;
  }
}

FieldInList::~FieldInList() {
  if (nullptr != args_) {
    for (k_uint32 i = 0; i < num_; ++i) {
      SafeFreePointer(args_[i]);
    }
    SafeFreePointer(args_);
    args_ = nullptr;
    num_ = 0;
    count_ = 0;
  }
}

FieldInList *FieldInList::get_field_in_list(const std::list<Field **> &fields,
                                            size_t count) {
  FieldInList *in_list = new FieldInList(fields, count);
  return in_list;
}

void FieldInList::malloc_args(size_t sz) {
  args_ = static_cast<Field ***>(malloc(sz * sizeof(Field **)));
}

k_bool FieldInList::compare(Field **fields) {
  ArgComparator cmp;
  k_bool ret = 0;
  for (k_uint32 i = 0; i < num_; ++i) {
    Field **args = args_[i];
    for (k_uint32 j = 0; j < count_; ++j) {
      Field *field = fields[j];
      Field *arg = args[j];
      if (arg->CheckNull() || field->CheckNull()) {
        ret = -1;
        continue;
      }
      cmp.set_cmp_func(&field, &arg);
      if (cmp.compare(nullptr, nullptr, false, false) != 0) {
        ret = 1;
        break;
      } else {
        ret = 0;
      }
    }
    if (ret == 0) {
      break;
    }
  }

  return ret;
}

bool CacheFieldInt::cmp() {
  k_int64 val = field_->ValInt();

  if (val != value_) {
    value_ = val;
    return 1;
  }

  return 0;
}

void CacheFieldInt::clear() { value_ = 0; }

bool CacheFieldReal::cmp() {
  k_double64 val = field_->ValReal();

  if (val == value_) {
    value_ = val;
    return 1;
  }

  return 0;
}

void CacheFieldReal::clear() { value_ = 0.0; }

bool CacheFieldStr::cmp() {
  String s1 = field_->ValStr();
  std::string val = std::string(s1.getptr(), s1.length_);

  if (value_.compare(val)) {
    value_ = val;
    return 1;
  }

  return 0;
}

void CacheFieldStr::clear() { value_ = 0.0; }

}  // namespace kwdbts
