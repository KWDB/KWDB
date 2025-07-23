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
#include <string>
#include <list>
#include <map>
#include "ee_pb_plan.pb.h"
#include "me_metadata.pb.h"
#include "kwdb_type.h"
#include "ee_string.h"

namespace kwdbts {

class Field;
class ArgComparator;
class LikeComparator;

enum Field_result {
  INVALID_RESULT = -1, /** not valid */
  STRING_RESULT = 0,   /** char* */
  REAL_RESULT,         /** double */
  INT_RESULT,          /** long long */
  DECIMAL_RESULT,      /** char *, to be converted to/from a decimal */
  NULL_RESULT,          /** NULL */
  TIMESTAMP_RESULT,    /** long long , Unified timestamp accuracy */
};

bool IsStorageString(roachpb::DataType a);

Field_result ResolveResultType(roachpb::DataType type);

#define FLT_COMPAR_TOL_FACTOR 4
#define FLT_EQUAL(_x, _y) \
  (fabs((_x) - (_y)) <= (FLT_COMPAR_TOL_FACTOR * FLT_EPSILON))
#define FLT_GREATER(_x, _y) (!FLT_EQUAL((_x), (_y)) && ((_x) > (_y)))
#define FLT_LESS(_x, _y) (!FLT_EQUAL((_x), (_y)) && ((_x) < (_y)))
#define FLT_GREATEREQUAL(_x, _y) (FLT_EQUAL((_x), (_y)) || ((_x) > (_y)))
#define FLT_LESSEQUAL(_x, _y) (FLT_EQUAL((_x), (_y)) || ((_x) < (_y)))

#define COMPARE_CHECK_NULL                                  \
  if (args_[0]->CheckNull() || args_[1]->CheckNull()) { \
    return 0;                                               \
  }
#define NextByte(p, plen) ((p)++, (plen)--)
#define GETCHAR(t) (t)
/* Set up to compile like_match.c for multibyte characters */
#define CHAREQ(p1, p2) wchareq((p1), (p2))
#define NextChar(p, plen) NextByte((p), (plen))

template <typename T>
inline T pointer_cast(void *p) {
  return static_cast<T>(p);
}

template <typename T>
inline const T pointer_cast(const void *p) {
  return static_cast<T>(p);
}

template <class T, size_t N>
constexpr size_t array_elements(T (&)[N]) noexcept {
  return N;
}

typedef int (ArgComparator::*arg_cmp_func)(char *ptr1, char *ptr2,
                                           k_bool is_null1, k_bool is_null2);
// ArgComparator for compare
class ArgComparator {
 public:
  ArgComparator() = default;
  ArgComparator(Field **left, Field **right) : left_(left), right_(right) {}

  bool set_cmp_func(Field **left, Field **right);

  inline int compare(char *ptr1, char *ptr2, k_bool is_null1, k_bool is_null2) {
    return (this->*func_)(ptr1, ptr2, is_null1, is_null2);
  }

  int compare_int_signed(char *ptr1, char *ptr2, k_bool is_null1,
                         k_bool is_null2);  // compare args[0] & args[1]
  int compare_real_float(char *ptr1, char *ptr2, k_bool is_null1,
                         k_bool is_null2);  // compare args[0] & args[1]
  int compare_real_double(char *ptr1, char *ptr2, k_bool is_null1,
                          k_bool is_null2);  // compare args[0] & args[1]
  int compare_string(char *ptr1, char *ptr2, k_bool is_null1,
                     k_bool is_null2);  // compare args[0] & args[1]
  int compare_binary_char(char *ptr1, char *ptr2, k_bool is_null1,
                          k_bool is_null2);  // compare args[0] & args[1]
  int compare_binary_string(char *ptr1, char *ptr2, k_bool is_null1,
                            k_bool is_null2);  // compare args[0] & args[1]
  int compare_timestamp(char *ptr1, char *ptr2, k_bool is_null1, k_bool is_null2);

 private:
  int compare_null(k_bool is_null1, k_bool is_null2);
  int compare_int(k_int64 val1, k_int64 val2);
  int compare_float(k_double64 val1, k_double64 val2);
  int compare_string(const String &str1, const String &str2);
  int compare_binary_char(const String &str1, const String &str2);
  int compare_binary_string(const String &str1, const String &str2);
  int compare_timestamp(k_int64 val1, k_int64 val2);

 private:
  Field **left_{nullptr};
  Field **right_{nullptr};
  arg_cmp_func func_{nullptr};

  // Used for unifying time accuracy
  k_int64 timestamp_scale_a_{1};
  k_int64 timestamp_scale_b_{1};
};

typedef int (LikeComparator::*like_cmp_func)();

class LikeComparator {
 public:
  LikeComparator() = default;
  LikeComparator(Field **left, Field **right) : left_(left), right_(right) {}

  bool set_cmp_func(Field **left, Field **right);
  //
  //  bool set_template_cmp_func(Field **field);

  inline int likecompare() { return (this->*func_)(); }

  //  int compare_int_signed();     // compare args[0] & args[1]
  //  int compare_real_float();     // compare args[0] & args[1]
  //  int compare_real_double();    // compare args[0] & args[1]
  int compare_string();         // compare args[0] & args[1]
  int compare_nchar_char();         // compare args[0] & args[1]
  int compare_char();         // compare args[0] & args[1]
  int compare_binary_char();    // compare args[0] & args[1]
  int compare_binary_string();  // compare args[0] & args[1]

  //  int compare_int_signed_in_template();
  //  int compare_real_float_in_template();
  //  int compare_real_double_in_template();
  //  int compare_string_in_template();
  //  int compare_binary_char_in_template();
  //  int compare_binary_string_in_template();
  void set_case(k_bool is_case);

 private:
  //  int compare_int(k_int64 val1, k_int64 val2);
  //  int compare_float(k_double64 val1, k_double64 val2);
  int compare_string(std::string &&str1, std::string &&str2);
  int compare_nchar_char(std::string &&str1, std::string &&str2);
  int compare_char(std::string &&str1, std::string &&str2);
  int compare_binary_char(const std::string &str1, const std::string &str2);
  int compare_binary_string(const std::string &str1, const std::string &str2);

 private:
  Field **left_{nullptr};
  Field **right_{nullptr};
  k_bool is_case_{0};
  like_cmp_func func_{nullptr};
};

class FieldInList {
 public:
  explicit FieldInList(const std::list<Field **> &fields, size_t count);

  virtual ~FieldInList();

  static FieldInList *get_field_in_list(const std::list<Field **> &fields,
                                        size_t count);

  k_bool compare(Field **fields);

 private:
  void malloc_args(size_t sz);

 protected:
  Field ***args_{nullptr};
  k_uint32 count_{0};  // inner count
  k_uint32 num_{0};    // outer count
  // ArgComparator cmp_;
};

// Cache base class
// cache the tmp result
class CacheField {
 protected:
  explicit CacheField(Field *field) : field_(field) {}

 public:
  virtual ~CacheField() = default;
  virtual bool cmp() = 0;
  virtual void clear() = 0;
  Field *getField() { return field_; }
  void SetTemplateField(Field *field) { field_in_tmp_table_ = field; }
  Field *GetTemplateField() { return field_in_tmp_table_; }

 public:
  Field *field_in_tmp_table_{nullptr};
  Field *field_{nullptr};  // The field whose value to cache.
};

// int cache
class CacheFieldInt : public CacheField {
 public:
  explicit CacheFieldInt(Field *field) : CacheField(field), value_(0) {}

  bool cmp() override;
  void clear() override;

 private:
  k_int64 value_;
};

// real cache
class CacheFieldReal : public CacheField {
 public:
  explicit CacheFieldReal(Field *field) : CacheField(field), value_(0.0) {}

  bool cmp() override;
  void clear() override;

 private:
  k_double64 value_;
};

class CacheFieldStr : public CacheField {
 public:
  explicit CacheFieldStr(Field *field) : CacheField(field) {}

  bool cmp() override;
  void clear() override;

 private:
  std::string value_;
};

/* A virtual column object in the form of "@1" in the operator */
class VirtualField {
 public:
  explicit VirtualField(k_uint32 num) { args_.push_back(num); }

  explicit VirtualField(std::list<k_uint32> args) { args_.swap(args); }
  explicit VirtualField(std::string func_arg) {
    func_args_.push_back(func_arg);
  }

  explicit VirtualField(std::list<std::string> func_args_) {
    func_args_.swap(func_args_);
  }

  std::list<k_uint32> args_;
  std::list<std::string> func_args_;
};

}  // namespace kwdbts
