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

#include <list>
#include <string>

#include "ee_field.h"
#include "ee_field_common.h"
#include "me_metadata.pb.h"

namespace kwdbts {

class FieldFuncBool : public FieldFunc {
 public:
  FieldFuncBool() : FieldFunc() {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  explicit FieldFuncBool(Field *a) : FieldFunc(a) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  FieldFuncBool(Field *a, Field *b) : FieldFunc(a, b) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  explicit FieldFuncBool(const std::list<Field *> &fields) : FieldFunc(fields) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }
};

class FieldFuncComparison : public FieldFuncBool {
 public:
  FieldFuncComparison(Field *a, Field *b) : FieldFuncBool(a, b) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
    cmp.set_cmp_func(&args_[0], &args_[1]);
  }
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  ArgComparator cmp;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncEq : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return EQ_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncNotEq : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return NOT_EQ_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncLess : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return LE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncLessEq : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return LE_EQ_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncGt : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return GT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldFuncGtEq : public FieldFuncComparison {
 public:
  using FieldFuncComparison::FieldFuncComparison;

  enum Functype functype() override { return GT_EQ_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;

  Field *field_to_copy() override;
};

class FieldLikeComparison : public FieldFuncBool {
 public:
  FieldLikeComparison(Field *a, Field *b, k_bool is_negation, k_bool is_case)
      : FieldFuncBool(a, b), negation_(is_negation), is_case_(is_case) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
    cmp.set_cmp_func(&args_[0], &args_[1]);
  }

  k_bool negation_{0};
  k_bool is_case_{0};

  LikeComparator cmp;
};

class FieldFuncLike : public FieldLikeComparison {
 public:
  using FieldLikeComparison::FieldLikeComparison;

  enum Functype functype() override { return LIKE_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldCond : public FieldFuncBool {
 public:
  explicit FieldCond(Field *a) : FieldFuncBool() {
    list_.push_back(a);
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
    type_ = FIELD_COND;
  }

  explicit FieldCond(Field *a, Field *b) : FieldFuncBool() {
    list_.push_back(a);
    list_.push_back(b);
    type_ = FIELD_COND;
  }

  explicit FieldCond(std::list<Field *> fields) : FieldFuncBool() {
    for (auto *it : fields) {
      list_.push_back(it);
    }
    type_ = FIELD_COND;
  }

  void add(Field *a) { list_.push_back(a); }
  std::list<Field *> list_;
};

class FieldCondAnd : public FieldCond {
 public:
  using FieldCond::FieldCond;

  enum Functype functype() override { return COND_AND_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldCondOr : public FieldCond {
 public:
  using FieldCond::FieldCond;

  enum Functype functype() override { return COND_OR_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
  k_double64 doublevalue_{0.0};
  String strvalue_{""};
};

class FieldFuncOptNeg : public FieldFuncBool {
 public:
  FieldFuncOptNeg(Field *field, const std::list<Field **> &values, size_t count,
                  k_bool is_negation)
      : FieldFuncBool(field), negation_(is_negation) {
    fix_fields(values, count);
  }

  FieldFuncOptNeg(const std::list<Field *> &fields,
                  const std::list<Field **> &values, size_t count,
                  k_bool is_negation, k_bool have_null)
      : FieldFuncBool(fields), negation_(is_negation), have_null_(have_null) {
    fix_fields(values, count);
  }

  virtual ~FieldFuncOptNeg();

  void fix_fields(const std::list<Field **> &values, size_t count);

  k_bool negation_{0};
  k_bool have_null_{0};
  FieldInList *in_list_{nullptr};
};

class FieldFuncIn : public FieldFuncOptNeg {
 public:
  using FieldFuncOptNeg::FieldFuncOptNeg;

  enum Functype functype() override { return IN_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldCondIsNull : public FieldFuncBool {
 public:
  explicit FieldCondIsNull(Field *a, k_bool is_negation)
      : FieldFuncBool(a), negation_(is_negation) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return IS_NULL_FUNC; }
  k_bool is_nullable() override { return KFALSE; }
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

  k_bool negation_{0};

 protected:
  k_int64 intvalue_{0};
};

class FieldCondIsUnknown : public FieldFuncBool {
 public:
  explicit FieldCondIsUnknown(Field *a, k_bool is_negation)
      : FieldFuncBool(a), negation_(is_negation) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return IS_UNKNOWN_FUNC; }
  k_bool is_nullable() override { return KFALSE; }
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

  k_bool negation_{0};

 protected:
  k_int64 intvalue_{0};
};

class FieldCondIsNan : public FieldFuncBool {
 public:
  explicit FieldCondIsNan(Field *a, k_bool is_negation)
      : FieldFuncBool(a), negation_(is_negation) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return IS_NAN_FUNC; }
  k_bool is_nullable() override { return KFALSE; }
  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

  k_bool negation_{0};

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncRegex : public FieldFuncBool {
 public:
  FieldFuncRegex(Field *a, Field *b, k_bool is_negation, k_bool is_case)
      : FieldFuncBool(a, b), negation_(is_negation), is_case_(is_case) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return REGEX_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;
  Field *field_to_copy() override;

 protected:
  k_bool field_is_nullable() override;

  k_bool negation_{0};
  k_bool is_case_{0};
  k_int64 intvalue_{0};
};

class FieldFuncAny : public FieldFuncBool {
 public:
  using FieldFuncBool::FieldFuncBool;
  explicit FieldFuncAny(const std::list<Field *> &fields)
      : FieldFuncBool(fields) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return ANY_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncAll : public FieldFuncBool {
 public:
  using FieldFuncBool::FieldFuncBool;
  explicit FieldFuncAll(const std::list<Field *> &fields) : FieldFuncBool(fields) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return ALL_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};

class FieldFuncNot : public FieldFuncBool {
 public:
  using FieldFuncBool::FieldFuncBool;
  explicit FieldFuncNot(const std::list<Field *> &fields) : FieldFuncBool(fields) {
    type_ = FIELD_CMP;
    sql_type_ = roachpb::DataType::BOOL;
    storage_type_ = roachpb::DataType::BOOL;
    storage_len_ = sizeof(bool);
  }

  enum Functype functype() override { return NOT_FUNC; }

  k_int64 ValInt() override;
  k_double64 ValReal() override;
  String ValStr() override;
  char *get_ptr() override { return nullptr; }
  char *get_ptr(RowBatch *batch) override;

  Field *field_to_copy() override;

 protected:
  k_int64 intvalue_{0};
};
}  // namespace kwdbts
