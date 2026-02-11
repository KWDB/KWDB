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

#include <float.h>

#include <list>
#include <map>
#include <string>
#include "ee_pb_plan.pb.h"
#include "ee_decimal.h"
#include "ee_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_string.h"
#include "ee_field_common.h"

namespace kwdbts {

class TABLE;

class Field {
 public:
  enum Type {
    FIELD_UNKNOW = -1,

    FIELD_ITEM = 0,        // field
    FIELD_SUM = 1,         // agg func
    FIELD_CONSTANT = 2,    // constant
    FIELD_COND = 3,        // condition
    FIELD_ARITHMETIC = 4,  // operation
    FIELD_FUNC = 5,        // function
    FIELD_CMP = 6,         // compare
    FIELD_INTERVAL = 7,    // interval
    FIELD_TYPECAST = 8,    // cast
    FIELD_AGG = 9          // Aggregate calculation results
  };

  enum Sort {
    SORT_UNKNOW = -1,

    SORT_ASC = 0,
    SORT_DESC = 1
  };

 public:
  Field() {}
  explicit Field(k_uint32 num, roachpb::DataType storage_type, k_uint32 length,
                 Type type)
      : num_(num),
        sql_type_(storage_type),
        storage_type_(storage_type),
        storage_len_(length),
        type_{type} {}

  virtual ~Field() {}

  TABLE *table_{nullptr};  // The table object to which the field belongs

 public:
  virtual k_int64 ValInt() = 0;
  virtual k_int64 ValInt(k_char *ptr) = 0;
  virtual k_int64 ValInt(k_int64 *val, k_bool negative);
  virtual k_double64 ValReal() = 0;
  virtual k_double64 ValReal(k_char *ptr) = 0;
  virtual String ValStr() = 0;
  virtual String ValStr(k_char *ptr) = 0;
  virtual struct CKDecimal ValDecimal();
  virtual Field *field_to_copy() = 0;
  virtual k_bool fill_template_field(char *ptr) = 0;
  virtual char *get_ptr() = 0;
  virtual char *get_ptr(RowBatch *batch) { return nullptr; }
  virtual k_uint32 field_in_template_length();
  virtual k_bool is_nullable();
  virtual k_bool is_condition_met();
  virtual k_bool is_over_flow() { return false; }
  virtual k_bool is_real_over_flow() { return false; }
  virtual k_uint16 ValStrLength(k_char *ptr) { return 0; }
  String ValTempStr(char *ptr);
  virtual String ValStrFromBatch(RowBatch* batch) {
    char *ptr = get_ptr(batch);
    if (!ptr) {
      return String();
    }
    String str(storage_len_);
    memcpy(str.getptr(), ptr, storage_len_);
    return str;
  }
  void set_num(k_uint32 num) { num_ = num; }
  k_uint32 get_num() const { return num_; }

  void set_sql_type(roachpb::DataType type) { sql_type_ = type; }
  roachpb::DataType get_sql_type() const { return sql_type_; }

  void set_storage_type(roachpb::DataType type) { storage_type_ = type; }
  roachpb::DataType get_storage_type() const { return storage_type_; }

  void set_storage_length(k_uint32 len) { storage_len_ = len; }
  k_uint32 get_storage_length() const { return storage_len_; }

  void set_column_offset(k_uint64 offset) { col_offset_ = offset; }
  k_uint64 get_column_offset() const { return col_offset_; }

  void set_variable_length_type(roachpb::VariableLengthType type) {
    variable_length_type_ = type;
  }
  roachpb::VariableLengthType get_variable_length_type() const {
    return variable_length_type_;
  }

  void set_column_type(roachpb::KWDBKTSColumn::ColumnType type) {
    column_type_ = type;
  }
  roachpb::KWDBKTSColumn::ColumnType get_column_type() const {
    return column_type_;
  }

  void set_return_type(KWDBTypeFamily type) { return_type_ = type; }
  KWDBTypeFamily get_return_type() const { return return_type_; }

  void set_field_type(Type type) { type_ = type; }
  Type get_field_type() const { return type_; }

  // void set_field_result(Field_result result) { field_result_ = result; }
  // Field_result get_field_result() const { return field_result_; }

  void set_field_statistic(k_bool ret) { is_statistic_ = ret; }
  k_bool get_field_statistic() const { return is_statistic_; }

  void set_clearup_diff(k_bool ret) { is_clear_ = ret; }

  virtual void backup() {}
  virtual void restore() {}
  virtual void reset() {}

 protected:
  struct CKDecimal int_to_decimal();
  struct CKDecimal double_to_decimal();

 public:
  k_int32 group_by_copy_in_aggs_index_{-1};   // the index of column (group by column)
  bool is_chunk_{false};    // Whether to read data from databatch
  Field *next_{nullptr};  // It is used to handle cases where there are multiple
                          // parameters in and Func

 protected:
  k_uint32 num_{0};                          // col num
  k_uint32 col_idx_in_rs_{0};                 // col index in result set
  roachpb::DataType sql_type_{roachpb::UNKNOWN};                        // sql type
  roachpb::DataType storage_type_{roachpb::UNKNOWN};                    // storage type
  k_uint32 storage_len_{0};                  // storage len
  k_uint64 col_offset_{0};                   // col offset
  roachpb::VariableLengthType variable_length_type_{roachpb::ColStorageTypeTuple};  // variable len type
  roachpb::KWDBKTSColumn::ColumnType column_type_{roachpb::KWDBKTSColumn::TYPE_DATA};                   // column type
  KWDBTypeFamily return_type_{AnyFamily};               // return type
  Type type_{FIELD_UNKNOW};                  // FIELD type
  k_bool is_statistic_{false};               // use statistic count
  bool allow_null_{true};
  k_bool is_clear_{false};               // check

 public:
  [[nodiscard]] bool CheckNull() {
    return allow_null_ && is_nullable();
  }

  void set_allow_null(bool nullable) {
    allow_null_ = nullable;
  }

  bool is_allow_null() { return allow_null_; }

  void setColIdxInRs(k_uint32 colIdxInRs) {
      col_idx_in_rs_ = colIdxInRs;
  }

  [[nodiscard]] k_uint32 getColIdxInRs() const {
    return col_idx_in_rs_;
  }
};

// FIELD_ITEM basic class
class FieldNum : public Field {
  typedef Field super;

 public:
  FieldNum() { type_ = FIELD_ITEM; }
  explicit FieldNum(k_uint32 num, roachpb::DataType type, k_uint32 length)
      : super(num, type, length, FIELD_ITEM) {}
  virtual ~FieldNum() {}
  k_bool is_nullable() override;
  char *get_ptr() override;
  char *get_ptr(RowBatch *batch) override {
    return batch->GetData(col_idx_in_rs_, storage_len_, column_type_, storage_type_);
  }
  String ValStrFromBatch(RowBatch* batch) override;
  k_bool fill_template_field(char *ptr) override;
  k_bool is_over_flow() override;
};

// char
class FieldChar : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
  Field *field_to_copy() override;
  String ValStrFromBatch(RowBatch* batch) override;
};

// nchar
class FieldNchar : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;

  Field *field_to_copy() override;
  String ValStrFromBatch(RowBatch* batch) override;
};

// bool
class FieldBool : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;

  Field *field_to_copy() override;
};

// short
class FieldShort : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(k_char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(k_char *ptr) override;
  String ValStr() override;
  String ValStr(k_char *ptr) override;
  Field *field_to_copy() override;
  k_bool fill_template_field(char *ptr) override;
};

// int
class FieldInt : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// longlong
class FieldLonglong : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

typedef FieldLonglong FieldTimestamp;

class FieldTimestampTZ : public FieldNum {
 public:
  using FieldNum::FieldNum;

  char *get_ptr() override;
  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// float
class FieldFloat : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// double
class FieldDouble : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// handle decimal column in DataChunk
class FieldDecimal : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// statistic sum
class FieldSumInt : public FieldNum {
 public:
  using FieldNum::FieldNum;
  explicit FieldSumInt(k_uint32 num, roachpb::DataType storage_type, k_uint32 storage_len)
    : FieldNum(num, storage_type, storage_len) {
    if (storage_type >= roachpb::DataType::SMALLINT && storage_type <= roachpb::DataType::BIGINT) {
      storage_type_ = roachpb::DataType::DECIMAL;
    }
    if (storage_type == roachpb::DataType::FLOAT) {
      storage_type_ = roachpb::DataType::DOUBLE;
    }
  }

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
  k_bool is_real_over_flow() override;
};

// blob
class FieldBlob : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
};

// varchar
class FieldVarchar : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  virtual String ValStr(char *ptr);
  virtual Field *field_to_copy();
  virtual String ValStrFromBatch(RowBatch* batch);
};

class FieldTagVarchar : public FieldVarchar {
 public:
  using FieldVarchar::FieldVarchar;
  String ValStr(char *ptr) override;
  String ValStrFromBatch(RowBatch* batch) override;
  Field *field_to_copy() override {
    FieldTagVarchar *field = new FieldTagVarchar(*this);
    field->is_chunk_ = false;
    return field;
  }
};

// varblob
class FieldVarBlob : public FieldNum {
 public:
  using FieldNum::FieldNum;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  Field *field_to_copy() override;
  k_uint16 ValStrLength(char *ptr) override;
  String ValStrFromBatch(RowBatch* batch) override;
};

typedef FieldVarBlob FieldNvarchar;


class FieldFuncBase : public Field {
  typedef Field super;

 public:
  FieldFuncBase() : args_(embedded_arguments_) { type_ = FIELD_FUNC; }

  explicit FieldFuncBase(Field *a) : args_(embedded_arguments_) {
    type_ = FIELD_FUNC;
    args_[0] = a;
    arg_count_ = 1;
  }

  FieldFuncBase(Field *a, Field *b) : args_(embedded_arguments_) {
    type_ = FIELD_FUNC;
    args_[0] = a;
    args_[1] = b;
    arg_count_ = 2;
  }

  explicit FieldFuncBase(const std::list<Field *> &fields);

  virtual ~FieldFuncBase();

 private:
  k_bool alloc_args(size_t sz);

 protected:
  Field **args_{nullptr};
  Field *embedded_arguments_[2] = {nullptr, nullptr};
  k_int32 arg_count_{0};
};

class FieldFunc : public FieldFuncBase {
  typedef FieldFuncBase super;

 public:
  using FieldFuncBase::FieldFuncBase;

  enum Functype {
    UNKNOWN_FUNC = -1,
    PLUS_FUNC = 1,
    MINUS_FUNC,
    MULT_FUNC,
    DIV_FUNC,
    DIVZ_FUNC,
    REMAINDER_FUNC,
    PERCENT_FUNC,
    POWER_FUNC,
    ANDCAL_FUNC,
    ORCAL_FUNC,
    NOTCAL_FUNC,
    REGEX_FUNC,
    MOD_FUNC,
    EQ_FUNC,
    NOT_EQ_FUNC,
    LE_FUNC,
    LE_EQ_FUNC,
    GT_FUNC,
    GT_EQ_FUNC,
    COND_AND_FUNC,
    COND_OR_FUNC,
    IN_FUNC,
    LIKE_FUNC,
    NOW_FUNC,
    TIME_BUCKET_FUNC,
    TIME_OF_DAY_FUNC,
    AGE_FUNC,
    CAST_CHECK_TS,
    CURRENT_DATE_FUNC,
    CURRENT_TIMESTAMP_FUNC,
    DATE_TRUNC_FUNC,
    EXTRACT_FUNC,
    STRFTIME_FUNC,
    IS_NULL_FUNC,
    IS_UNKNOWN_FUNC,
    IS_NAN_FUNC,
    CEIL_FUNC,
    FLOOR_FUNC,
    CASE_FUNC,
    ABS_FUNC,
    THEN_FUNC,
    ELSE_FUNC,
    COALESCE_FUNC,
    LOG_FUNC,
    SQRT_FUNC,
    ROUND_FUNC,
    SIN_FUNC,
    COS_FUNC,
    TAN_FUNC,
    ASIN_FUNC,
    ACOS_FUNC,
    ATAN_FUNC,
    LENGTH_FUNC,
    GETBIT_FUNC,
    INITCAP_FUNC,
    CONCAT_FUNC,
    ISNAN_FUNC,
    LN_FUNC,
    SIGN_FUNC,
    TRUNC_FUNC,
    RADIANS_FUNC,
    RANDOM_FUNC,
    WIDTHBUCKET_FUNC,
    COT_FUNC,
    ATAN2_FUNC,
    CBRT_FUNC,
    CRC32C_FUNC,
    CRC32I_FUNC,
    DEGRESS_FUNC,
    EXP_FUNC,
    DIVMATH_FUNC,
    LTRIM_FUNC,
    RTRIM_FUNC,
    LPAD_FUNC,
    RPAD_FUNC,
    SUBSTR_FUNC,
    LOWER_FUNC,
    FNV32_FUNC,
    FNV32A_FUNC,
    FNV64_FUNC,
    FNV64A_FUNC,
    ENCODE_FUNC,
    UPPER_FUNC,
    LEFT_FUNC,
    RIGHT_FUNC,
    ANY_FUNC,
    ALL_FUNC,
    NOT_FUNC,
    DIFF_FUNC,
    WINDOW_GROUP_FUNC
  };

  k_int64 ValInt() override { return 0; }
  k_double64 ValReal() override { return 0.0; }
  String ValStr() override { return String(""); }
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal(char *ptr) override;
  String ValStr(char *ptr) override;

  char *get_ptr() override;
  k_bool is_nullable() override;
  k_bool fill_template_field(char *ptr) override;

  virtual enum Functype functype() = 0;

 protected:
  virtual k_bool field_is_nullable();
};

class FieldSumStatisticTagSum : public FieldFuncBase {
 public:
  explicit FieldSumStatisticTagSum(Field *field) : FieldFuncBase(field) {
    // sql_type_ = roachpb::DataType::DOUBLE;
    sql_type_ = field->get_storage_type();
    storage_len_ = sizeof(k_int64);
    // field_result_ = Field_result::INT_RESULT;
    field_result_ = ResolveResultType(field->get_storage_type());
    storage_type_ =
        (field_result_ == Field_result::INT_RESULT ? roachpb::DataType::DECIMAL
                                                   : roachpb::DataType::DOUBLE);
  }

  char *get_ptr() override;
  k_bool is_nullable() override;
  Field *field_to_copy() override;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  k_uint32 field_in_template_length() override;
  k_bool fill_template_field(char *ptr) override;
  k_bool is_over_flow() override;

 private:
  Field_result field_result_{Field_result::INVALID_RESULT};
};

class FieldSumStatisticTagCount : public FieldFuncBase {
 public:
  explicit FieldSumStatisticTagCount(Field *field) : FieldFuncBase(field) {
    sql_type_ = roachpb::DataType::DOUBLE;
    storage_len_ = sizeof(k_int64);
    field_result_ = Field_result::INT_RESULT;
    storage_type_ =
        (field_result_ == Field_result::INT_RESULT ? roachpb::DataType::BIGINT
                                                   : roachpb::DataType::DOUBLE);
  }

  char *get_ptr() override;
  k_bool is_nullable() override;
  Field *field_to_copy() override;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;
  k_uint32 field_in_template_length() override;
  k_bool fill_template_field(char *ptr) override;

 private:
  Field_result field_result_{Field_result::INVALID_RESULT};
};


class FieldCache : public Field {
 public:
  explicit FieldCache(Field *field) {
    field_ = field;
    sql_type_ = field->get_sql_type();
    storage_type_ = field->get_storage_type();
    storage_len_ = field->get_storage_length();
    col_offset_ = field->get_column_offset();
    variable_length_type_ = field->get_variable_length_type();
    column_type_ = field->get_column_type();
    return_type_ = field->get_return_type();
    type_ = field->get_field_type();
  }

  Field *field_to_copy() override;
  char *get_ptr() override;

  static FieldCache *get_cache(Field *field);

 protected:
  Field *field_{nullptr};
};

class FieldCacheInt : public FieldCache {
 public:
  using FieldCache::FieldCache;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;

  k_bool fill_template_field(char *ptr) override;
};

class FieldCacheReal : public FieldCache {
 public:
  using FieldCache::FieldCache;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;

  k_bool fill_template_field(char *ptr) override;
};

class FieldCacheStr : public FieldCache {
 public:
  using FieldCache::FieldCache;

  k_int64 ValInt() override;
  k_int64 ValInt(char *ptr) override;
  k_double64 ValReal() override;
  k_double64 ValReal(char *ptr) override;
  String ValStr() override;
  String ValStr(char *ptr) override;

  k_bool fill_template_field(char *ptr) override;
};

}  // namespace kwdbts
