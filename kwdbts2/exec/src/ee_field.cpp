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

#include "ee_field.h"

#include "ee_field_common.h"
#include "ee_global.h"
#include "ee_kwthd_context.h"
#include "ee_table.h"

namespace kwdbts {
extern std::unordered_map<roachpb::DataType, Field_result> reslut_map;

struct CKDecimal Field::ValDecimal() {
  switch (storage_type_) {
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
      return int_to_decimal();
    }
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      return double_to_decimal();
    }
    default: {
      return CKDecimal();
    }
  }
}

k_int64 Field::ValInt(k_int64 *val, k_bool negative) { return this->ValInt(); }

k_bool Field::is_nullable() {
  return current_thd->GetRowBatch()->IsNull(col_idx_in_rs_,
                                                       column_type_);
}

k_uint32 Field::field_in_template_length() {
  if (IsStorageString(storage_type_)) {
    return storage_len_ + 2;
  }

  return storage_len_;
}

k_bool Field::is_condition_met() { return KTRUE; }

String Field::ValTempStr(char *ptr) {
  k_uint16 len = 0;
  memcpy(&len, ptr, sizeof(k_uint16));
  return {ptr + sizeof(k_uint16), len};
}

struct CKDecimal Field::int_to_decimal() {
  bool unsigned_flag = 0;
  k_int64 val = ValInt();
  if (val < 0) {
    unsigned_flag = 0;
  } else {
    unsigned_flag = 1;
  }

  return IntToDecimal(std::abs(val), unsigned_flag);
}

struct CKDecimal Field::double_to_decimal() {
  bool unsigned_flag = 0;
  k_double64 val = ValReal();
  if (val < 0) {
    unsigned_flag = 0;
  } else {
    unsigned_flag = 1;
  }

  if (0 == unsigned_flag && val - static_cast<k_int64>(val) == 0) {
    k_uint64 val_u = -val;
    return IntToDecimal(val_u, unsigned_flag);
  } else if (1 == unsigned_flag && val - static_cast<k_uint64>(val) == 0) {
    k_uint64 val_u = val;
    return IntToDecimal(val_u, unsigned_flag);
  } else {
    return DoubleToDecimal(std::abs(val), unsigned_flag);
  }
}

k_bool FieldNum::is_nullable() {
  if (false == is_chunk_) {
    return Field::is_nullable();
  } else {
    return table_->is_nullable2(num_, column_type_);
  }
}

char *FieldNum::get_ptr() {
  if (false == is_chunk_) {
    return static_cast<char *>(current_thd->GetRowBatch()->GetData(
        col_idx_in_rs_, storage_len_, column_type_, storage_type_));
  } else {
    return static_cast<char *>(current_thd->GetDataChunk()->GetData(num_));
  }
}
k_bool FieldNum::is_over_flow() {
  return current_thd->GetRowBatch()->IsOverflow(col_idx_in_rs_, column_type_);
}

k_bool FieldNum::fill_template_field(char *ptr) {
  if (is_nullable()) {
    return 1;
  }

  if (IsStorageString(storage_type_)) {
    String str = ValStr();
    k_uint16 len = static_cast<k_uint16>(str.size());
    memcpy(ptr, &len, sizeof(k_uint16));
    memcpy(ptr + sizeof(k_uint16), str.c_str(), str.size());
  } else {
    memcpy(ptr, get_ptr(), storage_len_);
  }

  return 0;
}

k_int64 FieldChar::ValInt() { return 0; }

k_int64 FieldChar::ValInt(char *ptr) { return 0; }

k_double64 FieldChar::ValReal() { return 0.0; }

k_double64 FieldChar::ValReal(char *ptr) { return 0.0; }

String FieldChar::ValStr() {
  char *c = get_ptr();

  return ValStr(c);
}

String FieldChar::ValStr(char *ptr) {
  if (false == is_chunk_) {
    return String(ptr);
  } else {
    return ValTempStr(ptr);
  }
}

Field *FieldChar::field_to_copy() {
  FieldChar *field = new FieldChar(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldNchar::ValInt() { return 0; }

k_int64 FieldNchar::ValInt(char *ptr) { return 0; }

k_double64 FieldNchar::ValReal() { return 0.0; }

k_double64 FieldNchar::ValReal(char *ptr) { return 0.0; }

String FieldNchar::ValStr() {
  char *c = get_ptr();

  return ValStr(c);
}

String FieldNchar::ValStr(char *ptr) {
  if (false == is_chunk_) {
    size_t typ_len;
    // calc valid length， the max length is field_length+1
    if (typ_len = strnlen(static_cast<char *>(ptr), storage_len_),
        typ_len >= storage_len_) {
      typ_len = storage_len_;
    }
    return String{static_cast<char *>(ptr), typ_len};
  } else {
    return ValTempStr(ptr);
  }
}

Field *FieldNchar::field_to_copy() {
  FieldNchar *field = new FieldNchar(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldBool::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldBool::ValInt(char *ptr) {
  k_char c = ptr[0];
  if ('0' == c || '1' == c) {
    return (k_int64)(c - '0');
  }

  return c;
}

k_double64 FieldBool::ValReal() { return ValInt(); }

k_double64 FieldBool::ValReal(char *ptr) { return ValInt(ptr); }

String FieldBool::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldBool::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldBool::field_to_copy() {
  FieldBool *field = new FieldBool(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldShort::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldShort::ValInt(char *ptr) {
  k_int16 val = 0;

  memcpy(&val, ptr, storage_len_);

  return (k_int64)val;
}

k_double64 FieldShort::ValReal() { return ValInt(); }

k_double64 FieldShort::ValReal(char *ptr) { return ValInt(ptr); }

String FieldShort::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldShort::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldShort::field_to_copy() {
  FieldShort *field = new FieldShort(*this);
  field->is_chunk_ = false;
  return field;
}

k_bool FieldShort::fill_template_field(char *ptr) {
  if (is_nullable()) {
    return 1;
  }

  memcpy(ptr, get_ptr(), storage_len_);

  return 0;
}

k_int64 FieldInt::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldInt::ValInt(char *ptr) {
  k_int32 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

k_double64 FieldInt::ValReal() { return ValInt(); }

k_double64 FieldInt::ValReal(k_char *ptr) { return ValInt(ptr); }

String FieldInt::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldInt::ValStr(k_char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldInt::field_to_copy() {
  FieldInt *field = new FieldInt(*this);
  if (nullptr == field) {
    return nullptr;
  }
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldLonglong::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldLonglong::ValInt(char *ptr) {
  k_int64* val_ptr = reinterpret_cast<k_int64*>(ptr);
  return *val_ptr;
}

k_double64 FieldLonglong::ValReal() { return ValInt(); }

k_double64 FieldLonglong::ValReal(char *ptr) { return ValInt(ptr); }

String FieldLonglong::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldLonglong::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldLonglong::field_to_copy() {
  FieldLonglong *field = new FieldLonglong(*this);
  field->is_chunk_ = false;
  return field;
}

char *FieldTimestampTZ::get_ptr() {
  if (false == is_chunk_) {
    return static_cast<char *>(current_thd->GetRowBatch()->GetData(
        col_idx_in_rs_, 0 == num_ ? storage_len_ + 8 : storage_len_,
        column_type_, storage_type_));
  } else {
    return static_cast<char *>(current_thd->GetDataChunk()->GetData(num_));
  }
}

k_int64 FieldTimestampTZ::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldTimestampTZ::ValInt(char *ptr) {
  return *reinterpret_cast<k_int64*>(ptr);
}

k_double64 FieldTimestampTZ::ValReal() { return ValInt(); }

k_double64 FieldTimestampTZ::ValReal(char *ptr) { return ValInt(ptr); }

String FieldTimestampTZ::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldTimestampTZ::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldTimestampTZ::field_to_copy() {
  FieldTimestampTZ *field = new FieldTimestampTZ(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldFloat::ValInt() { return ValReal(); }

k_int64 FieldFloat::ValInt(char *ptr) { return ValReal(ptr); }

k_double64 FieldFloat::ValReal() { return ValReal(get_ptr()); }

k_double64 FieldFloat::ValReal(char *ptr) {
  k_float32 val = 0.0f;

  memcpy(&val, ptr, storage_len_);

  return static_cast<k_double64>(val);
}

String FieldFloat::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldFloat::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFloat::field_to_copy() {
  FieldFloat *field = new FieldFloat(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldDouble::ValInt() { return ValReal(); }

k_int64 FieldDouble::ValInt(char *ptr) { return ValReal(ptr); }

k_double64 FieldDouble::ValReal() {
  return *reinterpret_cast<k_double64 *>(get_ptr());
}

k_double64 FieldDouble::ValReal(char *ptr) {
  return *reinterpret_cast<k_double64*>(ptr);
}

String FieldDouble::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldDouble::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldDouble::field_to_copy() {
  FieldDouble *field = new FieldDouble(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldDecimal::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldDecimal::ValInt(char *ptr) {
  k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
  k_int64 val = 0;
  if (is_double) {
    k_double64 d_val = *reinterpret_cast<k_double64*>(ptr + sizeof(k_bool));
    val = (k_int64)d_val;
  } else {
    val = *reinterpret_cast<k_int64*>(ptr + sizeof(k_bool));
  }

  return val;
}

k_double64 FieldDecimal::ValReal() { return ValReal(get_ptr()); }

k_double64 FieldDecimal::ValReal(char *ptr) {
  k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
  k_double64 val = 0.0f;
  if (is_double) {
    val = *reinterpret_cast<k_double64*>(ptr + sizeof(k_bool));
  } else {
    k_int64 i_val = *reinterpret_cast<k_int64*>(ptr + sizeof(k_bool));
    val = static_cast<k_double64>(i_val);
  }

  return val;
}

String FieldDecimal::ValStr() { return ValStr(get_ptr()); }

String FieldDecimal::ValStr(char *ptr) {
  k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
  String s(storage_len_);
  if (is_double) {
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
    s.length_ = strlen(s.ptr_);
  } else {
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
  }
  return s;
}

Field *FieldDecimal::field_to_copy() {
  FieldDecimal *field = new FieldDecimal(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldSumInt::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldSumInt::ValInt(char *ptr) {
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

k_double64 FieldSumInt::ValReal() {
  return ValReal(get_ptr());
}

k_double64 FieldSumInt::ValReal(char *ptr) {
  if (storage_type_ == roachpb::DataType::DECIMAL) {
    if (is_over_flow()) {
      k_double64 val = 0.0f;
      memcpy(&val, ptr, storage_len_);
      return val;
    } else {
      return ValInt(ptr);
    }
  }
  k_double64 val = 0.0f;
  memcpy(&val, ptr, storage_len_);
  return val;
}

String FieldSumInt::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldSumInt::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

k_bool FieldSumInt::is_real_over_flow() {
  bool overflow = 0;
  memcpy(&overflow, get_ptr(), sizeof(bool));
  return overflow;
}

Field *FieldSumInt::field_to_copy() { return nullptr; }

k_int64 FieldBlob::ValInt() { return 0; }

k_int64 FieldBlob::ValInt(char *ptr) { return 0; }

k_double64 FieldBlob::ValReal() { return 0.0; }

k_double64 FieldBlob::ValReal(char *ptr) { return 0.0; }

String FieldBlob::ValStr() { return ValStr(get_ptr()); }

String FieldBlob::ValStr(char *ptr) {
  if (false == is_chunk_) {
    k_uint16 typ_len = *static_cast<k_uint16 *>(static_cast<void *>(ptr));
    if (KWDBTypeFamily::StringFamily == return_type_) {
      // calc length
      if (typ_len = strnlen(static_cast<char *>(ptr), storage_len_),
          typ_len >= storage_len_) {
        typ_len = storage_len_;
      }
    }
    return String{static_cast<char *>(ptr) + sizeof(k_uint16), typ_len};
  } else {
    return ValTempStr(ptr);
  }
}

Field *FieldBlob::field_to_copy() {
  FieldBlob *field = new FieldBlob(*this);
  field->is_chunk_ = false;
  return field;
}

k_int64 FieldVarchar::ValInt() { return 0; }

k_int64 FieldVarchar::ValInt(char *ptr) { return 0; }

k_double64 FieldVarchar::ValReal() { return 0.0; }

k_double64 FieldVarchar::ValReal(char *ptr) { return 0.0; }

String FieldVarchar::ValStr() { return ValStr(get_ptr()); }

String FieldVarchar::ValStr(char *ptr) {
  if (false == is_chunk_) {
    return String{static_cast<char *>(ptr)};
  } else {
    return ValTempStr(ptr);
  }
}

Field *FieldVarchar::field_to_copy() {
  FieldVarchar *field = new FieldVarchar(*this);
  field->is_chunk_ = false;
  return field;
}

// varblob
k_int64 FieldVarBlob::ValInt() { return 0; }

k_int64 FieldVarBlob::ValInt(char *ptr) { return 0; }

k_double64 FieldVarBlob::ValReal() { return 0.0; }

k_double64 FieldVarBlob::ValReal(char *ptr) { return 0.0; }

String FieldVarBlob::ValStr() { return ValStr(get_ptr()); }

String FieldVarBlob::ValStr(char *ptr) {
  if (false == is_chunk_) {
    k_uint16 len = current_thd->GetRowBatch()->GetDataLen(
        col_idx_in_rs_, storage_len_, column_type_);
    return String(ptr, len, false);
  } else {
    return ValTempStr(ptr);
  }
}

k_uint16 FieldVarBlob::ValStrLength(char *ptr) {
  if (false == is_chunk_) {
    return current_thd->GetRowBatch()->GetDataLen(
        col_idx_in_rs_, storage_len_, column_type_);
  }

  return *reinterpret_cast<k_uint16 *>(ptr);
}

Field *FieldVarBlob::field_to_copy() {
  FieldVarBlob *field = new FieldVarBlob(*this);
  field->is_chunk_ = false;
  return field;
}

template<class T>
bool mul_integer_overflow(T& a, T b) {
  T c;
  if (__builtin_mul_overflow(a, b, &c)) {
    return true;
  }
  return false;
}

char *FieldSumStatisticTagSum::get_ptr() {
  return static_cast<char *>(current_thd->GetRowBatch()->GetData(
      num_, -1, column_type_, storage_type_));
}

k_bool FieldSumStatisticTagSum::is_nullable() {
  return args_[0]->CheckNull() ||
         current_thd->GetRowBatch()->IsNull(num_, column_type_);
}

Field *FieldSumStatisticTagSum::field_to_copy() {
  return nullptr;
}

k_bool FieldSumStatisticTagSum::is_over_flow() {
  if (args_[0]->CheckNull()) {
    return false;
  }
  k_int64 count = 0;
  memcpy(&count, get_ptr(), storage_len_);
  k_int64 val = args_[0]->ValInt();
  if (mul_integer_overflow<k_int64>(val, count)) {
    return true;
  }
  // int64_t tmp;
  // if (__builtin_smulll_overflow(args_[0]->ValInt(), count, &tmp)) {
  //  return true;
  //}
  return false;
}

k_int64 FieldSumStatisticTagSum::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldSumStatisticTagSum::ValInt(char *ptr) {
  if (args_[0]->CheckNull()) {
    return 0;
  }
  k_int64 count = 0;
  memcpy(&count, ptr, storage_len_);

  k_int64 val = 1;
  val = args_[0]->ValInt() * count;
  return val;
}

k_double64 FieldSumStatisticTagSum::ValReal() { return ValReal(get_ptr()); }

k_double64 FieldSumStatisticTagSum::ValReal(char *ptr) {
  if (args_[0]->CheckNull()) {
    return 0;
  }
  // for (size_t i = 0; i < arg_count_; ++i) {
  //   val *= args_[i]->ValReal();
  // }
  k_int64 count = 0;
  memcpy(&count, ptr, storage_len_);
  k_double64 val = args_[0]->ValReal() * count;
  return val;
}

String FieldSumStatisticTagSum::ValStr() { return String(""); }
String FieldSumStatisticTagSum::ValStr(char *ptr) { return String(""); }
k_uint32 FieldSumStatisticTagSum::field_in_template_length() { return true; }
k_bool FieldSumStatisticTagSum::fill_template_field(char *ptr) { return true; }

char *FieldSumStatisticTagCount::get_ptr() {
  return static_cast<char *>(
      current_thd->GetRowBatch()->GetData(num_, -1, column_type_, storage_type_));
}

k_bool FieldSumStatisticTagCount::is_nullable() {
  return current_thd->GetRowBatch()->IsNull(num_, column_type_);
}

Field *FieldSumStatisticTagCount::field_to_copy() {
  return nullptr;
}

k_int64 FieldSumStatisticTagCount::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldSumStatisticTagCount::ValInt(char *ptr) {
  if (args_[0]->CheckNull()) {
    return 0;
  }
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

k_double64 FieldSumStatisticTagCount::ValReal() { return ValReal(get_ptr()); }

k_double64 FieldSumStatisticTagCount::ValReal(char *ptr) {
  if (args_[0]->CheckNull()) {
    return 0;
  }
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

String FieldSumStatisticTagCount::ValStr() { return String(""); }
String FieldSumStatisticTagCount::ValStr(char *ptr) { return String(""); }
k_uint32 FieldSumStatisticTagCount::field_in_template_length() { return true; }
k_bool FieldSumStatisticTagCount::fill_template_field(char *ptr) { return true; }


FieldFuncBase::FieldFuncBase(const std::list<Field *> &fields) {
  type_ = FIELD_FUNC;
  alloc_args(fields.size());
  size_t i = 0;
  for (auto it : fields) {
    args_[i] = it;
    ++i;
  }
}

FieldFuncBase::~FieldFuncBase() {
  if (arg_count_ > array_elements(embedded_arguments_)) {
    SafeFreePointer(args_);
  }
}

k_bool FieldFuncBase::alloc_args(size_t sz) {
  if (sz <= array_elements(embedded_arguments_)) {
    args_ = embedded_arguments_;
  } else {
    args_ = static_cast<Field **>(malloc(sz * sizeof(Field *)));
    if (nullptr == args_) {
      arg_count_ = 0;
      return true;
    }
  }

  arg_count_ = sz;
  return false;
}

k_int64 FieldFunc::ValInt(char *ptr) {
  if (roachpb::DataType::SMALLINT == storage_type_ ||
      roachpb::DataType::INT == storage_type_) {
    k_int32 val = 0;
    memcpy(&val, ptr, storage_len_);
    return val;
  }
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

k_double64 FieldFunc::ValReal(char *ptr) {
  if (roachpb::DataType::FLOAT == storage_type_) {
    k_float32 val = 0;
    memcpy(&val, ptr, storage_len_);
    return val;
  }

  k_double64 val = 0;
  memcpy(&val, ptr, storage_len_);

  return val;
}

String FieldFunc::ValStr(char *ptr) { return ValTempStr(ptr); }

char *FieldFunc::get_ptr() { return nullptr; }

k_bool FieldFunc::is_nullable() { return field_is_nullable(); }

k_bool FieldFunc::field_is_nullable() {
  for (k_uint32 i = 0; i < arg_count_; ++i) {
    if (args_[i]->CheckNull()) {
      return true;
    }
  }

  return false;
}

k_bool FieldFunc::fill_template_field(char *ptr) {
  if (field_is_nullable()) {
    return 1;
  }

  switch (storage_type_) {
    case roachpb::DataType::BOOL: {
      k_bool val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::SMALLINT: {
      k_int16 val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::INT: {
      k_int32 val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE: {
      k_int64 val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::FLOAT: {
      k_float32 val = ValReal();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::DOUBLE: {
      k_double64 val = ValReal();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      String str = ValStr();
      k_uint16 len = static_cast<k_uint16>(str.size());
      memcpy(ptr, &len, sizeof(k_uint16));
      memcpy(ptr + sizeof(k_uint16), str.c_str(), len);
      break;
    }
    default: {
      break;
    }
  }

  return 0;
}

Field *FieldCache::field_to_copy() { return nullptr; }

char *FieldCache::get_ptr() { return field_->get_ptr(); }

FieldCache *FieldCache::get_cache(Field *field) {
  Field_result result = reslut_map[field->get_storage_type()];
  FieldCache *cache = nullptr;
  switch (result) {
    case Field_result::INT_RESULT: {
      cache = new FieldCacheInt(field);
      break;
    }
    case Field_result::REAL_RESULT: {
      cache = new FieldCacheReal(field);
      break;
    }
    case Field_result::STRING_RESULT: {
      cache = new FieldCacheStr(field);
      break;
    }
    default:
      break;
  }

  return cache;
}

k_int64 FieldCacheInt::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldCacheInt::ValInt(char *ptr) { return field_->ValInt(ptr); }

k_double64 FieldCacheInt::ValReal() { return ValInt(); }

k_double64 FieldCacheInt::ValReal(char *ptr) { return ValInt(ptr); }

String FieldCacheInt::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

String FieldCacheInt::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

k_bool FieldCacheInt::fill_template_field(char *ptr) {
  char *source = field_->get_ptr();
  if (nullptr != source) {
    memcpy(ptr, source, storage_len_);
    return 0;
  }
  switch (storage_type_) {
    case roachpb::DataType::BOOL: {
      k_bool value = static_cast<k_bool>(field_->ValInt());
      memcpy(ptr, &value, sizeof(k_bool));
      break;
    }
    case roachpb::DataType::SMALLINT: {
      k_int16 value = static_cast<k_int16>(field_->ValInt());
      memcpy(ptr, &value, sizeof(k_int16));
      break;
    }
    case roachpb::DataType::INT: {
      k_int32 value = static_cast<k_int32>(field_->ValInt());
      memcpy(ptr, &value, sizeof(k_int32));
      break;
    }
    default: {
      k_int64 value = static_cast<k_int64>(field_->ValInt());
      memcpy(ptr, &value, sizeof(k_int64));
      break;
    }
  }
  return 0;
}

k_int64 FieldCacheReal::ValInt() { return ValInt(get_ptr()); }

k_int64 FieldCacheReal::ValInt(char *ptr) { return ValReal(ptr); }

k_double64 FieldCacheReal::ValReal() { return ValReal(get_ptr()); }

k_double64 FieldCacheReal::ValReal(char *ptr) { return field_->ValReal(ptr); }

String FieldCacheReal::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldCacheReal::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

k_bool FieldCacheReal::fill_template_field(char *ptr) {
  char *source = field_->get_ptr();
  if (nullptr != source) {
    memcpy(ptr, source, storage_len_);
  } else {
    if (roachpb::DataType::FLOAT == storage_type_) {
      k_float32 val = field_->ValReal();
      memcpy(ptr, &val, storage_len_);
    } else {
      k_double64 val = field_->ValReal();
      memcpy(ptr, &val, storage_len_);
    }
  }

  return 0;
}

k_int64 FieldCacheStr::ValInt() { return 0; }

k_int64 FieldCacheStr::ValInt(char *ptr) { return 0; }

k_double64 FieldCacheStr::ValReal() { return 0.0; }

k_double64 FieldCacheStr::ValReal(char *ptr) { return 0.0; }

String FieldCacheStr::ValStr() { return ValStr(get_ptr()); }

String FieldCacheStr::ValStr(char *ptr) {
  k_uint16 len = 0;
  memcpy(&len, ptr, sizeof(k_uint16));
  return String(ptr + sizeof(k_uint16), len);
}

k_bool FieldCacheStr::fill_template_field(char *ptr) {
  String str = field_->ValStr();
  if (str.isNull()) {
    return 1;
  }
  k_uint16 len = str.size();
  memcpy(ptr, &len, sizeof(k_uint16));
  memcpy(ptr + sizeof(k_uint16), str.c_str(), len);

  return 0;
}

}  // namespace kwdbts
