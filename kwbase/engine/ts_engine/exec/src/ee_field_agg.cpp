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

#include "ee_field_agg.h"
#include "ee_aggregate_op.h"
#include "ee_agg_scan_op.h"
#include "ee_aggregate_func.h"

namespace kwdbts {

k_bool FieldAggNum::is_nullable() {
  HashAggregateOperator *hash_agg_op =
      dynamic_cast<HashAggregateOperator *>(op_);
  if (hash_agg_op != nullptr) {
    DatumRowPtr data = hash_agg_op->ht_->CurrentLine();
    if (!data) return false;

    char *bitmap = data + hash_agg_op->agg_null_offset_;
    return AggregateFunc::IsNull(bitmap, num_);
  }

  OrderedAggregateOperator *ordered_agg_op =
      dynamic_cast<OrderedAggregateOperator *>(op_);
  if (ordered_agg_op != nullptr) {
    return ordered_agg_op->temporary_data_chunk_->IsNull(num_);
  }

  AggTableScanOperator *agg_tablescan = dynamic_cast<AggTableScanOperator *>(op_);
  if (nullptr != agg_tablescan) {
    return agg_tablescan->temporary_data_chunk_->IsNull(num_);
  }

  return false;
}

char *FieldAggNum::get_ptr() {
  HashAggregateOperator *hash_agg_op =
      dynamic_cast<HashAggregateOperator *>(op_);
  if (hash_agg_op != nullptr) {
    // data + offset
    DatumRowPtr data = hash_agg_op->ht_->CurrentLine();
    if (!data) return nullptr;

    return hash_agg_op->funcs_[num_]->Result(data);
    // return data + hash_agg_op->funcs_[num_]->GetOffset();
  }

  OrderedAggregateOperator *ordered_agg_op =
      dynamic_cast<OrderedAggregateOperator *>(op_);
  if (ordered_agg_op != nullptr) {
    return ordered_agg_op->temporary_data_chunk_->GetData(num_);
  }

  AggTableScanOperator *agg_tablescan = dynamic_cast<AggTableScanOperator *>(op_);
  if (nullptr != agg_tablescan) {
    return agg_tablescan->temporary_data_chunk_->GetData(num_);
  }

  return nullptr;
}

////////////// FieldAggBool ////////////

k_int64 FieldAggBool::ValInt() {
  return ValInt(get_ptr());
}

k_int64 FieldAggBool::ValInt(char *ptr) {
  k_char c = ptr[0];
  if ('0' == c || '1' == c) {
    return (k_int64)(c - '0');
  }
  return c;
}

k_double64 FieldAggBool::ValReal() {
    return ValInt();
}

k_double64 FieldAggBool::ValReal(char *ptr) {
    return ValInt(ptr);
}

String FieldAggBool::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldAggBool::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

////////////// FieldAggShort ////////////
k_int64 FieldAggShort::ValInt() {
    return ValInt(get_ptr());
}

k_int64 FieldAggShort::ValInt(char *ptr) {
  k_int16 val = 0;
  memcpy(&val, ptr, sizeof(k_int16));

  return val;
}

k_double64 FieldAggShort::ValReal() {
    return ValInt();
}

k_double64 FieldAggShort::ValReal(char *ptr) {
    return ValInt(ptr);
}

String FieldAggShort::ValStr() {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
    return s;
}

String FieldAggShort::ValStr(k_char *ptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
}


////////////// FieldAggInt ////////////

k_int64 FieldAggInt::ValInt() {
    return ValInt(get_ptr());
}

k_int64 FieldAggInt::ValInt(char *ptr) {
  k_int32 val = 0;
  memcpy(&val, ptr, sizeof(k_int32));

  return val;
}

k_double64 FieldAggInt::ValReal() {
    return ValInt();
}

k_double64 FieldAggInt::ValReal(char *ptr) {
    return ValInt(ptr);
}

String FieldAggInt::ValStr() {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
    return s;
}

String FieldAggInt::ValStr(k_char *ptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
}

////////////// FieldAggLonglong ////////////

k_int64 FieldAggLonglong::ValInt() {
    return ValInt(get_ptr());
}

k_int64 FieldAggLonglong::ValInt(char *ptr) {
  k_int64 val = 0;
  memcpy(&val, ptr, sizeof(k_int64));

  return val;
}

k_double64 FieldAggLonglong::ValReal() {
    return ValInt();
}

k_double64 FieldAggLonglong::ValReal(char *ptr) {
    return ValInt(ptr);
}

String FieldAggLonglong::ValStr() {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
    s.length_ = strlen(s.ptr_);
    return s;
}

String FieldAggLonglong::ValStr(k_char *ptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
}

////////////////  FieldAggFloat /////////////////

k_int64 FieldAggFloat::ValInt() {
    return ValReal();
}

k_int64 FieldAggFloat::ValInt(char *ptr) {
    return ValReal(ptr);
}

k_double64 FieldAggFloat::ValReal() {
    return ValReal(get_ptr());
}

k_double64 FieldAggFloat::ValReal(char *ptr) {
  k_float32 val = 0.0f;
  memcpy(&val, ptr, storage_len_);

  return static_cast<k_double64>(val);
}

String FieldAggFloat::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
  s.length_ = strlen(s.ptr_);
  return s;
}

String FieldAggFloat::ValStr(char *ptr) {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
  s.length_ = strlen(s.ptr_);
  return s;
}

////////////////  FieldAggDouble /////////////////

k_int64 FieldAggDouble::ValInt() {
    return ValReal();
}

k_int64 FieldAggDouble::ValInt(char *ptr) {
    return ValReal(ptr);
}

k_double64 FieldAggDouble::ValReal() {
    return ValReal(get_ptr());
}

k_double64 FieldAggDouble::ValReal(char *ptr) {
  k_double64 val = 0.0f;
  memcpy(&val, ptr, storage_len_);

  return val;
}

String FieldAggDouble::ValStr() {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
    s.length_ = strlen(s.ptr_);
    return s;
}

String FieldAggDouble::ValStr(k_char *ptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
}

////////////// decimal ///////////////////
k_int64 FieldAggDecimal::ValInt() {
    return ValInt(get_ptr());
}

k_int64 FieldAggDecimal::ValInt(char *ptr) {
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

k_double64 FieldAggDecimal::ValReal() {
    return ValReal(get_ptr());
}

k_double64 FieldAggDecimal::ValReal(char *ptr) {
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

String FieldAggDecimal::ValStr() { return ValStr(get_ptr()); }

String FieldAggDecimal::ValStr(char *ptr) {
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

////////////// string ////////////////////

k_int64 FieldAggString::ValInt() { return 0; }

k_int64 FieldAggString::ValInt(char *ptr) { return 0; }

k_double64 FieldAggString::ValReal() { return 0.0; }

k_double64 FieldAggString::ValReal(char *ptr) { return 0.0; }

String FieldAggString::ValStr() { return ValStr(get_ptr()); }

String FieldAggString::ValStr(char *ptr) {
  k_uint16 len = 0;
  memcpy(&len, ptr, sizeof(k_uint16));
  return String {ptr + sizeof(k_uint16), len};
}

//////////////  avg /////////////////////
k_int64 FieldAggAvg::ValInt() {
    return ValReal();
}

k_int64 FieldAggAvg::ValInt(char *ptr) {
    return ValReal(ptr);
}

k_double64 FieldAggAvg::ValReal() {
    return ValReal(get_ptr());
}

k_double64 FieldAggAvg::ValReal(char *ptr) {
  k_double64 sum = 0.0f;
  k_int64 count = 0;
  memcpy(&sum, ptr, sizeof(k_double64));
  memcpy(&count, ptr + sizeof(k_double64), sizeof(k_int64));

  return sum / count;
}

String FieldAggAvg::ValStr() {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal());
    s.length_ = strlen(s.ptr_);
    return s;
}

String FieldAggAvg::ValStr(k_char *ptr) {
    String s(storage_len_);
    snprintf(s.ptr_, storage_len_ + 1, "%f", ValReal(ptr));
    s.length_ = strlen(s.ptr_);
    return s;
}

}   // namespace kwdbts
