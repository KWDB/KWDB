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

#include "ee_base_hash_table.h"

#include "ee_aggregate_func.h"
#include "ee_combined_group_key.h"
#include "ee_common.h"

namespace kwdbts {
namespace {

inline k_uint32 GetGroupColumnEffectiveWidth(DatumPtr ptr, roachpb::DataType type,
                                             k_uint32 declared_len) {
  if (IsVarStringType(type)) {
    return STRING_WIDE + *reinterpret_cast<k_uint16*>(ptr);
  }
  if (IsFixedStringType(type)) {
    return STRING_WIDE + declared_len;
  }
  if (type == roachpb::DataType::DECIMAL) {
    return BOOL_WIDE + declared_len;
  }
  return declared_len;
}

}  // namespace

BaseHashTable::BaseHashTable(
    const std::vector<roachpb::DataType>& group_types,
    const std::vector<k_uint32>& group_lens, k_uint32 agg_width,
    const std::vector<bool>& group_allow_null, k_bool allow_abandoned)
    : capacity_(0),
      group_types_(group_types),
      group_lens_(group_lens),
      agg_width_(agg_width),
      group_allow_null_(group_allow_null),
      allow_abandoned_(allow_abandoned) {
  group_num_ = group_types_.size();
  group_data_ = KNEW GroupColData[group_num_];
  for (int i = 0; i < group_num_; i++) {
    group_offsets_.push_back(group_width_);

    group_width_ += group_lens_[i];
    group_effective_width_ += group_lens_[i];
    if (IsStringType(group_types_[i])) {
      group_width_ += STRING_WIDE;
      if (IsVarStringType(group_types_[i])) {
        group_effective_width_ -= group_lens_[i];
      }
      group_effective_width_ += STRING_WIDE;
    } else if (group_types_[i] == roachpb::DataType::DECIMAL) {
      group_width_ += BOOL_WIDE;
      group_effective_width_ += BOOL_WIDE;
    }
  }
  group_null_offset_ = group_width_;
  group_width_ += (group_types_.size() + 7) / 8;
  group_effective_width_ += (group_types_.size() + 7) / 8;

  tuple_size_ = group_width_ + agg_width_;
  agg_effective_width_ = agg_width_;
  tuple_effective_size_ = group_effective_width_ + agg_effective_width_;
}

BaseHashTable::~BaseHashTable() {
  // 基类析构函数，派生类会负责释放具体资源
  SafeDeleteArray(group_data_);
}

void BaseHashTable::UpdateAggEffectiveWidth(k_uint32 agg_effective_width) {
  if (agg_effective_width > agg_effective_width_) {
    agg_effective_width_ = agg_effective_width;
    tuple_effective_size_ = group_effective_width_ + agg_effective_width_;
  }
}

void BaseHashTable::UpdateGroupEffectiveWidth(DatumPtr tuple_data) {
  if (tuple_data == nullptr) {
    return;
  }
  k_uint32 width = (group_num_ + 7) / 8;
  const char* null_bitmap = tuple_data + group_null_offset_;
  for (int i = 0; i < group_num_; ++i) {
    if (group_allow_null_[i] && AggregateFunc::IsNull(null_bitmap, i)) {
      continue;
    }
    width += GetGroupColumnEffectiveWidth(tuple_data + group_offsets_[i],
                                          group_types_[i], group_lens_[i]);
  }
  if (width > group_effective_width_) {
    group_effective_width_ = width;
    tuple_effective_size_ = group_effective_width_ + agg_effective_width_;
  }
}

bool CompareColumn(DatumPtr left_ptr, DatumPtr right_ptr,
                  roachpb::DataType type) {
  switch (type) {
    case roachpb::DataType::BOOL: {
      return *reinterpret_cast<k_bool*>(left_ptr) ==
             *reinterpret_cast<k_bool*>(right_ptr);
    }
    case roachpb::DataType::SMALLINT: {
      return *reinterpret_cast<k_int16*>(left_ptr) ==
             *reinterpret_cast<k_int16*>(right_ptr);
    }
    case roachpb::DataType::INT: {
      return *reinterpret_cast<k_int32*>(left_ptr) ==
             *reinterpret_cast<k_int32*>(right_ptr);
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT: {
      return *reinterpret_cast<k_int64*>(left_ptr) ==
             *reinterpret_cast<k_int64*>(right_ptr);
    }
    case roachpb::DataType::FLOAT: {
      return *reinterpret_cast<k_float32*>(left_ptr) ==
             *reinterpret_cast<k_float32*>(right_ptr);
    }
    case roachpb::DataType::DOUBLE: {
      return *reinterpret_cast<k_double64*>(left_ptr) ==
             *reinterpret_cast<k_double64*>(right_ptr);
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      k_uint16 left_len = *reinterpret_cast<k_uint16*>(left_ptr);
      k_uint16 right_len = *reinterpret_cast<k_uint16*>(right_ptr);
      if (left_len != right_len) return false;
      return memcmp(left_ptr + sizeof(k_uint16), right_ptr + sizeof(k_uint16), left_len) == 0;
    }
    case roachpb::DataType::DECIMAL: {
      k_bool left_is_double = *reinterpret_cast<k_bool*>(left_ptr);
      k_bool right_is_double = *reinterpret_cast<k_bool*>(right_ptr);
      if (left_is_double != right_is_double) return false;
      if (left_is_double) {
        return *reinterpret_cast<k_double64*>(left_ptr + sizeof(bool)) ==
               *reinterpret_cast<k_double64*>(right_ptr + sizeof(bool));
      } else {
        return *reinterpret_cast<k_int64*>(left_ptr + sizeof(bool)) ==
               *reinterpret_cast<k_int64*>(right_ptr + sizeof(bool));
      }
    }
    default:
      // Handle unsupported data types here
      LOG_ERROR("Unsupported data type.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
      return false;
  }
}

bool CompareGroups(const BaseHashTable& left, k_uint64 lloc,
                  const BaseHashTable& right, k_uint64 rloc) {
  DatumPtr left_ptr = left.GetTuple(lloc);
  DatumPtr right_ptr = right.GetTuple(rloc);

  // Compare null bitmaps
  char* left_null_bitmap = left_ptr + left.group_null_offset_;
  char* right_null_bitmap = right_ptr + right.group_null_offset_;

  for (int i = 0; i < left.GroupNum(); i++) {
    bool left_null = AggregateFunc::IsNull(left_null_bitmap, i);
    bool right_null = AggregateFunc::IsNull(right_null_bitmap, i);

    if (left_null != right_null) {
      return false;
    }

    if (left_null && right_null) {
      continue;
    }

    // Compare actual values
    DatumPtr left_val = left_ptr + left.group_offsets_[i];
    DatumPtr right_val = right_ptr + right.group_offsets_[i];

    if (!CompareColumn(left_val, right_val, left.group_types_[i])) {
      return false;
    }
  }

  return true;
}

}  // namespace kwdbts
