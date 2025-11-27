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

#include "ee_hash_table.h"

#include "ee_aggregate_func.h"
#include "ee_combined_group_key.h"
#include "ee_common.h"

namespace kwdbts {

HashTableIterator& HashTableIterator::operator++() {
  if (loc_idx_ < ht_->Size()) {
    ++loc_idx_;
  }
  return *this;
}

DatumPtr HashTableIterator::operator*() {
  if (loc_idx_ >= ht_->Size()) return nullptr;

  k_uint64 loc = this->ht_->entries_[loc_idx_];
  return this->ht_->GetAggResult(loc);
}

bool HashTableIterator::operator==(const HashTableIterator& t) const {
  return t.loc_idx_ == this->loc_idx_;
}

bool HashTableIterator::operator!=(const HashTableIterator& t) const {
  return t.loc_idx_ != this->loc_idx_;
}

LinearProbingHashTable::LinearProbingHashTable(
    const std::vector<roachpb::DataType>& group_types,
    const std::vector<k_uint32>& group_lens, k_uint32 agg_width,
    const std::vector<bool>& group_allow_null)
    : capacity_(0),
      group_types_(group_types),
      group_lens_(group_lens),
      agg_width_(agg_width),
      group_allow_null_(group_allow_null) {
  group_num_ = group_types_.size();
  group_data_ = KNEW GroupColData[group_num_];
  for (int i = 0; i < group_num_; i++) {
    group_offsets_.push_back(group_width_);

    group_width_ += group_lens_[i];
    if (IsStringType(group_types_[i])) {
      group_width_ += STRING_WIDE;
    } else if (group_types_[i] == roachpb::DataType::DECIMAL) {
      group_width_ += BOOL_WIDE;
    }
  }
  group_null_offset_ = group_width_;
  group_width_ += (group_types_.size() + 7) / 8;

  tuple_size_ = group_width_ + agg_width_;
}

LinearProbingHashTable::~LinearProbingHashTable() {
  SafeDeleteArray(data_);
  SafeDeleteArray(this->used_bitmap_);
  SafeDeleteArray(group_data_)
}

int LinearProbingHashTable::Resize(k_uint64 size) {
  if (size <= capacity_) {
    return 0;
  }
  mask_ = size - 1;

  if (entries_.size() > 0) {
    auto new_ht = make_unique<LinearProbingHashTable>(group_types_, group_lens_, agg_width_, group_allow_null_);
    if (!new_ht || new_ht->Resize(size) < 0) {
      return -1;
    }

    for (k_uint64 entry : entries_) {
      char* ptr = GetTuple(entry);

      // find position in new hash table
      k_uint64 loc;
      if (new_ht->FindOrCreateGroups(*this, entry, &loc) < 0) {
        return -1;
      }
      char* new_ptr = new_ht->GetTuple(loc);
      std::memcpy(new_ptr, ptr, tuple_size_);
      new_ht->SetUsed(loc);
    }

    SafeDeleteArray(this->data_);
    SafeDeleteArray(this->used_bitmap_);
    data_ = new_ht->data_;
    new_ht->data_ = nullptr;
    used_bitmap_ = new_ht->used_bitmap_;
    new_ht->used_bitmap_ = nullptr;
    entries_ = std::move(new_ht->entries_);

    capacity_ = new_ht->capacity_;
    mask_ = new_ht->mask_;
  } else {
    data_ = KNEW char[size * tuple_size_];
    if (data_ == nullptr) {
      return -1;
    }
    std::memset(data_, 0, size * tuple_size_);
    k_uint64 used_size = (size + 7) / 8;
    used_bitmap_ = KNEW char[used_size];
    if (used_bitmap_ == nullptr) {
      return -1;
    }
    std::memset(used_bitmap_, 0, used_size);
    capacity_ = size;
    mask_ = size - 1;
  }
  return 0;
}

void LinearProbingHashTable::HashColumn(const DatumPtr ptr,
                                        roachpb::DataType type,
                                        std::size_t* h) const {
  switch (type) {
    case roachpb::DataType::BOOL: {
      k_bool val = *reinterpret_cast<k_bool*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::SMALLINT: {
      k_int16 val = *reinterpret_cast<k_int16*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::INT: {
      k_int32 val = *reinterpret_cast<k_int32*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT: {
      k_int64 val = *reinterpret_cast<k_int64*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::FLOAT: {
      k_float32 val = *reinterpret_cast<k_float32*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::DOUBLE: {
      k_double64 val = *reinterpret_cast<k_double64*>(ptr);
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      k_uint16 len = *reinterpret_cast<k_uint16*>(ptr);
      std::string_view val = string_view{ptr + sizeof(k_uint16), len};
      hash_combine(*h, val);
      break;
    }
    case roachpb::DataType::DECIMAL: {
      k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
      if (is_double) {
        k_double64 val = *reinterpret_cast<k_double64*>(ptr + sizeof(bool));
        hash_combine(*h, val);
      } else {
        k_int64 val = *reinterpret_cast<k_int64*>(ptr + sizeof(bool));
        hash_combine(*h, val);
      }
      break;
    }
    default:
      // Handle unsupported data types here
      LOG_ERROR("Unsupported data type.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
      break;
  }
}

std::size_t LinearProbingHashTable::HashGroups(
    IChunk* chunk, k_uint64 row,
    const std::vector<k_uint32>& group_cols) const {
  std::size_t h = INIT_HASH_VALUE;
  for (int i = 0; i < group_num_; i++) {
    k_uint32 col = group_cols[i];
    if (group_allow_null_[i]) {
      bool is_null = chunk->IsNull(row, col);
      group_data_[i].null = is_null;
      if (is_null) {
        continue;
      }
    }
    DatumPtr ptr = chunk->GetData(row, col);
    HashColumn(ptr, group_types_[i], &h);
    group_data_[i].ptr = ptr;
  }

  return h;
}

std::size_t LinearProbingHashTable::HashGroups(k_uint64 loc) const {
  std::size_t h = INIT_HASH_VALUE;
  char* ptr = GetTuple(loc);
  char* null_bitmap = ptr + group_null_offset_;
  for (int i = 0; i < group_num_; i++) {
    if (group_allow_null_[i] && AGG_RESULT_IS_NULL(null_bitmap, i)) {
      continue;
    }
    HashColumn(ptr + group_offsets_[i], group_types_[i], &h);
  }

  return h;
}

k_uint64 LinearProbingHashTable::CreateNullGroups() {
  // hash the group columns
  size_t hash_val = INIT_HASH_VALUE;
  k_uint64 loc = hash_val & mask_;

  if (IsUsed(loc)) {
    return loc;
  }

  entries_.push_back(loc);
  return loc;
}

int LinearProbingHashTable::FindOrCreateGroups(
    IChunk* chunk, k_uint64 row,
    const std::vector<k_uint32>& group_cols, k_uint64 *loc) {
  if (entries_.size() > capacity_ / 2 && Resize(capacity_ * 2) < 0) {
      return -1;
  }
  // hash the group columns
  size_t hash_val = HashGroups(chunk, row, group_cols);
  *loc = hash_val & mask_;
  while (true) {
    if (!IsUsed(*loc)) {
      break;
    }
    if (CompareGroups(group_cols, *loc)) {
      return 0;
    }

    *loc += 1;
    *loc &= mask_;
  }

  entries_.push_back(*loc);
  return 1;
}

int LinearProbingHashTable::FindOrCreateGroups(
    const LinearProbingHashTable& other, k_uint64 row, k_uint64 *loc) {
  // hash the group columns
  size_t hash_val = other.HashGroups(row);
  *loc = hash_val & mask_;

  while (true) {
    if (!IsUsed(*loc)) {
      break;
    }
    if (kwdbts::CompareGroups(*this, *loc, other, row)) {
      return 0;
    }

    *loc += 1;
    *loc &= mask_;
  }

  entries_.push_back(*loc);
  return 0;
}

void LinearProbingHashTable::CopyGroups(IChunk* chunk, k_uint64 row,
                                        const std::vector<k_uint32>& group_cols,
                                        k_uint64 loc) {
  auto null_bitmap = GetTuple(loc) + group_null_offset_;
  for (int i = 0; i < group_num_; i++) {
    k_uint32 col = group_cols[i];
    if (group_allow_null_[i]) {
      auto is_null = chunk->IsNull(row, col);
      if (is_null) {
        AggregateFunc::SetNull(null_bitmap, i);
        continue;
      }
    }

    DatumPtr src = chunk->GetData(row, col);
    DatumPtr dest = GetTuple(loc) + group_offsets_[i];

    if (IsStringType(group_types_[i])) {
      std::memcpy(dest, src, group_lens_[i] + STRING_WIDE);
    } else if (group_types_[i] == roachpb::DataType::DECIMAL) {
      std::memcpy(dest, src, group_lens_[i] + BOOL_WIDE);
    } else {
      std::memcpy(dest, src, group_lens_[i]);
    }
    AggregateFunc::SetNotNull(null_bitmap, i);
  }
}

bool LinearProbingHashTable::CompareGroups(const std::vector<k_uint32>& group_cols, k_uint64 loc) {
  auto tuple = GetTuple(loc);
  auto null_bitmap = tuple + group_null_offset_;
  for (int i = 0; i < group_num_; i++) {
    k_uint32 col = group_cols[i];
    auto ht_is_null = false;
    if (group_allow_null_[i]) {
      auto is_null = group_data_[i].null;
      ht_is_null = AGG_RESULT_IS_NULL(null_bitmap, i);

      if (ht_is_null != is_null) {
        return false;
      }
    }

    if (!ht_is_null && !CompareColumn(group_data_[i].ptr, tuple + group_offsets_[i], group_types_[i])) {
      return false;
    }
  }
  return true;
}

bool CompareColumn(DatumPtr left_ptr, DatumPtr right_ptr,
                   roachpb::DataType type) {
  switch (type) {
    case roachpb::DataType::BOOL: {
      k_bool left = *reinterpret_cast<k_bool*>(left_ptr);
      k_bool right = *reinterpret_cast<k_bool*>(right_ptr);
      if (left != right) {
        return false;
      }
      break;
    }
    case roachpb::DataType::SMALLINT: {
      k_int16 left = *reinterpret_cast<k_int16*>(left_ptr);
      k_int16 right = *reinterpret_cast<k_int16*>(right_ptr);
      if (left != right) {
        return false;
      }
      break;
    }
    case roachpb::DataType::INT: {
      k_int32 left = *reinterpret_cast<k_int32*>(left_ptr);
      k_int32 right = *reinterpret_cast<k_int32*>(right_ptr);
      if (left != right) {
        return false;
      }
      break;
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT: {
      k_int64 left = *reinterpret_cast<k_int64*>(left_ptr);
      k_int64 right = *reinterpret_cast<k_int64*>(right_ptr);
      if (left != right) {
        return false;
      }
      break;
    }
    case roachpb::DataType::FLOAT: {
      k_float32 left = *reinterpret_cast<k_float32*>(left_ptr);
      k_float32 right = *reinterpret_cast<k_float32*>(right_ptr);
      if (fabs(left - right) > std::numeric_limits<float>::epsilon()) {
        return false;
      }
      break;
    }
    case roachpb::DataType::DOUBLE: {
      k_double64 left = *reinterpret_cast<k_double64*>(left_ptr);
      k_double64 right = *reinterpret_cast<k_double64*>(right_ptr);
      if (fabs(left - right) > std::numeric_limits<double>::epsilon()) {
        return false;
      }
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      k_uint16 left_len = *reinterpret_cast<k_uint16*>(left_ptr);
      k_uint16 right_len = *reinterpret_cast<k_uint16*>(right_ptr);
      if (left_len != right_len) {
        return false;
      }

      std::string_view left =
          std::string_view{left_ptr + sizeof(k_uint16), left_len};
      std::string_view right =
          std::string_view{right_ptr + sizeof(k_uint16), right_len};
      if (left != right) {
        return false;
      }
      break;
    }
    case roachpb::DataType::DECIMAL: {
      k_bool left_is_double = *reinterpret_cast<k_bool*>(left_ptr);
      k_bool right_is_double = *reinterpret_cast<k_bool*>(right_ptr);
      if (left_is_double != right_is_double) {
        return false;
      }

      if (left_is_double) {
        k_double64 left =
            *reinterpret_cast<k_double64*>(left_ptr + sizeof(bool));
        k_double64 right =
            *reinterpret_cast<k_double64*>(right_ptr + sizeof(bool));
        if (fabs(left - right) > std::numeric_limits<double>::epsilon()) {
          return false;
        }
      } else {
        k_int64 left = *reinterpret_cast<k_int64*>(left_ptr + sizeof(bool));
        k_int64 right = *reinterpret_cast<k_int64*>(right_ptr + sizeof(bool));
        if (left != right) {
          return false;
        }
      }
      break;
    }
    default:
      break;
  }
  return true;
}

bool CompareGroups(const LinearProbingHashTable& left, k_uint64 lloc, const LinearProbingHashTable& right,
                   k_uint64 rloc) {
  auto left_null_bitmap = left.GetTuple(lloc) + left.group_null_offset_;
  auto right_null_bitmap = right.GetTuple(rloc) + right.group_null_offset_;

  for (int r = 0; r < left.group_num_; ++r) {
    if (left.group_types_[r] != right.group_types_[r]) return false;
    auto other_is_null = false;
    if (left.group_allow_null_[r]) {
      auto is_null = AGG_RESULT_IS_NULL(left_null_bitmap, r);
      other_is_null = AGG_RESULT_IS_NULL(right_null_bitmap, r);
      if (other_is_null != is_null) {
        return false;
      }
    }
    if (!other_is_null && !CompareColumn(left.GetTuple(lloc) + left.group_offsets_[r],
                                         right.GetTuple(rloc) + right.group_offsets_[r], left.group_types_[r])) {
      return false;
    }
  }
  return true;
}

}  // namespace kwdbts
