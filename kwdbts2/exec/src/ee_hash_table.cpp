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

#include <algorithm>

#include "ee_aggregate_func.h"
#include "ee_combined_group_key.h"
#include "ee_common.h"

namespace kwdbts {
namespace {

inline k_uint32 GetGroupValueSize(DatumPtr ptr, roachpb::DataType type,
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

inline k_uint32 GroupNullBitmapBytes(k_uint32 group_num) {
  return (group_num + 7) / 8;
}

inline k_uint64 RoundUpPowerOfTwo(k_uint64 v) {
  if (v <= 1) {
    return 1;
  }
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;
  return v + 1;
}

inline k_uint32 SerializeTupleForSpill(
    DatumPtr tuple, DatumPtr out, k_uint32 group_num,
    const std::vector<roachpb::DataType>& group_types,
    const std::vector<k_uint32>& group_lens,
    const std::vector<k_uint32>& group_offsets,
    const std::vector<bool>& group_allow_null, k_uint32 group_null_offset,
    k_uint32 group_width, k_uint32 agg_width) {
  k_uint32 pos = 0;
  k_uint32 null_bytes = GroupNullBitmapBytes(group_num);
  DatumPtr null_bitmap = tuple + group_null_offset;
  std::memcpy(out + pos, null_bitmap, null_bytes);
  pos += null_bytes;

  for (k_uint32 i = 0; i < group_num; ++i) {
    if (group_allow_null[i] && AggregateFunc::IsNull(null_bitmap, i)) {
      continue;
    }
    DatumPtr src = tuple + group_offsets[i];
    k_uint32 bytes = GetGroupValueSize(src, group_types[i], group_lens[i]);
    std::memcpy(out + pos, src, bytes);
    pos += bytes;
  }

  std::memcpy(out + pos, tuple + group_width, agg_width);
  pos += agg_width;
  return pos;
}

inline KStatus DeserializeTupleFromSpill(
    DatumPtr packed, k_uint32 packed_size, DatumPtr tuple, k_uint32 tuple_size,
    k_uint32 group_num, const std::vector<roachpb::DataType>& group_types,
    const std::vector<k_uint32>& group_lens,
    const std::vector<k_uint32>& group_offsets,
    const std::vector<bool>& group_allow_null, k_uint32 group_null_offset,
    k_uint32 group_width, k_uint32 agg_width) {
  std::memset(tuple, 0, tuple_size);
  k_uint32 pos = 0;
  k_uint32 null_bytes = GroupNullBitmapBytes(group_num);
  if (packed_size < null_bytes + agg_width) {
    return KStatus::FAIL;
  }

  DatumPtr null_bitmap = tuple + group_null_offset;
  std::memcpy(null_bitmap, packed + pos, null_bytes);
  pos += null_bytes;

  for (k_uint32 i = 0; i < group_num; ++i) {
    if (group_allow_null[i] && AggregateFunc::IsNull(null_bitmap, i)) {
      continue;
    }
    if (IsVarStringType(group_types[i]) && pos + STRING_WIDE > packed_size) {
      return KStatus::FAIL;
    }
    k_uint32 bytes =
        GetGroupValueSize(packed + pos, group_types[i], group_lens[i]);
    if (pos + bytes > packed_size) {
      return KStatus::FAIL;
    }
    std::memcpy(tuple + group_offsets[i], packed + pos, bytes);
    pos += bytes;
  }

  if (pos + agg_width > packed_size) {
    return KStatus::FAIL;
  }
  std::memcpy(tuple + group_width, packed + pos, agg_width);
  return KStatus::SUCCESS;
}

}  // namespace

LinearProbingHashTable::LinearProbingHashTable(
    const std::vector<roachpb::DataType>& group_types,
    const std::vector<k_uint32>& group_lens, k_uint32 agg_width,
    const std::vector<bool>& group_allow_null,
    k_bool allow_abandoned)
    : BaseHashTable(group_types, group_lens, agg_width, group_allow_null, allow_abandoned) {}

LinearProbingHashTable::~LinearProbingHashTable() {
  EE_MemPoolFree(g_pstBufferPoolInfo, hash_entry_data_);
  hash_entry_ = nullptr;
  EE_MemPoolFree(g_pstBufferPoolInfo, spill_serialize_buf_);
  spill_serialize_buf_ = nullptr;
  spill_serialize_buf_size_ = 0;
  SafeDeleteArray(this->used_bitmap_);
  // for (DatumPtr tuple_data : tuple_data_list_) {
  //   EE_MemPoolFree(g_pstBufferPoolInfo, tuple_data);
  // }
}

bool LinearProbingHashTable::IsUsed(k_uint64 loc) {
  char* ptr = GetTuple(loc);
  return ptr != nullptr;
}

DatumPtr LinearProbingHashTable::GetTuple(k_uint64 loc) const {
  return hash_entry_[loc].GetPointer();
}

k_uint64 LinearProbingHashTable::GetHashVal(k_uint64 loc) const {
  return hash_entry_[loc].GetSalt();
}

DatumPtr LinearProbingHashTable::GetAggResult(k_uint64 loc) const {
  return hash_entry_[loc].GetPointer() + group_width_;
}

KStatus LinearProbingHashTable::Initialize(k_uint64 capacity) {
  capacity_ = capacity;
  mask_ = capacity - 1;

  hash_entry_data_ = EE_MemPoolMalloc(g_pstBufferPoolInfo, capacity * sizeof(hash_table_entry_t));
  if (hash_entry_data_ == nullptr) {
    return KStatus::FAIL;
  }
  hash_entry_ = reinterpret_cast<hash_table_entry_t*>(hash_entry_data_);
  std::memset(hash_entry_, 0, capacity * sizeof(hash_table_entry_t));

  if (EnsureSpillSerializeBuffer(tuple_size_) != KStatus::SUCCESS) {
    EE_MemPoolFree(g_pstBufferPoolInfo, hash_entry_data_);
    hash_entry_data_ = nullptr;
    hash_entry_ = nullptr;
    return KStatus::FAIL;
  }
  tuple_data_ = std::make_unique<MemoryTupleData>(tuple_size_, capacity_ / 2, allow_abandoned_);

  return KStatus::SUCCESS;
}

KStatus LinearProbingHashTable::Resize(k_uint64 size, PTDFeedBack feedback,
                                   k_uint32 tuple_data_index) {
  if (size < capacity_) {
    return KStatus::SUCCESS;
  }
  mask_ = size - 1;
  // 清空原有hash表
  EE_MemPoolFree(g_pstBufferPoolInfo, hash_entry_data_);

  hash_entry_data_ =
      EE_MemPoolMalloc(g_pstBufferPoolInfo, size * sizeof(hash_table_entry_t));
  if (hash_entry_data_ == nullptr) {
    return KStatus::FAIL;
  }
  hash_entry_ = reinterpret_cast<hash_table_entry_t*>(hash_entry_data_);
  std::memset(hash_entry_, 0, size * sizeof(hash_table_entry_t));

  capacity_ = size;

  mask_ = size - 1;

  while (true) {
    DatumPtr ptr = nullptr;
    EEIteratorErrCode err_code = tuple_data_->NextTuple(ptr);
    if (err_code == EEIteratorErrCode::EE_END_OF_RECORD) {
      break;
    }

    // find position in new hash table
    k_uint64 loc;
    size_t hash_val;
    k_bool is_used;
    if (FindOrCreateGroups(ptr, &loc, &hash_val, &is_used) < 0) {
      return KStatus::FAIL;
    }
  }
  tuple_data_->current_line_ = -1;

  return KStatus::SUCCESS;
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

std::size_t LinearProbingHashTable::HashGroups(DatumPtr tuple_data) const {
  std::size_t h = INIT_HASH_VALUE;
  char* null_bitmap = tuple_data + group_null_offset_;
  for (int i = 0; i < GroupNum(); i++) {
    if (AggregateFunc::IsNull(null_bitmap, i)) {
      continue;
    }
    HashColumn(tuple_data + group_offsets_[i], group_types_[i], &h);
  }

  return h;
}

int LinearProbingHashTable::CreateNullGroups(k_uint64 *loc, k_bool *is_used) {
  // hash the group columns
  size_t hash_val = INIT_HASH_VALUE;
  *loc = hash_val & mask_;

  if (IsUsed(*loc)) {
    *is_used = true;
    return 0;
  }
  *is_used = false;
  hash_entry_[*loc].SetSalt(hash_val);
  return 0;
}

KStatus LinearProbingHashTable::FindOrCreateGroupsAndAddTuple(
    IChunk* chunk, k_uint64 row,
    const std::vector<k_uint32>& group_cols, DatumPtr &agg_ptr, size_t *hash_val,
    k_bool *is_used, k_bool *is_abandoned) {
  // hash the group columns
  *hash_val = HashGroups(chunk, row, group_cols);
  const k_uint64 probe_salt = hash_table_entry_t::ExtractSalt(*hash_val);
  auto loc = *hash_val & mask_;
  while (true) {
    if (!IsUsed(loc)) {
      break;
    }
    if (hash_entry_[loc].GetSalt() == probe_salt &&
        CompareGroups(group_cols, loc)) {
      *is_used = true;
      *is_abandoned = false;
      agg_ptr = hash_entry_[loc].GetPointer() + group_width_;
      return KStatus::SUCCESS;
    }

    loc += 1;
    loc &= mask_;
  }

  *is_used = false;

  DatumPtr tuple = nullptr;
  PTDFeedBack feedback = tuple_data_->GetNextTuplePtr(*hash_val, tuple);
  if (feedback == PTDFeedBack::FAIL) {
    return KStatus::FAIL;
  } else if (feedback == PTDFeedBack::REHASH) {
    Resize(capacity_ * 2, feedback);
    return FindOrCreateGroupsAndAddTuple(chunk, row, group_cols, agg_ptr, hash_val, is_used, is_abandoned);
  } else if (feedback == PTDFeedBack::ABANDONED) {
    CopyGroups(chunk, row, group_cols, tuple, *hash_val);
    UpdateGroupEffectiveWidth(tuple);

    abandoned_count_++;
    *is_abandoned = true;
    agg_ptr = tuple + group_width_;
    return KStatus::SUCCESS;
  }
  hash_entry_[loc].SetSalt(*hash_val);
  hash_entry_[loc].SetPointer(tuple);
  count_++;

  CopyGroups(chunk, row, group_cols, tuple, *hash_val);
  UpdateGroupEffectiveWidth(tuple);
  *is_abandoned = false;
  agg_ptr = tuple + group_width_;
  return KStatus::SUCCESS;
}

int LinearProbingHashTable::FindOrCreateGroups(
    const BaseHashTable& other, k_uint64 row, k_uint64 *loc, k_bool *is_used) {
  // hash the group columns
  size_t hash_val = other.HashGroups(row);
  const k_uint64 probe_salt = hash_table_entry_t::ExtractSalt(hash_val);
  *loc = hash_val & mask_;

  while (true) {
    if (!IsUsed(*loc)) {
      break;
    }
    if (hash_entry_[*loc].GetSalt() == probe_salt &&
        kwdbts::CompareGroups(*this, *loc, other, row)) {
      *is_used = true;
      return 0;
    }

    *loc += 1;
    *loc &= mask_;
  }
  *is_used = false;
  hash_entry_[*loc].SetSalt(hash_val);
  return 0;
}

int LinearProbingHashTable::FindOrCreateGroups(
    const DatumPtr tuple_data, k_uint64 *loc, size_t *hash_val, k_bool *is_used) {
  // hash the group columns
  *hash_val = HashGroups(tuple_data);
  const k_uint64 probe_salt = hash_table_entry_t::ExtractSalt(*hash_val);
  *loc = *hash_val & mask_;

  while (true) {
    if (!IsUsed(*loc)) {
      break;
    }
    if (hash_entry_[*loc].GetSalt() == probe_salt &&
        CompareGroups(tuple_data, *loc)) {
      *is_used = true;
      return KStatus::SUCCESS;
    }

    *loc += 1;
    *loc &= mask_;
  }
  *is_used = false;
  hash_entry_[*loc].SetSalt(*hash_val);
  hash_entry_[*loc].SetPointer(tuple_data);
  UpdateGroupEffectiveWidth(tuple_data);
  return KStatus::SUCCESS;
}

KStatus LinearProbingHashTable::FindOrCreateGroupsAndAddTuple(
    const DatumPtr tuple_data, k_uint64 *loc, size_t *hash_val, k_bool *is_used, k_bool *is_abandoned) {
  // hash the group columns
  *hash_val = HashGroups(tuple_data);
  const k_uint64 probe_salt = hash_table_entry_t::ExtractSalt(*hash_val);
  *loc = *hash_val & mask_;

  while (true) {
    if (!IsUsed(*loc)) {
      break;
    }
    if (hash_entry_[*loc].GetSalt() == probe_salt &&
        CompareGroups(tuple_data, *loc)) {
      *is_used = true;
      *is_abandoned = false;
      return KStatus::SUCCESS;
    }

    *loc += 1;
    *loc &= mask_;
  }
  *is_used = false;

  DatumPtr tuple = nullptr;
  PTDFeedBack feedback = tuple_data_->GetNextTuplePtr(*hash_val, tuple);
  if (feedback == PTDFeedBack::FAIL) {
    return KStatus::FAIL;
  } else if (feedback == PTDFeedBack::REHASH) {
    Resize(capacity_, feedback);
    return FindOrCreateGroupsAndAddTuple(tuple_data, loc, hash_val, is_used, is_abandoned);
  } else if (feedback == PTDFeedBack::ABANDONED) {
    for (int i = 0; i < group_num_; ++i) {
      DatumPtr src = tuple_data + group_offsets_[i];
      DatumPtr dest = tuple + group_offsets_[i];
      std::memcpy(dest, src,
                  GetGroupValueSize(src, group_types_[i], group_lens_[i]));
    }
    std::memcpy(tuple + group_null_offset_, tuple_data + group_null_offset_,
                tuple_size_ - group_null_offset_);
    UpdateGroupEffectiveWidth(tuple);
    if (KStatus::SUCCESS != SaveAggTupleToDisk(tuple + group_width_)) {
      return KStatus::FAIL;
    }
    abandoned_count_++;
    *is_abandoned = true;
    return KStatus::SUCCESS;
  }
  for (int i = 0; i < group_num_; ++i) {
    DatumPtr src = tuple_data + group_offsets_[i];
    DatumPtr dest = tuple + group_offsets_[i];
    std::memcpy(dest, src,
                GetGroupValueSize(src, group_types_[i], group_lens_[i]));
  }
  std::memcpy(tuple + group_null_offset_, tuple_data + group_null_offset_,
              tuple_size_ - group_null_offset_);
  UpdateGroupEffectiveWidth(tuple);
  hash_entry_[*loc].SetSalt(*hash_val);
  hash_entry_[*loc].SetPointer(tuple);
  count_++;
  *is_abandoned = false;
  return KStatus::SUCCESS;
}

void LinearProbingHashTable::CopyGroups(IChunk* chunk, k_uint64 row,
                                        const std::vector<k_uint32>& group_cols,
                                        k_uint64 loc, size_t hash_val) {
  auto tuple = GetTuple(loc);
  CopyGroups(chunk, row, group_cols, tuple, hash_val);
}

void LinearProbingHashTable::CopyGroups(IChunk* chunk, k_uint64 row,
                                        const std::vector<k_uint32>& group_cols,
                                        DatumPtr tuple, size_t hash_val) {
  auto null_bitmap = tuple + group_null_offset_;
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
    DatumPtr dest = tuple + group_offsets_[i];

    std::memcpy(dest, src,
                GetGroupValueSize(src, group_types_[i], group_lens_[i]));
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

bool LinearProbingHashTable::CompareGroups(
    DatumPtr tuple_data, k_uint64 loc) {
  auto left_null_bitmap = tuple_data + group_null_offset_;
  auto right_tuple = GetTuple(loc);
  auto right_null_bitmap = right_tuple + group_null_offset_;

  for (int r = 0; r < group_num_; ++r) {
    auto other_is_null = false;
    if (group_allow_null_[r]) {
      auto is_null = AGG_RESULT_IS_NULL(left_null_bitmap, r);
      other_is_null = AGG_RESULT_IS_NULL(right_null_bitmap, r);
      if (other_is_null != is_null) {
        return false;
      }
    }
    DatumPtr left_ptr = tuple_data + group_offsets_[r];
    DatumPtr right_ptr = right_tuple + group_offsets_[r];
    if (!other_is_null && !CompareColumn(left_ptr, right_ptr, group_types_[r])) {
      return false;
    }
  }
  return true;
}

KStatus LinearProbingHashTable::Combine(std::vector<AggregateFunc*>* funcs, k_uint32 agg_null_offset) {
  if (abandoned_count_ == 0) {
    return KStatus::SUCCESS;
  }
  DatumPtr disk_tuple_data = nullptr;
  if (EnsureSpillSerializeBuffer(tuple_size_) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  DatumPtr packed_tuple_data = spill_serialize_buf_;
  k_uint32 packed_size = 0;

  if (!spill_read_phase_) {
    StartSpillReadPhase();
  }

  while (read_partition_idx_ < kSpillPartitionNum) {
    const auto& part = (*read_spill_partitions_)[read_partition_idx_];
    if (part.read_count_ < part.count_) {
      break;
    }
    read_partition_idx_++;
  }

  if (read_partition_idx_ >= kSpillPartitionNum) {
    spill_read_phase_ = false;
    return KStatus::SUCCESS;
  }

  // Reset memory data for current partition rebuild.
  tuple_data_->Reset();
  std::memset(hash_entry_, 0, capacity_ * sizeof(hash_table_entry_t));

  auto& current_part = (*read_spill_partitions_)[read_partition_idx_];
  const k_uint64 spill_remaining = current_part.count_ - current_part.read_count_;
  const k_uint64 max_tuple_capacity =
      std::max<k_uint64>(1, BaseTupleData::MAX_MEMORY_SIZE /
                                std::max<k_uint64>(1, tuple_size_));
  k_uint64 target_tuple_capacity = tuple_data_->GetCapacity();
  if (spill_remaining > target_tuple_capacity) {
    target_tuple_capacity =
        std::min(max_tuple_capacity, RoundUpPowerOfTwo(spill_remaining));
  }
  if (target_tuple_capacity > tuple_data_->GetCapacity()) {
    if (tuple_data_->Resize(target_tuple_capacity) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
  }

  const k_uint64 target_hash_capacity = RoundUpPowerOfTwo(
      std::max<k_uint64>(capacity_, tuple_data_->GetCapacity() * 2));
  if (target_hash_capacity > capacity_) {
    if (Resize(target_hash_capacity, PTDFeedBack::REHASH) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
  }

  disk_tuple_data = EE_MemPoolMalloc(g_pstBufferPoolInfo, tuple_size_);
  if (disk_tuple_data == nullptr) {
    return KStatus::FAIL;
  }
  while (current_part.read_count_ < current_part.count_) {
    auto ret = LoadNextFromSpillPartition(read_partition_idx_, packed_tuple_data,
                                          tuple_size_, &packed_size);
    if (ret == EEIteratorErrCode::EE_ERROR) {
      EE_MemPoolFree(g_pstBufferPoolInfo, disk_tuple_data);
      return KStatus::FAIL;
    }
    if (DeserializeTupleFromSpill(
            packed_tuple_data, packed_size, disk_tuple_data, tuple_size_,
            group_num_, group_types_, group_lens_, group_offsets_,
            group_allow_null_, group_null_offset_, group_width_, agg_width_) !=
        KStatus::SUCCESS) {
      EE_MemPoolFree(g_pstBufferPoolInfo, disk_tuple_data);
      return KStatus::FAIL;
    }

    auto tuple_data = disk_tuple_data;
    k_uint64 loc;
    size_t hash_val;
    k_bool is_used;
    k_bool is_abandoned;
    if (FindOrCreateGroupsAndAddTuple(tuple_data, &loc, &hash_val, &is_used,
                                      &is_abandoned) != KStatus::SUCCESS) {
      EE_MemPoolFree(g_pstBufferPoolInfo, disk_tuple_data);
      return KStatus::FAIL;
    }
    // One spill record has been consumed from the current read partition.
    // Re-abandoned tuples will increment this counter again in SaveAggTupleToDisk path.
    if (abandoned_count_ > 0) {
      abandoned_count_--;
    }
    if (is_abandoned) {
      continue;
    }
    if (!is_used) {
      DatumPtr tuple_ptr = GetTuple(loc);
      for (int g = 0; g < group_num_; ++g) {
        DatumPtr src = tuple_data + group_offsets_[g];
        DatumPtr dest = tuple_ptr + group_offsets_[g];
        std::memcpy(dest, src,
                    GetGroupValueSize(src, group_types_[g], group_lens_[g]));
      }
      std::memcpy(tuple_ptr + group_null_offset_,
                  tuple_data + group_null_offset_,
                  tuple_size_ - group_null_offset_);
      UpdateGroupEffectiveWidth(tuple_ptr);
    } else {
      auto agg_ptr = GetAggResult(loc);
      for (auto& func : *funcs) {
        func->combine(agg_ptr, agg_ptr + agg_null_offset,
                      tuple_data + group_width_,
                      tuple_data + agg_null_offset);
      }
    }
  }
  EE_MemPoolFree(g_pstBufferPoolInfo, disk_tuple_data);
  read_partition_idx_++;
  if (read_partition_idx_ >= kSpillPartitionNum) {
    spill_read_phase_ = false;
  }
  return KStatus::SUCCESS;
}

KStatus LinearProbingHashTable::SaveAggTupleToDisk(DatumPtr agg_ptr) {
  if (agg_ptr == nullptr) {
    return KStatus::FAIL;
  }
  if (EnsureSpillSerializeBuffer(tuple_size_) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  DatumPtr tuple = agg_ptr - group_width_;
  size_t hash_val = HashGroups(tuple);
  k_uint32 part_id = static_cast<k_uint32>(hash_val & (kSpillPartitionNum - 1));
  k_uint32 packed_size = SerializeTupleForSpill(
      tuple, spill_serialize_buf_, group_num_, group_types_, group_lens_,
      group_offsets_, group_allow_null_, group_null_offset_, group_width_,
      agg_width_);
  return SaveToSpillPartition(part_id, spill_serialize_buf_, packed_size);
}

KStatus LinearProbingHashTable::EnsureSpillSerializeBuffer(k_uint32 min_size) {
  if (spill_serialize_buf_size_ >= min_size && spill_serialize_buf_ != nullptr) {
    return KStatus::SUCCESS;
  }
  EE_MemPoolFree(g_pstBufferPoolInfo, spill_serialize_buf_);
  spill_serialize_buf_ = EE_MemPoolMalloc(g_pstBufferPoolInfo, min_size);
  if (spill_serialize_buf_ == nullptr) {
    spill_serialize_buf_size_ = 0;
    return KStatus::FAIL;
  }
  spill_serialize_buf_size_ = min_size;
  return KStatus::SUCCESS;
}

KStatus LinearProbingHashTable::SaveToSpillPartition(k_uint32 part_id,
                                                     DatumPtr tuple_data,
                                                     k_uint32 tuple_size) {
  if (part_id >= kSpillPartitionNum || tuple_data == nullptr || tuple_size == 0) {
    return KStatus::FAIL;
  }
  if (*write_spill_partitions_ == nullptr) {
    spill_partitions_1_ = std::make_unique<SpillPartitionState[]>(kSpillPartitionNum);
    spill_partitions_2_ = std::make_unique<SpillPartitionState[]>(kSpillPartitionNum);
    write_spill_partitions_ = &spill_partitions_1_;
    read_spill_partitions_ = &spill_partitions_2_;
  }
  auto& part = (*write_spill_partitions_)[part_id];
  if (part.io_cache_handler_.Write(reinterpret_cast<char*>(&tuple_size),
                                   sizeof(tuple_size)) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  if (part.io_cache_handler_.Write(tuple_data, tuple_size) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  part.count_++;
  return KStatus::SUCCESS;
}

EEIteratorErrCode LinearProbingHashTable::LoadNextFromSpillPartition(
    k_uint32 part_id, DatumPtr tuple_data, k_uint32 tuple_capacity,
    k_uint32* tuple_size) {
  if (part_id >= kSpillPartitionNum || tuple_data == nullptr || tuple_size == nullptr) {
    return EEIteratorErrCode::EE_ERROR;
  }
  auto& part = (*read_spill_partitions_)[part_id];
  if (part.read_count_ >= part.count_) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }
  k_uint32 payload_size = 0;
  if (part.io_cache_handler_.Read(reinterpret_cast<char*>(&payload_size),
                                  part.offset_, sizeof(payload_size)) !=
      KStatus::SUCCESS) {
    return EEIteratorErrCode::EE_ERROR;
  }
  part.offset_ += sizeof(payload_size);
  if (payload_size > tuple_capacity) {
    return EEIteratorErrCode::EE_ERROR;
  }
  if (part.io_cache_handler_.Read(tuple_data, part.offset_, payload_size) !=
      KStatus::SUCCESS) {
    return EEIteratorErrCode::EE_ERROR;
  }
  part.offset_ += payload_size;
  part.read_count_++;
  *tuple_size = payload_size;
  return EEIteratorErrCode::EE_OK;
}

void LinearProbingHashTable::ResetSpillPartitions(
    std::unique_ptr<SpillPartitionState[]>& parts) {
  for (size_t i = 0; i < kSpillPartitionNum; ++i) {
    auto& part = parts[i];
    part.io_cache_handler_.Reset();
    part.offset_ = 0;
    part.count_ = 0;
    part.read_count_ = 0;
  }
}

void LinearProbingHashTable::StartSpillReadPhase() {
  if (write_spill_partitions_ == &spill_partitions_1_) {
    read_spill_partitions_ = &spill_partitions_1_;
    write_spill_partitions_ = &spill_partitions_2_;
  } else {
    read_spill_partitions_ = &spill_partitions_2_;
    write_spill_partitions_ = &spill_partitions_1_;
  }

  ResetSpillPartitions(*write_spill_partitions_);
  for (size_t i = 0; i < kSpillPartitionNum; ++i) {
    auto& part = (*read_spill_partitions_)[i];
    part.read_count_ = 0;
  }
  read_partition_idx_ = 0;
  spill_read_phase_ = true;
}

DatumPtr LinearProbingHashTable::NextLine() {
  DatumPtr ptr = nullptr;
  EEIteratorErrCode err_code = tuple_data_->NextTuple(ptr);
  if (err_code != EEIteratorErrCode::EE_OK) {
    return nullptr;
  }
  return ptr + group_width_;
}
}  // namespace kwdbts
