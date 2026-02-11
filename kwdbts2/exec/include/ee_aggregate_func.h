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

#include <functional>
#include <memory>
#include <vector>
#include <string>
#include <limits>

#include "ee_combined_group_key.h"
#include "ee_base_op.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "ee_common.h"
#include "ee_hash_table.h"

namespace kwdbts {

#define AGG_RESULT_IS_NULL(bitmap, col) ((bitmap[col >> 3] & (0x80 >> (col & 7))) == 0)

struct DistinctOpt {
  bool needDistinct;
  std::vector<roachpb::DataType>& col_types;
  std::vector<k_uint32>& col_lens;
  std::vector<k_uint32>& group_cols;
  std::vector<bool>& group_allow_null;
};

struct ElapsedInfo {
  k_double64 result;
  k_int64 min;
  k_int64 max;
  k_int64 timeUnit;
  ElapsedInfo() { timeUnit = 1; }
};

struct Point1 {
  k_int64 tKey;
  k_double64 val;
};

struct TwaInfo {
  k_double64 dOutput;
  k_int64 nums;
  Point1 lastV;
  k_int64 start;
  k_int64 end;
};

#define INIT_POINT(_p, _k, _v) \
  do {                              \
    (_p).tKey = (_k);                \
    (_p).val = (_v);                \
  } while (0)

static k_int64 resolveTimeUnit(string& str) {
  k_int64 time_unit = 1;
  if (str == "'00:00:00.001':::INTERVAL") {
    time_unit = 1;
  } else if (str == "'00:00:01':::INTERVAL") {
    time_unit = 1000;
  } else if (str == "'00:01:00':::INTERVAL") {
    time_unit = 60 * 1000;
  } else if (str == "'01:00:00':::INTERVAL") {
    time_unit = 60 * 60 * 1000;
  } else if (str == "'1 day':::INTERVAL") {
    time_unit = 24 * 60 * 60 * 1000;
  } else if (str == "'7 days':::INTERVAL") {
    time_unit = 7 * 24 * 60 * 60 * 1000;
  }
  return time_unit;
}

class AggregateFunc {
 public:
  AggregateFunc(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : col_idx_(col_idx), len_(len) {
    arg_idx_.push_back(arg_idx);
  }

  virtual ~AggregateFunc() {
    SafeDeletePointer(seen_);
  }

  /**
   * @brief agg update function used by Hash Agg OP.
   * @param dest the target location that the agg result is saving to.
   * @param bitmap the nullable bitmap of agg results.
   * @param chunk the input data chunk coming from previous OP.
   * @param line the processing line in input data chunk.
   */
  virtual void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) {}

  /**
   * @brief agg update function used by Ordered Agg OP (column-mode).
   * @param chunks the target data chunks that the agg results are saving to.
   * @param start_line_in_begin_chunk the begin line in the first target data chunk.
   * @param data_container the input data chunk coming from previous OP.
   * @param group_by_metadata the orderby information for the input records.
   * @param distinctOpt the distinct options.
   */
  virtual int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk,
                          IChunk* data_container, GroupByMetadata& group_by_metadata,
                          DistinctOpt& distinctOpt) { return 0; }

  /**
   * @brief agg update function used by Agg Scan OP.
   * @param chunks the target data chunks that the agg results are saving to.
   * @param start_line_in_begin_chunk the begin line in the first target data chunk.
   * @param data_container the input DataContainer coming from storage layer.
   * @param group_by_metadata the orderby information for the input records.
   * @param renders render definitions for input data.
   */
  virtual void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk,
                           RowBatch* row_batch, GroupByMetadata& group_by_metadata, Field** renders) {}

  /**
   * @brief agg update function used by Agg Scan OP to process input data chunk using batch mode.
   * @param chunk the input data chunk coming from previous OP.
   * @param ht the hash table to save the agg results.
   * @param bitmap_offset the bitmap of agg results.
   * @param distinctOpt the distinct options.
   */
  int AddOrUpdate(IChunk* chunk, LinearProbingHashTable* ht,
                  k_uint32 bitmap_offset, DistinctOpt& distinctOpt) {
    for (k_uint32 line = 0; line < chunk->Count(); ++line) {
      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(chunk, line, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct, distinctOpt.group_allow_null) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      k_uint64 loc;
      if (ht->FindOrCreateGroups(chunk, line, distinctOpt.group_cols, &loc) < 0) {
        return -1;
      }
      auto agg_ptr = ht->GetAggResult(loc);

      addOrUpdate(agg_ptr, agg_ptr + bitmap_offset, chunk, line);
    }
    return 0;
  }

  virtual char* Result(DatumRowPtr dest) { return dest + offset_; }

  void SetOffset(k_uint32 offset) {
    offset_ = offset;
  }

  inline k_uint32 GetOffset() const {
    return offset_;
  }

  inline k_uint32 GetLen() const {
    return len_;
  }

  static k_bool IsNull(const char* bitmap, k_uint32 col) {
    return ((bitmap[col >> 3] & (0x80 >> (col & 7))) == 0);
  }

  // 0 indicates Null，1 indicates not Null
  static void SetNotNull(char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    bitmap[index] |= mask;
  }

  // 0 indicates Null，1 indicates not Null
  static void SetNull(char* bitmap, k_uint32 col) {
    k_uint32 index = col >> 3;     // col / 8
    unsigned int pos = 1 << 7;    // binary 1000 0000
    unsigned int mask = pos >> (col & 7);     // pos >> (col % 8)
    bitmap[index] &= ~mask;
  }

  static int CompareDecimal(DatumPtr src, DatumPtr dest) {
    k_bool src_is_double = *reinterpret_cast<k_bool*>(src);
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (!src_is_double && !dest_is_double) {
      k_int64 src_val = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
      if (src_val > dest_val)
        return 1;
      else if (src_val < dest_val)
        return -1;
    } else {
      k_double64 src_val, dest_val;
      if (src_is_double) {
        src_val = *reinterpret_cast<k_double64*>(src + sizeof(k_bool));
      } else {
        k_int64 src_ival = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
        src_val = (k_double64) src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64) dest_ival;
      }

      if (src_val - dest_val > std::numeric_limits<double>::epsilon()) {
        return 1;
      } else if (dest_val - src_val > std::numeric_limits<double>::epsilon()) {
        return -1;
      }
    }

    return 0;
  }

  static void ConstructGroupKeys(IChunk* chunk, std::vector<k_uint32>& all_cols,
                                 k_uint32 line, CombinedGroupKey& field_keys) {
    for (k_int32 i = 0; i < all_cols.size(); i++) {
      auto idx = all_cols[i];
      bool is_null = chunk->IsNull(line, idx);
      if (is_null) {
        field_keys.AddGroupKey(nullptr, i);
        continue;
      }
      DatumPtr ptr = chunk->GetData(line, idx);
      field_keys.AddGroupKey(ptr, i);
    }
  }

  int isDistinct(IChunk* chunk, k_uint32 line,
                 std::vector<roachpb::DataType>& col_types,
                 std::vector<k_uint32>& col_lens,
                 std::vector<k_uint32>& group_cols,
                 k_bool* is_distinct,
                 std::vector<bool>& group_allow_null);

  virtual roachpb::DataType GetStorageType() const { return roachpb::DataType::UNKNOWN; }

  virtual k_uint32 GetStorageLength() const { return 0; }

  void SetRefOffset(k_uint32 offset) { ref_offset_ = offset; }

 protected:
  LinearProbingHashTable* seen_{nullptr};

  // The output column index in the result data container.
  k_uint32 col_idx_;

  // The input column index for current aggregation function.
  std::vector<k_uint32> arg_idx_;

  // The offset of the aggregate result in the bucket
  k_uint32 offset_{};

  // The offset of ref column aggregate result in the bucket for max_extend/min_extend.
  k_uint32 ref_offset_{0};

  // The length of the aggregate result in the output data container.
  k_uint32 len_;
};

////////////////// AnyNotNullAggregate /////////////////////////

template<typename T>
class AnyNotNullAggregate : public AggregateFunc {
 public:
  AnyNotNullAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AnyNotNullAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // if the data's value is NULL，return directly
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      DatumPtr src = chunk->GetData(line, arg_idx_[0]);
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        AddRow(data_container, row, arg_idx, current_data_chunk_, target_row);
      } else if (row == 0) {
        AddRow(data_container, row, arg_idx, current_data_chunk_, target_row);
      }

      data_container->NextLine();
    }
    return 0;
  }

  void AddRow(kwdbts::IChunk* data_container, kwdbts::k_uint32 row,
              kwdbts::k_uint32 arg_idx, kwdbts::DataChunk* current_data_chunk_,
              kwdbts::k_int32 target_row) {
    if (!data_container->IsNull(row, arg_idx)) {
      char* src_ptr = data_container->GetData(row, arg_idx);
      current_data_chunk_->InsertData(target_row, col_idx_, src_ptr);
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }

        auto dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        if (IsStringType(storage_type)) {
          String str = arg_field->ValStrFromBatch(row_batch);
          current_data_chunk_->InsertData(target_row, col_idx_, str.ptr_, str.length_, true);
        } else {
          char* src_ptr = arg_field->get_ptr(row_batch);
          current_data_chunk_->InsertData(target_row, col_idx_, src_ptr, len_, true);
        }
      }
      row_batch->NextLine();
    }
  }
};

////////////////////////// MaxAggregate //////////////////////////

template<typename T>
class MaxAggregate : public AggregateFunc {
 private:
  bool is_dest_null_ = true;
  T max_val_;

 public:
  MaxAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~MaxAggregate() override = default;
  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // if the data's value is NULL，return directly
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // The aggregate row of the bucket is assigned for the first time and then
      // returned
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    if constexpr (std::is_same_v<T, String>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + offset_);
      auto dest_val = std::string_view(dest + offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) > 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      if (AggregateFunc::CompareDecimal(src, dest + offset_) > 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (src_val - dest_val > std::numeric_limits<T>::epsilon()) {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val > dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      return;
    }
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    if (is_dest_null_) {
      max_val_ = std::numeric_limits<T>::lowest();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char *>(&max_val_), len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val_ = std::numeric_limits<T>::lowest();
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null_ = false;
        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val_ > std::numeric_limits<T>::epsilon()) {
            max_val_ = src_val;
          }
        } else {
          if (src_val > max_val_) {
            max_val_ = src_val;
          }
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char*>(&max_val_), len_, true);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                     GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      return;
    }
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, max_val_, len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val_ = nullptr;
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        if (max_val_ == nullptr) {
          is_dest_null_ = false;
          max_val_ = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, max_val_) > 0) {
            max_val_ = src_ptr;
          }
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, max_val_, len_, true);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      if (!is_dest_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, max_val_.ptr_, max_val_.length_, true);
      }
      return;
    }

    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();
    String max_val = max_val_;

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, max_val.ptr_, max_val.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        String src_val = String(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null_) {
          is_dest_null_ = false;
          max_val = src_val;
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      max_val_ = max_val.clone();
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      return;
    }

    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    if (is_dest_null_) {
      max_val_ = std::numeric_limits<T>::lowest();
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char*>(&max_val_), len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val_ = std::numeric_limits<T>::lowest();
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null_ = false;
        char* src_ptr = arg_field->get_ptr(row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val_ > std::numeric_limits<T>::epsilon()) {
            max_val_ = src_val;
          }
        } else {
          if (src_val > max_val_) {
            max_val_ = src_val;
          }
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char*>(&max_val_), len_, true);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      return;
    }
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, max_val_, len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val_ = nullptr;
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        char* src_ptr = arg_field->get_ptr(row_batch);

        if (max_val_ == nullptr) {
          is_dest_null_ = false;
          max_val_ = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, max_val_) > 0) {
            max_val_ = src_ptr;
          }
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, max_val_, len_, true);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      if (!is_dest_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, max_val_.ptr_, max_val_.length_, true);
      }
      return;
    }
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();
    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();
    String max_val = max_val_;
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, max_val.ptr_, max_val.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val = String();
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        String src_val = arg_field->ValStrFromBatch(row_batch);

        if (is_dest_null_) {
          is_dest_null_ = false;
          max_val = src_val;
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      max_val_ = max_val.clone();
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// MinAggregate //////////////////////////
template<typename T>
class MinAggregate : public AggregateFunc {
 private:
  bool is_dest_null_ = true;
  T min_val_;

 public:
  MinAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~MinAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // The aggregate row of the bucket is assigned for the first time and then
      // returned
      std::memcpy(dest + offset_, src, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    if constexpr (std::is_same_v<T, String>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + offset_);
      auto dest_val = std::string_view(dest + offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) < 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      if (AggregateFunc::CompareDecimal(src, dest + offset_) < 0) {
        std::memcpy(dest + offset_, src, len_);
      }
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (dest_val - src_val > std::numeric_limits<T>::epsilon()) {
          std::memcpy(dest + offset_, src, len_);
        }
      } else {
        if (src_val < dest_val) {
          std::memcpy(dest + offset_, src, len_);
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      return;
    }
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    if (is_dest_null_) {
      min_val_ = std::numeric_limits<T>::max();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char *>(&min_val_), len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val_ = std::numeric_limits<T>::max();
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null_ = false;

        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val_ - src_val > std::numeric_limits<T>::epsilon()) {
            min_val_ = src_val;
          }
        } else {
          if (src_val < min_val_) {
            min_val_ = src_val;
          }
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char*>(&min_val_), len_, true);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                     GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      return;
    }
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, min_val_, len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val_ = nullptr;
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);
        if (min_val_ == nullptr) {
          is_dest_null_ = false;
          min_val_ = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, min_val_) < 0) {
            min_val_ = src_ptr;
          }
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, min_val_, len_, true);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      if (!is_dest_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, min_val_.ptr_, min_val_.length_, true);
      }
      return;
    }

    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();
    String min_val = min_val_;
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, min_val.ptr_, min_val.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        is_dest_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);
        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        String src_val = String(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null_) {
          is_dest_null_ = false;
          min_val = src_val;
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
        }
      }

      data_container->NextLine();
    }
    if (!is_dest_null_) {
      min_val_ = min_val.clone();
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      return;
    }

    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    if (is_dest_null_) {
      min_val_ = std::numeric_limits<T>::max();
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char *>(&min_val_), len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val_ = std::numeric_limits<T>::max();
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null_ = false;

        char* src_ptr = arg_field->get_ptr(row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val_ - src_val > std::numeric_limits<T>::epsilon()) {
            min_val_ = src_val;
          }
        } else {
          if (src_val < min_val_) {
            min_val_ = src_val;
          }
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, reinterpret_cast<char*>(&min_val_), len_, true);
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      return;
    }
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, min_val_, len_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val_ = nullptr;
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        char* src_ptr = arg_field->get_ptr(row_batch);
        if (min_val_ == nullptr) {
          is_dest_null_ = false;
          min_val_ = src_ptr;
        } else {
          if (AggregateFunc::CompareDecimal(src_ptr, min_val_) < 0) {
            min_val_ = src_ptr;
          }
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      current_data_chunk_->InsertData(target_row, col_idx_, min_val_, len_, true);
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
        k_uint32 arg_idx = arg_idx_[0];

    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (row_batch == nullptr) {
      if (!is_dest_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, min_val_.ptr_, min_val_.length_, true);
      }
      return;
    }
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();
    auto* arg_field = renders[arg_idx];
    auto storage_type = arg_field->get_storage_type();
    String min_val = min_val_;
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, min_val.ptr_, min_val.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val = String();
        is_dest_null_ = true;
      }

      if (!(arg_field->CheckNull())) {
        String src_val = arg_field->ValStrFromBatch(row_batch);
        if (is_dest_null_) {
          is_dest_null_ = false;
          min_val = src_val;
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
        }
      }

      row_batch->NextLine();
    }
    if (!is_dest_null_) {
      min_val_ = min_val.clone();
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// SumIntAggregate //////////////////////////

/**
 * SUM_INT aggregation expects int64 as input/output type
*/
class SumIntAggregate : public AggregateFunc {
 public:
  SumIntAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~SumIntAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      std::memcpy(dest + offset_, src, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    k_int64 src_val = *reinterpret_cast<k_int64*>(src);
    k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + offset_);
    k_int64 sum_int = src_val + dest_val;
    std::memcpy(dest + offset_, &sum_int, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      if (!is_dest_null) {
        sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val_i, sizeof(k_int64));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_i = 0;
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr);

        sum_val_i += src_val;
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val_i, sizeof(k_int64));
    }
    return 0;
  }
};

////////////////////////// SumAggregate //////////////////////////

/**
 * SUM aggregation input/output type summary:
 *
 *    INPUT TYPE            OUTPUT TYPE
 *    int16/int32/int64     decimal
 *    decimal               decimal
 *    float/double          double
*/
template<typename T_SRC, typename T_DEST>
class SumAggregate : public AggregateFunc {
 public:
  SumAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~SumAggregate() override = default;

  void handleDouble(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
      T_DEST dest_val = src_val;
      std::memcpy(dest, &dest_val, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
    T_DEST dest_val = *reinterpret_cast<T_DEST*>(dest);
    T_DEST sum = src_val + dest_val;
    std::memcpy(dest, &sum, len_);
  }

  void handleDecimal(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      std::memcpy(dest, src, len_);
      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    // double flag
    k_bool src_is_double = *reinterpret_cast<k_bool*>(src);
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (!src_is_double && !dest_is_double) {
      // Integer + Integer
      k_int64 src_val = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));

      if ((dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val) ||
          (dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val)) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64) dest_val + (k_double64) src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_double64));
      } else {
        k_int64 sum = dest_val + src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      }
    } else {
      k_double64 src_val, dest_val;
      if (src_is_double) {
        src_val = *reinterpret_cast<k_double64*>(src + sizeof(k_bool));
        std::memcpy(dest, &src_is_double, sizeof(k_bool));
      } else {
        k_int64 src_ival = *reinterpret_cast<k_int64*>(src + sizeof(k_bool));
        src_val = (k_double64) src_ival;
      }

      if (dest_is_double) {
        dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
      } else {
        k_int64 dest_ival = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
        dest_val = (k_double64) dest_ival;
      }

      k_double64 sum = src_val + dest_val;
      std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
    }
  }

  void handleInteger(DatumPtr src, DatumPtr dest, k_bool is_dest_null, char* bitmap) {
    if (is_dest_null) {
      k_bool is_double = false;
      std::memcpy(dest, &is_double, sizeof(k_bool));

      T_SRC src_val = *reinterpret_cast<T_SRC*>(src);
      auto dest_val = (k_int64) src_val;
      std::memcpy(dest + sizeof(k_bool), &dest_val, sizeof(k_int64));

      // set not null
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    T_SRC src_val = *reinterpret_cast<T_SRC*>(src);

    // double flag
    k_bool dest_is_double = *reinterpret_cast<k_bool*>(dest);

    if (dest_is_double) {
      k_double64 dest_val = *reinterpret_cast<k_double64*>(dest + sizeof(k_bool));
      k_double64 sum = dest_val + (k_double64) src_val;
      std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_double64));
    } else {
      k_int64 dest_val = *reinterpret_cast<k_int64*>(dest + sizeof(k_bool));
      if ((dest_val > 0 && src_val > 0 && INT64_MAX - dest_val < src_val) ||
          (dest_val < 0 && src_val < 0 && INT64_MIN - dest_val > src_val)) {
        // sum result overflow, change result type to double
        dest_is_double = true;
        std::memcpy(dest, &dest_is_double, sizeof(k_bool));
        k_double64 sum = (k_double64) dest_val + (k_double64) src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      } else {
        k_int64 sum = dest_val + src_val;
        std::memcpy(dest + sizeof(k_bool), &sum, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      handleDouble(src, dest + offset_, is_dest_null, bitmap);
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      // input type: decimal
      handleDecimal(src, dest + offset_, is_dest_null, bitmap);
    } else {
      // input type: int16/int32/int64
      handleInteger(src, dest + offset_, is_dest_null, bitmap);
    }
  }

  int handleDouble(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                   GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum_val = 0.0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val = 0.0;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct, distinctOpt.group_allow_null) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);

        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);
        sum_val += src_val;
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val, len_);
    }
    return 0;
  }

  int handleInteger(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct, distinctOpt.group_allow_null) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);

        if (dest_is_double) {
          sum_val_d += src_val;
        } else {
          if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
              (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
            dest_is_double = true;
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
          } else {
            sum_val_i += src_val;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
    return 0;
  }

  int handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double = false;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct, distinctOpt.group_allow_null) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        k_bool src_is_double = *reinterpret_cast<k_bool*>(src_ptr);


        if (src_is_double) {
          k_double64 src_val = *reinterpret_cast<k_double64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += src_val;
          } else {
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
            dest_is_double = true;
          }
        } else {
          k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += (k_double64) src_val;
          } else {
            if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
                (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
              dest_is_double = true;
              sum_val_d = static_cast<k_double64>(sum_val_i);
              sum_val_d += (k_double64) src_val;
            } else {
              sum_val_i += src_val;
            }
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
    return 0;
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      if (handleDouble(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      if (handleDecimal(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    } else {
      // input type: int16/int32/int64
      if (handleInteger(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt) < 0) {
        return -1;
      }
    }
    return 0;
  }

  void handleDouble(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum_val = 0.0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val = 0.0;
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);

        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);
        sum_val += src_val;
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val, len_);
    }
  }

  void handleInteger(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);
        T_SRC src_val = *reinterpret_cast<T_SRC*>(src_ptr);

        if (dest_is_double) {
          sum_val_d += src_val;
        } else {
          if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
              (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
            dest_is_double = true;
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
          } else {
            sum_val_i += src_val;
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
  }

  void handleDecimal(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                     GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool dest_is_double = false;
    k_double64 sum_val_d = 0.0;
    k_int64 sum_val_i = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);

      dest_is_double = *reinterpret_cast<k_bool*>(dest_ptr);

      if (!is_dest_null) {
        if (dest_is_double) {
          sum_val_d = *reinterpret_cast<k_double64*>(dest_ptr + sizeof(k_bool));
        } else {
          sum_val_i = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_bool));
        }
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
          if (dest_is_double) {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
          } else {
            std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum_val_d = 0.0;
        sum_val_i = 0;
        dest_is_double = false;
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);
        k_bool src_is_double = *reinterpret_cast<k_bool*>(src_ptr);


        if (src_is_double) {
          k_double64 src_val = *reinterpret_cast<k_double64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += src_val;
          } else {
            sum_val_d = static_cast<k_double64>(sum_val_i);
            sum_val_d += src_val;
            dest_is_double = true;
          }
        } else {
          k_int64 src_val = *reinterpret_cast<k_int64*>(src_ptr + sizeof(k_bool));

          if (dest_is_double) {
            sum_val_d += (k_double64) src_val;
          } else {
            if ((sum_val_i > 0 && src_val > 0 && INT64_MAX - sum_val_i < src_val) ||
                (sum_val_i < 0 && src_val < 0 && INT64_MIN - sum_val_i > src_val)) {
              dest_is_double = true;
              sum_val_d = static_cast<k_double64>(sum_val_i);
              sum_val_d += (k_double64) src_val;
            } else {
              sum_val_i += src_val;
            }
          }
        }
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &dest_is_double, sizeof(k_bool));
      if (dest_is_double) {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_d, sizeof(k_double64));
      } else {
        std::memcpy(dest_ptr + sizeof(k_bool), &sum_val_i, sizeof(k_int64));
      }
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    if constexpr (std::is_floating_point<T_SRC>::value) {
      // input type: float/double
      handleDouble(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T_SRC, k_decimal>) {
      handleDecimal(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else {
      // input type: int16/int32/int64
      handleInteger(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// CountAggregate //////////////////////////

/*
    The return type of CountAggregate is BIGINT
*/
class CountAggregate : public AggregateFunc {
 public:
  CountAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~CountAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      // first assign
      k_int64 count = 0;
      std::memcpy(dest + offset_, &count, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_int64 val = *reinterpret_cast<k_int64*>(dest + offset_);
    ++val;
    std::memcpy(dest + offset_, &val, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      // Distinct Agg
      if (distinctOpt.needDistinct) {
        k_bool is_distinct;
        if (isDistinct(data_container, row, distinctOpt.col_types, distinctOpt.col_lens,
                       distinctOpt.group_cols, &is_distinct, distinctOpt.group_allow_null) < 0) {
          return -1;
        }
        if (is_distinct == false) {
          continue;
        }
      }

      if (!data_container->IsNull(row, arg_idx)) {
        ++val;
      }

      data_container->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      if (!(arg_field->CheckNull())) {
        ++val;
      }

      row_batch->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
  }
};

////////////////////////// CountRowAggregate //////////////////////////

/*
    The return type of CountRowAggregate is BIGINT
*/
class CountRowAggregate : public AggregateFunc {
 public:
  CountRowAggregate(k_uint32 col_idx, k_uint32 len) : AggregateFunc(col_idx, 0, len) {
  }

  ~CountRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    if (is_dest_null) {
      // first assign
      k_int64 count = 0;
      std::memcpy(dest + offset_, &count, len_);
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    k_int64 val = *reinterpret_cast<k_int64*>(dest + offset_);
    ++val;
    std::memcpy(dest + offset_, &val, len_);
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      ++val;

      data_container->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_int64 val = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        val = *reinterpret_cast<k_int64*>(dest_ptr);
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &val, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        val = 0;
      }

      ++val;

      row_batch->NextLine();
    }

    if (target_row >= 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &val, len_);
    }
  }
};

////////////////////////// AVGRowAggregate //////////////////////////
template<typename T>
class AVGRowAggregate : public AggregateFunc {
 public:
  AVGRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len) : AggregateFunc(col_idx, arg_idx, len) {
  }

  ~AVGRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);

    if (is_dest_null) {
      // first assign
      k_double64 sum = 0.0f;
      std::memcpy(dest + offset_, &sum, sizeof(k_double64));
      k_int64 count = 0;
      std::memcpy(dest + offset_ + sizeof(k_double64), &count, sizeof(k_int64));
      AggregateFunc::SetNotNull(bitmap, col_idx_);
    }

    T src_val = *reinterpret_cast<T*>(src);
    k_double64 dest_val = *reinterpret_cast<k_double64*>(dest + offset_);
    k_double64 sum_val = src_val + dest_val;
    std::memcpy(dest + offset_, &sum_val, sizeof(k_double64));

    k_int64 count_val = *reinterpret_cast<k_int64*>(dest + offset_ + sizeof(k_double64));
    ++count_val;
    std::memcpy(dest + offset_ + sizeof(k_double64), &count_val, sizeof(k_int64));
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum = 0.0f;
    k_int64 count = 0;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      bool is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum = *reinterpret_cast<k_double64*>(dest_ptr);
        count = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_double64));
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (target_row >= 0 && count > 0) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        sum = 0.0f;
        count = 0;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        sum += src_val;
        ++count;
      }

      data_container->NextLine();
    }

    if (target_row >= 0 && count > 0) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                                              GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_double64 sum_val = 0.0;
    k_int64 count = 0;
    bool is_dest_null = true;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetRawData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        sum_val = *reinterpret_cast<k_double64*>(dest_ptr);
        count = *reinterpret_cast<k_int64*>(dest_ptr + sizeof(k_double64));
      }
    }

    auto* arg_field = renders[arg_idx];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, &sum_val, len_);
          std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetRawData(target_row, col_idx_);
        sum_val = 0.0;
        count = 0;
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        sum_val += src_val;
        ++count;
      }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, &sum_val, len_);
      std::memcpy(dest_ptr + sizeof(k_double64), &count, sizeof(k_int64));
    }
  }
};

////////////////////////// STDDEVRowAggregate //////////////////////////

class STDDEVRowAggregate : public AggregateFunc {
 public:
  STDDEVRowAggregate(k_uint32 col_idx, k_uint32 len) : AggregateFunc(col_idx, 0, len) {
  }

  ~STDDEVRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    // do nothing temporarily.
  }
};

////////////////////////// LastAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class LastAggregate : public AggregateFunc {
 public:
  LastAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_int64 point_time, k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx), point_time_(point_time) {
  }

  ~LastAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    k_bool is_ts_null = chunk->IsNull(line, ts_idx_);
    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    DatumPtr dest_ptr = dest + offset_;
    k_int64 point_ts = point_time_;

    if (is_dest_null) {
      if (ts > point_ts || is_ts_null) {
        return;
      }
      // first assign
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));
    if (ts > last_ts && (ts <= point_ts)) {
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr,
                  sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!input_chunk) {
      if (last_data_ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
      }
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = input_chunk->Count();

    auto chunk_capacity = current_data_chunk_->Capacity();
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_data_ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        last_ts_ = INT64_MIN;
        last_data_ptr_ = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        k_int64 point_ts = point_time_;
        if (last_data_ptr_ == nullptr) {
          if (ts > point_ts || input_chunk->IsNull(row, ts_idx_)) {
            continue;
          }
        }
        if (ts > last_ts_ && (ts <= point_ts)) {
          last_ts_ = ts;
          last_data_ptr_ = input_chunk->GetData(row, arg_idx);
          k_uint32 len = len_;
          char* str = last_data_ptr_;
          if (IS_STRING_FAMILY) {
            len = *reinterpret_cast<k_uint16*>(last_data_ptr_);
            str = last_data_ptr_ + STRING_WIDE;
          }
          last_data_str_ = String(str, len);
        }
      }
    }
    if (last_data_ptr_ != nullptr) {
      last_data_str_ = last_data_str_.clone();
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (last_data_str_.ptr_ != nullptr) {
        if (!(last_ts_ > point_time_)) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
        }
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_data_str_.ptr_ != nullptr) {
          if (!(last_ts_ > point_time_)) {
            current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        last_ts_ = INT64_MIN;
        last_data_str_ = String();
      }

      if (!(arg_field->CheckNull())) {
        char* ts_src_ptr = ts_field->get_ptr(row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        k_int64 point_ts = point_time_;
        if (last_data_str_.ptr_ == nullptr) {
          if (ts > point_ts ||
              (ts_field->CheckNull())) {
            continue;
          }
        }
        if (ts > last_ts_ && (ts <= point_ts)) {
          last_ts_ = ts;
          last_data_str_ = arg_field->ValStrFromBatch(row_batch);
        }
      }

      row_batch->NextLine();
    }
    if (last_data_str_.ptr_ != nullptr) {
      last_data_str_ = last_data_str_.clone();
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
  k_int64 point_time_ = -1;
  char* last_data_ptr_ = nullptr;
  String last_data_str_;
};

////////////////////////// LastRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class LastRowAggregate : public AggregateFunc {
 public:
  LastRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));

    if (ts > last_ts) {
      k_bool is_data_null = chunk->IsNull(line, arg_idx_[0]);
      if (is_data_null) {
        SetNull(bitmap, col_idx_);
      } else {
        DatumPtr src = chunk->GetData(line, arg_idx_[0]);
        std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
        SetNotNull(bitmap, col_idx_);
      }
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!data_container) {
      if (last_data_ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
      }
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_data_ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        last_ts_ = INT64_MIN;
        last_data_ptr_ = nullptr;
      }

      char* ts_src_ptr = data_container->GetData(row, ts_idx_);
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        if (!data_container->IsNull(row, arg_idx)) {
          last_data_ptr_ = data_container->GetData(row, arg_idx);
          k_uint32 len = len_;
          char* str = last_data_ptr_;
          if (IS_STRING_FAMILY) {
            len = *reinterpret_cast<k_uint16*>(last_data_ptr_);
            str = last_data_ptr_ + STRING_WIDE;
          }
          last_data_str_ = String(str, len);
        } else {
          last_data_ptr_ = nullptr;
        }
      }

      data_container->NextLine();
    }
    if (last_data_ptr_) {
      last_data_str_ = last_data_str_.clone();
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (last_data_str_.ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_data_str_.ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_data_str_.ptr_, last_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        last_ts_ = INT64_MIN;
        last_data_str_ = String();
      }

      char* ts_src_ptr = ts_field->get_ptr(row_batch);
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        if (!(arg_field->CheckNull())) {
          last_data_str_ = arg_field->ValStrFromBatch(row_batch);
        } else {
          last_data_str_ = String();
        }
      }
      row_batch->NextLine();
    }
    if (last_data_str_.ptr_ != nullptr) {
      last_data_str_ = last_data_str_.clone();
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
  char* last_data_ptr_ = nullptr;
  String last_data_str_;
};

////////////////////////// LastTSAggregate //////////////////////////

class LastTSAggregate : public AggregateFunc {
 public:
  LastTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx,
                  k_int64 point_time, k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len),
        ts_idx_(ts_idx),
        point_time_(point_time) {}

  ~LastTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;
    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    k_int64 point_ts = point_time_;  // for last point
    if (is_dest_null) {
      if (ts > point_ts) {
        return;
      }
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto last_ts =
        *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));
    if (ts > last_ts && (ts <= point_ts)) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!input_chunk) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }
    k_int64 point_ts = point_time_;  // for last point
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          if (!(last_ts_ > point_ts)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if ((last_line_ptr == nullptr || ts > last_ts_) && (ts <= point_ts)) {
          last_ts_ = ts;
          last_line_ptr = ts_src_ptr;
        }
      }
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    k_int64 point_ts = point_time_;  // for last point
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          if (!(last_ts_ > point_ts)) {
            current_data_chunk_->SetNotNull(target_row, col_idx_);
            std::memcpy(dest_ptr, last_line_ptr, len_);
          }
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      if (!(arg_field->CheckNull())) {
        char* ts_src_ptr = ts_field->get_ptr(row_batch);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if ((last_line_ptr == nullptr || ts > last_ts_) && (ts <= point_ts)) {
          last_ts_ = ts;
          last_line_ptr = ts_src_ptr;
        }
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
  k_int64 point_time_ = -1;
};

////////////////////////// LastRowTSAggregate //////////////////////////

class LastRowTSAggregate : public AggregateFunc {
 public:
  LastRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~LastRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto last_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));

    if (ts > last_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!input_chunk) {
      return 0;
    }
    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, last_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts > last_ts_) {
        last_ts_ = ts;
        last_line_ptr = ts_src_ptr;
      }
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* last_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        last_line_ptr = dest_ptr;
      }
    }

    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (last_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, last_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        last_ts_ = INT64_MIN;
        last_line_ptr = nullptr;
      }

      char* ts_src_ptr = ts_field->get_ptr(row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (last_line_ptr == nullptr || ts > last_ts_) {
        last_ts_ = ts;
        last_line_ptr = ts_src_ptr;
      }

      row_batch->NextLine();
    }

    if (last_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, last_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp last_ts_ = INT64_MIN;
};

////////////////////////// FirstAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class FirstAggregate : public AggregateFunc {
 public:
  FirstAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    if (is_dest_null) {
      //  first assign
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));
    if (ts < first_ts) {
      std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!input_chunk) {
      if (first_data_ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
      }
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_data_ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        first_ts_ = INT64_MAX;
        first_data_ptr_ = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (ts < first_ts_) {
          first_ts_ = ts;
          first_data_ptr_ = input_chunk->GetData(row, arg_idx);
          k_uint32 len = len_;
          char* str = first_data_ptr_;
          if (IS_STRING_FAMILY) {
            len = *reinterpret_cast<k_uint16*>(first_data_ptr_);
            str = first_data_ptr_ + STRING_WIDE;
          }
          first_data_str_ = String(str, len);
        }
      }
    }
    if (first_data_ptr_) {
      first_data_str_ = first_data_str_.clone();
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (first_data_str_.ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_data_str_.ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        first_ts_ = INT64_MAX;
        first_data_str_ = String();
      }

      if (!(arg_field->CheckNull())) {
        char* ts_src_ptr = ts_field->get_ptr(row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (ts < first_ts_) {
          first_ts_ = ts;
          first_data_str_ = arg_field->ValStrFromBatch(row_batch);
        }
      }

      row_batch->NextLine();
    }
    if (first_data_str_.ptr_ != nullptr) {
      first_data_str_ = first_data_str_.clone();
    }
  }


 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
  char *first_data_ptr_ = nullptr;
  String first_data_str_;
};

////////////////////////// FirstRowAggregate //////////////////////////

template<bool IS_STRING_FAMILY = false>
class FirstRowAggregate : public AggregateFunc {
 public:
  FirstRowAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + len_ - sizeof(KTimestamp));

    if (ts < first_ts) {
      k_bool is_data_null = chunk->IsNull(line, arg_idx_[0]);
      if (is_data_null) {
        SetNull(bitmap, col_idx_);
      } else {
        DatumPtr src = chunk->GetData(line, arg_idx_[0]);
        std::memcpy(dest_ptr, src, len_ - sizeof(KTimestamp));
        SetNotNull(bitmap, col_idx_);
      }
      std::memcpy(dest_ptr + len_ - sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!input_chunk) {
      if (first_data_ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
      }
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_data_ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        first_ts_ = INT64_MAX;
        first_data_ptr_ = nullptr;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        if (!input_chunk->IsNull(row, arg_idx)) {
          first_data_ptr_ = input_chunk->GetData(row, arg_idx);
          k_uint32 len = len_;
          char* str = first_data_ptr_;
          if (IS_STRING_FAMILY) {
            len = *reinterpret_cast<k_uint16*>(first_data_ptr_);
            str = first_data_ptr_ + STRING_WIDE;
          }
          first_data_str_ = String(str, len);
        } else {
          first_data_ptr_ = nullptr;
        }
      }
    }
    if (first_data_ptr_) {
      first_data_str_ = first_data_str_.clone();
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    k_int32 target_row = start_line_in_begin_chunk;
    k_uint32 chunk_idx = 0;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (first_data_str_.ptr_ != nullptr) {
        current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];
    auto storage_type = arg_field->get_storage_type();

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_data_str_.ptr_ != nullptr) {
          current_data_chunk_->InsertData(target_row, col_idx_, first_data_str_.ptr_, first_data_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        first_ts_ = INT64_MAX;
        first_data_str_ = String();
      }

      char* ts_src_ptr = ts_field->get_ptr(row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        if (!(arg_field->CheckNull())) {
          first_data_str_ = arg_field->ValStrFromBatch(row_batch);
        } else {
          first_data_str_ = String();
        }
      }

      row_batch->NextLine();
    }
    if (first_data_str_.ptr_ != nullptr) {
      first_data_str_ = first_data_str_.clone();
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
  char* first_data_ptr_{nullptr};
  String first_data_str_;
};

////////////////////////// FirstTSAggregate //////////////////////////
class FirstTSAggregate : public AggregateFunc {
 public:
  FirstTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    if (is_dest_null) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
      return;
    }

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));
    if (ts < first_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!input_chunk) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      if (!input_chunk->IsNull(row, arg_idx)) {
        char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (first_line_ptr == nullptr || ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = ts_src_ptr;
        }
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }

    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    auto* arg_field = renders[arg_idx];
    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      if (!(arg_field->CheckNull())) {
        char* ts_src_ptr = ts_field->get_ptr(row_batch);

        KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
        if (first_line_ptr == nullptr || ts < first_ts_) {
          first_ts_ = ts;
          first_line_ptr = ts_src_ptr;
        }
      }

      row_batch->NextLine();
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

////////////////////////// FirstRowTSAggregate //////////////////////////

class FirstRowTSAggregate : public AggregateFunc {
 public:
  FirstRowTSAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx, k_uint32 len) :
      AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
  }

  ~FirstRowTSAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, ts_idx_)) {
      return;
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);
    DatumPtr dest_ptr = dest + offset_;

    auto ts = *reinterpret_cast<KTimestamp*>(ts_ptr);
    auto first_ts = *reinterpret_cast<KTimestamp*>(dest_ptr + sizeof(KTimestamp));

    if (ts < first_ts) {
      std::memcpy(dest_ptr, ts_ptr, sizeof(KTimestamp));
      SetNotNull(bitmap, col_idx_);
      std::memcpy(dest_ptr + sizeof(KTimestamp), ts_ptr, sizeof(KTimestamp));
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* input_chunk,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!input_chunk) {
      return 0;
    }
    auto data_container_count = input_chunk->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (input_chunk->IsNull(row, ts_idx_)) {
        continue;
      }

      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      char* ts_src_ptr = input_chunk->GetData(row, ts_idx_);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (ts < first_ts_) {
        first_ts_ = ts;
        first_line_ptr = ts_src_ptr;
      }
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    char* first_line_ptr = nullptr;

    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      if (!current_data_chunk_->IsNull(target_row, col_idx_)) {
        first_line_ptr = dest_ptr;
      }
    }

    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (first_line_ptr != nullptr) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          std::memcpy(dest_ptr, first_line_ptr, len_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_ts_ = INT64_MAX;
        first_line_ptr = nullptr;
      }

      char* ts_src_ptr = ts_field->get_ptr(row_batch);

      KTimestamp ts = *reinterpret_cast<KTimestamp*>(ts_src_ptr);
      if (first_line_ptr == nullptr || ts < first_ts_) {
        first_ts_ = ts;
        first_line_ptr = ts_src_ptr;
      }

      row_batch->NextLine();
    }

    if (first_line_ptr != nullptr) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      std::memcpy(dest_ptr, first_line_ptr, len_);
    }
  }

 private:
  k_uint32 ts_idx_;
  KTimestamp first_ts_ = INT64_MAX;
};

////////////////////////// TwaAggregate //////////////////////////

template<typename T>
class TwaAggregate : public AggregateFunc {
 public:
  TwaAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 ts_idx,
               k_double64 const_val, k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len), ts_idx_(ts_idx) {
    if (arg_idx == INT32_MAX) {
      use_const_ = true;
      const_value_ = const_val;
    }
  }

  ~TwaAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (!use_const_ && chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = nullptr;
    if (!use_const_) {
      src = chunk->GetData(line, arg_idx_[0]);
    }

    DatumPtr ts_ptr = chunk->GetData(line, ts_idx_);

    if (is_dest_null) {
      // first assign
      T src_val = const_value_;
      if (!use_const_) {
        src_val = *reinterpret_cast<T*>(src);
      }

      k_double64 twa = (k_double64)src_val;
      std::memcpy(dest + offset_, &twa, sizeof(k_double64));
      TwaInfo twaInfo;
      init_twa_info(twaInfo, ts_ptr, src_val);
      std::memcpy(dest + offset_ + sizeof(k_double64), &twaInfo, sizeof(TwaInfo));
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    Point1 st = {0};
    T src_val = const_value_;
    if (!use_const_) {
      src_val = *reinterpret_cast<T*>(src);
    }
    INIT_POINT(st, *reinterpret_cast<KTimestamp*>(ts_ptr), src_val);
    TwaInfo twa =
        *reinterpret_cast<TwaInfo*>(dest + offset_ + sizeof(k_double64));
    if (twa.lastV.tKey == st.tKey) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_INDETERMINATE_DATATYPE,
          "duplicate timestamps not allowed in twa function");
      return;
    }
    twa.dOutput += get_twa_area(twa.lastV, st);
    twa.end = *reinterpret_cast<KTimestamp*>(ts_ptr);
    twa.lastV = st;
    std::memcpy(dest + offset_, &twa.dOutput, sizeof(k_double64));
    std::memcpy(dest + offset_ + sizeof(k_double64), &twa, sizeof(TwaInfo));
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool first_init = true;
    k_double64 output = 0.0f;
    k_int64 count = 0;
    bool is_dest_null = true;
    TwaInfo twa;
    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        output = *reinterpret_cast<k_double64*>(dest_ptr);
        twa = *reinterpret_cast<TwaInfo*>(dest_ptr + sizeof(k_double64));
        first_init = false;
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          double output = compute_twa(twa);
          std::memcpy(dest_ptr, &output, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &twa, sizeof(TwaInfo));
        } else if (target_row >= 0) {
          current_data_chunk_->SetNull(target_row, col_idx_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_init = true;
        is_dest_null = true;
      }

      if (use_const_ || !data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        T src_val;
        if (use_const_) {
          src_val = const_value_;
        } else {
          char* src_ptr = nullptr;
          src_ptr = data_container->GetData(row, arg_idx);
          src_val = *reinterpret_cast<T*>(src_ptr);
        }
        char* ts_ptr = data_container->GetData(row, ts_idx_);
        if (first_init) {
          init_twa_info(twa, ts_ptr, src_val);
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          first_init = false;
        } else {
          Point1 st = {0};
          INIT_POINT(st, *reinterpret_cast<KTimestamp*>(ts_ptr), src_val);
          if (twa.lastV.tKey == st.tKey) {
            EEPgErrorInfo::SetPgErrorInfo(
                ERRCODE_INDETERMINATE_DATATYPE,
                "duplicate timestamps not allowed in twa function");
            return -1;
          }
          twa.dOutput += get_twa_area(twa.lastV, st);
          twa.end = *reinterpret_cast<KTimestamp*>(ts_ptr);
          twa.lastV = st;
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      double out = compute_twa(twa);
      std::memcpy(dest_ptr, &out, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &twa, sizeof(TwaInfo));
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                                              GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    k_bool first_init = true;
    k_double64 output = 0.0f;
    bool is_dest_null = true;
    TwaInfo twa;
    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        output = *reinterpret_cast<k_double64*>(dest_ptr);
        twa = *reinterpret_cast<TwaInfo*>(dest_ptr + sizeof(k_double64));
        first_init = false;
      }
    }

    Field* arg_field = nullptr;
    if (!use_const_) {
      k_uint32 arg_idx = arg_idx_[0];
      arg_field = renders[arg_idx];
    }

    auto* ts_field = renders[ts_idx_];

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          double out = compute_twa(twa);
          std::memcpy(dest_ptr, &out, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &twa, sizeof(TwaInfo));
          first_init = false;
        } else if (target_row >= 0) {
          current_data_chunk_->SetNull(target_row, col_idx_);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        first_init = true;
        is_dest_null = true;
      }

    if (use_const_ || (!arg_field->CheckNull())) {
      is_dest_null = false;
      T src_val;
      if (use_const_) {
        src_val = const_value_;
      } else {
        char* src_ptr = nullptr;
        src_ptr = arg_field->get_ptr(row_batch);
        src_val = *reinterpret_cast<T*>(src_ptr);
      }
      char* ts_ptr = ts_field->get_ptr(row_batch);
      if (first_init) {
        init_twa_info(twa, ts_ptr, src_val);
        current_data_chunk_->SetNotNull(target_row, col_idx_);
        first_init = false;
      } else {
        Point1 st = {0};
        INIT_POINT(st, *reinterpret_cast<KTimestamp*>(ts_ptr), src_val);
        twa.dOutput += get_twa_area(twa.lastV, st);
        twa.end = *reinterpret_cast<KTimestamp*>(ts_ptr);
        twa.lastV = st;
      }
    }

      row_batch->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      double out = compute_twa(twa);
      std::memcpy(dest_ptr, &out, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &twa, sizeof(TwaInfo));
    }
  }

  char* Result(DatumRowPtr dest) {
    TwaInfo twa = *reinterpret_cast<TwaInfo*>(dest + offset_ + sizeof(k_double64));
    k_double64 output = compute_twa(twa);
    std::memcpy(dest + offset_, &output, sizeof(k_double64));
    return dest + offset_;
  }

  void init_twa_info(TwaInfo& twa, char* ts, T& src_val) {
    twa.dOutput = 0.0f;
    twa.start = *reinterpret_cast<KTimestamp*>(ts);
    twa.end = *reinterpret_cast<KTimestamp*>(ts);
    twa.lastV.tKey = *reinterpret_cast<KTimestamp*>(ts);
    twa.lastV.val = src_val;
  }

  k_double64 get_twa_area(Point1& s, Point1& e) {
    if (e.tKey == INT64_MAX || s.tKey == INT64_MIN) {
      return 0;
    }

    if ((s.val >= 0 && e.val >= 0) || (s.val <= 0 && e.val <= 0)) {
      return (s.val + e.val) * (e.tKey - s.tKey) / 2;
    }

    double x = (s.tKey * e.val - e.tKey * s.val) / (e.val - s.val);
    double val = (s.val * (x - s.tKey) + e.val * (e.tKey - x)) / 2;
    return val;
  }

  k_double64 compute_twa(TwaInfo& twa) {
    if (twa.end == twa.start) {
      twa.dOutput = twa.lastV.val;
    } else if (twa.end == INT64_MAX || twa.start == INT64_MIN) {
      twa.dOutput = 0;
    } else {
      twa.dOutput = twa.dOutput / (twa.end - twa.start);
    }
    return twa.dOutput;
  }

 private:
  k_uint32 ts_idx_;
  k_double64 const_value_;
  k_bool use_const_{false};
};

////////////////////////// ElapsedAggregate //////////////////////////

class ElapsedAggregate : public AggregateFunc {
 public:
  ElapsedAggregate(k_uint32 col_idx, k_uint32 arg_idx, std::string& time_unit, roachpb::DataType dataType,
                   k_uint32 len)
      : AggregateFunc(col_idx, arg_idx, len) {
    if (dataType == roachpb::DataType::TIMESTAMPTZ_MICRO) {
      if (time_unit == "'00:00:00.000001':::INTERVAL") {
        timeUnit_ = 1;
      } else if (time_unit == "'00:00:00.000000001':::INTERVAL") {
        timeUnit_ = resolveTimeUnit(time_unit);
        time_unit_extra_ *= 1000;
      } else {
        timeUnit_ = resolveTimeUnit(time_unit) * 1000;
      }
    } else if (dataType == roachpb::DataType::TIMESTAMPTZ_NANO) {
      if (time_unit == "'00:00:00.000000001':::INTERVAL") {
        timeUnit_ = 1;
      } else if (time_unit == "'00:00:00.000001':::INTERVAL") {
        timeUnit_ = resolveTimeUnit(time_unit) * 1000;
      } else {
        timeUnit_ = resolveTimeUnit(time_unit) * 1000 * 1000;
      }
    } else if (dataType == roachpb::DataType::TIMESTAMPTZ) {
      if (time_unit == "'00:00:00.000001':::INTERVAL") {
        timeUnit_ = resolveTimeUnit(time_unit);
        time_unit_extra_ *= 1000;
      } else if (time_unit == "'00:00:00.000000001':::INTERVAL") {
        timeUnit_ = resolveTimeUnit(time_unit);
        time_unit_extra_ = 1000 * 1000;
      } else {
        timeUnit_ = resolveTimeUnit(time_unit);
      }
    }
  }

  ~ElapsedAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    if (chunk->IsNull(line, arg_idx_[0])) {
      return;
    }

    k_bool is_dest_null = AggregateFunc::IsNull(bitmap, col_idx_);
    DatumPtr src = chunk->GetData(line, arg_idx_[0]);
    if (is_dest_null) {
      // first assign
      KTimestamp ts = *reinterpret_cast<KTimestamp*>(src);
      ElapsedInfo info;
      info.max = ts;
      info.min = ts;
      info.result = 0.0f;
      info.timeUnit = timeUnit_;
      std::memcpy(dest + offset_, &info.result, sizeof(k_double64));
      std::memcpy(dest + offset_ + sizeof(k_double64), &info, sizeof(ElapsedInfo));
      AggregateFunc::SetNotNull(bitmap, col_idx_);
      return;
    }

    KTimestamp src_val = *reinterpret_cast<KTimestamp*>(src);
    ElapsedInfo info = *reinterpret_cast<ElapsedInfo*>(dest + offset_ + sizeof(k_double64));
    if (src_val < info.min) {
      info.min = src_val;
    } else if (info.max < src_val) {
      info.max = src_val;
    }

    std::memcpy(dest + offset_ + sizeof(k_double64), &info, sizeof(ElapsedInfo));
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if (!data_container) {
      return 0;
    }
    k_uint32 arg_idx = arg_idx_[0];

    auto data_container_count = data_container->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    ElapsedInfo info;
    info.timeUnit = timeUnit_;
    bool is_dest_null = true;
    k_bool first_init = true;
    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        info = *reinterpret_cast<ElapsedInfo*>(dest_ptr + sizeof(k_double64));
        first_init = false;
      }
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_double64 result = (k_double64)(info.max - info.min);
          result = (result >= 0) ? result : -result;
          info.result = result / info.timeUnit * time_unit_extra_;
          std::memcpy(dest_ptr, &info.result, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &info, sizeof(ElapsedInfo));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        is_dest_null = true;
        first_init = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(src_ptr);
        if (first_init) {
          first_init = false;
          info.max = ts;
          info.min = ts;
          info.result = 0.0f;
          current_data_chunk_->SetNotNull(target_row, col_idx_);
        } else {
          if (ts < info.min) {
            info.min = ts;
          } else if (info.max < ts) {
            info.max = ts;
          }
        }
      }

      data_container->NextLine();
    }

    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_double64 result = (k_double64)(info.max - info.min);
      result = (result >= 0) ? result : -result;
      info.result = result / info.timeUnit * time_unit_extra_;
      std::memcpy(dest_ptr, &info.result, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &info, sizeof(ElapsedInfo));
    }
    return 0;
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                                              GroupByMetadata& group_by_metadata, Field** renders) override {
    if (!row_batch) {
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    auto data_container_count = row_batch->Count();
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    auto chunk_capacity = current_data_chunk_->Capacity();

    char* dest_ptr;
    ElapsedInfo info;
    bool is_dest_null = true;
    k_bool first_init = true;
    if (target_row >= 0) {
      dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
      is_dest_null = current_data_chunk_->IsNull(target_row, col_idx_);
      if (!is_dest_null) {
        info = *reinterpret_cast<ElapsedInfo*>(dest_ptr + sizeof(k_double64));
        first_init = false;
      }
    }

    auto* arg_field = renders[arg_idx];
    info.timeUnit = timeUnit_;
    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null) {
          current_data_chunk_->SetNotNull(target_row, col_idx_);
          k_double64 result = (k_double64)(info.max - info.min);
          result = (result >= 0) ? result : -result;
          info.result = result / info.timeUnit * time_unit_extra_;
          std::memcpy(dest_ptr, &info.result, sizeof(k_double64));
          std::memcpy(dest_ptr + sizeof(k_double64), &info, sizeof(ElapsedInfo));
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        dest_ptr = current_data_chunk_->GetData(target_row, col_idx_);
        is_dest_null = true;
        first_init = true;
      }

      if (!arg_field->CheckNull()) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);
        KTimestamp ts = *reinterpret_cast<KTimestamp*>(src_ptr);
        if (first_init) {
          first_init = false;
          info.max = ts;
          info.min = ts;
          info.result = 0.0f;
          current_data_chunk_->SetNotNull(target_row, col_idx_);
        } else {
          if (ts < info.min) {
            info.min = ts;
          } else if (info.max < ts) {
            info.max = ts;
          }
        }

        row_batch->NextLine();
      }
    }
    if (!is_dest_null) {
      current_data_chunk_->SetNotNull(target_row, col_idx_);
      k_double64 result = (k_double64)(info.max - info.min);
      result = (result >= 0) ? result : -result;
      info.result = result / info.timeUnit * time_unit_extra_;
      std::memcpy(dest_ptr, &info.result, sizeof(k_double64));
      std::memcpy(dest_ptr + sizeof(k_double64), &info, sizeof(ElapsedInfo));
    }
  }
  char* Result(DatumRowPtr dest) {
    ElapsedInfo info = *reinterpret_cast<ElapsedInfo*>(dest + offset_ + sizeof(k_double64));
    k_double64 result = (k_double64)(info.max - info.min);
    result = (result >= 0) ? result : -result;
    info.result = result / info.timeUnit * time_unit_extra_;
    std::memcpy(dest + offset_, &info.result, sizeof(k_double64));
    return dest + offset_;
  }

 private:
  k_int64 timeUnit_{1};
  k_int32 time_unit_extra_{1};
};


////////////////////////// MaxExtendAggregate //////////////////////////

template<typename T>
class MaxExtendAggregate : public AggregateFunc {
 private:
  T max_val_;
  bool is_dest_null_{true};
  bool is_last_null_{true};
  char *last_ptr_{nullptr};
  String last_str_;
  k_uint16 last_len_{0};
  bool is_string_{false};

 public:
  MaxExtendAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len,
                     k_uint32 arg_idx2, bool is_string = false)
      : AggregateFunc(col_idx, arg_idx, len) {
    arg_idx_.push_back(arg_idx2);
    if constexpr (!std::is_same_v<T, String>) {
      max_val_ = std::numeric_limits<T>::lowest();
    }
    is_string_ = is_string;
  }

  ~MaxExtendAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk, k_uint32 line) override {
    auto arg_idx = arg_idx_[0];
    auto arg_idx2 = arg_idx_[1];
    k_bool is_dest_null = chunk->IsNull(line, arg_idx);
    if (is_dest_null) {
      return;
    }
    DatumPtr src = chunk->GetData(line, arg_idx);
    DatumPtr src_extend = chunk->GetData(line, arg_idx2);

    if constexpr (std::is_same_v<T, String>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + ref_offset_);
      auto dest_val =
          std::string_view(dest + ref_offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) == 0) {
        if (chunk->IsNull(line, arg_idx2)) {
          AggregateFunc::SetNull(bitmap, col_idx_);
        } else {
          AggregateFunc::SetNotNull(bitmap, col_idx_);
          std::memcpy(dest + offset_, src_extend, len_);
        }
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("not support decimal type.")
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + ref_offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (std::abs(src_val - dest_val) <= std::numeric_limits<T>::epsilon()) {
          if (chunk->IsNull(line, arg_idx2)) {
            AggregateFunc::SetNull(bitmap, col_idx_);
          } else {
            AggregateFunc::SetNotNull(bitmap, col_idx_);
            std::memcpy(dest + offset_, src_extend, len_);
          }
        }
      } else {
        if (src_val == dest_val) {
          if (chunk->IsNull(line, arg_idx2)) {
            AggregateFunc::SetNull(bitmap, col_idx_);
          } else {
            AggregateFunc::SetNotNull(bitmap, col_idx_);
            std::memcpy(dest + offset_, src_extend, len_);
          }
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!data_container) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    T max_val;

    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      max_val = max_val_;
    } else {
      max_val = std::numeric_limits<T>::lowest();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val = std::numeric_limits<T>::lowest();
        is_dest_null = true;
        last_ptr_ = nullptr;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;
        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val > std::numeric_limits<T>::epsilon()) {
            max_val = src_val;
            is_last_null_ = data_container->IsNull(row, arg_idx2);
            if (!is_last_null_) {
              last_ptr_ = data_container->GetData(row, arg_idx2);
              if (is_string_) {
                last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
              } else {
                last_str_ = String(last_ptr_, len_);
              }
            }
          }
        } else {
          if (src_val > max_val || max_val == std::numeric_limits<T>::lowest()) {
            max_val = src_val;
            is_last_null_ = data_container->IsNull(row, arg_idx2);
            if (!is_last_null_) {
              last_ptr_ = data_container->GetData(row, arg_idx2);
              if (is_string_) {
                last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
              } else {
                last_str_ = String(last_ptr_, len_);
              }
            }
          }
        }
      }

      data_container->NextLine();
    }
    max_val_ = max_val;
    is_dest_null_ = is_dest_null;
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    String max_val;
    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      max_val = max_val_;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val = String();
        is_dest_null = true;
        is_last_null_ = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        String src_val = String(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null) {
          is_dest_null = false;
          max_val = src_val;
          is_last_null_ = data_container->IsNull(row, arg_idx2);
          if (!is_last_null_) {
            last_ptr_ = data_container->GetData(row, arg_idx2);
            if (is_string_) {
              last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
            } else {
              last_str_ = String(last_ptr_, len_);
            }
          }
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
          is_last_null_ = data_container->IsNull(row, arg_idx2);
          if (!is_last_null_) {
            last_ptr_ = data_container->GetData(row, arg_idx2);
            if (is_string_) {
              last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
            } else {
              last_str_ = String(last_ptr_, len_);
            }
          }
        }
      }

      data_container->NextLine();
    }
    is_dest_null_ = is_dest_null;
    if (!is_dest_null_) {
      max_val_ = max_val.clone();
    }
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("max_extend doesn't support decimal.");
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = is_dest_null_;
    T max_val;

    auto* arg_field = renders[arg_idx];
    auto* arg_field2 = renders[arg_idx2];
    auto storage_type2 = arg_field2->get_storage_type();
    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      max_val = max_val_;
    } else {
      max_val = std::numeric_limits<T>::lowest();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val = std::numeric_limits<T>::lowest();
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;
        char* src_ptr = arg_field->get_ptr(row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (src_val - max_val > std::numeric_limits<T>::epsilon()) {
            max_val = src_val;
            is_last_null_ = arg_field2->CheckNull();
            last_str_ = arg_field2->ValStrFromBatch(row_batch);
          }
        } else {
          if (src_val > max_val || max_val == std::numeric_limits<T>::lowest()) {
            max_val = src_val;
            is_last_null_ = arg_field2->CheckNull();
            last_str_ = arg_field2->ValStrFromBatch(row_batch);
          }
        }
      }

      row_batch->NextLine();
    }
    max_val_ = max_val;
    is_dest_null_ = is_dest_null;
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }

    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    T max_val;

    auto* arg_field = renders[arg_idx];
    auto* arg_field2 = renders[arg_idx2];
    auto storage_type = arg_field->get_storage_type();
    auto storage_type2 = arg_field2->get_storage_type();
    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      max_val = max_val_;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        max_val = String();
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        String src_val = arg_field->ValStrFromBatch(row_batch);
        if (is_dest_null) {
          is_dest_null = false;
          max_val = src_val;
          is_last_null_ = arg_field2->CheckNull();
          last_str_ = arg_field2->ValStrFromBatch(row_batch);
        } else if (src_val.compare(max_val) > 0) {
          max_val = src_val;
          is_last_null_ = arg_field2->CheckNull();
          last_str_ = arg_field2->ValStrFromBatch(row_batch);
        }
      }

      row_batch->NextLine();
    }
    is_dest_null_ = is_dest_null;
    max_val_ = max_val.clone();
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("max_extend doesn't support decimal.");
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

////////////////////////// MinExtendAggregate //////////////////////////
template<typename T>
class MinExtendAggregate : public AggregateFunc {
 private:
  T min_val_;
  bool is_dest_null_{true};
  bool is_last_null_{true};
  char *last_ptr_{nullptr};
  String last_str_;
  k_uint16 last_len_{0};
  bool is_string_{false};

 public:
  MinExtendAggregate(k_uint32 col_idx, k_uint32 arg_idx, k_uint32 len,
                     k_uint32 arg_idx2, bool is_string = false)
      : AggregateFunc(col_idx, arg_idx, len) {
    arg_idx_.push_back(arg_idx2);
    if constexpr (!std::is_same_v<T, std::string>) {
      min_val_ = std::numeric_limits<T>::max();
    }
    is_string_ = is_string;
  }

  ~MinExtendAggregate() override = default;

  void addOrUpdate(DatumRowPtr dest, char* bitmap, IChunk* chunk,
                   k_uint32 line) override {
    auto arg_idx = arg_idx_[0];
    auto arg_idx2 = arg_idx_[1];
    k_bool is_dest_null = chunk->IsNull(line, arg_idx);
    if (is_dest_null) {
      return;
    }
    DatumPtr src = chunk->GetData(line, arg_idx);
    DatumPtr src_extend = chunk->GetData(line, arg_idx2);

    if constexpr (std::is_same_v<T, String>) {
      k_uint16 src_len = *reinterpret_cast<k_uint16*>(src);
      auto src_val = std::string_view(src + sizeof(k_uint16), src_len);
      k_uint16 dest_len = *reinterpret_cast<k_uint16*>(dest + ref_offset_);
      auto dest_val =
          std::string_view(dest + ref_offset_ + sizeof(k_uint16), dest_len);
      if (src_val.compare(dest_val) == 0) {
        if (chunk->IsNull(line, arg_idx2)) {
          AggregateFunc::SetNull(bitmap, col_idx_);
        } else {
          AggregateFunc::SetNotNull(bitmap, col_idx_);
          std::memcpy(dest + offset_, src_extend, len_);
        }
      }
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("not support decimal type.")
    } else {
      T src_val = *reinterpret_cast<T*>(src);
      T dest_val = *reinterpret_cast<T*>(dest + ref_offset_);
      if constexpr (std::is_floating_point<T>::value) {
        if (std::abs(src_val - dest_val) <= std::numeric_limits<T>::epsilon()) {
          if (chunk->IsNull(line, arg_idx2)) {
            AggregateFunc::SetNull(bitmap, col_idx_);
          } else {
            AggregateFunc::SetNotNull(bitmap, col_idx_);
            std::memcpy(dest + offset_, src_extend, len_);
          }
        }
      } else {
        if (src_val == dest_val) {
          if (chunk->IsNull(line, arg_idx2)) {
            AggregateFunc::SetNull(bitmap, col_idx_);
          } else {
            AggregateFunc::SetNotNull(bitmap, col_idx_);
            std::memcpy(dest + offset_, src_extend, len_);
          }
        }
      }
    }
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!data_container) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    T min_val;

    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      min_val = min_val_;
    } else {
      min_val = std::numeric_limits<T>::max();
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val = std::numeric_limits<T>::max();
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        is_dest_null = false;

        char* src_ptr = data_container->GetData(row, arg_idx);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val - src_val > std::numeric_limits<T>::epsilon()) {
            min_val = src_val;
            is_last_null_ = data_container->IsNull(row, arg_idx2);
            if (!is_last_null_) {
              last_ptr_ = data_container->GetData(row, arg_idx2);
              if (is_string_) {
                last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
              } else {
                last_str_ = String(last_ptr_, len_);
              }
            }
          }
        } else {
          if (src_val < min_val || min_val == std::numeric_limits<T>::max()) {
            min_val = src_val;
            is_last_null_ = data_container->IsNull(row, arg_idx2);
            if (!is_last_null_) {
              last_ptr_ = data_container->GetData(row, arg_idx2);
              if (is_string_) {
                last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
              } else {
                last_str_ = String(last_ptr_, len_);
              }
            }
          }
        }
      }

      data_container->NextLine();
    }
    min_val_ = min_val;
    is_dest_null_ = is_dest_null;
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                    GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (data_container == nullptr) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = data_container->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    String min_val;

    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      min_val = min_val_;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val = String();
        is_dest_null = true;
      }

      if (!data_container->IsNull(row, arg_idx)) {
        char* src_ptr = data_container->GetData(row, arg_idx);

        k_uint16 src_len = *reinterpret_cast<k_uint16*>(src_ptr);
        String src_val = String(src_ptr + STRING_WIDE, src_len);

        if (is_dest_null) {
          is_dest_null = false;
          min_val = src_val;
          is_last_null_ = data_container->IsNull(row, arg_idx2);
          if (!is_last_null_) {
            last_ptr_ = data_container->GetData(row, arg_idx2);
            if (is_string_) {
              last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
            } else {
              last_str_ = String(last_ptr_, len_);
            }
          }
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
          is_last_null_ = data_container->IsNull(row, arg_idx2);
          if (!is_last_null_) {
            last_ptr_ = data_container->GetData(row, arg_idx2);
            if (is_string_) {
              last_str_ = String(last_ptr_ + STRING_WIDE, *reinterpret_cast<k_uint16*>(last_ptr_));
            } else {
              last_str_ = String(last_ptr_, len_);
            }
          }
        }
      }

      data_container->NextLine();
    }
    is_dest_null_ = is_dest_null;
    if (!is_dest_null_) {
      min_val_ = min_val.clone();
    }
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  int addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, IChunk* data_container,
                  GroupByMetadata& group_by_metadata, DistinctOpt& distinctOpt) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("min_extend doesn't support decimal.");
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, data_container, group_by_metadata, distinctOpt);
    }
    return 0;
  }

  void handleNumber(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    T min_val;
    auto* arg_field = renders[arg_idx];
    auto* arg_field2 = renders[arg_idx2];
    auto storage_type2 = arg_field2->get_storage_type();
    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      min_val = min_val_;
    } else {
      min_val = std::numeric_limits<T>::max();
    }


    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val = std::numeric_limits<T>::max();
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        is_dest_null = false;

        char* src_ptr = arg_field->get_ptr(row_batch);

        T src_val = *reinterpret_cast<T*>(src_ptr);
        if constexpr (std::is_floating_point<T>::value) {
          if (min_val - src_val > std::numeric_limits<T>::epsilon()) {
            min_val = src_val;
            is_last_null_ = arg_field2->CheckNull();
            last_str_ = arg_field2->ValStrFromBatch(row_batch);
          }
        } else {
          if (src_val < min_val || min_val == std::numeric_limits<T>::max()) {
            min_val = src_val;
            is_last_null_ = arg_field2->CheckNull();
            last_str_ = arg_field2->ValStrFromBatch(row_batch);
          }
        }
      }

      row_batch->NextLine();
    }

    min_val_ = min_val;
    is_dest_null_ = is_dest_null;
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void handleString(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                    GroupByMetadata& group_by_metadata, Field** renders) {
    k_uint32 chunk_idx = 0;
    k_int32 target_row = start_line_in_begin_chunk;
    auto current_data_chunk_ = chunks[chunk_idx];
    if (!row_batch) {
      if (!is_dest_null_ && !is_last_null_) {
        current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
      }
      return;
    }
    k_uint32 arg_idx = arg_idx_[0];
    k_uint32 arg_idx2 = arg_idx_[1];
    auto data_container_count = row_batch->Count();
    auto chunk_capacity = current_data_chunk_->Capacity();

    bool is_dest_null = true;
    String min_val;

    auto* arg_field = renders[arg_idx];
    auto* arg_field2 = renders[arg_idx2];
    auto storage_type = arg_field->get_storage_type();
    auto storage_type2 = arg_field2->get_storage_type();
    if (target_row >= 0) {
      is_dest_null = is_dest_null_;
      min_val = min_val_;
    }

    for (k_uint32 row = 0; row < data_container_count; ++row) {
      if (group_by_metadata.isNewGroup(row)) {
        // save the agg result of last bucket
        if (!is_dest_null && !is_last_null_) {
          current_data_chunk_->InsertData(target_row, col_idx_, last_str_.ptr_, last_str_.length_, true);
        }

        // if the current chunk is full.
        if (target_row == chunk_capacity - 1) {
          current_data_chunk_ = chunks[++chunk_idx];
          target_row = 0;
        } else {
          ++target_row;
        }
        min_val = String();
        is_dest_null = true;
      }

      if (!(arg_field->CheckNull())) {
        String src_val = arg_field->ValStrFromBatch(row_batch);
        if (is_dest_null) {
          is_dest_null = false;
          min_val = src_val;
          is_last_null_ = arg_field2->CheckNull();
          last_str_ = arg_field2->ValStrFromBatch(row_batch);
        } else if (src_val.compare(min_val) < 0) {
          min_val = src_val;
          is_last_null_ = arg_field2->CheckNull();
          last_str_ = arg_field2->ValStrFromBatch(row_batch);
        }
      }

      row_batch->NextLine();
    }
    is_dest_null_ = is_dest_null;
    if (!is_dest_null_) {
      min_val_ = min_val.clone();
    }
    if (!is_last_null_) {
      last_str_ = last_str_.clone();
    }
  }

  void addOrUpdate(std::vector<DataChunk*>& chunks, k_int32 start_line_in_begin_chunk, RowBatch* row_batch,
                   GroupByMetadata& group_by_metadata, Field** renders) override {
    if constexpr (std::is_same_v<T, String>) {
      handleString(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    } else if constexpr (std::is_same_v<T, k_decimal>) {
      LOG_ERROR("min_extend doesn't support decimal.");
    } else {
      handleNumber(chunks, start_line_in_begin_chunk, row_batch, group_by_metadata, renders);
    }
  }
};

class AggregateFuncFactory {
 public:
  static AggregateFunc* CreateMax(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                  k_uint32 len);
  static AggregateFunc* CreateMin(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                  k_uint32 len);
  static AggregateFunc* CreateAnyNotNull(roachpb::DataType storage_type, k_int32 col_index,
                                                         k_int32 col_id, k_uint32 len);
  static AggregateFunc* CreateSum(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                  k_uint32 len);
  static AggregateFunc* CreateSumInt(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                     k_uint32 len);
  static AggregateFunc* CreateCount(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                    k_uint32 len);
  static AggregateFunc* CreateCountRow(k_int32 col_index, k_uint32 len);
  static AggregateFunc* CreateLast(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                   k_uint32 len, k_uint32 ts_col_id, k_int64 time);
  static AggregateFunc* CreateLastTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                     k_uint32 ts_col_id, k_int64 time);
  static AggregateFunc* CreateLastRow(roachpb::DataType storage_type, k_int32 col_index,
                                                      k_int32 col_id, k_uint32 len, k_uint32 ts_col_id);
  static AggregateFunc* CreateLastRowTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                        k_uint32 ts_col_id);
  static AggregateFunc* CreateFirst(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                    k_uint32 len, k_uint32 ts_col_id);
  static AggregateFunc* CreateFirstTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                      k_uint32 ts_col_id);
  static AggregateFunc* CreateFirstRow(roachpb::DataType storage_type, k_int32 col_index,
                                                       k_int32 col_id, k_uint32 len, k_uint32 ts_col_id);

  static AggregateFunc* CreateFirstRowTS(k_int32 col_index, k_int32 col_id, k_uint32 len,
                                                         k_uint32 ts_col_id);

  static AggregateFunc* CreateSTDDEVRow(roachpb::DataType storage_type, k_int32 col_index,
                                                        k_int32 col_id, k_uint32 len);

  static AggregateFunc* CreateAVGRow(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                     k_uint32 len);
  static AggregateFunc* CreateTwa(roachpb::DataType storage_type, k_int32 col_index, k_int32 col_id,
                                                  k_uint32 len, k_uint32 ts_col_id, k_double64 const_val);
  static AggregateFunc* CreateElapsed(roachpb::DataType storage_type, k_int32 col_index,
                                                      k_int32 col_id, k_uint32 len, std::string& time);
  static AggregateFunc* CreateMaxExtend(roachpb::DataType storage_type, k_int32 col_index,
                                                        k_int32 col_id, k_uint32 len2, k_int32 col_id2, bool is_string);
  static AggregateFunc* CreateMinExtend(roachpb::DataType storage_type, k_int32 col_index,
                                                        k_int32 col_id, k_uint32 len2, k_int32 col_id2, bool is_string);
};

}  // namespace kwdbts
