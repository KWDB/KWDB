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

#include <vector>
#include <memory>

#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"
#include "ee_hash_table_entry.h"
#include "ee_hash_table_tuple_data.h"

namespace kwdbts {

class AggregateFunc;
class BaseHashTable;

/**
   Abstract base class for hash tables used in aggregation operations.
   Provides common interface and shared implementation for different hash table types.
*/
class BaseHashTable {
  struct GroupColData {
    bool null{false};
    DatumPtr ptr;
  };

 public:
  BaseHashTable(const std::vector<roachpb::DataType>& group_types,
                const std::vector<k_uint32>& group_lens, k_uint32 agg_width,
                const std::vector<bool>& group_allow_null, k_bool allow_abandoned);

  virtual ~BaseHashTable();

  BaseHashTable(const BaseHashTable&) = delete;
  BaseHashTable& operator=(const BaseHashTable&) = delete;

  virtual KStatus Initialize(k_uint64 capacity = INIT_CAPACITY) = 0;

  inline k_uint64 GroupNum() const { return group_types_.size(); }

  inline k_uint64 Size() const { return count_; }

  inline k_uint64 TupleSize() const { return tuple_effective_size_; }

  inline k_uint64 TupleStorageSize() const { return tuple_size_; }

  inline k_uint64 AggWidth() const { return agg_effective_width_; }

  inline k_uint64 AggStorageWidth() const { return agg_width_; }

  inline k_uint64 AbandonedSize() const { return abandoned_count_; }

  inline k_uint64 Capacity() const { return capacity_; }

  inline bool Empty() { return count_ == 0; }

  /**
   * @brief resize the hash table
   * @param[in] size size needs to be a power of 2
   * @param[in] feedback feedback from tuple data
   * @param[in] tuple_data_index index of tuple data in the hash table
   * @param[out] return KStatus::SUCCESS if resize successfully
   */
  virtual KStatus Resize(k_uint64 size = INIT_CAPACITY,
                         PTDFeedBack feedback = PTDFeedBack::REHASH,
                         k_uint32 tuple_data_index = 0) = 0;

  /**
   * @brief find the location of existing group in the hash table or create a
   * new group based on the row of data chunk
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols index of tuple data in the hash table
   * @param[out] location of existing group or new group.
   * @param[out] hash_val hash value of the group keys
   * @param[out] is_used whether the location in the hash table is used
   *
   * @return return KStatus::SUCCESS if find or create groups successfully
   */
  virtual KStatus FindOrCreateGroupsAndAddTuple(IChunk* chunk, k_uint64 row,
                         const std::vector<k_uint32>& group_cols,
                         DatumPtr &agg_ptr, size_t *hash_val, k_bool *is_used, k_bool *is_abandoned) = 0;

  /**
   * @brief find the location of the group keys during resize
   * @param[in] other hash table before resize
   * @param[in] row location of hash table before resize
   * @param[out] location in the resized hash table
   *
   * @return return -1 if unexpected error occurs
   */
  virtual int FindOrCreateGroups(const BaseHashTable& other,
                         k_uint64 row, k_uint64 *loc, k_bool *is_used) = 0;


  /**
   * @brief find the location of the group keys during resize
   * @param[in] tuple_data 
   * @param[out] location in the resized hash table
   * @param[out] hash_val hash value of the group keys
   * @param[out] is_used whether the location in the hash table is used
   *
   * @return return -1 if unexpected error occurs
   */
  virtual int FindOrCreateGroups(const DatumPtr tuple_data, k_uint64 *loc, size_t *hash_val, k_bool *is_used) = 0;

    /**
   * @brief find the location of the group keys and add tuple to the hash table
   * @param[in] tuple_data 
   * @param[out] location in the resized hash table
   * @param[out] hash_val hash value of the group keys
   * @param[out] is_used whether the location in the hash table is used
   *
   * @return return -1 if unexpected error occurs
   */
  virtual KStatus FindOrCreateGroupsAndAddTuple(const DatumPtr tuple_data,
                                                k_uint64* loc, size_t* hash_val,
                                                k_bool* is_used,
                                                k_bool* is_abandoned) = 0;

  /**
   * @brief find the location for NULL group key
   */
  virtual int CreateNullGroups(k_uint64 *loc, k_bool *is_used) = 0;

  /**
   * @brief copy group columns from the row of data chunk to hash table
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols
   * @param[in] loc location of the group keys in the hash table
   * @param[in] hash_val hash value of the group keys
   */
  virtual void CopyGroups(IChunk* chunk, k_uint64 row,
                 const std::vector<k_uint32>& group_cols, k_uint64 loc, size_t hash_val) = 0;

  /**
   * @brief whether the location in the hash table is used. When the tuple
   * in the hash table is not used, it is caller's responsibility to intialize
   * aggregation result columns (InitFirstLastTimeStamp).
   */
  virtual bool IsUsed(k_uint64 loc) = 0;

  /**
   * @brief get tuple pointer in the hash table
   */
  virtual DatumPtr GetTuple(k_uint64 loc) const = 0;

  virtual k_uint64 GetHashVal(k_uint64 loc) const = 0;

  /**
   * @brief get aggregation result pointer in the hash table
   */
  virtual DatumPtr GetAggResult(k_uint64 loc) const = 0;

  /**
   * @brief combine hash table data from disk
   * @param[in] funcs aggregation functions
   * @param[in] agg_null_offset offset to agg null bitmap
   *
   * @return If the combine is successful, return KStatus::SUCCESS; otherwise return
   * KStatus::ERROR
   */
  virtual KStatus Combine(std::vector<AggregateFunc*>* funcs, k_uint32 agg_null_offset) = 0;

  virtual KStatus SaveAggTupleToDisk(DatumPtr agg_ptr) = 0;

  /**
   * @brief hash one column
   */
  virtual void HashColumn(const DatumPtr ptr, roachpb::DataType type,
                 std::size_t* h) const = 0;

  /**
   * @brief combined hash of group columns from the data chunk
   */
  virtual std::size_t HashGroups(IChunk* chunk, k_uint64 row,
                    const std::vector<k_uint32>& group_cols) const = 0;

  /**
   * @brief combined hash of group columns in the hash table
   */
  virtual std::size_t HashGroups(k_uint64 loc) const = 0;

  /**
   * @brief combined hash of group columns in the tuple data
   */
  virtual std::size_t HashGroups(DatumPtr tuple_data) const = 0;

  /**
   * @brief compare group keys between data chunk and  hash tables
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols
   * @param[in] loc
   *
   * @return If the group keys are identical, return true; otherwise return
   * false
   */
  virtual bool CompareGroups(const std::vector<k_uint32>& group_cols, k_uint64 loc) = 0;

  /**
   * @brief compare group keys between data chunk and  hash tables
   * @param[in] tuple_data pointer to the tuple data
   * @param[in] loc location of the group in the hash table 
   *
   * @return If the group keys are identical, return true; otherwise return
   * false
   */
  virtual bool CompareGroups(DatumPtr tuple_data, k_uint64 loc) = 0;

  virtual DatumRowPtr NextLine() = 0;

  DatumRowPtr CurrentLine() {return tuple_data_->CurrentTuple() + group_width_;}

  void UpdateAggEffectiveWidth(k_uint32 agg_effective_width);

  k_uint32 group_null_offset_{0};  // offset to group null bitmap
  std::vector<roachpb::DataType> group_types_;
  std::vector<k_uint32> group_offsets_;
  std::unique_ptr<BaseTupleData> tuple_data_{nullptr};

 protected:
  std::vector<k_uint32> group_lens_;

  k_uint64 capacity_{0};
  k_uint64 mask_{0};
  k_uint64 count_{0};

  k_uint32 group_width_{0};        // group columns + null bitmap
  k_uint32 agg_width_{0};          // agg result columns + null_bitmap (storage width)
  k_uint32 agg_effective_width_{0};

  // storage width = group_width_ + agg_width_
  k_uint32 tuple_size_{0};
  k_uint32 tuple_effective_size_{0};
  k_uint32 group_effective_width_{0};

  void UpdateGroupEffectiveWidth(DatumPtr tuple_data);

  DatumPtr hash_entry_data_{nullptr};
  hash_table_entry_t* hash_entry_{nullptr};

  k_uint64 abandoned_count_{0};

  k_bool is_partitioned_{false};

  k_uint32 group_num_{0};
  std::vector<bool> group_allow_null_;
  char* used_bitmap_{nullptr};
  GroupColData *group_data_;
  k_bool allow_abandoned_{false};

  static const std::size_t INIT_HASH_VALUE = 13;
  static const k_uint64 INIT_CAPACITY = 4 * 1024;
};

/**
 * @brief compare column values
 * @param[in] left_ptr
 * @param[in] right_ptr
 * @param[in] type
 *
 * @return If the two column values are identical, return true; otherwise return
 * false
 */
bool CompareColumn(DatumPtr left_ptr, DatumPtr right_ptr,
                  roachpb::DataType type);

/**
 * @brief compare group keys in different hash tables during resize
 * @param[in] left
 * @param[in] lloc
 * @param[in] right
 * @param[in] rloc
 *
 * @return If the group keys are identical, return true; otherwise return false
 */
bool CompareGroups(const BaseHashTable& left, k_uint64 lloc,
                  const BaseHashTable& right, k_uint64 rloc);

}  // namespace kwdbts
