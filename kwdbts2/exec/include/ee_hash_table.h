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

#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"

namespace kwdbts {

class LinearProbingHashTable;

// hash table iterator
class HashTableIterator {
 public:
  HashTableIterator() {}

  HashTableIterator(const LinearProbingHashTable* ht, k_uint64 loc_idx)
      : ht_(ht), loc_idx_(loc_idx) {}

  HashTableIterator& operator++();

  DatumPtr operator*();

  bool operator==(const HashTableIterator& t) const;

  bool operator!=(const HashTableIterator& t) const;

 private:
  const LinearProbingHashTable* ht_{nullptr};
  k_uint64 loc_idx_{0};
};

/**
   A linear probing hash table that is used for computing aggregates groups.
   When agg_width_ is 0, LinearProbingHashTable is used as a hash set for
   distinct operation.
*/
class LinearProbingHashTable {
  struct GroupColData {
    bool null{false};
    DatumPtr ptr;
  };

 public:
  typedef HashTableIterator iterator;

  LinearProbingHashTable(const std::vector<roachpb::DataType>& group_types, const std::vector<k_uint32>& group_lens,
                         k_uint32 agg_width, const std::vector<bool>& group_allow_null);

  ~LinearProbingHashTable();

  LinearProbingHashTable(const LinearProbingHashTable&) = delete;

  inline k_uint64 GroupNum() const { return group_num_; }

  inline k_uint64 Size() const { return entries_.size(); }

  inline k_uint64 tupleSize() const { return tuple_size_; }

  inline k_uint64 Capacity() const { return capacity_; }

  inline bool Empty() { return entries_.size() == 0; }

  /**
   * @brief resize the hash table
   * @param[in] size size needs to be a power of 2
   * @param[out] return -1 when resize fails
   */
  int Resize(k_uint64 size = INIT_CAPACITY);

  /**
   * @brief find the location of existing group in the hash table or create a
   * new group based on the row of data chunk
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols
   * @param[out] location of existing group or new group.
   *
   * @return return -1 if resize fails
   */
  int FindOrCreateGroups(IChunk* chunk, k_uint64 row,
                        const std::vector<k_uint32>& group_cols,
                        k_uint64 *loc);

  /**
   * @brief find the location of the group keys during resize
   * @param[in] other hash table before resize
   * @param[in] row location of hash table before resize
   * @param[out] location in the resized hash table
   *
   * @return return -1 if unexpected error occurs
   */
  int FindOrCreateGroups(const LinearProbingHashTable& other,
                              k_uint64 row, k_uint64 *loc);

  /**
   * @brief find the location for NULL group key
   */
  k_uint64 CreateNullGroups();

  /**
   * @brief copy group columns from the row of data chunk to hash table
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols
   * @param[in] loc
   */
  void CopyGroups(IChunk* chunk, k_uint64 row,
                  const std::vector<k_uint32>& group_cols, k_uint64 loc);

  /**
   * @brief whether the location in the hash table is used. When the tuple
   * in the hash table is not used, it is caller's responsibility to intialize
   * aggregation result columns (InitFirstLastTimeStamp).
   */
  inline bool IsUsed(k_uint64 loc) {
    size_t idx = loc / 8;
    k_uint8 bit = loc % 8;
    return (used_bitmap_[idx] >> bit) & 1;
  }

  /**
   * @brief set the location in the hash table used
   */
  void SetUsed(k_uint64 loc) {
    size_t idx = loc / 8;
    k_uint8 bit = loc % 8;
    used_bitmap_[idx] |= (1 << bit);
  }

  /**
   * @brief get tuple pointer in the hash table
   */
  inline DatumPtr GetTuple(k_uint64 loc) const { return data_ + tuple_size_ * loc; }

  /**
   * @brief get aggregation result pointer in the hash table
   */
  DatumPtr GetAggResult(k_uint64 loc) const {
    return data_ + tuple_size_ * loc + group_width_;
  }

  // iterator begin & end
  iterator begin() const { return iterator(this, 0); }

  iterator end() const { return iterator(this, entries_.size()); }

  friend bool CompareGroups(const LinearProbingHashTable& left, k_uint64 lloc,
                            const LinearProbingHashTable& right, k_uint64 rloc);

  friend class HashTableIterator;

 private:
  /**
   * @brief hash one column
   */
  void HashColumn(const DatumPtr ptr, roachpb::DataType type,
                  std::size_t* h) const;

  /**
   * @brief combined hash of group columns from the data chunk
   */
  std::size_t HashGroups(IChunk* chunk, k_uint64 row, const std::vector<k_uint32>& group_cols) const;

  /**
   * @brief combined hash of group columns in the hash table
   */
  std::size_t HashGroups(k_uint64 loc) const;

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
  bool CompareGroups(const std::vector<k_uint32>& group_cols, k_uint64 loc);

  std::vector<roachpb::DataType> group_types_;
  k_uint32 group_num_{0};
  std::vector<k_uint32> group_lens_;
  std::vector<k_uint32> group_offsets_;
  std::vector<bool> group_allow_null_;

  k_uint64 capacity_{0};
  k_uint64 mask_{0};
  std::vector<k_uint64> entries_;

  k_uint32 group_null_offset_{0};  // offset to group null bitmap
  k_uint32 group_width_{0};        // group columns + null bitmap
  k_uint32 agg_width_{0};          // agg result columns + null_bitmap

  // status flag + group_width_ + agg_width_
  k_uint32 tuple_size_{0};

  char* data_{nullptr};
  char* used_bitmap_{nullptr};
  GroupColData *group_data_;
  static const std::size_t INIT_HASH_VALUE = 13;
  static const k_uint64 INIT_CAPACITY = 4*1024;
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
bool CompareGroups(const LinearProbingHashTable& left, k_uint64 lloc,
                   const LinearProbingHashTable& right, k_uint64 rloc);

}  // namespace kwdbts
