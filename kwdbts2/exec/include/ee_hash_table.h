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

#include <memory>
#include <array>
#include <vector>
#include "ee_base_hash_table.h"

namespace kwdbts {

class AggregateFunc;

/**
   A linear probing hash table that is used for computing aggregates groups.
   When agg_width_ is 0, LinearProbingHashTable is used as a hash set for
   distinct operation.
*/
class LinearProbingHashTable : public BaseHashTable {
 public:
  LinearProbingHashTable(const std::vector<roachpb::DataType>& group_types, const std::vector<k_uint32>& group_lens,
                         k_uint32 agg_width, const std::vector<bool>& group_allow_null, k_bool allow_abandoned);

  ~LinearProbingHashTable() override;

  KStatus Initialize(k_uint64 capacity = INIT_CAPACITY) override;

  KStatus Resize(k_uint64 size = INIT_CAPACITY,
                 PTDFeedBack feedback = PTDFeedBack::REHASH,
                 k_uint32 tuple_data_index = 0) override;

  inline k_uint64 GroupNum() const { return group_num_; }

  KStatus FindOrCreateGroupsAndAddTuple(IChunk* chunk, k_uint64 row,
                        const std::vector<k_uint32>& group_cols,
                        DatumPtr &agg_ptr, size_t *hash_val,
                        k_bool *is_used, k_bool *is_abandoned) override;

  int FindOrCreateGroups(const BaseHashTable& other,
                              k_uint64 row, k_uint64 *loc, k_bool *is_used) override;

  int FindOrCreateGroups(const DatumPtr tuple_data, k_uint64 *loc, size_t *hash_val, k_bool *is_used) override;

  KStatus FindOrCreateGroupsAndAddTuple(const DatumPtr tuple_data,
                                        k_uint64* loc, size_t* hash_val,
                                        k_bool* is_used,
                                        k_bool* is_abandoned) override;

  int CreateNullGroups(k_uint64 *loc, k_bool *is_used) override;

  void CopyGroups(IChunk* chunk, k_uint64 row,
                  const std::vector<k_uint32>& group_cols, k_uint64 loc, size_t hash_val) override;

  void CopyGroups(IChunk* chunk, k_uint64 row,
                  const std::vector<k_uint32>& group_cols, DatumPtr tuple, size_t hash_val);

  bool IsUsed(k_uint64 loc) override;

  DatumPtr GetTuple(k_uint64 loc) const override;

  k_uint64 GetHashVal(k_uint64 loc) const override;

  DatumPtr GetAggResult(k_uint64 loc) const override;

  KStatus Combine(std::vector<AggregateFunc*>* funcs, k_uint32 agg_null_offset) override;

  KStatus SaveAggTupleToDisk(DatumPtr agg_ptr) override;

  void HashColumn(const DatumPtr ptr, roachpb::DataType type,
                  std::size_t* h) const override;

  std::size_t HashGroups(IChunk* chunk, k_uint64 row,
                         const std::vector<k_uint32>& group_cols) const override;

  std::size_t HashGroups(k_uint64 loc) const override;

  std::size_t HashGroups(DatumPtr tuple_data) const override;

  bool CompareGroups(const std::vector<k_uint32>& group_cols, k_uint64 loc) override;

  bool CompareGroups(DatumPtr tuple_data, k_uint64 loc) override;

  friend bool CompareGroups(const BaseHashTable& left, k_uint64 lloc,
                            const BaseHashTable& right, k_uint64 rloc);

  DatumRowPtr NextLine() override;
  friend class HashTableIterator;

 private:
  struct SpillPartitionState {
    IOCacheHandler io_cache_handler_{0};
    k_uint64 offset_{0};
    k_uint32 count_{0};
    k_uint32 read_count_{0};
  };

  static constexpr k_uint32 kSpillPartitionNum = 32;

  KStatus EnsureSpillSerializeBuffer(k_uint32 min_size);
  KStatus SaveToSpillPartition(k_uint32 part_id, DatumPtr tuple_data,
                               k_uint32 tuple_size);
  EEIteratorErrCode LoadNextFromSpillPartition(k_uint32 part_id,
                                               DatumPtr tuple_data,
                                               k_uint32 tuple_capacity,
                                               k_uint32* tuple_size);
  void ResetSpillPartitions(std::unique_ptr<SpillPartitionState[]>& parts);
  void StartSpillReadPhase();

  DatumPtr spill_serialize_buf_{nullptr};
  k_uint32 spill_serialize_buf_size_{0};

  std::unique_ptr<SpillPartitionState[]> spill_partitions_1_;
  std::unique_ptr<SpillPartitionState[]> spill_partitions_2_;
  std::unique_ptr<SpillPartitionState[]>* write_spill_partitions_{&spill_partitions_1_};
  std::unique_ptr<SpillPartitionState[]>* read_spill_partitions_{&spill_partitions_2_};
  k_uint32 read_partition_idx_{0};
  k_bool spill_read_phase_{false};
};

}  // namespace kwdbts
