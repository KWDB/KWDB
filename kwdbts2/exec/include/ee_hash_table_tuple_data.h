// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

#include <vector>

#include "ee_data_container.h"
#include "ee_io_cache_handler.h"
#include "kwdb_type.h"

namespace kwdbts {

enum class PTDFeedBack {
  DO_NOTHING = 0,
  REHASH = 1,
  REPARTITION = 2,
  ABANDONED = 3,
  FAIL = 4,
};

// Forward declarations
class BaseTupleData;
class MemoryTupleData;
class PartitionedTupleData;

/**
 * @brief Base class for tuple data storage implementations
 * Provides common interface for different tuple data storage strategies
 */
class BaseTupleData {
 public:
  virtual ~BaseTupleData() = default;

  /**
   * @brief Get the next tuple pointer
   * @param[in] hash_val hash value for partitioning
   * @param[out] tuple_data pointer to the allocated tuple data
   * @return feedback indicating required action
   */
  virtual PTDFeedBack GetNextTuplePtr(size_t hash_val,
                                      DatumPtr& tuple_data) = 0;

  /**
   * @brief Get tuple data pointer at specific location
   * @param[in] loc location of tuple data
   * @return tuple data pointer
   */
  virtual DatumPtr GetTupleData(k_uint64 loc) const = 0;

  /**
   * @brief Resize the tuple data storage
   * @param[in] new_capacity new capacity
   * @return success or failure status
   */
  virtual KStatus Resize(k_uint64 new_capacity) = 0;

  /**
   * @brief Get current count of tuples
   * @return count of tuples
   */
  virtual k_uint64 GetCount() const = 0;

  /**
   * @brief Get current capacity
   * @return capacity of the storage
   */
  virtual k_uint64 GetCapacity() const = 0;

  /**
   * @brief Finish repartitioning process
   */
  virtual void FinishRepartition() = 0;

  virtual EEIteratorErrCode NextTuple(DatumPtr& tuple_data) = 0;

  /**
   * @brief Get current tuple pointer
   * @return current tuple pointer
   */
  virtual DatumPtr CurrentTuple() const = 0;


  virtual KStatus Reset() = 0;

  k_int32 current_line_{-1};

  // Common constants
  static const k_uint64 INIT_CAPACITY = 2 * 1024;

  static const k_uint64 MAX_MEMORY_SIZE = 256 * 1024 * 1024;  // 256MB
};
/**
 * @brief HashTableTupleData is used to store tuple data in multiple partitions.
 */
/**
 * @brief MemoryTupleData is used to store tuple data in memory without
 * partitioning Handles basic memory allocation and resizing for hash table
 * tuples
 */
class MemoryTupleData : public BaseTupleData {
 public:
  /**
   * @brief constructor
   * @param[in] tuple_size size of tuple data
   * @param[in] capacity capacity of tuple data
   */
  explicit MemoryTupleData(k_uint32 tuple_size, k_uint32 capacity = INIT_CAPACITY, k_bool allow_abandoned = true);

  ~MemoryTupleData() override;

  MemoryTupleData(const MemoryTupleData&) = delete;
  MemoryTupleData& operator=(const MemoryTupleData&) = delete;

  /**
   * @brief Get the next tuple pointer
   * @param[in] hash_val hash value (not used in non-partitioned implementation)
   * @param[out] tuple_data pointer to the allocated tuple data
   * @return feedback indicating required action
   */
  PTDFeedBack GetNextTuplePtr(size_t hash_val, DatumPtr& tuple_data) override;

  /**
   * @brief Get tuple data pointer at specific location
   * @param[in] loc location of tuple data
   * @return tuple data pointer
   */
  DatumPtr GetTupleData(k_uint64 loc) const override;

  /**
   * @brief Resize the tuple data storage
   * @param[in] new_capacity new capacity
   * @return success or failure status
   */
  KStatus Resize(k_uint64 new_capacity) override;

  /**
   * @brief Get current count of tuples
   * @return count of tuples
   */
  k_uint64 GetCount() const override { return count_; }

  /**
   * @brief Get current capacity
   * @return capacity of the storage
   */
  k_uint64 GetCapacity() const override { return capacity_; }

  /**
   * @brief Finish repartitioning process
   */
  void FinishRepartition() override;

  /**
   * @brief Get the next tuple pointer
   * @param[out] tuple_data pointer to the allocated tuple data
   * @return feedback indicating required action
   */
  EEIteratorErrCode NextTuple(DatumPtr& tuple_data) override;

  /**
   * @brief Get current tuple pointer
   * @return current tuple pointer
   */
  DatumPtr CurrentTuple() const override;

  KStatus Reset() override;

 private:
  k_uint32 tuple_size_;                    // Size of each tuple in bytes
  DatumPtr tuple_data_;                    // Pointer to the allocated memory
  k_uint32 count_{0};                      // Current number of tuples
  k_uint32 capacity_{0};                   // Maximum capacity
  k_bool allow_abandoned_{false};
  DatumPtr abandoned_tuple_;  // Pointer to the abandoned tuple data
};
}  // namespace kwdbts
