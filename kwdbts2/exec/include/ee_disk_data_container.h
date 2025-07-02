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
#include <queue>
#include <vector>

#include "data_type.h"
#include "ee_data_chunk.h"
#include "ee_data_container.h"
#include "ee_exec_pool.h"
#include "ee_io_cache_handler.h"
#include "ee_memory_data_container.h"
#include "ee_sort_row_chunk.h"
#include "kwdb_type.h"
namespace kwdbts {

const k_uint8 MAX_CHUNK_BATCH_NUM = 7;

struct io_file_reader_t {
  SortRowChunkPtr chunk_ptr_{nullptr};
  k_uint32 chunk_index_{0};
};

struct chunk_info_t {
  k_uint32 count_{0};
  k_uint32 start_row_index_{0};
  k_uint64 offset_{0};
  k_uint32 non_constant_data_size_{0};
};

struct merge_info_t {
  std::vector<chunk_info_t> chunk_infos_;
  std::vector<std::queue<k_size_t>> batch_chunk_indexs_;
  k_uint32 count_{0};
  IOCacheHandler io_cache_handler_{0};
};
/**
 * @class LoserTree
 * @brief A loser tree used for efficient k-way merging. It helps to find the minimum element 
 *        among multiple sorted sequences in O(log k) time.
 */
class LoserTree {
 public:
  LoserTree() = default;
    /**
   * @brief Initializes the loser tree.
   * 
   * @param size The number of input sequences (i.e., the size of the loser tree).
   * @param reader_ptrs A reference to a vector of file readers, each representing a sorted sequence.
   * @param compare A pointer to a column comparator used to compare elements.
   */
  void Init(k_int32 size, std::vector<io_file_reader_t>& reader_ptrs,
            ColumnCompare* compare);
  void Adjust(k_int32 s);
  EEIteratorErrCode GetWinner(DatumPtr& data_ptr, k_int32& winner_index);
  void Input(DatumPtr data_ptr);
  ColumnCompare* compare_;
  k_int32 size_{0};
  std::vector<k_int32> tree_;
  std::vector<DatumPtr> data_;
};

/**
 * @class DiskDataContainer
 * @brief A class representing a data container that stores and sort data on disk.
 * 
 * This class inherits from `DataContainer` and `ISortableChunk`, which means it provides
 * the basic functionality of a data container and can sort the stored data. It manages
 * disk I/O operations, handles data appending, sorting, and retrieval. It uses a loser tree
 * for efficient merging of sorted data chunks and maintains various metadata about the
 * stored data.
 */
class DiskDataContainer : public DataContainer, public ISortableChunk {
 public:
  DiskDataContainer(std::vector<ColumnOrderInfo>& order_info,
                         ColumnInfo* col_info, k_int32 col_num)
      : order_info_(order_info),
        col_info_(col_info),
        col_num_(col_num),
        row_size_(SortRowChunk::ComputeRowSize(col_info, order_info_, col_num)),
        sort_row_size_(
            SortRowChunk::ComputeSortRowSize(col_info, order_info_, col_num)) {
    write_merge_infos_ = &merge_info_1_;
    read_merge_infos_ = &merge_info_2_;
  }

  ~DiskDataContainer() override { Reset(); }

  KStatus Init() override;

  void Sort() override;

  KStatus Append(DataChunkPtr& chunk);
  k_uint32 Count() override { return count_; }

  EEIteratorErrCode NextChunk(DataChunkPtr& data_chunk) override;

  [[nodiscard]] k_uint32 GetSortRowSize() override { return sort_row_size_; }

  KStatus Append(std::queue<DataChunkPtr>& buffer) override { return FAIL; }

  bool IsNull(k_uint32 row, k_uint32 col) override;

  bool IsNull(k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col) override;

  DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len) override;

  DatumPtr GetData(k_uint32 col) override;

    std::vector<ColumnOrderInfo>* GetOrderInfo() override { return &order_info_; }

  ColumnInfo* GetColumnInfo() override { return col_info_; }

  k_uint32* GetColOffset() override { return col_offset_; }

  k_int32 NextLine() override;

 private:
  k_uint32 ComputeCapacity();

  void Reset();

  KStatus UpdateReadCacheChunk();
  KStatus UpdateWriteCacheChunk();
  KStatus UpdateTempCacheChunk();
  void ReverseFile();

 private:
  std::vector<ColumnOrderInfo> order_info_;

  ColumnInfo* col_info_{nullptr};  // column info
  k_int32 col_num_{0};
  k_uint32 row_size_{0};
  k_uint32 sort_row_size_{0};
  k_uint64 count_{0};         // total row number
  k_int64 sorted_count_{0};         // total row number
  k_int64 current_line_{-1};  // current row
  k_uint8 current_read_pool_size_{MAX_CHUNK_BATCH_NUM};
  k_bool all_constant_{true};
  k_bool all_constant_in_order_col_{true};

  k_int32 current_chunk_{-1};
  std::vector<io_file_reader_t> cache_chunk_readers_;
  SortRowChunkPtr write_cache_chunk_ptr_{nullptr};
  DataChunkPtr output_chunk_ptr_{nullptr};
  k_uint32 output_chunk_start_row_index_{0};
  ColumnCompare* compare_{nullptr};
  LoserTree loser_tree_;

  merge_info_t merge_info_1_;
  merge_info_t merge_info_2_;
  merge_info_t* read_merge_infos_{nullptr};
  merge_info_t* write_merge_infos_{nullptr};

  k_bool read_force_constant_{false};
  k_bool write_force_constant_{false};

  KStatus SortAndFlushLastChunk(k_bool force_merge);
/**
 * @brief Merges multiple sorted lists using the loser tree and appends the merged data to the write cache chunk.
 * 
 * This function uses the loser tree to merge multiple sorted data lists from different chunks.
 * It iteratively selects the smallest element from the available lists, appends it to the write cache chunk,
 * and updates the loser tree accordingly. It also handles cases where a chunk is exhausted and needs to be replaced.
 * 
 * @param start_batch_index The starting index of the batch from which data chunks are merged.
 * @param sorted_count Pointer to the number of already sorted and merged rows.
 * @param with_limit_offset A boolean flag indicating whether to apply limit and offset constraints.
 * @return EEIteratorErrCode Returns EE_OK if data is successfully merged, EE_END_OF_RECORD if there is no more data to merge,
 *         or EE_ERROR if an error occurs during the merge process.
 */
  EEIteratorErrCode mergeMultipleLists(k_uint32 start_batch_index,
                                       k_int64* sorted_count,
                                       k_bool with_limit_offset = false);

/**
 * @brief Performs a divide-and-conquer merge operation on the data chunks in the disk data container.
 * 
 * This function recursively divides the data chunks into smaller batches and merges them.
 * It uses the loser tree to efficiently merge sorted chunks. After each merge, it checks
 * if further merging is required and calls itself recursively if necessary.
 * 
 * @return KStatus Returns KStatus::SUCCESS if the merge operation is successful, 
 *         otherwise returns an appropriate error status.
 */
  KStatus divideAndConquerMerge();
  KStatus Write(SortRowChunkPtr& chunk);
  /**
 * @brief Reads the next data chunk from the disk into the cache.
 * 
 * This function attempts to read the next data chunk from the disk based on the given batch index.
 * It checks if the batch index is valid and if there are more chunks in the batch. If valid,
 * it updates the cache reader with the new chunk's information and reads the chunk data from disk.
 * 
 * @param batch_index The index of the batch from which to read the next chunk.
 * @return EEIteratorErrCode Returns EE_OK if the chunk is successfully read, 
 *         EE_ERROR if the batch index is invalid, or EE_END_OF_RECORD if there are no more chunks in the batch.
 */
  EEIteratorErrCode ReadNextChunk(k_uint32 batch_index);

/**
 * @brief Reloads the read pointers for the disk data container.
 * 
 * This function resets existing chunk readers, expands the reader pool if necessary,
 * and loads new data chunks into the readers based on the provided batch index range.
 * It also updates the comparison function based on whether the columns are all constant.
 * 
 * @param ptr_pool_size The size of the pointer pool to be used for read operations.
 * @param start_batch_index The starting index of the batch from which data chunks will be loaded.
 */
  void ReloadReadPtr(k_uint32 ptr_pool_size, k_uint32 start_batch_index);
};

}  //  namespace kwdbts
