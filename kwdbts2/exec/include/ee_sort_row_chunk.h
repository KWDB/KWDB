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
#include <map>
#include <memory>
#include <vector>

#include "ee_common.h"
#include "ee_field.h"
#include "ee_radix_encoding.h"
#include "ee_row_batch.h"
#include "ee_table.h"
#include "kwdb_type.h"
#include "ts_common.h"
namespace kwdbts {
static const k_uint16 NON_CONSTANT_PLACE_HOLDER_WIDE = sizeof(k_uint64);

static const k_uint8 NULL_INDECATOR_WIDE = sizeof(k_bool);
static const k_uint8 LINE_NUMBER_WIDE = sizeof(k_uint64);
static const k_int8 NOT_ENCODED_COL = -1;

static const k_uint32 DEFAULT_CHUNK_SIZE = 64 * 1024;

class SortRowChunk;

typedef std::unique_ptr<SortRowChunk> SortRowChunkPtr;

enum NonConstantSaveMode { POINTER_MODE, OFFSET_MODE };

/**
 * @class ISortableChunk
 * @brief An abstract base class representing a sortable data chunk.
 * 
 * This class inherits from `DataChunk`, which means it retains the basic functionality of a data chunk.
 * It adds an interface for sorting operations, defining a set of pure virtual functions that derived classes
 * must implement. These functions are used to perform operations such as getting the size of a sortable row,
 * retrieving sorting order information, and getting column offsets. This class serves as a contract for all
 * sortable data chunk implementations in the system.
 */
class ISortableChunk : public DataChunk {
 public:
  virtual k_uint32 GetSortRowSize() = 0;
  virtual std::vector<ColumnOrderInfo> *GetOrderInfo() = 0;
  virtual k_uint32 *GetColOffset() = 0;

  KStatus EncodeColData(k_uint32 col, uint8_t *col_ptr, uint8_t *val_ptr);
  KStatus DecodeColData(k_uint32 col, DatumPtr col_ptr, uint8_t *val_ptr);
  KStatus EncodeColDataInvert(k_uint32 col, uint8_t *col_ptr, uint8_t *val_ptr);
  KStatus DecodeColDataInvert(k_uint32 col, DatumPtr col_ptr, uint8_t *val_ptr);
};
class ColumnCompare {
 public:
  virtual ~ColumnCompare() = default;
  virtual bool operator()(DatumPtr a_ptr, DatumPtr b_ptr) = 0;
  k_uint32 sort_row_size_;
};

/**
 * @class AllConstantColumnCompare
 * @brief A comparison class for rows with all constant columns.
 *
 * This class inherits from `ColumnCompare` and is designed to compare two rows that contain all constant columns.
 * It uses the provided sorting order information and column metadata to perform the comparison.
 * This comparison is crucial for sorting operations on data chunks where all columns have constant values.
 */
class AllConstantColumnCompare : public ColumnCompare {
 public:
  explicit AllConstantColumnCompare(ISortableChunk *chunk) : chunk_(chunk) {
    sort_row_size_ = chunk->GetSortRowSize();
  }

  ~AllConstantColumnCompare() override = default;

  bool operator()(DatumPtr a_ptr, DatumPtr b_ptr) override;

 protected:
  ISortableChunk *chunk_;
};
/**
 * @class HasNonConstantColumnCompare
 * @brief A comparison class for rows with non-constant columns.
 * 
 * This class inherits from `ColumnCompare` and is designed to compare two rows that contain non-constant columns.
 * It uses the provided sorting order information and column metadata to perform the comparison.
 * This comparison is crucial for sorting operations on data chunks that have varying values in some columns.
 */
class HasNonConstantColumnCompare : public ColumnCompare {
 public:
  explicit HasNonConstantColumnCompare(ISortableChunk *chunk)
      : chunk_(chunk),
        order_info_(chunk->GetOrderInfo()),
        col_info_(chunk->GetColumnInfo()),
        col_offset_(chunk->GetColOffset()) {
          sort_row_size_ = chunk->GetSortRowSize();
        }

  ~HasNonConstantColumnCompare() override = default;

  bool operator()(DatumPtr a_ptr, DatumPtr b_ptr) override;

 protected:
  ISortableChunk *chunk_;
  std::vector<ColumnOrderInfo> *order_info_{nullptr};
  ColumnInfo *col_info_{nullptr};
  k_uint32 *col_offset_{nullptr};
};
/**
 * @class SortRowChunk
 * @brief A class representing a chunk of sortable rows.
 * 
 * SortRowChunk inherits from ISortableChunk, which means it provides the basic functionality
 * of a sortable data chunk. It is designed to store and manage a collection of rows that can be sorted.
 * This class handles operations such as appending data, retrieving data, encoding and decoding column data,
 * and sorting the rows using different radix sort algorithms. It also manages metadata about the chunk,
 * such as the number of rows, column information, and sorting order.
 */
class SortRowChunk : public ISortableChunk {
 protected:
  k_uint32 chunk_size_{DEFAULT_CHUNK_SIZE};  // chunk size
  std::vector<ColumnOrderInfo> order_info_;
  k_uint32 row_size_{0};
  k_int8 *is_encoded_col_{nullptr};  // is_encoded_row
  k_uint32 sort_row_size_{0};
  k_uint64 base_count_{0};
  k_uint32 max_output_count_{UINT32_MAX};
  k_bool all_constant_{true};
  k_bool all_constant_in_order_col_{true};
  std::vector<k_uint32> non_constant_col_offsets_;
  k_uint64 non_constant_data_size_{0};
  k_bool is_ordered_{false};
  k_bool force_constant_{false};
  k_uint32 non_constant_max_row_size_{0};
  k_uint64 non_constant_max_size_{0};

 public:
  char *non_constant_data_{nullptr};
  NonConstantSaveMode non_constant_save_mode_{OFFSET_MODE};
  virtual ~SortRowChunk();
  explicit SortRowChunk(ColumnInfo *col_info,
                        std::vector<ColumnOrderInfo> order_info,
                        k_int32 col_num, k_uint64 chunk_size,
                        k_int64 max_output_count, k_uint64 base_count,
                        k_bool force_constant)
      : chunk_size_(chunk_size),
        order_info_(order_info),
        base_count_(base_count),
        max_output_count_(max_output_count),
        force_constant_(force_constant) {
    col_info_ = col_info;
    col_num_ = col_num;
  }

  explicit SortRowChunk(ColumnInfo *col_info,
                        std::vector<ColumnOrderInfo> order_info,
                        k_int32 col_num, k_uint32 capacity,
                        k_int64 max_output_count, k_uint64 base_count,
                        k_bool force_constant)
      : order_info_(order_info),
        base_count_(base_count),
        max_output_count_(max_output_count),
        force_constant_(force_constant) {
    capacity_ = capacity;
    col_info_ = col_info;
    col_num_ = col_num;
  }

  k_bool Initialize() override;
  ColumnInfo *GetColumnInfo() override { return ISortableChunk::col_info_; }

  // Append all columns whose row number are in [begin_row, end_row)
  KStatus Append(DataChunkPtr &data_chunk_ptr, k_uint32 &begin_row,
                 k_uint32 end_row);
  KStatus Append(SortRowChunkPtr &data_chunk_ptr, DatumPtr row_data_ptr);
  DatumPtr GetData(k_uint32 row, k_uint32 col) override;
  DatumPtr GetData(k_uint32 col) override;
  DatumPtr GetRowData(k_uint32 row);
  DatumPtr GetRowData();
  KStatus CopyWithSortFrom(SortRowChunkPtr &data_chunk_ptr);
  KStatus Sort();
  [[nodiscard]] inline k_uint32 Capacity() const { return capacity_; }
  [[nodiscard]] inline DatumPtr GetData() const { return data_; }
  [[nodiscard]] inline DatumPtr GetNonConstantData() const {
    return non_constant_data_;
  }
  [[nodiscard]] inline k_uint32 GetNonConstantDataSize() const {
    return non_constant_data_size_;
  }
  inline void SetMaxOutputCount(k_uint32 max_output_count) {
    max_output_count_ = max_output_count;
  }
  [[nodiscard]] inline k_bool IsAllConstant() const { return all_constant_; }
  [[nodiscard]] inline k_bool IsAllConstantInOrderCol() const {
    return all_constant_in_order_col_;
  }
  /**
   * @brief Check if the datachunk is full
   */
  [[nodiscard]] inline bool isFull() const { return count_ == capacity_; }
  [[nodiscard]] inline bool IsOrdered() const { return is_ordered_; }
  [[nodiscard]] k_uint32 Size() const { return count_ * row_size_; }
  [[nodiscard]] k_uint32 GetRowSize() const { return row_size_; }

  [[nodiscard]] k_uint32 GetSortRowSize() override { return sort_row_size_; };
  std::vector<k_uint32> *GetNonConstantColOffset() {
    return &non_constant_col_offsets_;
  }
  std::vector<ColumnOrderInfo> *GetOrderInfo() override { return &order_info_; }
  k_uint32 *GetColOffset() override { return col_offset_; }
  /**
   * @brief Get string pointer at  (row, col), and return the
   * string length
   * @param[in] row
   * @param[in] col
   * @param[in/out] string length
   */
  DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16 &len) override;

  void Reset(k_bool force_constant = false);

  /**
   * data count
   */
  k_uint32 Count() override;
  /**
   *  Move the cursor to the next line, default 0
   */
  k_int32 NextLine() override;
  /**
   *  Move the cursor to the first line
   */
  virtual void ResetLine();

  bool IsNull(k_uint32 row, k_uint32 col) override;

  bool IsNull(k_uint32 col) override;

  KStatus DecodeData();

  KStatus Expand(k_uint32 new_count, k_bool copy = true);

  k_uint32 *GetCount() { return &count_; }

  virtual bool SetCurrentLine(k_int32 line);
  void SetCount(k_uint32 count) {
    if (count > capacity_) {
      Expand(count, false);
    }
    count_ = count;
  }
  void SetBaseCount(k_uint32 base_count) { base_count_ = base_count; }

  static k_uint32 ComputeRowSize(ColumnInfo *col_info,
                                 std::vector<ColumnOrderInfo> order_info,
                                 k_uint32 col_num);
  static k_uint32 ComputeSortRowSize(ColumnInfo *col_info,
                                     std::vector<ColumnOrderInfo> &order_info,
                                     k_uint32 col_num);
  EEIteratorErrCode VectorizeData(kwdbContext_p ctx,
                                  DataInfo *data_info) override;

  void RadixSortLSD(DatumPtr input_data, DatumPtr output_data);
  void RadixSortMSD(DatumPtr input_data, DatumPtr output_data, k_uint32 start,
                    k_uint32 end, k_uint32 byte_pos);
  void ReplaceNonConstantRowData(DatumPtr src_row_data,
                                 DatumPtr src_non_const_data,
                                 k_uint32 row_index, NonConstantSaveMode mode);
  /**
 * @brief Calculates the row size assuming all columns are constant.
 * 
 * This function first checks the `all_constant_` flag. If all columns are already constant,
 * it directly returns the current row size `row_size_`. Otherwise, it estimates the row size
 * when all columns are treated as constant. For string columns, it considers the placeholder width,
 * string length indicator width, and the maximum string length. For non-string columns,
 * it uses the fixed storage length.
 * 
 * @return k_uint32 The estimated row size when all columns are constant.
 */
  k_uint32 ComputeRowSizeIfAllConstant();
  void ChangeOffsetToPointer();
  void ChangePointerToOffset();
};

};  // namespace kwdbts
