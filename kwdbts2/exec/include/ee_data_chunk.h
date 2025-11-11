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

#include <map>
#include <memory>
#include <numeric>
#include <queue>
#include <vector>

#include "cm_kwdb_context.h"
#include "ee_data_container.h"
#include "ee_encoding.h"
#include "ee_executor.h"
#include "ee_field.h"
#include "ee_field_func.h"
#include "ee_global.h"
#include "ee_string_info.h"
#include "ee_tag_row_batch.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"

namespace kwdbts {

/**
 * The data chunk class is the intermediate representation used by the execution
 * engine. DataChunk is initialized by the operators who needs to send data to
 * the father operators. For example:
 *           .----------------------.
 *           |    Synchronizer Op   |
 *           .----------------------.
 *                      ^
 *                      |  DataChuck
 *                      |
 *           .----------------------.
 *           |     TableScan Op     |
 *           .----------------------.
 *                      ^
 *                      |  Batch
 *                      |
 *             +-----------------+
 *             |     Storage     |
 *             +-----------------+
 *
 * Data in the chunk is organized in column format as following example. In
 * addition to holding the data, the DataChuck also owns columns' type/length
 * information and calculates the column offsets during initialization.
 *    - extra 2 bytes for strings column to keep string length
 *    - reserves maximum space for varchar/varbytes column. (Enhancement in the
 * future)
 *    - null bitmap at the begin of each row, length = (column_num + 7) / 8
 *
 */
class DataChunk : public IChunk {
 public:
  DataChunk() = default;

  /* Constructor & Deconstructor */
  explicit DataChunk(ColumnInfo* col_info, k_int32 col_num,
                     k_uint32 capacity = 0);

  explicit DataChunk(ColumnInfo* col_info, k_int32 col_num, const char* buf,
                     k_uint32 count, k_uint32 capacity);

  virtual ~DataChunk();
  /**
   * @return return false if memory allocation fails
   */
  virtual k_bool Initialize();

  void DebugPrintData();

  /* Getter && Setter */
  [[nodiscard]] inline k_uint32 ColumnNum() const { return col_num_; }

  [[nodiscard]] inline k_uint32 RowSize() const { return row_size_; }

  [[nodiscard]] inline k_uint32 BitmapSize() const { return bitmap_size_; }

  [[nodiscard]] virtual inline char* GetData() const { return data_; }

  ColumnInfo* GetColumnInfo() override { return col_info_; }

  k_uint32* GetColumnOffset() { return col_offset_; }

  k_uint32* GetBitmapOffset() { return bitmap_offset_; }

  virtual char* GetBitmapPtr(k_uint32 col);

  [[nodiscard]] inline k_uint32 Capacity() const { return capacity_; }

  [[nodiscard]] bool isDisorder() const { return disorder_; }

  void setDisorder(bool disorder) { disorder_ = disorder; }

  bool IsDataOwner() const { return is_data_owner_; }

  void SetCount(k_uint32 count) { count_ = count; }

  // data size in data chunk
  inline k_uint32 Size() { return data_size_; }

  /* override methods */
  virtual DatumPtr GetData(k_uint32 row, k_uint32 col);

  virtual DatumPtr GetDataPtr(k_uint32 col);

  virtual DatumPtr GetData(k_uint32 col);

  // get data pointer of a column for a specific row for multiple model
  // processing
  virtual DatumPtr GetDataPtr(k_uint32 row, k_uint32 col);

  virtual k_int32 NextLine();

  virtual k_uint32 Count();

  virtual bool IsNull(k_uint32 row, k_uint32 col);

  virtual bool IsNull(k_uint32 col);

  virtual KStatus Append(DataChunk* chunk);

  virtual KStatus Append(std::queue<DataChunkPtr>& buffer);

  // Append all columns whose row number are in [begin_row, end_row)
  virtual KStatus Append(DataChunk* chunk, k_uint32 begin_row,
                         k_uint32 end_row);

  KStatus Append_Selective(DataChunk* src, const k_uint32* indexes,
                           k_uint32 size);
  KStatus Append_Selective(DataChunk* src, k_uint32 row);
  k_int32 Compare(size_t left, size_t right, k_uint32 col_idx, DataChunk* rhs);
  DataChunkPtr CloneEmpty(k_uint32 num_rows) {
    DataChunkPtr chunk = std::make_unique<DataChunk>(col_info_, col_num_, num_rows);
    if (chunk == nullptr) {
      return nullptr;
    }
    if (!chunk->Initialize()) {
      chunk = nullptr;
      return nullptr;
    }
    return chunk;
  }
  ////////////////   Basic Methods   ///////////////////

  /**
   * @brief Check if the datachunk is full
   */
  [[nodiscard]] inline bool isFull() const { return count_ == capacity_; }

  /**
   * @brief increase the count
   */
  void AddCount(k_uint32 count = 1) { count_ += count; }

  /**
   * @brief reset current read line
   */
  void ResetLine();

  /**
   * @brief Set null at (row, col)
   * @param[in] row
   * @param[in] col
   */
  void SetNull(k_uint32 row, k_uint32 col);

  /**
   * @brief Set not null at (row, col)
   * @param[in] row
   * @param[in] col
   */
  void SetNotNull(k_uint32 row, k_uint32 col);

  /**
   * @brief Set all fields null in the data chunk
   */
  void SetAllNull();

  /**
   * @brief Get string pointer at  (row, col), and return the
   * string length
   * @param[in] row
   * @param[in] col
   * @param[in/out] string length
   */
  virtual DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len);

  /**
   * @brief Get string pointer at  (current_line_, col), and return the
   * string length
   * @param[in] row
   * @param[in] col
   * @param[in/out] string length
   */
  DatumPtr GetVarData(k_uint32 col, k_uint16& len);

  ////////////////   Insert/Copy Data   ///////////////////

  /**
   * @brief Insert data into location at (row, col)
   * @param[in] row
   * @param[in] col
   * @param[in] value data pointer to insert
   * @param[in] len data length
   */
  KStatus InsertData(k_uint32 row, k_uint32 col, DatumPtr value, k_uint16 len,
                     bool set_not_null = true);

  /**
   * @brief Put data into the data chunk, and the existing data will be
   * overwritten.
   * @param[in] ctx   kwdb context
   * @param[in] value data to be put into data chunk
   * @param[in] count number of rows
   */
  KStatus PutData(kwdbContext_p ctx, DatumPtr value, k_uint32 count);

  /**
   * @brief Insert one row from value or renders
   * @param[in] ctx
   * @param[in] value
   * @param[in] renders
   */
  KStatus InsertData(kwdbContext_p ctx, IChunk* value, Field** renders);
  /**
   * @brief Insert one row from value or renders
   * @param[in] ctx
   * @param[in] value
   * @param[in] Field*
   */
  KStatus InsertData(kwdbContext_p ctx, IChunk* value,
                     std::vector<Field*>& output_fields);

  /**
   * @brief Insert data into location at (row, col). Expected return
   * type of the column is KWDBTypeFamily::DecimalFamily, however there is no
   * primitive decimal in C++. We use mixed double64/int64 as
   * workaround and an extra bool value indicates whether it is a double.
   *
   * For example: to sum up all values in an int64 column, if the result is
   * larger than int64 max value (9223372036854775807), the column type casts to
   * double.
   * @param[in] row
   * @param[in] col
   * @param[in] value data pointer to insert
   * @param[in] is_double whether it's a double64 value or int64 value
   */
  KStatus InsertDecimal(k_uint32 row, k_uint32 col, DatumPtr value,
                        k_bool is_double);

  /**
   * @brief Copy data from another data chunk
   * @param[in] other
   * @param[in] begin
   * @param[in] end
   */
  void CopyFrom(std::unique_ptr<DataChunk>& other, k_uint32 begin,
                k_uint32 end) {
    count_ = end - begin + 1;
    size_t batch_buf_length = other->RowSize() * count_;
    size_t offset = begin * RowSize();
    memcpy(data_ + offset, other->GetData(), batch_buf_length);
  }

  /**
   * @brief Copy data from another data chunk
   * @param[in] other
   * @param[in] begin
   * @param[in] end
   */
  void CopyFrom(std::unique_ptr<DataChunk>& other, k_uint32 begin, k_uint32 end,
                bool is_reverse) {
    count_ = end - begin + 1;
    if (count_ <= 0) {
      return;
    }
    std::vector<k_uint32> selection;
    other->OffsetSort(selection, is_reverse);

    for (k_uint32 col_idx = 0; col_idx < col_num_; ++col_idx) {
      for (k_uint32 row = 0, src_row = begin; row < count_; ++row, ++src_row) {
        if (other->IsNull(selection[src_row], col_idx)) {
          SetNull(row, col_idx);
        } else {
          char* src_ptr = other->GetData(selection[src_row], col_idx);
          k_uint32 col_offset =
              row * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx];
          std::memcpy(data_ + col_offset, src_ptr,
                      col_info_[col_idx].fixed_storage_len);
          SetNotNull(row, col_idx);
        }
      }
    }
  }

  KStatus ReplaceRow(DataChunkPtr& other, k_uint32 row) {
    if (row >= count_) {
      return FAIL;
    }
    for (k_uint32 col_idx = 0; col_idx < col_num_; ++col_idx) {
      if (other->IsNull(col_idx)) {
        SetNull(row, col_idx);
      } else {
        char* src_ptr = other->GetData(col_idx);
        k_uint32 col_offset =
            row * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx];
        std::memcpy(data_ + col_offset, src_ptr,
                    col_info_[col_idx].fixed_storage_len);
        SetNotNull(row, col_idx);
      }
    }
  }

  KStatus ReplaceRow(DataChunkPtr& other, k_uint32 row, k_uint32 other_row) {
    if (row >= count_ || other_row >= other->Count()) {
      return FAIL;
    }
    for (k_uint32 col_idx = 0; col_idx < col_num_; ++col_idx) {
      if (other->IsNull(other_row, col_idx)) {
        SetNull(row, col_idx);
      } else {
        char* src_ptr = other->GetData(other_row, col_idx);
        k_uint32 col_offset =
            row * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx];
        std::memcpy(data_ + col_offset, src_ptr,
                    col_info_[col_idx].fixed_storage_len);
        SetNotNull(row, col_idx);
      }
    }
    return SUCCESS;
  }

  KStatus OffsetSort(std::vector<k_uint32>& selection, bool is_reverse);

  /**
   * @brief Copy current line using row mode
   * @param[in] ctx
   * @param[in] renders
   */
  void AddRecordByRow(kwdbContext_p ctx, RowBatch* row_batch, k_uint32 col,
                      Field* field);

  /**
   * @brief Copy current line using column mode
   * @param[in] ctx
   * @param[in] renders
   */
  KStatus AddRecordByColumn(kwdbContext_p ctx, RowBatch* row_batch, Field** renders);
  KStatus AddRecordByColumnWithSelection(kwdbContext_p ctx, RowBatch* row_batch, Field** renders);
  /**
   * @brief Copy all data from the RowBatch
   * @param[in] ctx
   * @param[in] row_batch
   * @param[in] renders
   */
  KStatus AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch,
                          Field** renders, bool batch_copy = false);

  ////////////////   Encoding func  ///////////////////
  KStatus Encoding(kwdbContext_p ctx, TsNextRetState nextState,
                   bool use_query_short_circuit, k_int64* command_limit,
                   std::atomic<k_int64>* count_for_limit);
  /**
   * @brief Encode data at coordinate location (row, col) using kwbase protocol.
   * @param[in] ctx
   * @param[in] row
   * @param[in] col
   * @param[in] info
   */
  KStatus EncodingValue(kwdbContext_p ctx, k_uint32 row, k_uint32 col,
                        const EE_StringInfo& info);

  /**
   * @brief Encode one row using pgwire protocol.
   * @param[in] ctx
   * @param[in] row
   * @param[in] info
   */
  KStatus PgResultData(kwdbContext_p ctx, k_uint32 row,
                       const EE_StringInfo& info);

  virtual EEIteratorErrCode VectorizeData(kwdbContext_p ctx,
                                          DataInfo* data_info);

  void ResetDataPtr(char* data_ptr, k_int32 data_count) {
    data_ = data_ptr;
    count_ = data_count;
    current_line_ = -1;
  }

  /**
   * @brief Reset the data ptr and free the previous buffer if it's data owner
   * @param[in] data_ptr  the new data ptr to be set
   */
  void ResetDataPtr(char* data_ptr) {
    if (data_ && is_data_owner_) {
      kwdbts::EE_MemPoolFree(g_pstBufferPoolInfo, data_);
    }
    data_ = data_ptr;
    current_line_ = -1;
    is_data_owner_ = false;
  }

  /**
   * @brief Encode decimal value (actually double64 or int64) using kwbase
   * protocol.
   * @param[in] raw
   * @param[in] info
   */
  template <typename T>
  void EncodeDecimal(DatumPtr raw, const EE_StringInfo& info) {
    T val;
    std::memcpy(&val, raw, sizeof(T));
    if constexpr (std::is_floating_point<T>::value) {
      // encode floating number
      k_int32 len = ValueEncoding::EncodeComputeLenFloat(0);
      KStatus ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        return;
      }
      CKSlice slice;
      slice.data = info->data + info->len;
      slice.len = len;
      ValueEncoding::EncodeFloatValue(&slice, 0, val);
      info->len = info->len + len;
    } else {
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      KStatus ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        return;
      }
      CKSlice slice;
      slice.data = info->data + info->len;
      slice.len = len;
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
    }
  }
  template <typename T>
  KStatus PgEncodeDecimal(DatumPtr raw, const EE_StringInfo& info);

  //  use to limit the return size in Next functions.
  static const int SIZE_LIMIT = ROW_BUFFER_SIZE;
  static const int MIN_CAPACITY = 1;

  static k_uint32 EstimateCapacity(ColumnInfo* column_info, k_int32 col_num);

  static k_uint32 ComputeRowSize(ColumnInfo* column_info, k_int32 col_num);

  // convert one row to tag data format
  KStatus ConvertToTagData(kwdbContext_p ctx, k_uint32 row, k_uint32 col,
                           TagRawData& tag_raw_data, DatumPtr& rel_data_ptr);

  bool IsEncoding() { return encoding_buf_ != nullptr; }
  k_uint32 GetEncodingBufferLength() { return encoding_len_; }

  char* GetEncodingBuffer() { return encoding_buf_; }

  bool SetEncodingBuf(const unsigned char* buf, k_uint32 len);

  void GetEncodingBuffer(char** buf, k_uint32* len, k_uint32* count) {
    *buf = encoding_buf_;
    *len = encoding_len_;
    *count = count_;
    is_buf_owner_ = false;
  }

  /**
   * temporary solution to store entity indexs for tag data,
   * will remove it after tag scan result is completely changed
   * from tag row batch to data chunk.
   */
  KStatus InsertEntities(TagRowBatch* tag_row_batch);

  /**
   * temporary solution to get entity index from data chunk,
   * will remove it after tag scan result is completely changed
   * from tag row batch to data chunk.
   */
  EntityResultIndex& GetEntityIndex(k_uint32 row);

  void Reset() {
    count_ = 0;
    memset(data_, 0, data_size_);
  }

 protected:
  bool is_data_owner_{true};
  char* data_{nullptr};               // Multiple rows of column data（not tag）
  ColumnInfo* col_info_{nullptr};     // column info
  k_uint32* col_offset_{nullptr};     // column offset
  k_uint32* bitmap_offset_{nullptr};  // bitmap offset

  k_uint32 capacity_{0};     // data capacity
  k_uint32 count_{0};        // total number
  k_uint32 bitmap_size_{0};  // length of bitmap
  k_uint32 row_size_{0};     // the total length of one row
  k_bits32 col_num_{0};      // the number of col
  k_uint32 data_size_{
      0};  // data size (capacity_ + 7) / 8 * col_num_ + capacity_ * row_size_;

  k_int32 current_line_{-1};  // current row
  char* encoding_buf_{nullptr};
  k_uint32 encoding_len_{0};
  bool is_buf_owner_{true};
  /**
   * temporary solution to store entity indexs for tag data,
   * will remove it after tag scan result is completely changed
   * from tag row batch to data chunk.
   */
  std::vector<EntityResultIndex> entity_indexs_;

 private:
  bool disorder_{false};
};

}  //  namespace kwdbts
