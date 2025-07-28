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
//

#pragma once

#include <memory>
#include <list>
#include <vector>
#include <queue>
#include <algorithm>
#include <sstream>
#include <utility>

#include "cm_kwdb_context.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_pb_plan.pb.h"
#include "ee_global.h"



namespace kwdbts {

typedef char* DatumPtr;
// A pointer to a row of data
typedef char* DatumRowPtr;

class DataContainer;

class DataChunk;

class DiskDataContainer;

typedef std::unique_ptr<DataContainer> DataContainerPtr;
typedef std::unique_ptr<DataChunk> DataChunkPtr;
typedef std::shared_ptr<DataChunk> DataChunkSPtr;

/**
 * Structure to store column type, length.
 */
struct ColumnInfo {
  k_uint32 storage_len;
  k_uint32 fixed_storage_len{0};
  k_uint16 max_string_len{0};
  roachpb::DataType storage_type;
  KWDBTypeFamily return_type;
  bool is_string{false};
  bool allow_null{true};
  ColumnInfo() {}
  ColumnInfo(k_uint32 storage_len, roachpb::DataType storage_type,
             KWDBTypeFamily return_type)
      : storage_len(storage_len),
        storage_type(storage_type),
        return_type(return_type) {
    if (storage_type == roachpb::DataType::CHAR ||
        storage_type == roachpb::DataType::VARCHAR ||
        storage_type == roachpb::DataType::NCHAR ||
        storage_type == roachpb::DataType::BINARY ||
        storage_type == roachpb::DataType::NVARCHAR ||
        storage_type == roachpb::DataType::VARBINARY) {
      is_string = true;
    }
  }
};

class IChunk {
 public:
  /**
   * @brief Check whether it is null at (row, col)
   * @param[in] row
   * @param[in] col
   */
  virtual bool IsNull(k_uint32 row, k_uint32 col) = 0;

  /**
   * @brief Check whether it is null at (current_line, col)
   * @param[in] col
   */
  virtual bool IsNull(k_uint32 col) = 0;

  /**
   * @brief Get data pointer at (row, col)
   * @param[in] row
   * @param[in] col
   */
  virtual DatumPtr GetData(k_uint32 row, k_uint32 col) = 0;

  /**
   * @brief Get data pointer at (current_line, col)
   * @param[in] col
   */
  virtual DatumPtr GetData(k_uint32 col) = 0;

  /**
   * @brief Get string pointer at  (row, col), and return the
   * string length
   * @param[in] row
   * @param[in] col
   * @param[in/out] string length
   */
  virtual DatumPtr GetData(k_uint32 row, k_uint32 col, k_uint16& len) = 0;

  /**
   * @brief Get the Chunk Schema
   * @return
   */
  virtual ColumnInfo* GetColumnInfo() = 0;

  /**
   * @brief Move to next line
   * @return Return row index of next line when it's valid, otherwise return -1.
   */
  virtual k_int32 NextLine() = 0;

  /**
   * @brief return count
   */
  virtual k_uint32 Count() = 0;
};

/**
 * @brief DataContainer
 * @details Sort container interface，derived classes include
 * MemRowContainer and DiskRowContainer
 */
class DataContainer : public IChunk {
 public:
  DataContainer() = default;

  virtual ~DataContainer() = default;

  /**
   * @brief Initialize row container. Now DiskRowContainer override it.
   */
  virtual KStatus Init() = 0;

  /**
   * @brief Sort all rows in the container
   */
  virtual KStatus Sort() = 0;

  /**
   * @brief Read data in datachunk and append all rows into row container
   * @param[in] chunk datachunk unique_ptr
   */
  virtual KStatus Append(DataChunkPtr& chunk) = 0;

  /**
   * @brief Read all data from a datachunk queue and append into row container
   * @param[in] chunk_queue queue of datachunk unique_ptr
   */
  virtual KStatus Append(std::queue<DataChunkPtr>& buffer) = 0;

  virtual EEIteratorErrCode NextChunk(DataChunkPtr& data_chunk) {return EEIteratorErrCode::EE_ERROR;}

  void SetMaxOutputRows(k_uint32 limit) { max_output_rows_ = limit; }

  void SetLimitOffset(k_uint32 limit, k_uint32 offset) {
    limit_ = limit;
    offset_ = offset;
    max_output_rows_ = limit + offset;
  }

  k_uint32 max_output_rows_{UINT32_MAX};
  k_uint32 limit_{UINT32_MAX};
  k_uint32 offset_{0};
};

struct ColumnOrderInfo {
  k_uint32 col_idx;
  TSOrdering_Column_Direction direction;
};

class OrderColumnCompare {
 public:
  OrderColumnCompare(DataContainer* container, std::vector<ColumnOrderInfo>& order_info)
      : container_(container), order_info_(order_info) {
  }

  bool operator()(k_uint32 a, k_uint32 b);

 private:
  DataContainer* container_;
  std::vector<ColumnOrderInfo> order_info_;
};

}  // namespace kwdbts
