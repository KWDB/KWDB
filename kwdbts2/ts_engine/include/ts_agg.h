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

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>
#include "data_type.h"
#include "ts_common.h"
#include "ts_bitmap.h"


/*
 * Block Aggregation structure V2
 *
 * +-------------+-------------+-------------+---------------+---------------+
 * |    bitmap   | col1 agg    | col2 agg    | col3 agg      |   col_N agg   |
 * +-------------+-------------+-------------+---------------+---------------+
 *
 * Bytes:
 * - bitmap: (column_count + 7) / 8
 *
 * --------------------------------------------------------------------------
 *
 * Integer and floating-point types column aggregation structure
 * +-------+-----+-----+-----+
 * | count | max | min | sum |
 * +-------+-----+-----+-----+
 *
 * Bytes:
 * - count: uint16_t
 * - max: column type size, e.g. double type is 8 bytes
 * - min: column type size, e.g. double type is 8 bytes
 * - sum: 1 byte for is_null + 8 bytes for sum value (only for SUM aggregation)
 *
 * --------------------------------------------------------------------------
 *
 * Count-only type: char/bytes/varchar...
 * +-------+
 * | count |
 * +-------+

 * Bytes:
 * - count: uint16_t
 *
 */



/*
 * Block Aggregation structure V1
 * +-------------+-------------+-------------+---------------+---------------+---------------+
 * | col1 offset | col2 offset | col3 offset | col1 agg      | col2 agg      | col3 agg      |
 * +-------------+-------------+-------------+---------------+---------------+---------------+
 *
 * Bytes:
 * offset: uint32_t
 * col agg: Which is related to the size of the data type
 */

/*
 * Fixed-length column aggregation structure
 * +-------+-----+-----+
 * | count | max | min |
 * +-------+-----+-----+
 *
 * Bytes:
 * - count: uint16_t
 * - max: column type size, e.g. double type is 8 bytes
 * - min: column type size, e.g. double type is 8 bytes
 */

/*
 * Variable-length column aggregation structure:
 * +-------+---------+---------+---------+---------+
 * | count | max_len | min_len | max_str | min_str |
 * +-------+---------+---------+---------+---------+

 * Bytes:
 * - count: uint16_t
 * - max_len: uint32_t
 * - min_len: uint32_t
 * - max_str: Actual string length
 * - min_str: Actual string length
 */

namespace kwdbts {

constexpr uint32_t BLOCK_AGG_SPARSE_LAYOUT_VERSION = 2;
constexpr uint64_t PARTITION_AGG_OFFSET_LAYOUT_VERSION = 0;
constexpr uint64_t PARTITION_AGG_SPARSE_LAYOUT_VERSION = 1;
constexpr uint64_t CURRENT_PARTITION_AGG_VERSION = PARTITION_AGG_SPARSE_LAYOUT_VERSION;
constexpr size_t BLOCK_AGG_COUNT_SIZE = sizeof(uint16_t);
constexpr size_t BLOCK_AGG_SUM_SIZE = sizeof(bool) + sizeof(int64_t);
constexpr size_t PARTITION_AGG_COUNT_SIZE = sizeof(uint64_t);
constexpr size_t PARTITION_AGG_SUM_SIZE = sizeof(bool) + sizeof(int64_t);

struct FixedBlockAggColumnLayout {
  size_t offset{0};
  size_t size{0};
};

inline size_t GetBlockAggValueSize(const std::vector<AttributeInfo>& attrs, uint32_t col_idx) {
  return col_idx == 0 ? sizeof(timestamp64) : static_cast<size_t>(attrs[col_idx].size);
}

inline bool IsBlockAggCountOnlyType(DATATYPE type) {
  return isVarLenType(type) || type == DATATYPE::CHAR || type == DATATYPE::BINARY || type == DATATYPE::STRING;
}

inline size_t GetFixedBlockAggColumnSize(const std::vector<AttributeInfo>& attrs, uint32_t col_idx) {
  auto type = static_cast<DATATYPE>(attrs[col_idx].type);
  if (IsBlockAggCountOnlyType(type)) {
    return BLOCK_AGG_COUNT_SIZE;
  }
  size_t value_size = GetBlockAggValueSize(attrs, col_idx);
  size_t agg_size = BLOCK_AGG_COUNT_SIZE + value_size * 2;
  if (isSumType(type)) {
    agg_size += BLOCK_AGG_SUM_SIZE;
  }
  return agg_size;
}

inline std::vector<FixedBlockAggColumnLayout> BuildFixedBlockAggLayout(const std::vector<AttributeInfo>& attrs) {
  std::vector<FixedBlockAggColumnLayout> layout;
  layout.reserve(attrs.size());
  size_t offset = 0;
  for (uint32_t i = 0; i < attrs.size(); ++i) {
    size_t size = GetFixedBlockAggColumnSize(attrs, i);
    layout.push_back({offset, size});
    offset += size;
  }
  return layout;
}

inline size_t GetSparseAggBitmapSize(size_t col_count) {
  return (col_count + 7) / 8;
}

inline bool SparseAggHasColumn(const char* bitmap, uint32_t col_idx) {
  return (static_cast<uint8_t>(bitmap[col_idx / 8]) & (1 << (col_idx % 8))) != 0;
}

inline void SetSparseAggColumn(char* bitmap, uint32_t col_idx) {
  bitmap[col_idx / 8] = static_cast<char>(static_cast<uint8_t>(bitmap[col_idx / 8]) | (1 << (col_idx % 8)));
}

inline size_t GetSparseBlockAggBitmapSize(size_t col_count) {
  return GetSparseAggBitmapSize(col_count);
}

inline bool SparseBlockAggHasColumn(const char* bitmap, uint32_t col_idx) {
  return SparseAggHasColumn(bitmap, col_idx);
}

inline void SetSparseBlockAggColumn(char* bitmap, uint32_t col_idx) {
  SetSparseAggColumn(bitmap, col_idx);
}

inline size_t GetSparseBlockAggColumnOffset(const std::vector<FixedBlockAggColumnLayout>& layout,
                                            const char* bitmap, uint32_t col_idx) {
  size_t offset = GetSparseBlockAggBitmapSize(layout.size());
  for (uint32_t i = 0; i < col_idx; ++i) {
    if (SparseBlockAggHasColumn(bitmap, i)) {
      offset += layout[i].size;
    }
  }
  return offset;
}

inline KStatus GetOffsetAggColumnRange(const char* offsets, size_t offsets_size, uint32_t col_idx,
                                       uint32_t* start_offset, uint32_t* end_offset) {
  *start_offset = 0;
  *end_offset = 0;
  if (offsets == nullptr || (static_cast<size_t>(col_idx) + 1) * sizeof(uint32_t) > offsets_size) {
    return KStatus::FAIL;
  }
  if (col_idx != 0) {
    std::memcpy(start_offset, offsets + (col_idx - 1) * sizeof(uint32_t), sizeof(uint32_t));
  }
  std::memcpy(end_offset, offsets + col_idx * sizeof(uint32_t), sizeof(uint32_t));
  if (*end_offset < *start_offset) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

inline size_t GetSparsePartitionAggBitmapSize(size_t col_count) {
  return GetSparseAggBitmapSize(col_count);
}

inline size_t GetPartitionAggValueSize(const std::vector<AttributeInfo>& attrs, uint32_t col_idx) {
  return col_idx == 0 ? sizeof(timestamp64) : static_cast<size_t>(attrs[col_idx].size);
}

// Same count-only policy as block agg: varlen/CHAR/BINARY/STRING columns only
// store COUNT in pre-aggregation payloads. MAX/MIN/SUM on these types must fall
// back to the raw aggregation path.
inline size_t GetFixedPartitionAggColumnSize(const std::vector<AttributeInfo>& attrs, uint32_t col_idx) {
  auto type = col_idx == 0 ? DATATYPE::TIMESTAMP64 : static_cast<DATATYPE>(attrs[col_idx].type);
  if (IsBlockAggCountOnlyType(type)) {
    return PARTITION_AGG_COUNT_SIZE;
  }
  size_t value_size = GetPartitionAggValueSize(attrs, col_idx);
  size_t agg_size = PARTITION_AGG_COUNT_SIZE + value_size * 2;
  if (isSumType(type)) {
    agg_size += PARTITION_AGG_SUM_SIZE;
  }
  return agg_size;
}

inline KStatus GetSparsePartitionAggColumnSlice(const std::vector<AttributeInfo>& attrs, TSSlice entity_agg,
                                                uint32_t col_idx, TSSlice* col_agg, bool* has_column) {
  *col_agg = {nullptr, 0};
  *has_column = false;
  if (col_idx >= attrs.size()) {
    return KStatus::FAIL;
  }
  const size_t bitmap_size = GetSparsePartitionAggBitmapSize(attrs.size());
  if (entity_agg.len < bitmap_size) {
    return KStatus::FAIL;
  }

  const char* bitmap = entity_agg.data;
  if (!SparseAggHasColumn(bitmap, col_idx)) {
    return KStatus::SUCCESS;
  }

  size_t offset = bitmap_size;
  for (uint32_t i = 0; i < attrs.size(); ++i) {
    if (!SparseAggHasColumn(bitmap, i)) {
      continue;
    }
    if (offset > entity_agg.len) {
      return KStatus::FAIL;
    }

    auto type = i == 0 ? DATATYPE::TIMESTAMP64 : static_cast<DATATYPE>(attrs[i].type);
    size_t col_len = 0;
    if (IsBlockAggCountOnlyType(type)) {
      col_len = PARTITION_AGG_COUNT_SIZE;
    } else if (isVarLenType(type)) {
      const size_t fixed_header_size = PARTITION_AGG_COUNT_SIZE + 2 * sizeof(uint16_t);
      if (entity_agg.len - offset < fixed_header_size) {
        return KStatus::FAIL;
      }
      uint16_t max_len = 0;
      uint16_t min_len = 0;
      std::memcpy(&max_len, entity_agg.data + offset + PARTITION_AGG_COUNT_SIZE, sizeof(uint16_t));
      std::memcpy(&min_len, entity_agg.data + offset + PARTITION_AGG_COUNT_SIZE + sizeof(uint16_t), sizeof(uint16_t));
      col_len = fixed_header_size + max_len + min_len;
    } else {
      col_len = GetFixedPartitionAggColumnSize(attrs, i);
    }
    if (col_len > entity_agg.len - offset) {
      return KStatus::FAIL;
    }
    if (i == col_idx) {
      *col_agg = {entity_agg.data + offset, col_len};
      *has_column = true;
      return KStatus::SUCCESS;
    }
    offset += col_len;
  }
  return KStatus::FAIL;
}

inline KStatus GetOffsetPartitionAggColumnSlice(TSSlice entity_agg, size_t col_count, uint32_t col_idx,
                                                TSSlice* col_agg) {
  *col_agg = {nullptr, 0};
  const size_t agg_header_size = col_count * sizeof(uint32_t);
  if (col_idx >= col_count || entity_agg.len < agg_header_size) {
    return KStatus::FAIL;
  }

  uint32_t start_offset = 0;
  uint32_t end_offset = 0;
  auto s = GetOffsetAggColumnRange(entity_agg.data, agg_header_size, col_idx, &start_offset, &end_offset);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  if (end_offset > entity_agg.len - agg_header_size) {
    return KStatus::FAIL;
  }
  if (end_offset > start_offset) {
    *col_agg = {entity_agg.data + agg_header_size + start_offset, end_offset - start_offset};
  }
  return KStatus::SUCCESS;
}

class AggCalculatorV2 {
 public:
  AggCalculatorV2(char* mem, const TsBitmapBase* bitmap, DATATYPE type, int32_t size, int32_t count) :
      mem_(mem), bitmap_(bitmap), type_(type), size_(size), count_(count) {
  }

  KStatus CalcAggForFlush(bool is_not_null, bool& is_overflow, uint16_t& count,
                          void* max_addr, void* min_addr, void* sum_addr);

 private:
  [[nodiscard]] bool isnull(size_t row) const;

 private:
  char* mem_;
  const TsBitmapBase* bitmap_ = nullptr;
  DATATYPE type_;
  int32_t size_;
  int32_t count_;
};

class VarColAggCalculatorV2 {
 public:
  explicit VarColAggCalculatorV2(const std::vector<string>& var_rows) : var_rows_(var_rows) {
  }

  void CalcAggForFlush(string& max, string& min, uint64_t& count);

 private:
  const std::vector<string>& var_rows_;
};



}  // namespace kwdbts
