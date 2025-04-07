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
#include <cstdint>
#include <memory>

#include "data_type.h"
#include "ts_bitmap.h"
#include "ts_io.h"

namespace kwdbts {

class MetaBlockBase {
 public:
  virtual char* GetName() const = 0;
  virtual void Serialize(std::string* dst) = 0;
};

// first 8 byte of `md5 -s kwdbts::TsLastSegment`
static constexpr uint64_t FOOTER_MAGIC = 0xcb2ffe9321847271;

struct TsLastSegmentFooter {
  uint64_t block_info_idx_offset, n_data_block;
  uint64_t meta_block_idx_offset, n_meta_block;
  uint8_t padding[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t file_version;
  const uint64_t magic_number = FOOTER_MAGIC;
};
static_assert(sizeof(TsLastSegmentFooter) == 64);

struct TsLastSegmentBlockIndex {
  uint64_t offset, length;
  uint64_t table_id;
  uint32_t table_version, n_entity;
  int64_t min_ts, max_ts;
  uint64_t min_entity_id, max_entity_id;
};

struct TsLastSegmentBlockInfo {
  uint64_t block_offset;
  uint32_t nrow;
  uint32_t ncol;
  uint32_t var_offset;
  uint32_t var_len;
  struct ColInfo {
    uint32_t offset;
    uint16_t bitmap_len;
    uint32_t data_len;
  };
  std::vector<ColInfo> col_infos;
};
const size_t LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE = sizeof(uint64_t) + 4 * sizeof(uint32_t);
const size_t LAST_SEGMENT_BLOCK_INFO_COL_INFO_SIZE =
    sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint32_t);

struct TsLastSegmentColumnBlock {
  TsBitmap bitmap;
  std::string buffer;
};

struct TsLastSegmentBlock {
  std::vector<TsLastSegmentColumnBlock> column_blocks;  // entity id, seq number and metric columns
  std::string var_buffer;

  DataFlags GetBitmap(uint32_t col_idx, uint32_t row_idx) {
    return column_blocks[col_idx].bitmap[row_idx];
  }

  uint64_t GetEntityId(uint32_t row_idx) {
    return *reinterpret_cast<uint64_t*>(&column_blocks[0].buffer[row_idx * sizeof(uint64_t)]);
  }

  uint32_t GetSeqNo(uint32_t row_idx) {
    return *reinterpret_cast<uint32_t*>(&column_blocks[1].buffer[row_idx * sizeof(uint32_t)]);
  }

  timestamp64 GetTimestamp(uint32_t row_idx) {
    return *reinterpret_cast<timestamp64*>(&column_blocks[2].buffer[row_idx * sizeof(timestamp64)]);
  }

  TSSlice GetData(uint32_t col_idx, uint32_t row_idx, DATATYPE type, size_t data_len) {
    TSSlice value;
    if (type != DATATYPE::VARSTRING && type != DATATYPE::VARBINARY) {
      value.data = &column_blocks[col_idx].buffer[row_idx * data_len];
      value.len = data_len;
    } else {
      size_t offset =
          *reinterpret_cast<size_t*>(&column_blocks[col_idx].buffer[row_idx * data_len]);
      value.len = *reinterpret_cast<uint16_t*>(&var_buffer[offset]);
      value.data = &var_buffer[offset + sizeof(uint16_t)];
    }
    return value;
  }
};

class TsLastSegment {
 private:
  uint32_t ver_;  // not the schema version;

  std::unique_ptr<TsFile> file_;

 public:
  TsLastSegment(uint32_t ver, TsFile* file) : ver_(ver), file_(file) {}

  ~TsLastSegment() = default;

  TsStatus Append(const TSSlice& data);

  TsStatus Flush();

  size_t GetFileSize() const;

  TsFile* GetFilePtr();

  uint32_t GetVersion() const;

  KStatus GetFooter(TsLastSegmentFooter* footer);

  KStatus GetAllBlockIndex(TsLastSegmentFooter& footer,
                           std::vector<TsLastSegmentBlockIndex>* block_indexes);

  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes);

  KStatus GetBlockInfo(TsLastSegmentBlockIndex& block_index, TsLastSegmentBlockInfo* block_info);

  KStatus GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block);
};

struct TsLastSegmentSlice {
  TsLastSegment* last_seg_;
  uint32_t offset;
  uint32_t count;
};
}  // namespace kwdbts