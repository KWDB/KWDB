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

#include <fcntl.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "ts_bitmap.h"
#include "ts_engine_schema_manager.h"
#include "ts_env.h"
#include "ts_io.h"
#include "ts_payload.h"
#include "ts_table_schema_manager.h"
#include "utils/big_table_utils.h"

namespace kwdbts {

/*
 *
 * # Structure of LastSegment
 * ==========================
 *
 * +--------+------+-------+------+------------+---------+--------+
 * | Metric | Info | Index | Meta | Meta Index | Padding | Footer |
 * +--------+------+-------+------+------------+---------+--------+
 *
 * Notice: The Padding between `Meta Block Index` and `Footer` is just make sure that `Footer`
 *         will not accross tow pages.
 *
 * ## Layout of Data Blocks
 * ========================
 *
 * +-------+-------+-------+-----+-----------+-----------+
 * | Col_1 | Col_2 | Col_3 | ... | Col_{N_1} | Entity ID | <- Block 1
 * +-------+-------+-------+-----+-----------+-----------+
 * | Col_1 | Col_2 | Col_3 | ... | Col_{N_2} | Entity ID | <- Block 2
 * +-------+-------+-------+-----+-----------+-----------+
 * |  ...  |  ...  |  ...  | ... |    ...    |    ...    |
 * +-------+-------+-------+-----+-----------+-----------+
 * | Col_1 | Col_2 | Col_3 | ... | Col_{N_m} | Entity ID | <- Block m
 * +-------+-------+-------+-----+-----------+-----------+


 * |Info 1 |Info 2 |Info 3 | ... |  Info m   |
 * +-------+-------+-------+-----+-----------+
 * ** Info = BlockInfo
 *
 * ## Layout of BlockInfo
 * ======================
 *
 * +---------------+---------+-------------------------------------+
 * |  block_offset | tableID |  version  | nrow |  ncol  | ndevice |
 * +---------------+---------+-------------+-----------------------+
 * | col_offset[0] | col_offset[1] |  ...  |   col_offset[ncol-1]  |
 * +---------------+-------+-------+-------+-----------------------+
 * |   bitmap[0]   |   bitmap[1]   |  ...  |     bitmap[ncol-1]    | ?
 * +---------------+---------------+-------+-----------------------+
 *
 *
 *
 * With data type:
 *   block_offset, tableID        : fix64;
 *   nrow, ncol, ndevice, version : fix32;
 *   col_offset : fix32;
 *
 * ** Layout of Block Index **
 * ===========================
 *
 * 0        8       16      24             32             40
 * +--------+-------+-------+--------------+--------------+
 * | offset | mints | maxts | min deviceID | max deviceID | <- Block 1
 * +--------+-------+-------+--------------+--------------+
 * | offset | mints | maxts | min deviceID | max deviceID | <- Block 2
 * +--------+-------+-------+--------------+--------------+
 * |                         ...                          |
 * +--------+-------+-------+--------------+--------------+
 * | offset | mints | maxts | min deviceID | max deviceID | <- Block m
 * +--------+-------+-------+--------------+--------------+
 *
 * ** Layout of Meta Blocks **
 * ===========================
 *
 * +------------------+------------+---------------------+
 * | name (varstring) | len(fix32) | serialization data  |
 * +------------------+------------+---------------------+
 *
 * Meta Blocks can records:
 *   1. Compression type for each data type.
 *   2. Statistic information.
 *   3. Bloom Filter.
 *   4. ....
 *
 * ** Layout of Meta Blocks Index **
 * =================================
 *
 * 0           8           16  ...                 8 * nmeta
 * +-----------+-----------+-----+-----------------+
 * | offset[0] | offset[1] | ... | offset[nmeta-1] |
 * +-----------+-----------+-----+-----------------+
 *
 *
 * ** Layout of Footer **
 * ======================
 *
 * 0                         8                       16
 * +-------------------------+-----------------------+
 * | Data Block Index Offset | Number of Data Blocks |
 * +-------------------------+-----------------------+
 * | Meta Block Index Offset | Number of Meta Blocks |
 * +-------------------------+-----------------------+
 * |                  Padding(reserve)               |
 * +-------------------------+-----------------------+
 * |     Format Version      |      Magic Number     |
 * +-------------------------------------------------+
 *
 */

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
  uint64_t offset;
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

  KStatus GetBlockInfo(TsLastSegmentBlockIndex& block_index, size_t col_num,
                       TsLastSegmentBlockInfo* block_info);

  KStatus GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block);

  KStatus GetBlock(TsLastSegmentBlockIndex& block_index, size_t col_num, TsLastSegmentBlock* block);
};

class TsLastSegmentBuilder {
  static constexpr int kNRowPerBlock = 4 << 10;
  std::shared_ptr<TsLastSegment> last_segment_;

  struct BlockInfo;
  class MetricBlockBuilder;  // Helper for build DataBlock
  class InfoHandle;
  class IndexHandle;

  // TODO(zzr) Meta Blocks Handle...

  std::unique_ptr<MetricBlockBuilder> data_block_builder_;
  std::unique_ptr<InfoHandle> info_handle_;
  std::unique_ptr<IndexHandle> index_handle_;

  TSTableID table_id_ = -1;  // INVALID ID
  uint32_t version_ = -1;    // INVALID ID

  size_t nblock_ = 0;
  struct EntityPayload {
    rocksdb::SequenceNumber seq_no;
    TSEntityID entity_id;
    TSSlice metric;
  };
  std::vector<EntityPayload> payload_buffer_;
  const TsEngineSchemaManager* schema_mgr_;

  KStatus WriteMetricBlock(MetricBlockBuilder* builder);
  KStatus FlushPayloadBuffer();

 public:
  TsLastSegmentBuilder(TsEngineSchemaManager* schema_mgr,
                       std::shared_ptr<TsLastSegment> last_segment)
      : last_segment_(last_segment),
        data_block_builder_(std::make_unique<MetricBlockBuilder>(schema_mgr)),
        info_handle_(std::make_unique<InfoHandle>()),
        index_handle_(std::make_unique<IndexHandle>()),
        schema_mgr_(schema_mgr) {}

  int Flush() {
    auto s = last_segment_->Flush();
    return s.ok() ? 0 : 1;
  }

  KStatus Finalize();
  KStatus PutRowData(TSTableID table_id, uint32_t version, TSEntityID entity_id,
                     rocksdb::SequenceNumber seq_no, TSSlice row_data);

  bool ConsistentWith(TSTableID table_id, uint32_t version) const {
    return table_id == table_id_ && version_ == version;
  }
};

struct TsLastSegmentBuilder::BlockInfo {
  struct ColInfo {
    uint32_t col_offset;
    uint16_t bitmap_len;
    uint32_t data_len;
  };
  TSTableID table_id;
  uint32_t version;
  uint32_t nrow;
  uint32_t ndevice;
  uint32_t var_offset;
  uint32_t var_len;
  int64_t min_ts, max_ts;
  uint64_t min_entity_id, max_entity_id;
  std::vector<ColInfo> col_infos;
  BlockInfo() { Reset(-1, -1); }
  void Reset(TSTableID table_id, uint32_t version) {
    this->table_id = table_id;
    this->version = version;

    nrow = ndevice = var_offset = var_len = 0;
    max_ts = INT64_MIN;
    min_ts = INT64_MAX;
    min_entity_id = UINT64_MAX;
    max_entity_id = 0;
    col_infos.clear();
  }
};

class TsLastSegmentBuilder::InfoHandle {
 private:
  bool finished_;
  uint64_t cursor_ = 0;
  std::vector<BlockInfo> infos_;
  std::vector<uint64_t> offset_;

  size_t length_ = 0;  // for debug;

 public:
  size_t RecordBlock(size_t block_length, const BlockInfo& info);
  KStatus WriteInfo(TsFile*);
};

class TsLastSegmentBuilder::IndexHandle {
 private:
  bool finished_;
  uint64_t cursor_ = 0;
  std::vector<TsLastSegmentBlockIndex> indices_;

 public:
  void RecordBlockInfo(size_t info_length, const BlockInfo& info);
  void ApplyInfoBlockOffset(size_t offset);
  KStatus WriteIndex(TsFile*);
};
class TsLastSegmentBuilder::MetricBlockBuilder {
 private:
  class ColumnBlockBuilder;
  std::vector<std::unique_ptr<ColumnBlockBuilder>> colblocks_;

  TsEngineSchemaManager* schema_mgr_;
  std::shared_ptr<MMapMetricsTable> table_schema_;

  std::vector<AttributeInfo> metric_schema_;
  std::unique_ptr<TsRawPayloadRowParser> parser_;

  std::string varchar_buffer_;
  bool finished_ = true;

  BlockInfo info_;
  TSEntityID last_entity_id_ = -1;

 public:
  // do not copy;
  MetricBlockBuilder(const MetricBlockBuilder&) = delete;
  void operator=(const MetricBlockBuilder&) = delete;

  explicit MetricBlockBuilder(TsEngineSchemaManager* schema_mgr);

  KStatus Reset(TSTableID table_id, uint32_t table_version);

  __attribute__((visibility("hidden"))) void Add(TSEntityID entity_id,
                                                 rocksdb::SequenceNumber seq_no,
                                                 TSSlice metric_data);
  void Finish();
  bool IsFinished() const { return finished_; }
  BlockInfo GetBlockInfo() const;
  int GetNRows() const { return info_.nrow; }
  int GetNColumns() const { return colblocks_.size(); }
  bool Empty() const { return info_.nrow == 0; }
  void Reserve(size_t nrow);

  TSSlice GetColumnData(size_t i);
  std::vector<TSSlice> GetColumnDatas();
  TSSlice GetColumnBitmap(size_t i);

  TSSlice GetVarcharBuffer() { return {varchar_buffer_.data(), varchar_buffer_.size()}; }
};

class TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder {
 private:
  bool has_bitmap_;
  TsBitmap bitmap_;
  std::string bitmap_buffer_;
  std::string data_buffer_;
  DATATYPE dtype_;

  uint32_t row_cnt_ = 0;
  int dsize_ = -1;

 public:
  // do not copy
  ColumnBlockBuilder(const ColumnBlockBuilder&) = delete;
  void operator=(const ColumnBlockBuilder&) = delete;

  explicit ColumnBlockBuilder(DATATYPE dtype, bool has_bitmap) : has_bitmap_(has_bitmap), dtype_(dtype) {
    if (dtype_ == TIMESTAMP64_LSN_MICRO || dtype_ == TIMESTAMP64_LSN ||
        dtype_ == TIMESTAMP64_LSN_NANO) {
      // discard LSN
      dsize_ = 8;
    } else {
      dsize_ = getDataTypeSize(dtype);
    }
  }
  __attribute__((visibility("hidden"))) void Add(const TSSlice& col_data) noexcept;
  DATATYPE GetDatatype() const { return dtype_; }
  void Compress();
  void Reserve(size_t nrow) {
    data_buffer_.reserve(nrow * getDataTypeSize(dtype_));
    bitmap_.Reset(has_bitmap_ ? nrow : 0);
  }
  TSSlice GetData() { return TSSlice{data_buffer_.data(), data_buffer_.size()}; }

  TSSlice GetBitmap() { return TSSlice{bitmap_buffer_.data(), bitmap_buffer_.size()}; }
};

struct TsLastSegmentSlice {
  TsLastSegment* last_seg_;
  uint32_t offset;
  uint32_t count;
};

const uint32_t MAX_COMPACT_NUM = 10;

class TsLastSegmentManager {
 private:
  std::filesystem::path dir_path_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;

  uint32_t ver_ = 0;
  uint32_t compacted_ver_ = 0;

 public:
  explicit TsLastSegmentManager(const string& dir_path) : dir_path_(dir_path) {}

  ~TsLastSegmentManager() {}

  KStatus NewLastSegment(std::shared_ptr<TsLastSegment>& last_segment);

  std::vector<std::shared_ptr<TsLastSegment>> GetCompactLastSegments();

  bool NeedCompact();

  void SetCompactedVer(uint32_t ver);
};

}  // namespace kwdbts
