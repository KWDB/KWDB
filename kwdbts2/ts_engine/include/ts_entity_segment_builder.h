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
#include <cstdint>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "ts_block.h"
#include "ts_engine_schema_manager.h"
#include "ts_entity_segment_data.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_entity_segment.h"
#include "ts_filename.h"
#include "ts_lastsegment_builder.h"
#include "ts_metric_block.h"
#include "ts_version.h"


namespace kwdbts {

class TsEntitySegmentEntityItemFileBuilder {
 private:
  string file_path_;
  uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  TsEntityItemFileHeader header_;

 public:
  explicit TsEntitySegmentEntityItemFileBuilder(const string& file_path, uint64_t file_number)
      : file_path_(file_path), file_number_(file_number) {
    memset(&header_, 0, sizeof(TsEntityItemFileHeader));
  }

  ~TsEntitySegmentEntityItemFileBuilder() {
    header_.magic = TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
    header_.status = TsFileStatus::READY;
    header_.entity_num = w_file_->GetFileSize() / sizeof(TsEntityItem);
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char*>(&header_), sizeof(TsEntityItemFileHeader)});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentEntityItemFileBuilder Append failed, file_path=%s", file_path_.c_str())
      assert(false);
    }
  }

  KStatus Open();

  KStatus AppendEntityItem(TsEntityItem& entity_item);

  void MarkDelete() { w_file_->MarkDelete(); }

  uint64_t GetFileNumber() { return file_number_; }
};

class TsEntitySegmentBlockItemFileBuilder {
 private:
  string file_path_;
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  TsBlockItemFileHeader header_;
  bool override_ = false;

 public:
  explicit TsEntitySegmentBlockItemFileBuilder(const string& file_path, bool override)
      : file_path_(file_path), override_(override) {
    memset(&header_, 0, sizeof(TsBlockItemFileHeader));
  }

  ~TsEntitySegmentBlockItemFileBuilder() {}

  KStatus Open();
  KStatus AppendBlockItem(TsEntitySegmentBlockItem& block_item);
  size_t GetFileSize() { return w_file_->GetFileSize(); }
};

class TsEntitySegmentBlockFileBuilder {
 private:
  string file_path_;
  std::unique_ptr<TsAppendOnlyFile> w_file_ = nullptr;
  TsAggAndBlockFileHeader header_;
  bool override_ = false;

 public:
  explicit TsEntitySegmentBlockFileBuilder(const string& file_path, bool override)
      : file_path_(file_path), override_(override) {
    memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
  }

  ~TsEntitySegmentBlockFileBuilder() {}

  KStatus Open();
  KStatus AppendBlock(const TSSlice& block, uint64_t* offset);
  size_t GetFileSize() { return w_file_->GetFileSize(); }
};

class TsEntitySegmentAggFileBuilder {
 private:
  string file_path_;
  std::unique_ptr<TsAppendOnlyFile> w_file_ = nullptr;
  TsAggAndBlockFileHeader header_;
  bool override_ = false;

 public:
  explicit TsEntitySegmentAggFileBuilder(const string& file_path, bool override)
    : file_path_(file_path), override_(override) {
    memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
  }

  ~TsEntitySegmentAggFileBuilder() {}

  KStatus Open();
  KStatus AppendAggBlock(const TSSlice& agg, uint64_t* offset);
  size_t GetFileSize() { return w_file_->GetFileSize(); }
};

class TsEntitySegmentBuilder;
class TsEntityBlockBuilder {
 private:
  uint32_t table_id_ = 0;
  uint32_t table_version_ = 0;
  uint64_t entity_id_ = 0;
  std::vector<AttributeInfo> metric_schema_;

  TsEntitySegmentBlockInfo block_info_;
  std::vector<TsEntitySegmentColumnBlock> column_blocks_;

  uint32_t n_rows_ = 0;
  uint32_t n_cols_ = 0;

  TS_LSN min_lsn_ = UINT64_MAX;
  TS_LSN max_lsn_ = 0;

 public:
  TsEntityBlockBuilder() = delete;
  TsEntityBlockBuilder(uint32_t table_id, uint32_t table_version, uint64_t entity_id,
                       std::vector<AttributeInfo>& metric_schema);
  ~TsEntityBlockBuilder() {}

  bool HasData() { return n_rows_ > 0; }

  size_t GetRowNum() { return n_rows_; }

  TSTableID GetTableId() { return table_id_; }

  uint32_t GetTableVersion() { return table_version_; }

  std::vector<AttributeInfo> GetMetricSchema() { return metric_schema_; }

  uint64_t GetLSN(uint32_t row_idx);

  timestamp64 GetTimestamp(uint32_t row_idx);

  KStatus GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value, std::vector<DataFlags>& data_flags);

  KStatus Append(shared_ptr<TsBlockSpan> span, bool& is_full);

  KStatus GetCompressData(TsEntitySegmentBlockItem& blk_item, string& data_buffer, string& agg_buffer);

  void Clear();
};

class TsEntitySegmentBuilder {
 private:
  struct TsEntityKey {
    TSTableID table_id = 0;
    uint32_t table_version = 0;
    uint64_t entity_id = 0;

    bool operator==(const TsEntityKey& other) const {
      return entity_id == other.entity_id && table_version == other.table_version && table_id == other.table_id;
    }
    bool operator!=(const TsEntityKey& other) const {
      return !(*this == other);
    }
  };

  KStatus NewLastSegmentFile(std::unique_ptr<TsAppendOnlyFile>*, uint64_t *file_number);

  KStatus UpdateEntityItem(TsEntityKey& entity_key, TsEntitySegmentBlockItem& block_item);

  KStatus WriteBlock(TsEntityKey& entity_key);

  KStatus WriteCachedBlockSpan(TsEntityKey& entity_key);

  std::filesystem::path root_path_;
  TsEngineSchemaManager* schema_manager_;
  TsVersionManager* version_manager_;

  PartitionIdentifier partition_id_;
  std::shared_ptr<TsEntitySegment> cur_entity_segment_;
  std::vector<std::shared_ptr<TsLastSegment>> last_segments_;

  std::shared_ptr<TsEntitySegmentEntityItemFileBuilder> entity_item_builder_ = nullptr;
  std::shared_ptr<TsEntitySegmentBlockItemFileBuilder> block_item_builder_ = nullptr;
  std::shared_ptr<TsEntitySegmentBlockFileBuilder> block_file_builder_ = nullptr;
  std::shared_ptr<TsEntitySegmentAggFileBuilder> agg_file_builder_ = nullptr;
  std::unique_ptr<TsLastSegmentBuilder> builder_ = nullptr;
  std::shared_ptr<TsEntityBlockBuilder> block_ = nullptr;

  std::shared_mutex mutex_;

  TsEntityItem cur_entity_item_;

  std::map<uint32_t, TsEntityItem> entity_items_;

  std::deque<std::shared_ptr<TsBlockSpan>> cached_spans_;
  size_t cached_count_ = 0;

 public:
  explicit TsEntitySegmentBuilder(const std::string& root_path, TsEngineSchemaManager* schema_manager,
                                  TsVersionManager* version_manager, PartitionIdentifier partition_id,
                                  std::shared_ptr<TsEntitySegment> entity_segment, uint64_t entity_header_file_num,
                                  std::vector<std::shared_ptr<TsLastSegment>> last_segments)
      : root_path_(root_path),
        schema_manager_(schema_manager),
        version_manager_(version_manager),
        partition_id_(partition_id),
        cur_entity_segment_(entity_segment),
        last_segments_(last_segments) {
    // entity header file
    std::string entity_header_file_path = root_path + "/" + EntityHeaderFileName(entity_header_file_num);
    entity_item_builder_ =
        std::make_unique<TsEntitySegmentEntityItemFileBuilder>(entity_header_file_path, entity_header_file_num);
    bool override = cur_entity_segment_ == nullptr ? true : false;
    // block header file
    std::string block_header_file_path = root_path + "/" + block_item_file_name;
    block_item_builder_ = std::make_unique<TsEntitySegmentBlockItemFileBuilder>(block_header_file_path, override);
    // block data file
    std::string block_file_path = root_path + "/" + block_data_file_name;
    block_file_builder_ = std::make_unique<TsEntitySegmentBlockFileBuilder>(block_file_path, override);
    // block agg file
    std::string agg_file_path = root_path + "/" + block_agg_file_name;
    agg_file_builder_ = std::make_unique<TsEntitySegmentAggFileBuilder>(agg_file_path, override);
  }

  TsEntitySegmentBuilder(const std::string& root_path,
                         PartitionIdentifier partition_id,
                         std::shared_ptr<TsEntitySegment> entity_segment,
                         uint64_t entity_header_file_num)
    : root_path_(root_path),
      partition_id_(partition_id),
      cur_entity_segment_(entity_segment) {
    // entity header file
    std::string entity_header_file_path = root_path + "/" + EntityHeaderFileName(entity_header_file_num);
    entity_item_builder_ =
      std::make_unique<TsEntitySegmentEntityItemFileBuilder>(entity_header_file_path, entity_header_file_num);
    bool override = cur_entity_segment_ == nullptr ? true : false;
    // block header file
    std::string block_header_file_path = root_path + "/" + block_item_file_name;
    block_item_builder_ = std::make_unique<TsEntitySegmentBlockItemFileBuilder>(block_header_file_path, override);
    // block data file
    std::string block_file_path = root_path + "/" + block_data_file_name;
    block_file_builder_ = std::make_unique<TsEntitySegmentBlockFileBuilder>(block_file_path, override);
    // block agg file
    std::string agg_file_path = root_path + "/" + block_agg_file_name;
    agg_file_builder_ = std::make_unique<TsEntitySegmentAggFileBuilder>(agg_file_path, override);
  }

  KStatus Open();

  KStatus Compact(TsVersionUpdate *update);

  KStatus WriteBatch(uint32_t entity_id, uint32_t table_version, TS_LSN lsn, TSSlice data);

  KStatus WriteBatchFinish(TsVersionUpdate *update);

  void MarkDelete();
};

}  // namespace kwdbts
