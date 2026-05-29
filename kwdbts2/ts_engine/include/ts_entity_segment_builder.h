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
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "kwdb_type.h"
#include "ts_block.h"
#include "ts_bufferbuilder.h"
#include "ts_engine_schema_manager.h"
#include "ts_entity_segment_data.h"
#include "ts_entity_segment_handle.h"
#include "ts_io.h"
#include "ts_entity_segment.h"
#include "ts_filename.h"
#include "ts_segment.h"
#include "ts_sliceguard.h"
#include "ts_version.h"


namespace kwdbts {

class TsEntitySegmentEntityItemFileBuilder {
 private:
  TsIOEnv* io_env_;
  string file_path_;
  uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  TsEntityItemFileHeader header_;

 public:
  explicit TsEntitySegmentEntityItemFileBuilder(TsIOEnv* env, const string& file_path, uint64_t file_number)
      : io_env_(env), file_path_(file_path), file_number_(file_number) {
    memset(&header_, 0, sizeof(TsEntityItemFileHeader));
  }

  ~TsEntitySegmentEntityItemFileBuilder() {}

  [[nodiscard]] KStatus Finalize() {
    assert(w_file_ != nullptr);
    header_.magic = TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC;
    header_.status = TsFileStatus::READY;
    header_.entity_num = w_file_->GetFileSize() / sizeof(TsEntityItem);
    KStatus s = w_file_->Append(TSSlice{reinterpret_cast<char*>(&header_), sizeof(TsEntityItemFileHeader)});
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsEntitySegmentEntityItemFileBuilder Append failed, file_path=%s", file_path_.c_str())
      return FAIL;
    }
    if (w_file_->Close() == KStatus::FAIL) {
      LOG_ERROR("TsEntitySegmentEntityItemFileBuilder Close failed, file_path=%s", file_path_.c_str())
      return FAIL;
    }
    return SUCCESS;
  }

  [[nodiscard]] KStatus Open();

  [[nodiscard]] KStatus AppendEntityItem(TsEntityItem& entity_item);

  void MarkDelete() { w_file_->MarkDelete(); }

  uint64_t GetFileNumber() { return file_number_; }
};

class TsEntitySegmentBlockItemFileBuilder {
 private:
  TsIOEnv* io_env_;
  string file_path_;
  uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  TsBlockItemFileHeader header_;
  size_t file_size_ = 0;

 public:
  explicit TsEntitySegmentBlockItemFileBuilder(TsIOEnv* env, const string& file_path, uint64_t file_number,
                                               size_t file_size)
      : io_env_(env), file_path_(file_path), file_number_(file_number), file_size_(file_size) {
    memset(&header_, 0, sizeof(TsBlockItemFileHeader));
  }

  ~TsEntitySegmentBlockItemFileBuilder() {}

  [[nodiscard]] KStatus Open();
  [[nodiscard]] KStatus AppendBlockItem(TsEntitySegmentBlockItem& block_item);
  [[nodiscard]] KStatus Finalize() {
    assert(w_file_ != nullptr);
    if (w_file_->Close() == KStatus::FAIL) {
      LOG_ERROR("TsEntitySegmentBlockItemFileBuilder Close failed, file_path=%s", file_path_.c_str());
      return FAIL;
    }
    return SUCCESS;
  }
  MetaFileInfo GetFileInfo() { return {file_number_, w_file_->GetFileSize()}; }

  void MarkDelete() { w_file_->MarkDelete(); }
};

class TsEntitySegmentBlockFileBuilder {
 private:
  TsIOEnv* io_env_;
  string file_path_;
  uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_ = nullptr;
  TsAggAndBlockFileHeader header_;
  size_t file_size_ = 0;

 public:
  explicit TsEntitySegmentBlockFileBuilder(TsIOEnv* env, const string& file_path, uint64_t file_number,
                                           size_t file_size)
      : io_env_(env), file_path_(file_path), file_number_(file_number), file_size_(file_size) {
    memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
  }

  ~TsEntitySegmentBlockFileBuilder() {}

  [[nodiscard]] KStatus Open();
  [[nodiscard]] KStatus AppendBlock(const TSSlice& block, uint64_t* offset);
  MetaFileInfo GetFileInfo() { return {file_number_, w_file_->GetFileSize()}; }
  [[nodiscard]] KStatus Finalize() {
    assert(w_file_ != nullptr);
    if (w_file_->Close() == KStatus::FAIL) {
      LOG_ERROR("TsEntitySegmentBlockFileBuilder Close failed, file_path=%s", file_path_.c_str());
      return FAIL;
    }
    return SUCCESS;
  }

  void MarkDelete() { w_file_->MarkDelete(); }
};

class TsEntitySegmentAggFileBuilder {
 private:
  TsIOEnv* io_env_;
  string file_path_;
  uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_ = nullptr;
  TsAggAndBlockFileHeader header_;
  size_t file_size_ = 0;

 public:
  explicit TsEntitySegmentAggFileBuilder(TsIOEnv* env, const string& file_path, uint64_t file_number, size_t file_size)
      : io_env_(env), file_path_(file_path), file_number_(file_number), file_size_(file_size) {
    memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
  }

  ~TsEntitySegmentAggFileBuilder() {}

  [[nodiscard]] KStatus Open();
  [[nodiscard]] KStatus AppendAggBlock(const TSSlice& agg, uint64_t* offset);
  MetaFileInfo GetFileInfo() { return {file_number_, w_file_->GetFileSize()}; }
  [[nodiscard]] KStatus Finalize() {
    assert(w_file_ != nullptr);
    if (w_file_->Close() == KStatus::FAIL) {
      LOG_ERROR("TsEntitySegmentAggFileBuilder Close failed, file_path=%s", file_path_.c_str());
      return FAIL;
    }
    return SUCCESS;
  }

  void MarkDelete() { w_file_->MarkDelete(); }
};

class TsEntitySegmentBuilder;

struct TsEntitySegmentColumnBlockBuilder {
  std::unique_ptr<TsBitmapBase> bitmap;
  TsBufferBuilder buffer;
  TsBufferBuilder agg;
  std::vector<std::string> var_rows;
};

class TsEntityBlockBuilder {
 private:
  uint32_t table_id_ = 0;
  uint32_t table_version_ = 0;
  uint64_t entity_id_ = 0;
  std::vector<AttributeInfo> metric_schema_;

  TsEntitySegmentBlockInfo block_info_;
  std::vector<TsEntitySegmentColumnBlockBuilder> column_blocks_;

  uint32_t n_rows_ = 0;
  uint32_t n_cols_ = 0;

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

  [[nodiscard]] KStatus GetMetricValue(uint32_t row_idx, std::vector<TSSlice>& value,
                                       std::vector<DataFlags>& data_flags);

  [[nodiscard]] KStatus Append(const shared_ptr<TsBlockSpan>& span, bool& is_full);

  [[nodiscard]] KStatus GetCompressData(TsEntitySegmentBlockItem& blk_item, TsBufferBuilder* data_buffer,
                                        TsBufferBuilder* agg_buffer);

  void Clear();

  void Reset(uint64_t entity_id) {
    this->entity_id_ = entity_id;
    Clear();
  }
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
    bool operator!=(const TsEntityKey& other) const { return !(*this == other); }
  };

  [[nodiscard]] KStatus UpdateEntityItem(TsEntityKey& entity_key, TsEntitySegmentBlockItem& block_item);

  [[nodiscard]] KStatus WriteBlock(TsEntityKey& entity_key, TsSegmentWriteStats* stats);

  [[nodiscard]] KStatus WriteCachedBlockSpan(bool call_by_vacuum, TsEntityKey& entity_key, TsSegmentWriteStats* stats);


  TsIOEnv* io_env_;
  fs::path root_path_;
  TsEngineSchemaManager* schema_manager_;
  TsVersionManager* version_manager_;

  PartitionIdentifier partition_id_;
  std::shared_ptr<TsEntitySegment> cur_entity_segment_;

  TsDataSource source_;

  std::unique_ptr<TsEntitySegmentEntityItemFileBuilder> entity_item_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentBlockItemFileBuilder> block_item_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentBlockFileBuilder> block_file_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentAggFileBuilder> agg_file_builder_ = nullptr;
  std::shared_ptr<TsEntityBlockBuilder> block_ = nullptr;
  uint64_t entity_item_file_number_ = 0;

  std::shared_mutex mutex_;
  bool write_batch_finished_ = false;

  TsEntityItem cur_entity_item_;

  std::map<uint32_t, TsEntityItem> entity_items_;

  std::deque<std::shared_ptr<TsBlockSpan>> cached_spans_;
  size_t cached_count_ = 0;
  std::vector<TsEntityCountStats> flush_infos_;

  std::vector<std::shared_ptr<TsBlockSpan>> block_spans_;
  std::vector<std::shared_ptr<TsBlockSpan>> lastsegment_block_spans_;

 public:
  explicit TsEntitySegmentBuilder(TsIOEnv* env, const std::string& root_path, TsEngineSchemaManager* schema_manager,
                                  TsVersionManager* version_manager, PartitionIdentifier partition_id,
                                  std::shared_ptr<TsEntitySegment> entity_segment, TsDataSource source)
      : io_env_(env),
        root_path_(root_path),
        schema_manager_(schema_manager),
        version_manager_(version_manager),
        partition_id_(partition_id),
        cur_entity_segment_(std::move(entity_segment)),
        source_(source) {
    entity_item_file_number_ = version_manager_->NewFileNumber();
  }

  ~TsEntitySegmentBuilder() = default;

  [[nodiscard]] KStatus Open();
  [[nodiscard]] KStatus Finalize();


  void PutBlockSpan(std::shared_ptr<TsBlockSpan> span) { block_spans_.push_back(std::move(span)); }
  void PutBlockSpans(std::vector<std::shared_ptr<TsBlockSpan>> spans) {
    std::move(spans.begin(), spans.end(), std::back_inserter(block_spans_));
  }

  [[nodiscard]] KStatus Compact(bool call_by_vacuum, TsVersionUpdate* update,
                                std::vector<std::shared_ptr<TsBlockSpan>>* residual_spans, TsSegmentWriteStats* stats);

  [[nodiscard]] KStatus WriteBatch(TSTableID tbl_id, uint32_t entity_id, uint32_t table_version, uint32_t batch_version,
                                   TSSlice data);

  [[nodiscard]] KStatus WriteBatchFinish(TsVersionUpdate* update);


  void WriteBatchCancel();

  std::vector<TsEntityCountStats> FlushInfos() {
    return flush_infos_;
  }
};

class TsEntitySegmentVacuumer {
 public:
  explicit TsEntitySegmentVacuumer(const std::string& root_path, TsVersionManager* version_manager, TsDataSource source);
  [[nodiscard]] KStatus Open();
  void Cancel();
  [[nodiscard]] KStatus AppendEntityItem(TsEntityItem& entity_item);
  [[nodiscard]] KStatus AppendBlock(const TSSlice& block, uint64_t* offset);
  [[nodiscard]] KStatus AppendAgg(const TSSlice& agg, uint64_t* offset);
  [[nodiscard]] KStatus AppendBlockItem(TsEntitySegmentBlockItem& block_item);
  [[nodiscard]] KStatus Finalize() {
    if (entity_item_builder_->Finalize() == FAIL) {
      return FAIL;
    }
    if (block_item_builder_->Finalize() == FAIL) {
      return FAIL;
    }
    if (block_file_builder_->Finalize() == FAIL) {
      return FAIL;
    }
    return agg_file_builder_->Finalize();
  }

  EntitySegmentMetaInfo GetHandleInfo();

 private:
  fs::path root_path_;
  TsVersionManager* version_manager_;

  std::unique_ptr<TsEntitySegmentEntityItemFileBuilder> entity_item_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentBlockItemFileBuilder> block_item_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentBlockFileBuilder> block_file_builder_ = nullptr;
  std::unique_ptr<TsEntitySegmentAggFileBuilder> agg_file_builder_ = nullptr;

  TsDataSource source_;
};

class TsMemEntitySegmentModifier {
 private:
  TsEntitySegment* entity_segment_;
  std::vector<TsEntityItem> entity_items_;

  TsSliceGuard GetBlockItems(TsEntitySegment *target);
  TsSliceGuard GetEntityItems(TsEntitySegment *target);

  auto GetEntityAndBlockItems(TsEntitySegment* target);
  auto Modify(TsEntitySegment* base);

  [[nodiscard]] KStatus FirstFlushBypass(EntitySegmentMetaInfo* info);
  [[nodiscard]] KStatus NormalFlush(TsEntitySegment* base, EntitySegmentMetaInfo* info);

 public:
  explicit TsMemEntitySegmentModifier(TsEntitySegment* entity_segment) : entity_segment_(entity_segment) {}

  [[nodiscard]] KStatus PersistToDisk(TsEntitySegment* base, EntitySegmentMetaInfo* info);
};
}  // namespace kwdbts
