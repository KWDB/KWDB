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
#include <list>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <unordered_map>

#include "ts_block.h"
#include "ts_compressor.h"
#include "ts_engine_schema_manager.h"
#include "ts_entity_segment_data.h"
#include "ts_entity_segment_handle.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_version.h"

namespace kwdbts {


struct TsEntitySegmentBlockItem {
  uint64_t block_id = 0;  // block item id
  uint64_t entity_id = 0;
  uint64_t prev_block_id = 0;  // pre block item id
  uint64_t block_offset = 0;
  uint32_t block_len = 0;
  uint32_t table_version = 0;
  uint32_t n_cols = 0;
  uint32_t n_rows = 0;
  timestamp64 min_ts = INT64_MAX;
  timestamp64 max_ts = INT64_MIN;
  uint64_t min_osn = UINT64_MAX;
  uint64_t max_osn = 0;
  uint64_t first_osn = 0;
  uint64_t last_osn = 0;
  uint64_t agg_offset = 0;
  uint32_t agg_len = 0;
  uint32_t block_version = INVALID_BLOCK_VERSION;
  char reserved[16] = {0};  // reserved for user-defined information.
};
static_assert(sizeof(TsEntitySegmentBlockItem) == 128,
              "wrong size of TsEntitySegmentBlockItem, please check compatibility.");
// static_assert(std::has_unique_object_representations_v<TsEntitySegmentBlockItem>,
//               "check padding in TsEntitySegmentBlockItem");

struct TsEntitySegmentBlockItemWithData {
  TsEntitySegmentBlockItem* block_item = nullptr;
  TsSliceGuard data{};
};

static constexpr uint64_t TS_ENTITY_SEGMENT_ENTITY_ITEM_FILE_MAGIC = 0xcb2ffe9321847272;
static constexpr uint64_t TS_ENTITY_SEGMENT_BLOCK_ITEM_FILE_MAGIC = 0xcb2ffe9321847273;

struct TsEntityItemFileHeader {
  uint64_t magic;       // Magic number for block.e file.
  int32_t encoding;     // Encoding scheme.
  int32_t status;       // status flag.
  uint64_t entity_num;  // entity num
  char reserved[40];    // reserved for user-defined meta data information.
};
static_assert(sizeof(TsEntityItemFileHeader) == 64, "wrong size of TsBlockFileHeader, please check compatibility.");
// static_assert(std::has_unique_object_representations_v<TsEntityItemFileHeader>,
//               "check padding in TsEntityItemFileHeader");

struct TsEntityItem {
  uint64_t entity_id = 0;
  uint64_t cur_block_id = 0;   // block id that is allocating space for writing.
  int64_t max_ts = INT64_MIN;  // max ts of current entity in this Partition
  int64_t min_ts = INT64_MAX;  // min ts of current entity in this Partition
  uint64_t row_written = 0;    // row num that has written into file.
  uint64_t table_id = 0;
  bool is_dropped = false;
  char reserved[79] = {0};     // reserved for user-defined information.
};
static_assert(sizeof(TsEntityItem) == 128, "wrong size of TsEntityItem, please check compatibility.");
// static_assert(std::has_unique_object_representations_v<TsEntityItem>, "check padding in TsEntityItem");

/**
 * TsEntitySegmentEntityItemFile used for managing entity_item file.
 * index of block items.
 */
class TsEntitySegmentEntityItemFile {
  friend class TsMemEntitySegmentModifier;

 private:
  TsIOEnv* io_env_ = nullptr;
  string file_path_;
  std::unique_ptr<TsRandomReadFile> r_file_;
  TsEntityItemFileHeader* header_ = nullptr;
  TsSliceGuard header_guard_{};

 public:
  TsEntitySegmentEntityItemFile() {}

  explicit TsEntitySegmentEntityItemFile(TsIOEnv* io_env, const string& file_path)
      : io_env_(io_env), file_path_(file_path) {}

  ~TsEntitySegmentEntityItemFile() {}

  KStatus Open();

  KStatus GetEntityItem(uint64_t entity_id, TsEntityItem& entity_item, bool& is_exist);

  KStatus SetEntityItemDropped(uint64_t entity_id);

  uint32_t GetFileNum();

  uint64_t GetEntityNum();

  bool IsReady();

  void MarkDelete() { r_file_->MarkDelete(); }
};

struct TsBlockItemFileHeader {
  uint64_t magic;         // Magic number for block.e file.
  int32_t encoding;       // Encoding scheme.
  int32_t status;         // status flag.
  char user_defined[48];  // reserved for user-defined meta data information.
};
static_assert(sizeof(TsBlockItemFileHeader) == 64, "wrong size of TsBlockItemFileHeader, please check compatibility.");
// static_assert(std::has_unique_object_representations_v<TsBlockItemFileHeader>, "check padding in TsBlockItemFileHeader");

class TsEntitySegmentBlockItemFile {
  friend class TsMemEntitySegmentModifier;

 private:
  TsIOEnv* io_env_ = nullptr;
  string file_path_;
  uint64_t file_size_ = 0;
  std::unique_ptr<TsRandomReadFile> r_file_;
  TsBlockItemFileHeader* header_ = nullptr;
  TsSliceGuard header_guard_{};

 public:
  TsEntitySegmentBlockItemFile() {}

  explicit TsEntitySegmentBlockItemFile(TsIOEnv* io_env, const string& file_path, uint64_t file_size)
      : io_env_(io_env), file_path_(file_path), file_size_(file_size) {}

  ~TsEntitySegmentBlockItemFile() {}

  KStatus Open();

  KStatus GetBlockItem(uint64_t blk_id, TsEntitySegmentBlockItem** blk_item, TsSliceGuard* blk_item_guard,
                       TsScanStats* ts_scan_stats = nullptr);

  uint64_t GetBlockNum() {
    assert((r_file_->GetFileSize() - sizeof(TsBlockItemFileHeader)) % sizeof(TsEntitySegmentBlockItem) == 0);
    return (r_file_->GetFileSize() - sizeof(TsBlockItemFileHeader)) / sizeof(TsEntitySegmentBlockItem);
  }

  void MarkDelete() { r_file_->MarkDelete(); }
};

class TsEntitySegment;
class TsEntitySegmentMetaManager {
  friend class TsMemEntitySegmentModifier;

 private:
  fs::path dir_path_;
  TsEntitySegmentEntityItemFile entity_header_;
  TsEntitySegmentBlockItemFile block_header_;

 public:
  TsEntitySegmentMetaManager() = default;

  explicit TsEntitySegmentMetaManager(TsIOEnv* env, const string& dir_path, EntitySegmentMetaInfo info);

  ~TsEntitySegmentMetaManager() {}

  uint32_t GetEntityHeaderFileNum() { return entity_header_.GetFileNum(); }

  uint64_t GetEntityNum() { return entity_header_.GetEntityNum(); }

  uint64_t GetBlockNum() { return block_header_.GetBlockNum(); }

  bool IsReady() { return entity_header_.IsReady(); }

  KStatus GetEntityItem(uint64_t entity_id, TsEntityItem& entity_item, bool& is_exist) {
    return entity_header_.GetEntityItem(entity_id, entity_item, is_exist);
  }

  KStatus SetEntityItemDropped(uint64_t entity_id) {
    return entity_header_.SetEntityItemDropped(entity_id);
  }

  KStatus Open();

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<TsEntitySegmentBlockItemWithData>* blk_items);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::shared_ptr<TsEntitySegment> entity_segment,
                        std::list<shared_ptr<TsBlockSpan>>& block_spans,
                        std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        std::shared_ptr<MMapMetricsTable>& scan_schema,
                        TsScanStats* ts_scan_stats = nullptr);

  void MarkDeleteEntityHeader() { entity_header_.MarkDelete(); }

  void MarkDeleteAll() {
    entity_header_.MarkDelete();
    block_header_.MarkDelete();
  }
};

struct TsEntitySegmentBlockInfo {
  TsSliceGuard col_block_offset{};
  TsSliceGuard col_agg_offset{};
};

struct TsEntitySegmentColumnBlock {
  std::unique_ptr<TsBitmapBase> bitmap;
  TsSliceGuard buffer;
  TsSliceGuard agg;
  std::vector<std::string> var_rows;
};

class TsEntityBlock : public TsBlock {
 public:
  // pre and next are used to support TsBlockCache.
  std::shared_ptr<TsEntityBlock> pre_{nullptr};
  std::shared_ptr<TsEntityBlock> next_{nullptr};

 private:
  uint32_t table_id_ = 0;
  uint32_t table_version_ = 0;
  uint64_t entity_id_ = 0;
  uint32_t block_id_ = 0;
  const std::vector<AttributeInfo>* metric_schema_ = nullptr;

  TsEntitySegmentBlockInfo block_info_;
  std::vector<std::shared_ptr<TsEntitySegmentColumnBlock>> column_blocks_;
  std::string extra_buffer_;

  uint32_t n_rows_ = 0;
  uint32_t n_cols_ = 0;

  timestamp64 first_ts_ = 0;
  timestamp64 last_ts_ = 0;
  uint64_t first_osn_ = 0;
  uint64_t last_osn_ = 0;
  uint64_t min_osn_ = 0;
  uint64_t max_osn_ = 0;

  std::shared_ptr<TsEntitySegment> entity_segment_ = nullptr;
  uint64_t block_offset_ = 0;
  uint32_t block_length_ = 0;
  uint64_t agg_offset_ = 0;
  uint32_t agg_length_ = 0;

  KRWLatch rw_latch_;

  // total memory size of all column blocks loaded.
  uint32_t memory_size_{0};
  uint32_t block_version_ = INVALID_BLOCK_VERSION;

 public:
  TsEntityBlock() = delete;
  TsEntityBlock(uint32_t table_id, TsEntitySegmentBlockItem* block_item,
                std::shared_ptr<TsEntitySegment>& block_segment);
  TsEntityBlock(const TsEntityBlock& other) = delete;
  ~TsEntityBlock() {}

  uint32_t GetBlockVersion() const override { return block_version_; }

  size_t GetRowNum() override { return n_rows_; }

  uint32_t GetMemorySize() { return memory_size_; }

  void AddMemory(uint32_t new_memory_size) {
    memory_size_ += new_memory_size;
  }

  uint64_t GetBlockID() override {
    return block_id_;
  }

  uint32_t GetNCols() { return n_cols_; }

  TSTableID GetTableId() override { return table_id_; }

  uint32_t GetTableVersion() override { return table_version_; }

  const TsEntitySegmentBlockInfo& GetBlockInfo() const { return block_info_; }

  uint64_t GetBlockOffset() const { return block_offset_; }

  uint32_t GetBlockLength() const { return block_length_; }

  uint64_t GetAggOffset() const { return agg_offset_; }

  uint32_t GetAggLength() const { return agg_length_; }

  inline bool HasAggDataNoLock(int32_t col_idx) {
    return column_blocks_.size() > col_idx + 1 && column_blocks_[col_idx + 1] != nullptr
            && !column_blocks_[col_idx + 1]->agg.empty();
  }

  inline bool HasAggData(int32_t col_idx) {
    return HasAggDataNoLock(col_idx);
  }

  inline bool HasDataCachedNoLock(int32_t col_idx) {
    assert(col_idx >= -1);
    return column_blocks_.size() > col_idx + 1 && column_blocks_[col_idx + 1] != nullptr
            && !column_blocks_[col_idx + 1]->buffer.empty();
  }

  inline bool HasDataCached(int32_t col_idx) {
    return HasDataCachedNoLock(col_idx);
  }

  char* GetMetricColAddr(uint32_t col_idx);

  KStatus GetMetricColValue(uint32_t row_idx, uint32_t col_idx, TSSlice& value);

  KStatus LoadColData(int32_t col_idx, const std::vector<AttributeInfo>* metric_schema, TsSliceGuard&& buffer);

  KStatus LoadAggData(int32_t col_idx, TsSliceGuard&& buffer);

  KStatus LoadBlockInfo(TsSliceGuard&& buffer);

  KStatus LoadAggInfo(TsSliceGuard&& buffer);

  KStatus GetRowSpans(const std::vector<STScanRange>& spans, std::vector<std::pair<int, int>>& row_spans,
                      TsScanStats* ts_scan_stats);

  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema, char** value,
                      TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                       std::unique_ptr<TsBitmapBase>* bitmap, TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema, TSSlice& value,
                        TsScanStats* ts_scan_stats = nullptr) override;

  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                  TsScanStats* ts_scan_stats = nullptr) override;

  timestamp64 GetTS(int row_num, TsScanStats* ts_scan_stats = nullptr) override;

  timestamp64 GetFirstTS() override;

  timestamp64 GetLastTS() override;

  void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) override;

  uint64_t GetFirstOSN() override;

  uint64_t GetLastOSN() override;

  const uint64_t* GetOSNAddr(int row_num, TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, TsBufferBuilder* data) override;

  bool HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) override;
  KStatus GetPreCount(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, uint16_t& count) override;
  KStatus GetPreSum(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats,
                    void*& pre_sum, bool& is_overflow) override;
  KStatus GetPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, void*& pre_max) override;
  KStatus GetPreMin(uint32_t blk_col_idx, int32_t size, TsScanStats* ts_scan_stats, void*& pre_max) override;
  KStatus GetVarPreMax(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_max) override;
  KStatus GetVarPreMin(uint32_t blk_col_idx, TsScanStats* ts_scan_stats, TSSlice& pre_min) override;

  std::string GetEntitySegmentPath();
  std::string GetHandleInfoStr();

  void RemoveFromSegment();

  void RdLock() {
    RW_LATCH_S_LOCK(&rw_latch_);
  }
  void WrLock() {
    RW_LATCH_X_LOCK(&rw_latch_);
  }
  void Unlock() {
    RW_LATCH_UNLOCK(&rw_latch_);
  }
};

struct EntitySegmentIOEnvSet {
  TsIOEnv* block_agg_io_env;
  TsIOEnv* header_io_env;
};
class TsEntitySegment : public TsSegmentBase, public enable_shared_from_this<TsEntitySegment> {
  friend class TsMemEntitySegmentModifier;

 private:
  string dir_path_;
  TsEntitySegmentMetaManager meta_mgr_;
  TsEntitySegmentBlockFile block_file_;
  TsEntitySegmentAggFile agg_file_;

  EntitySegmentMetaInfo info_;

  std::vector<std::shared_ptr<TsEntityBlock>> entity_blocks_;
  KRWLatch entity_blocks_rw_latch_;

 public:
  TsEntitySegment() = delete;

  explicit TsEntitySegment(EntitySegmentIOEnvSet io_env, const fs::path& root, EntitySegmentMetaInfo info);

  // Only for LRU block cache unit tests
  explicit TsEntitySegment(uint32_t max_blocks);

  ~TsEntitySegment() {}

  KStatus Open();

  uint64_t GetEntityHeaderFileNum() { return meta_mgr_.GetEntityHeaderFileNum(); }

  KStatus GetEntityItem(uint64_t entity_id, TsEntityItem& entity_item, bool& is_exist) {
    return meta_mgr_.GetEntityItem(entity_id, entity_item, is_exist);
  }

  KStatus SetEntityItemDropped(uint64_t entity_id) {
    return meta_mgr_.SetEntityItemDropped(entity_id);
  }

  uint64_t GetEntityNum() { return meta_mgr_.GetEntityNum(); }

  std::shared_ptr<TsEntityBlock> GetEntityBlock(uint64_t block_id) {
    RW_LATCH_S_LOCK(&entity_blocks_rw_latch_);
    std::shared_ptr<TsEntityBlock> block = entity_blocks_[block_id - 1];
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
    return block;
  }

  void AddEntityBlock(uint64_t block_id, std::shared_ptr<TsEntityBlock> block) {
    RW_LATCH_X_LOCK(&entity_blocks_rw_latch_);
    entity_blocks_[block_id - 1] = block;
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
  }

  void RemoveEntityBlock(uint64_t block_id) {
    RW_LATCH_X_LOCK(&entity_blocks_rw_latch_);
    entity_blocks_[block_id - 1] = nullptr;
    RW_LATCH_UNLOCK(&entity_blocks_rw_latch_);
  }

  KStatus GetAllBlockItems(TSEntityID entity_id, std::vector<TsEntitySegmentBlockItemWithData>* blk_items) {
    return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
  }

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& block_spans,
                        std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        std::shared_ptr<MMapMetricsTable>& scan_schema,
                        TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetBlockData(TsEntityBlock* block, TsSliceGuard* data);

  KStatus GetColumnBlock(int32_t col_idx, const std::vector<AttributeInfo>* metric_schema,
                          TsEntityBlock* block, TsScanStats* ts_scan_stats);

  KStatus GetAggData(TsEntityBlock* block, TsSliceGuard* data);

  KStatus GetColumnAgg(int32_t col_idx, TsEntityBlock* block, TsScanStats* ts_scan_stats);

  const EntitySegmentMetaInfo &GetHandleInfo() const { return info_; }

  void MarkDeleteEntityHeader() { meta_mgr_.MarkDeleteEntityHeader(); }

  // used by Vacuum, delete all data files.
  void MarkDeleteAll() {
    meta_mgr_.MarkDeleteAll();
    block_file_.MarkDelete();
    agg_file_.MarkDelete();
  }

  std::string GetPath() { return dir_path_; }
  std::string GetHandleInfoStr();

  uint64_t GetAggFileSize() { return agg_file_.Size(); }
};

}  // namespace kwdbts
