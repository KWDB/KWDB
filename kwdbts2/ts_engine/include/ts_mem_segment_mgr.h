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

#include <algorithm>
#include <atomic>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "libkwdbts2.h"
#include "ts_compatibility.h"
#include "ts_engine_schema_manager.h"
#include "ts_mem_seg_index.h"
#include "ts_payload.h"
#include "ts_segment.h"
namespace kwdbts {

class TsVGroup;

enum TsMemSegmentStatus : uint8_t {
  MEM_SEGMENT_IDLE = 1,
  MEM_SEGMENT_IMMUTABLE = 2,
  MEM_SEGMENT_WRITING = 3,
  MEM_SEGMENT_FLUSHING = 4,
};

class TsMemSegment : public TsSegmentBase, public enable_shared_from_this<TsMemSegment> {
  friend class TsMemSegmentManager;

 private:
  std::atomic<uint32_t> intent_row_num_{0};
  std::atomic<uint32_t> written_row_num_{0};
  std::atomic<uint32_t> payload_mem_usage_{0};
  std::atomic<TsMemSegmentStatus> status_{MEM_SEGMENT_IDLE};
  TsMemSegIndex skiplist_;

  explicit TsMemSegment(int32_t max_height);

 public:
  template <class... Args>
  static std::shared_ptr<TsMemSegment> Create(Args&&... args) {
    return std::shared_ptr<TsMemSegment>(new TsMemSegment(std::forward<Args>(args)...));
  }
  ~TsMemSegment() {}

  uint32_t GetPayloadMemUsage() { return payload_mem_usage_.load(std::memory_order_relaxed); }
  size_t Size() { return skiplist_.GetAllocator().MemoryAllocatedBytes(); }

  uint32_t GetRowNum() { return intent_row_num_.load(); }

  inline void AllocRowNum(uint32_t row_num) { intent_row_num_.fetch_add(row_num); }

  TSMemSegRowData* AllocOneRow(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id,
                               TSSlice row_data) {
    return skiplist_.AllocateMemSegRowData(db_id, tbl_id, tbl_version, en_id, row_data);
  }

  void AppendOneRow(TSMemSegRowData* row);

  // bool HasEntityRows(const TsScanFilterParams& filter);

  bool GetEntityRows(const TsBlockItemFilterParams& filter, std::list<const TSMemSegRowData*>* rows);

  bool GetAllEntityRows(std::list<const TSMemSegRowData*>* rows);

  inline uint32_t GetMemSegmentSize() { return skiplist_.GetAllocator().MemoryAllocatedBytes(); }

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                        const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        const std::shared_ptr<MMapMetricsTable>& scan_schema,
                        TsScanStats* ts_scan_stats = nullptr) override;
  KStatus GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& blocks, TsEngineSchemaManager* schema_mgr);
};

class TsMemSegBlock : public TsBlock {
 private:
  std::shared_ptr<TsMemSegment> mem_seg_;
  std::vector<const TSMemSegRowData*> row_data_;
  std::vector<TSMemSegRowDataWithGuard> row_data_guard_;
  timestamp64 min_ts_{INVALID_TS};
  timestamp64 max_ts_{INVALID_TS};
  std::shared_ptr<TsRawPayloadRowParser> parser_ = nullptr;
  std::unordered_map<uint32_t, char*> col_based_mems_;
  std::vector<uint64_t> col_major_osn_;
  std::unordered_map<uint32_t, std::unique_ptr<TsBitmap>> col_bitmaps_;
  // it's safe to return memory address if memory_addr_safe_ is true
  bool memory_addr_safe_{false};

 public:
  explicit TsMemSegBlock(std::shared_ptr<TsMemSegment> mem_seg) : mem_seg_(mem_seg) {}

  ~TsMemSegBlock() {
    for (auto& mem : col_based_mems_) {
      if (mem.second != nullptr) {
        free(mem.second);
      }
    }
    col_based_mems_.clear();
  }

  void SetParser(std::shared_ptr<TsRawPayloadRowParser>& parser) {
    parser_ = parser;
  }

  uint32_t GetBlockVersion() const override { return CURRENT_BLOCK_VERSION; }

  TSEntityID GetEntityId() {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetEntityId();
  }
  TSTableID GetTableId() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetTableId();
  }
  uint32_t GetTableVersion() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetTableVersion();
  }

  uint32_t GetDBId() const {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetDatabaseId();
  }
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
    *min_ts = min_ts_;
    *max_ts = max_ts_;
  }
  size_t GetRowNum() override { return row_data_.size(); }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema, TSSlice& value,
                        TsScanStats* ts_scan_stats = nullptr) override;
  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
                  TsScanStats* ts_scan_stats = nullptr) override;

  // if just get timestamp , this function return fast.
  timestamp64 GetTS(int row_num, TsScanStats* ts_scan_stats = nullptr) override {
    assert(row_data_.size() > row_num);
    return row_data_[row_num]->GetTS();
  }

  timestamp64 GetFirstTS() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetTS();
  }

  timestamp64 GetLastTS() override {
    assert(row_data_.size() > 0);
    return row_data_[row_data_.size() - 1]->GetTS();
  }

  void GetMinAndMaxOSN(uint64_t& min_osn, uint64_t& max_osn) override {
    assert(row_data_.size() > 0);
    min_osn = UINT64_MAX;
    max_osn = 0;
    for (auto& row : row_data_) {
      if (row->GetOSN() < min_osn) {
        min_osn = row->GetOSN();
      }
      if (row->GetOSN() > max_osn) {
        max_osn = row->GetOSN();
      }
    }
  }

  uint64_t GetFirstOSN() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->GetOSN();
  }

  uint64_t GetLastOSN() override {
    assert(row_data_.size() > 0);
    return row_data_[row_data_.size() - 1]->GetOSN();
  }

  const uint64_t* GetOSNAddr(int row_num, TsScanStats* ts_scan_stats = nullptr) override {
    assert(row_data_.size() > row_num);
    if (col_major_osn_.empty()) {
      col_major_osn_.resize(row_data_.size());
      std::transform(row_data_.begin(), row_data_.end(), col_major_osn_.begin(),
                     [](const TSMemSegRowData* row) { return row->GetOSN(); });
    }
    return &col_major_osn_[row_num];
  }

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema,
                       std::unique_ptr<TsBitmapBase>* bitmap, TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema, char** value,
                      TsScanStats* ts_scan_stats = nullptr) override;

  KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, TsBufferBuilder* data) override {
    return KStatus::FAIL;
  }

  void SetMemoryAddrSafe() {
    memory_addr_safe_ = true;
  }

  TSMemSegRowDataWithGuard& AllocateRow(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id) {
    row_data_guard_.emplace_back(db_id, tbl_id, tbl_version, en_id);
    return row_data_guard_.back();
  }

  bool InsertRow(const TSMemSegRowData* row) {
    bool can_insert = true;
    if (row_data_.size() != 0) {
      auto first = row_data_.front();
      if (!first->SameEntityAndTableVersion(row)) {
        can_insert = false;
      }
    }
    if (can_insert) {
      row_data_.push_back(row);
      if (min_ts_ == INVALID_TS || min_ts_ > row->GetTS()) {
        min_ts_ = row->GetTS();
      }
      if (max_ts_ == INVALID_TS || max_ts_ < row->GetTS()) {
        max_ts_ = row->GetTS();
      }
    }
    return can_insert;
  }

  uint64_t GetBlockID() override {
    return 0;
  }
};

class TsVersionManager;
class TsMemSegmentManager {
 private:
  TsVGroup* vgroup_;
  TsVersionManager* version_manager_;
  std::shared_ptr<TsMemSegment> cur_mem_seg_{nullptr};
  mutable std::shared_mutex segment_lock_;

  std::shared_ptr<TsMemSegment> CurrentMemSegmentAndAllocateRow(uint32_t row_num) const {
    std::shared_lock lock(segment_lock_);
    cur_mem_seg_->AllocRowNum(row_num);
    return cur_mem_seg_;
  }

 public:
  explicit TsMemSegmentManager(TsVGroup* vgroup, TsVersionManager* version_manager);

  std::shared_ptr<TsMemSegment> CurrentMemSegment() const {
    std::shared_lock lock(segment_lock_);
    return cur_mem_seg_;
  }

  bool SwitchMemSegment(TsMemSegment* expected_old_mem_seg, bool flush);

  KStatus PutData(const TSSlice& payload, const std::shared_ptr<TsTableSchemaManager>& tb_schema, TSEntityID entity_id);

  bool GetMetricSchemaAndMeta(const std::shared_ptr<TsTableSchemaManager>& tb_schema, uint32_t version,
                              const std::vector<AttributeInfo>** schema, DATATYPE* ts_type, LifeTime* lifetime = nullptr);
};

}  // namespace kwdbts
















