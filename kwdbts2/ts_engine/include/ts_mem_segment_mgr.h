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

#include <list>
#include <map>
#include <memory>
#include <unordered_map>
#include <atomic>
#include <deque>
#include <string>
#include <utility>
#include <vector>

#include "libkwdbts2.h"
#include "ts_engine_schema_manager.h"
#include "ts_payload.h"
#include "ts_mem_seg_index.h"
#include "ts_segment.h"
namespace kwdbts {

class TsVGroup;

enum TsMemSegmentStatus : uint8_t {
  MEM_SEGMENT_INITED = 1,
  MEM_SEGMENT_IMMUTABLE = 2,
  MEM_SEGMENT_FLUSHING = 3,
  MEM_SEGMENT_DELETING = 4,
};

class TsMemSegment : public TsSegmentBase, public enable_shared_from_this<TsMemSegment> {
 private:
  std::atomic<uint32_t> row_idx_{1};
  std::atomic<uint32_t> intent_row_num_{0};
  std::atomic<uint32_t> written_row_num_{0};
  std::atomic<TsMemSegmentStatus> status_{MEM_SEGMENT_INITED};
  TsMemSegIndex skiplist_;

  explicit TsMemSegment(int32_t max_height);

 public:
  template <class... Args>
  static std::shared_ptr<TsMemSegment> Create(Args&&... args) {
    return std::shared_ptr<TsMemSegment>(new TsMemSegment(std::forward<Args>(args)...));
  }
  ~TsMemSegment() {}

  void Traversal(std::function<bool(TSMemSegRowData* row)> func, bool waiting_done = false);

  size_t Size() { return skiplist_.GetAllocator().MemoryAllocatedBytes(); }

  uint32_t GetRowNum() { return intent_row_num_.load(); }

  inline void AllocRowNum(uint32_t row_num) { intent_row_num_.fetch_add(row_num); }

  bool AppendOneRow(TSMemSegRowData& row);

  bool HasEntityRows(const TsScanFilterParams& filter);

  bool GetEntityRows(const TsBlockItemFilterParams& filter, std::list<TSMemSegRowData*>* rows);

  bool GetAllEntityRows(std::list<TSMemSegRowData*>* rows);

  inline uint32_t GetMemSegmentSize() { return skiplist_.GetAllocator().MemoryAllocatedBytes(); }

  inline bool SetImm() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_INITED;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_IMMUTABLE);
  }

  inline bool SetFlushing() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_IMMUTABLE;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_FLUSHING);
  }

  inline void SetDeleting() { status_.store(MEM_SEGMENT_DELETING); }

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                        std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, uint32_t scan_version) override;
  KStatus GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& blocks, TsEngineSchemaManager* schema_mgr);
};

class TsMemSegBlock : public TsBlock {
 private:
  std::shared_ptr<TsMemSegment> mem_seg_;
  std::vector<TSMemSegRowData*> row_data_;
  timestamp64 min_ts_{INVALID_TS};
  timestamp64 max_ts_{INVALID_TS};
  std::unique_ptr<TsRawPayloadRowParser> parser_ = nullptr;
  std::unordered_map<uint32_t, char*> col_based_mems_;
  std::unordered_map<uint32_t, TsBitmap> col_bitmaps_;

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

  TSEntityID GetEntityId() {
    assert(row_data_.size() > 0);
    return row_data_[0]->entity_id;
  }
  TSTableID GetTableId() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->table_id;
  }
  uint32_t GetTableVersion() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->table_version;
  }
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
    *min_ts = min_ts_;
    *max_ts = max_ts_;
  }
  size_t GetRowNum() override { return row_data_.size(); }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) override;
  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) override;

  // if just get timestamp , this function return fast.
  timestamp64 GetTS(int row_num) override {
    assert(row_data_.size() > row_num);
    return row_data_[row_num]->ts;
  }

  timestamp64 GetFirstTS() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->ts;
  }

  timestamp64 GetLastTS() override {
    assert(row_data_.size() > 0);
    return row_data_[row_data_.size() - 1]->ts;
  }

  TS_LSN GetFirstLSN() override {
    assert(row_data_.size() > 0);
    return row_data_[0]->lsn;
  }

  TS_LSN GetLastLSN() override {
    assert(row_data_.size() > 0);
    return row_data_[row_data_.size() - 1]->lsn;
  }

  uint64_t* GetLSNAddr(int row_num) override {
    assert(row_data_.size() > row_num);
    return &row_data_[row_num]->lsn;
  }

  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap) override;

  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value) override;

  KStatus GetCompressDataFromFile(uint32_t table_version, int32_t nrow, std::string& data) override {
    return KStatus::FAIL;
  }

  bool InsertRow(TSMemSegRowData* row) {
    bool can_insert = true;
    if (row_data_.size() != 0) {
      auto first = row_data_.front();
      if (!first->SameEntityAndTableVersion(row)) {
        can_insert = false;
      }
    }
    if (can_insert) {
      row_data_.push_back(row);
      if (min_ts_ == INVALID_TS || min_ts_ > row->ts) {
        min_ts_ = row->ts;
      }
      if (max_ts_ == INVALID_TS || max_ts_ < row->ts) {
        max_ts_ = row->ts;
      }
    }
    return can_insert;
  }
};

class TsMemSegmentManager {
 private:
  TsVGroup* vgroup_;
  std::shared_ptr<TsMemSegment> cur_mem_seg_{nullptr};
  std::list<std::shared_ptr<TsMemSegment>> segment_;
  mutable std::shared_mutex segment_lock_;

 public:
  explicit TsMemSegmentManager(TsVGroup* vgroup);

  ~TsMemSegmentManager() {
    segment_.clear();
  }

  // WAL CreateCheckPoint call this function to persistent metric datas.

  std::shared_ptr<TsMemSegment> CurrentMemSegment() const {
    std::shared_lock lock(segment_lock_);
    return cur_mem_seg_;
  }

  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments);

  void RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg);

  void GetAllMemSegments(std::list<std::shared_ptr<TsMemSegment>>* mems) {
    std::shared_lock lock(segment_lock_);
    *mems = segment_;
  };

  KStatus PutData(const TSSlice& payload, TSEntityID entity_id, TS_LSN lsn,
    std::list<TSMemSegRowData>* rows = nullptr);

  bool GetMetricSchemaAndMeta(TSTableID table_id_, uint32_t version, std::vector<AttributeInfo>& schema,
                              LifeTime* lifetime = nullptr);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& block_spans,
                        std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        uint32_t scan_version = 0);
};

}  // namespace kwdbts
















