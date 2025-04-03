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
#include <vector>

#include "ts_env.h"
#include "libkwdbts2.h"
#include "ts_payload.h"
#include "inlineskiplist.h"
#include "ts_block_item_info.h"
#include "ts_arena.h"

namespace kwdbts {

class TsVGroup;

// store row-based data struct for certain entity.
struct TSMemSegRowData {
  uint32_t database_id;
  TSTableID table_id;
  uint32_t table_version;
  TSEntityID entity_id;
  timestamp64 ts;
  TS_LSN lsn;
  TSSlice row_data;

  int Compare(TSMemSegRowData* b) {
    if (database_id != b->database_id) {
      return database_id > b->database_id ? 1 : -1;
    }
    if (table_id != b->table_id) {
      return table_id > b->table_id ? 1 : -1;
    }
    if (table_version != b->table_version) {
      return table_version > b->table_version ? 1 : -1;
    }
     if (entity_id != b->entity_id) {
      return entity_id > b->entity_id ? 1 : -1;
     }
    if (ts != b->ts) {
      return ts > b->ts ? 1 : -1;
    }
    return lsn > b->lsn ? 1 : -1;
  }
};

enum TsMemSegmentStatus : uint8_t {
  MEM_SEGMENT_INITED = 1,
  MEM_SEGMENT_IMMUTABLE = 2,
  MEM_SEGMENT_FLUSHING = 3,
  MEM_SEGMENT_DELETING = 4,
};

struct TSRowDataComparator {
  typedef TSMemSegRowData* DecodedType;

  static DecodedType decode_key(const char* b) {
    return reinterpret_cast<TSMemSegRowData*>(*reinterpret_cast<char**>(const_cast<char*>(b)));
  }

  int operator()(const char* a, const char* b) const {
    auto a_row = decode_key(a);
    auto b_row = decode_key(b);
    return a_row->Compare(b_row);
  }

  int operator()(const char* a, const DecodedType b) const {
    auto a_row = decode_key(a);
    return a_row->Compare(b);
  }
};

class TsMemSegment {
 private:
  ConcurrentArena arena_;
  TSRowDataComparator comp_;
  InlineSkipList<TSRowDataComparator> skiplist_;
  std::atomic<uint32_t> cur_size_{0};
  std::atomic<uint32_t> intent_row_num_{0};
  std::atomic<uint32_t> written_row_num_{0};
  std::atomic<TsMemSegmentStatus> status_{MEM_SEGMENT_INITED};

 public:
  TsMemSegment() : skiplist_(comp_, &arena_) {}
  ~TsMemSegment() {
  }

  void Traversal(std::function<bool(TSMemSegRowData* row)> func);

  inline void AllocRowNum(uint32_t row_num) {
    intent_row_num_.fetch_add(row_num);
  }

  bool AppendOneRow(const TSMemSegRowData& row);

  bool GetEntityRows(uint32_t db_id, TSTableID table_id, TSEntityID entity_id, std::list<TSMemSegRowData*>* rows);

  bool GetAllEntityRows(std::list<TSMemSegRowData*>* rows);

  inline uint32_t GetMemSegmentSize() {
    return cur_size_.load();
  }

  inline bool SetImm() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_INITED;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_IMMUTABLE);
  }

  inline bool SetFlushing() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_IMMUTABLE;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_IMMUTABLE);
  }

  inline void SetDeleting() {
    status_.store(MEM_SEGMENT_DELETING);
  }
};

class TsMemSegBlockItemInfo : public TsBlockItemInfo {
 private:
  std::shared_ptr<TsMemSegment> mem_seg_;
  std::vector<TSMemSegRowData*> row_data_;
  timestamp64 min_ts_{INVALID_TS};
  timestamp64 max_ts_{INVALID_TS};
  std::unique_ptr<TsRawPayloadRowParser> parser_{nullptr};

 public:
  explicit TsMemSegBlockItemInfo(std::shared_ptr<TsMemSegment> mem_seg) : mem_seg_(mem_seg) {}
  TSEntityID GetEntityId() override {
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
  void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) override {
    *min_ts = min_ts_;
    *max_ts = max_ts_;
  }
  size_t GetRowNum() override {
    return row_data_.size();
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) override;

  // if just get timestamp , this function return fast.
  timestamp64 GetTS(int row_num) override {
    assert(row_data_.size() > row_num);
    return row_data_[row_num]->ts;
  }

  bool InsertRow(TSMemSegRowData* row) {
    bool can_insert = true;
    if (row_data_.size() != 0) {
      auto first = row_data_.front();
      if (first->database_id != row->database_id ||
          first->table_id != row->table_id ||
          first->entity_id != row->entity_id ||
          first->table_version != row->table_version) {
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
  std::deque<std::shared_ptr<TsMemSegment>> segment_;
  std::mutex segment_lock_;

 public:
  explicit TsMemSegmentManager(TsVGroup *vgroup) : vgroup_(vgroup) {}

  ~TsMemSegmentManager() {
    segment_.clear();
  }

  std::shared_ptr<TsMemSegment> GetActiveMemSeg() {
    return segment_.front();
  }

  // WAL CreateCheckPoint call this function to persistent metric datas.
  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments);

  void RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg);

  KStatus PutData(const TSSlice& payload, TSEntityID entity_id);

  bool GetMetricSchema(TSTableID table_id, uint32_t version, std::vector<AttributeInfo>& schema);

  KStatus GetBlockItems(uint32_t db_id, TSTableID table_id, TSEntityID entity_id,
                        std::list<std::shared_ptr<TsBlockItemInfo>>* blocks);
};


}  // namespace kwdbts
















