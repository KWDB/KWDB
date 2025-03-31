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
#include <unordered_map>
#include <atomic>
#include <deque>

#include "ts_env.h"
#include "libkwdbts2.h"
#include "ts_payload.h"
#include "TSLockfreeOrderList.h"

namespace kwdbts {

class TsVGroup;


struct TSMemSegRowData {
  TSTableID table_id;
  uint32_t table_version;
  TSEntityID entity_id;
  TSSlice row_data;
  timestamp64 ts;
  uint64_t seqno;
};


enum TsMemSegmentStatus : uint8_t {
  MEM_SEGMENT_INITED = 1,
  MEM_SEGMENT_IMMUTABLE = 2,
  MEM_SEGMENT_FLUSHING = 3,
  MEM_SEGMENT_DELETING = 4,
};

class TsMemSegment {
 private:
  std::unordered_map<TSTableID, std::map<TSEntityID, TSLockfreeOrderList<timestamp64, TSMemSegRowData*>>> rows_;
  std::atomic<uint32_t> cur_size_{0};
  std::atomic<uint32_t> intent_row_num_{0};
  std::atomic<uint32_t> written_row_num_{0};
  std::atomic<TsMemSegmentStatus> status_{MEM_SEGMENT_INITED};
  std::mutex map_lock_;

 public:
  ~TsMemSegment() {
    assert(status_.load() == MEM_SEGMENT_DELETING);
    for (auto& kv : rows_) {
      for (auto& entity : kv.second) {
        entity.second.Traversal([](timestamp64 ts, TSMemSegRowData* tbl) -> bool {
          free(tbl);
          return true;
        });
      }
    }
  }

  void Traversal(std::function<bool(TSMemSegRowData* row)> func) {
    assert(intent_row_num_.load() == written_row_num_.load());
    bool run_ok = true;
    std::list<TSLockfreeOrderList<timestamp64, TSMemSegRowData*>*> rows_list;
    map_lock_.lock();
    for (auto& kv : rows_) {
      for (auto& entity : kv.second) {
        rows_list.push_back(&(entity.second));
      }
    }
    map_lock_.unlock();
    auto it = rows_list.begin();
    while (it != rows_list.end()) {
      (*it)->Traversal([&](timestamp64 ts, TSMemSegRowData* tbl) -> bool {
          run_ok = func(tbl);
          return run_ok;
        });
      it++;
      // if error exists, no need exec left loop.
      if (!run_ok) {
        break;
      }
    }
  }

  void AllocRowNum(uint32_t row_num) {
    intent_row_num_.fetch_add(row_num);
  }

  bool AddPayload(const TSMemSegRowData& row) {
    size_t malloc_size = sizeof(TSMemSegRowData) + row.row_data.len;
    auto alloc_space = reinterpret_cast<char*>(malloc(malloc_size));
    if (alloc_space != nullptr) {
      TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(alloc_space);
      memcpy(cur_row, &row, sizeof(TSMemSegRowData));
      cur_row->row_data.data = alloc_space + sizeof(TSMemSegRowData);
      cur_row->row_data.len = row.row_data.len;
      memcpy(cur_row->row_data.data, row.row_data.data, row.row_data.len);
      map_lock_.lock();
      TSLockfreeOrderList<timestamp64, TSMemSegRowData*>& row_list = rows_[row.table_id][row.entity_id];
      map_lock_.unlock();
      bool ok = row_list.Insert(cur_row->ts, cur_row);
      if (ok) {
        cur_size_.fetch_add(malloc_size);
        written_row_num_.fetch_add(1);
      } else {
        free(alloc_space);
      }
      return ok;
    }
    return false;
  }

  uint32_t GetMemSegmentSize() {
    return cur_size_.load();
  }

  bool SetImm() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_INITED;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_IMMUTABLE);
  }

  bool SetFlushing() {
    TsMemSegmentStatus tmp = MEM_SEGMENT_IMMUTABLE;
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_IMMUTABLE);
  }

  void SetDeleting() {
    status_.store(MEM_SEGMENT_DELETING);
  }

};

class TsMemSegmentManager {
 private:
  TsVGroup* vgroup_;
  std::deque<std::shared_ptr<TsMemSegment>> segment_;
  std::mutex segment_lock_;

 public:
  TsMemSegmentManager(TsVGroup *vgroup) : vgroup_(vgroup) {}

  ~TsMemSegmentManager() {
    for (auto& seg : segment_) {
      seg->SetDeleting();
    }
    segment_.clear();
  }

  // WAL CreateCheckPoint call this function to persistent metric datas.
  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments);

  void RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg);

  KStatus PutData(const TSSlice& payload, TSEntityID entity_id);

};


}  // namespace kwdbts
















