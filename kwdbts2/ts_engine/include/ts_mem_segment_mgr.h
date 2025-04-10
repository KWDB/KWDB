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
#pragma pack(1)
struct TSMemSegRowData {
  uint32_t database_id;
  TSTableID table_id;
  uint32_t table_version;
  TSEntityID entity_id;
  timestamp64 ts;
  TS_LSN lsn;
  TSSlice row_data;

 private:
  char* skip_list_key_;

 public:
  TSMemSegRowData(uint32_t db_id, TSTableID tbl_id, uint32_t tbl_version, TSEntityID en_id) {
    database_id = db_id;
    table_id = tbl_id;
    table_version = tbl_version;
    entity_id = en_id;
  }
  void SetData(timestamp64 cts, TS_LSN clsn, const TSSlice& crow_data) {
    ts = cts;
    lsn = clsn;
    row_data = crow_data;
  }
  static size_t GetKeyLen() {
    return 4 + 8 + 4 + 8 + 8 + 8 + 8;
  }

#define HTOBEFUNC(buf, value, size) { \
  auto tmp = value; \
  memcpy(buf, &tmp, size); \
  buf += size; \
}

  void GenKey(char* buf) {
    skip_list_key_ = buf;
    HTOBEFUNC(buf, htobe32(database_id), sizeof(database_id));
    HTOBEFUNC(buf, htobe64(table_id), sizeof(table_id));
    HTOBEFUNC(buf, htobe32(table_version), sizeof(table_version));
    HTOBEFUNC(buf, htobe64(entity_id), sizeof(entity_id));
    uint64_t cts = ts - INT64_MIN;
    HTOBEFUNC(buf, htobe64(cts), sizeof(cts));
    HTOBEFUNC(buf, htobe64(lsn), sizeof(lsn));
    HTOBEFUNC(buf, htobe64(*reinterpret_cast<uint64_t*>(&row_data.data)), sizeof(uint64_t));
  }

  bool SameEntityAndTableVersion(TSMemSegRowData* b) {
    return memcmp(this, b, 24) == 0;
  }
  bool SameTableId(TSMemSegRowData* b) {
    return this->table_id == b->table_id;
  }

  int Compare(TSMemSegRowData* b) {
    auto ret = memcmp(skip_list_key_, b->skip_list_key_, GetKeyLen());
    // auto ret_1 = 0;
    // while (true) {
    //   if (database_id != b->database_id) {
    //     ret_1 = database_id > b->database_id ? 1 : -1;
    //     break;
    //   }
    //   if (table_id != b->table_id) {
    //     ret_1 = table_id > b->table_id ? 1 : -1;
    //     break;
    //   }
    //   if (table_version != b->table_version) {
    //     ret_1 = table_version> b->table_version ? 1 : -1;
    //      break;
    //   }
    //   if (entity_id != b->entity_id) {
    //     ret_1 = entity_id > b->entity_id ? 1 : -1;
    //     break;
    //   }
    //   if (ts != b->ts) {
    //     ret_1 = ts > b->ts ? 1 : -1;
    //     break;
    //   }
    //   if (lsn != b->lsn)
    //     ret_1 = lsn > b->lsn ? 1 : -1;
    //   break;
    // }
    // assert(ret * ret_1 >= 0);
    return ret;
  }
};
#pragma pack()

enum TsMemSegmentStatus : uint8_t {
  MEM_SEGMENT_INITED = 1,
  MEM_SEGMENT_IMMUTABLE = 2,
  MEM_SEGMENT_FLUSHING = 3,
  MEM_SEGMENT_DELETING = 4,
};

struct TSRowDataComparator {
  typedef TSMemSegRowData* DecodedType;

  static DecodedType decode_key(const char* b) {
    return reinterpret_cast<TSMemSegRowData*>(const_cast<char*>(b + TSMemSegRowData::GetKeyLen()));
  }

  int operator()(const char* a, const char* b) const {
    return memcmp(a, b, TSMemSegRowData::GetKeyLen());
    // auto a_row = decode_key(a);
    // auto b_row = decode_key(b);
    // return a_row->Compare(b_row);
  }

  int operator()(const char* a, const DecodedType b) const {
    return memcmp(a, reinterpret_cast<const char*>(b) - TSMemSegRowData::GetKeyLen(), TSMemSegRowData::GetKeyLen());
    // auto a_row = decode_key(a);
    // return a_row->Compare(b);
  }
};

class TsMemSegment {
 private:
  std::atomic<uint32_t> cur_size_{0};
  std::atomic<uint32_t> intent_row_num_{0};
  std::atomic<uint32_t> written_row_num_{0};
  std::atomic<TsMemSegmentStatus> status_{MEM_SEGMENT_INITED};
  ConcurrentArena arena_;
  TSRowDataComparator comp_;
  InlineSkipList<TSRowDataComparator> skiplist_;

 public:
  explicit TsMemSegment(int32_t max_height) : skiplist_(comp_, &arena_, max_height) {}
  ~TsMemSegment() {
  }

  void Traversal(std::function<bool(TSMemSegRowData* row)> func, bool waiting_done = false);

  size_t Size() {
    return arena_.MemoryAllocatedBytes();
  }

  uint32_t GetRowNum() {
    return intent_row_num_.load();
  }

  inline void AllocRowNum(uint32_t row_num) {
    intent_row_num_.fetch_add(row_num);
  }

  bool AppendOneRow(TSMemSegRowData& row);

  bool GetEntityRows(const TsBlockITemFilterParams& filter, std::list<TSMemSegRowData*>* rows);

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
    return status_.compare_exchange_strong(tmp, MEM_SEGMENT_FLUSHING);
  }

  inline void SetDeleting() {
    status_.store(MEM_SEGMENT_DELETING);
  }
};

class TsMemSegBlockItemInfo : public TsBlockSpanInfo {
 private:
  std::shared_ptr<TsMemSegment> mem_seg_;
  std::vector<TSMemSegRowData*> row_data_;
  timestamp64 min_ts_{INVALID_TS};
  timestamp64 max_ts_{INVALID_TS};
  std::unique_ptr<TsRawPayloadRowParser> parser_{nullptr};
  std::list<char*> col_based_mems_;

 public:
  explicit TsMemSegBlockItemInfo(std::shared_ptr<TsMemSegment> mem_seg) : mem_seg_(mem_seg) {}

  ~TsMemSegBlockItemInfo() {
    for (auto& mem : col_based_mems_) {
      free(mem);
    }
    col_based_mems_.clear();
  }

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
  inline bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) override;

  // if just get timestamp , this function return fast.
  timestamp64 GetTS(int row_num) override {
    assert(row_data_.size() > row_num);
    return row_data_[row_num]->ts;
  }

  char* GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema) override;

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
  std::mutex segment_lock_;

 public:
  explicit TsMemSegmentManager(TsVGroup *vgroup) : vgroup_(vgroup) {}

  ~TsMemSegmentManager() {
    segment_.clear();
  }

  // WAL CreateCheckPoint call this function to persistent metric datas.
  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments);

  void RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg);

  KStatus PutData(const TSSlice& payload, TSEntityID entity_id_);

  bool GetMetricSchema(TSTableID table_id_, uint32_t version, std::vector<AttributeInfo>& schema);

  KStatus GetBlockItems(const TsBlockITemFilterParams& filter, std::list<std::shared_ptr<TsBlockSpanInfo>>* blocks);
};

}  // namespace kwdbts
















