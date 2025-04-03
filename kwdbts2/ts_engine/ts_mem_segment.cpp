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

#include "ts_mem_segment_mgr.h"
#include "ts_vgroup.h"

namespace kwdbts {

// WAL CreateCheckPoint call this function to persistent metric datas.
void TsMemSegmentManager::SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments) {
  segments->reset();
  segment_lock_.lock();
  if (segment_.size() > 0) {
    *segments = segment_.front();
    segment_.push_back(std::make_shared<TsMemSegment>());
  }
  segment_lock_.unlock();
  if (segments->get() != nullptr) {
    if (!(*segments)->SetImm()) {
      LOG_ERROR("can not switch mem segment.");
    }
  }
}

void TsMemSegmentManager::RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  segment_lock_.lock();
  bool found_seg = false;
  // remove deleted mem segments.
  while (segment_.size() > 0) {
    std::shared_ptr<TsMemSegment>& cur_seg = segment_.front();
    if (cur_seg == nullptr) {
      segment_.pop_front();
    } else if (cur_seg.get() == mem_seg.get()) {
      found_seg = true;
      segment_.pop_front();
    } else {
      break;
    }
  }
  if (!found_seg) {
    auto it = segment_.begin();
    while (it != segment_.end()) {
      if (it->get() == mem_seg.get()) {
        it->reset();
        break;
      }
      it++;
    }
  }
  segment_lock_.unlock();
}

bool TsMemSegmentManager::GetMetricSchema(TSTableID table_id, uint32_t version, std::vector<AttributeInfo>& schema) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  auto s = vgroup_->GetEngineSchemaMgr()->GetTableSchemaMgr(table_id, schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] schema manager.", table_id);
    return false;
  }
  s = schema_mgr->GetColumnsExcludeDropped(schema, version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] with version[%u].", table_id, version);
    return false;
  }
  return true;
}

KStatus TsMemSegmentManager::PutData(const TSSlice& payload, TSEntityID entity_id) {
  TSMemSegRowData row_data;
  row_data.table_id = TsRawPayload::GetTableIDFromSlice(payload);
  row_data.table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  row_data.entity_id = entity_id;
  std::vector<AttributeInfo> schema;
  if (!GetMetricSchema(row_data.table_id, row_data.table_version, schema)) {
    LOG_ERROR("GetMetricSchema failed.");
    return KStatus::FAIL;
  }

  TsRawPayload pd(payload, schema);
  uint32_t row_num = pd.GetRowCount();
  segment_lock_.lock();
  if (segment_.size() == 0) {
    segment_.push_front(std::make_shared<TsMemSegment>());
  }
  std::shared_ptr<TsMemSegment> cur_mem_seg = segment_.front();
  cur_mem_seg->AllocRowNum(row_num);
  segment_lock_.unlock();
  for (size_t i = 0; i < row_num; i++) {
    row_data.row_data = pd.GetRowData(i);
    row_data.ts = pd.GetTS(i);
    bool ret = cur_mem_seg->AppendOneRow(row_data);
    if (!ret) {
      LOG_ERROR("failed to AppendOneRow for table [%lu]", row_data.table_id);
      cur_mem_seg->AllocRowNum(0 - (row_num - i));
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegmentManager::GetBlockItems(uint32_t db_id, TSTableID table_id, TSEntityID entity_id,
                                          std::list<std::shared_ptr<TsBlockItemInfo>>* blocks) {
  blocks->clear();
  segment_lock_.lock();
  std::deque<std::shared_ptr<TsMemSegment>> segments = segment_;
  segment_lock_.unlock();
  std::list<kwdbts::TSMemSegRowData*> row_datas;
  for (auto& mem : segments) {
    bool ok = mem->GetEntityRows(db_id, table_id, entity_id, &row_datas);
    if (!ok) {
      LOG_ERROR("GetBlockItems failed in GetEntityRows.");
      return KStatus::FAIL;
    }
    if (row_datas.size() == 0) {
      continue;
    }
    std::shared_ptr<TsMemSegBlockItemInfo> cur_blk_item = nullptr;
    for (auto row : row_datas) {
      if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(row)) {
        cur_blk_item = std::make_shared<TsMemSegBlockItemInfo>(mem);
        blocks->push_back(cur_blk_item);
        cur_blk_item->InsertRow(row);
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlockItemInfo::GetValueSlice(int row_num, int col_id,
  const std::vector<AttributeInfo>& schema, TSSlice& value) {
  assert(row_data_.size() > row_num);
  if (parser_ == nullptr) {
    parser_ = std::make_unique<TsRawPayloadRowParser>(schema);
  }
  auto ok = parser_->GetColValueAddr(row_data_[row_num]->row_data, col_id, &value);
  if (!ok) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

bool TsMemSegment::AppendOneRow(const TSMemSegRowData& row) {
  size_t malloc_size = sizeof(TSMemSegRowData) + row.row_data.len;
  auto alloc_space = arena_.AllocateAligned(malloc_size);
  if (alloc_space != nullptr) {
    TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(alloc_space);
    memcpy(cur_row, &row, sizeof(TSMemSegRowData));
    cur_row->row_data.data = alloc_space + sizeof(TSMemSegRowData);
    cur_row->row_data.len = row.row_data.len;
    memcpy(cur_row->row_data.data, row.row_data.data, row.row_data.len);

    char* buf = skiplist_.AllocateKey(sizeof(cur_row));
    if (buf != nullptr) {
      memcpy(buf, &cur_row, sizeof(cur_row));
      auto ok = skiplist_.InsertConcurrently(buf);
      if (ok) {
        cur_size_.fetch_add(malloc_size);
        written_row_num_.fetch_add(1);
      } else {
        LOG_ERROR("insert failed. duplicated rows.");
      }
      return ok;
    }
  }
  return false;
}

bool TsMemSegment::GetEntityRows(uint32_t db_id, TSTableID table_id,
                                  TSEntityID entity_id, std::list<TSMemSegRowData*>* rows) {
  rows->clear();
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  TSMemSegRowData begin;
  begin.database_id = db_id;
  begin.table_id = table_id;
  begin.entity_id = entity_id;
  begin.lsn = 0;
  begin.table_version = 0;
  begin.ts = 0;
  void* ptr = &begin;
  iter.Seek(reinterpret_cast<char*>(&ptr));
  while (iter.Valid()) {
    auto cur_row = TSRowDataComparator::decode_key(iter.key());
    assert(cur_row != nullptr);
    if (cur_row->table_id != table_id) {
      break;
    }
    if (cur_row->entity_id == entity_id) {
      rows->push_back(cur_row);
    }
    iter.Next();
  }
  return true;
}

bool TsMemSegment::GetAllEntityRows(std::list<TSMemSegRowData*>* rows) {
  rows->clear();
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  iter.SeekToFirst();
  while (iter.Valid()) {
    auto cur_row = TSRowDataComparator::decode_key(iter.key());
    assert(cur_row != nullptr);
    rows->push_back(cur_row);
    iter.Next();
  }
  return true;
}

void TsMemSegment::Traversal(std::function<bool(TSMemSegRowData* row)> func) {
  assert(intent_row_num_.load() == written_row_num_.load());
  bool run_ok = true;
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  iter.SeekToFirst();
  while (iter.Valid()) {
    auto cur_row = TSRowDataComparator::decode_key(iter.key());
    assert(cur_row != nullptr);
    auto ok = func(cur_row);
    if (!ok) {
      // exec failed. no need run left.
      return;
    }
    iter.Next();
  }
}


}  //  namespace kwdbts

