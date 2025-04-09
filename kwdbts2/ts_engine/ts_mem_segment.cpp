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
#include "ts_instance_params.h"

namespace kwdbts {

// WAL CreateCheckPoint call this function to persistent metric datas.
void TsMemSegmentManager::SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments) {
  segments->reset();
  segment_lock_.lock();
  if (segment_.size() > 0) {
    *segments = segment_.back();
    segment_.push_back(std::make_shared<TsMemSegment>(TsEngineInstanceParams::mem_segment_max_height));
    cur_mem_seg_ = segment_.back();
  }
  segment_lock_.unlock();
  if (segments->get() != nullptr) {
    if (!(*segments)->SetImm()) {
      LOG_ERROR("can not switch mem segment.");
    }
    auto row_num = (*segments)->GetRowNum();
    uint32_t new_heigh = log2(row_num) / 2;
    TsEngineInstanceParams::mem_segment_max_height = new_heigh;
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
  
  auto table_id = TsRawPayload::GetTableIDFromSlice(payload);
  auto table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  std::vector<AttributeInfo> schema;
  if (!GetMetricSchema(table_id, table_version, schema)) {
    LOG_ERROR("GetMetricSchema failed.");
    return KStatus::FAIL;
  }
  TSMemSegRowData row_data(vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id), table_id, table_version, entity_id);
  TsRawPayload pd(payload, schema);
  uint32_t row_num = pd.GetRowCount();
  if (cur_mem_seg_ == 0) {
    segment_lock_.lock();
    segment_.push_back(std::make_shared<TsMemSegment>(TsEngineInstanceParams::mem_segment_max_height));
    cur_mem_seg_ = segment_.back();
    segment_lock_.unlock();
  }
  auto cur_mem_seg = cur_mem_seg_;
  cur_mem_seg->AllocRowNum(row_num);
  for (size_t i = 0; i < row_num; i++) {
    // todo(liangbo01) add lsn of wal.
    // TODO(Yongyan): Somebody needs to update lsn later.
    row_data.SetData(pd.GetTS(i), 1, pd.GetRowData(i));
    bool ret = cur_mem_seg->AppendOneRow(row_data);
    if (!ret) {
      LOG_ERROR("failed to AppendOneRow for table [%lu]", row_data.table_id);
      cur_mem_seg->AllocRowNum(0 - (row_num - i));
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegmentManager::GetBlockItems(const TsBlockITemFilterParams& filter,
                                          std::list<std::shared_ptr<TsBlockSpanInfo>>* blocks) {
  blocks->clear();
  segment_lock_.lock();
  std::list<std::shared_ptr<TsMemSegment>> segments = segment_;
  segment_lock_.unlock();
  std::list<kwdbts::TSMemSegRowData*> row_datas;
  for (auto& mem : segments) {
    bool ok = mem->GetEntityRows(filter, &row_datas);
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

char* TsMemSegBlockItemInfo::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema) {
  assert(!isVarLenType(schema[col_id].type));
  auto col_len = schema[col_id].size;
  auto col_based_len = col_len * row_data_.size();
  char* col_based_mem = reinterpret_cast<char*>(malloc(col_based_len));
  if (col_based_mem == nullptr) {
    return nullptr;
  }
  col_based_mems_.push_back(col_based_mem);
  if (parser_ == nullptr) {
    parser_ = std::make_unique<TsRawPayloadRowParser>(schema);
  }
  TSSlice value;
  char* cur_offset = col_based_mem;
  for (auto& row : row_data_) {
    auto ok = parser_->GetColValueAddr(row->row_data, col_id, &value);
    if (!ok) {
      LOG_ERROR("GetColValueAddr failed.");
      return nullptr;
    }
    memcpy(cur_offset, value.data, value.len);
    cur_offset += value.len;
  }
  return col_based_mem;
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

bool TsMemSegBlockItemInfo::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) {
  assert(row_data_.size() > row_num);
  if (parser_ == nullptr) {
    parser_ = std::make_unique<TsRawPayloadRowParser>(schema);
  }
  return parser_->IsColNull(row_data_[row_num]->row_data, col_id);
}

bool TsMemSegment::AppendOneRow(TSMemSegRowData& row) {
  size_t malloc_size = sizeof(TSMemSegRowData) + row.row_data.len + TSMemSegRowData::GetKeyLen();
  char* buf = skiplist_.AllocateKey(malloc_size);
  if (buf != nullptr) {
    TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(buf + + TSMemSegRowData::GetKeyLen());
    memcpy(cur_row, &row, sizeof(TSMemSegRowData));
    cur_row->row_data.data = buf + sizeof(TSMemSegRowData) + TSMemSegRowData::GetKeyLen();
    cur_row->row_data.len = row.row_data.len;
    memcpy(cur_row->row_data.data, row.row_data.data, row.row_data.len);
    cur_row->GenKey(buf);
    auto ok = skiplist_.InsertConcurrently(buf);
    if (ok) {
      cur_size_.fetch_add(malloc_size);
      written_row_num_.fetch_add(1);
    } else {
      LOG_ERROR("insert failed. duplicated rows.");
    }
    return ok;
  }
  return false;
}

bool TsMemSegment::GetEntityRows(const TsBlockITemFilterParams& filter, std::list<TSMemSegRowData*>* rows) {
  rows->clear();
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  char key[TSMemSegRowData::GetKeyLen() + sizeof(TSMemSegRowData)];
  TSMemSegRowData* begin = new(key +TSMemSegRowData::GetKeyLen()) TSMemSegRowData(filter.db_id, filter.table_id, 0, filter.entity_id);
  begin->SetData(0, 0, {nullptr, 0});
  begin->GenKey(key);
  iter.Seek(reinterpret_cast<char*>(&key));
  while (iter.Valid()) {
    auto cur_row = TSRowDataComparator::decode_key(iter.key());
    assert(cur_row != nullptr);
    if (!cur_row->SameTableId(begin)) {
      break;
    }
    if (cur_row->entity_id == filter.entity_id && cur_row->InTsSpan(filter.start_ts, filter.end_ts)) {
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

void TsMemSegment::Traversal(std::function<bool(TSMemSegRowData* row)> func, bool waiting_done) {
  if (waiting_done) {
    int re_try_times = 0;
    while (intent_row_num_.load() != written_row_num_.load()) {
      if (++re_try_times % 10 == 0)
        LOG_WARN("TsMemSegment intent_row_num_[%u] != written_row_num_[%u], sleep 1ms. times[%d].",
                intent_row_num_.load(), written_row_num_.load(), re_try_times);
      usleep(1000);
    }
  }

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

