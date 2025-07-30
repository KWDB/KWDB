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

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include "kwdb_type.h"
#include "ts_mem_segment_mgr.h"
#include "ts_table_schema_manager.h"
#include "ts_vgroup.h"

namespace kwdbts {

TsMemSegment::TsMemSegment(int32_t height) : skiplist_(comp_, &arena_, height) {}

TsMemSegmentManager::TsMemSegmentManager(TsVGroup* vgroup)
    : vgroup_(vgroup), cur_mem_seg_(TsMemSegment::Create(EngineOptions::mem_segment_max_height)) {
  segment_.push_back(cur_mem_seg_);
}

// WAL CreateCheckPoint call this function to persistent metric datas.
void TsMemSegmentManager::SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments) {
  {
    std::unique_lock lock{segment_lock_};
    if (!cur_mem_seg_->SetImm()) {
      LOG_ERROR("can not switch mem segment.");
    }
    *segments = cur_mem_seg_;
    cur_mem_seg_ = TsMemSegment::Create(EngineOptions::mem_segment_max_height);
    segment_.push_back(cur_mem_seg_);
  }
  auto row_num = (*segments)->GetRowNum();
  uint32_t new_heigh = log2(row_num);
  if (EngineOptions::mem_segment_max_height < new_heigh) {
    EngineOptions::mem_segment_max_height = new_heigh;
  }
}

void TsMemSegmentManager::RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  std::unique_lock lock{segment_lock_};
  segment_.remove(mem_seg);
}

void TsMemSegmentManager::GetAllMemSegments(std::list<std::shared_ptr<TsMemSegment>>* mems) {
  std::shared_lock lock(segment_lock_);
  *mems = segment_;
}

bool TsMemSegmentManager::GetMetricSchemaAndMeta(TSTableID table_id, uint32_t version,
                                                 std::vector<AttributeInfo>& schema, LifeTime* lifetime) {
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
  *lifetime = schema_mgr->GetLifeTime();
  return true;
}

KStatus TsMemSegmentManager::PutData(const TSSlice& payload, TSEntityID entity_id, TS_LSN lsn,
                                     std::list<TSMemSegRowData>* rows) {
  auto table_id = TsRawPayload::GetTableIDFromSlice(payload);
  auto table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  // get column info and life time
  std::vector<AttributeInfo> schema;
  LifeTime life_time{};
  if (!GetMetricSchemaAndMeta(table_id, table_version, schema, &life_time)) {
    LOG_ERROR("GetMetricSchemaAndMeta failed.");
    return KStatus::FAIL;
  }
  // calculate acceptable timestamp with life time
  int64_t acceptable_ts = INT64_MIN;
  if (life_time.ts != 0) {
    auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    acceptable_ts = (now.time_since_epoch().count() - life_time.ts) * life_time.precision;
  }
  TSMemSegRowData row_data(vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id), table_id, table_version,
                           entity_id);
  TsRawPayload pd(payload, schema);
  uint32_t row_num = pd.GetRowCount();
  auto cur_mem_seg = CurrentMemSegment();
  cur_mem_seg->AllocRowNum(row_num);
  for (size_t i = 0; i < row_num; i++) {
    auto row_ts = pd.GetTS(i);
    if (row_ts < acceptable_ts) {
      // TODO(qinlipeng): add reject row_num
      cur_mem_seg->AllocRowNum(-1);
      continue;
    }
    // TODO(Yongyan): Somebody needs to update lsn later.
    row_data.SetData(row_ts, lsn, pd.GetRowData(i));
    bool ret = cur_mem_seg->AppendOneRow(row_data);
    if (!ret) {
      LOG_ERROR("failed to AppendOneRow for table [%lu]", row_data.table_id);
      cur_mem_seg->AllocRowNum(0 - (row_num - i));
      return KStatus::FAIL;
    }
    if (rows != nullptr) {
      rows->push_back(row_data);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegmentManager::GetBlockSpans(const TsBlockItemFilterParams& filter,
                                           std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                           std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                           uint32_t scan_version) {
  std::list<std::shared_ptr<TsMemSegment>> segments;
  {
    std::shared_lock lock(segment_lock_);
    segments = segment_;
  }
  std::list<kwdbts::TSMemSegRowData*> row_datas;
  std::list<std::shared_ptr<TsMemSegBlock>> mem_block;
  for (auto& mem : segments) {
    bool ok = mem->GetEntityRows(filter, &row_datas);
    if (!ok) {
      LOG_ERROR("GetBlockSpans failed in GetEntityRows.");
      return KStatus::FAIL;
    }
    if (row_datas.size() == 0) {
      continue;
    }
    std::shared_ptr<TsMemSegBlock> cur_blk_item = nullptr;
    if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
      TSMemSegRowData* last_row_data = nullptr;
      for (auto& row : row_datas) {
        if (last_row_data == nullptr || last_row_data->SameEntityAndTs(row)) {
          last_row_data = row;
          continue;
        }
        if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(last_row_data)) {
          cur_blk_item = std::make_shared<TsMemSegBlock>(mem);
          mem_block.push_back(cur_blk_item);
          cur_blk_item->InsertRow(last_row_data);
        }
        last_row_data = row;
      }
      if (last_row_data != nullptr) {
        if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(last_row_data)) {
          cur_blk_item = std::make_shared<TsMemSegBlock>(mem);
          mem_block.push_back(cur_blk_item);
          cur_blk_item->InsertRow(last_row_data);
        }
      }
    } else if (EngineOptions::g_dedup_rule == DedupRule::DISCARD) {
      TSMemSegRowData* last_row_data = nullptr;
      for (auto& row : row_datas) {
        if (last_row_data == nullptr || !last_row_data->SameEntityAndTs(row)) {
          if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(row)) {
            cur_blk_item = std::make_shared<TsMemSegBlock>(mem);
            mem_block.push_back(cur_blk_item);
            cur_blk_item->InsertRow(row);
          }
          last_row_data = row;
        }
      }
    } else {
      for (auto& row : row_datas) {
        if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(row)) {
          cur_blk_item = std::make_shared<TsMemSegBlock>(mem);
          mem_block.push_back(cur_blk_item);
          cur_blk_item->InsertRow(row);
        }
      }
    }
  }
  for (auto& mem_blk : mem_block) {
    uint32_t vgroup_id = vgroup_ ? vgroup_->GetVGroupID() : 0;
    block_spans.push_back(make_shared<TsBlockSpan>(vgroup_id, mem_blk->GetEntityId(), mem_blk, 0, mem_blk->GetRowNum(),
                                                   tbl_schema_mgr, scan_version));
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlock::GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap) {
  auto iter = col_bitmaps_.find(col_id);
  if (iter != col_bitmaps_.end()) {
    bitmap = iter->second;
    return KStatus::SUCCESS;
  }
  bitmap.SetCount(row_data_.size());
  for (int i = 0; i < row_data_.size(); i++) {
    auto row = row_data_[i];
    if (parser_->IsColNull(row->row_data, col_id)) {
      bitmap[i] = DataFlags::kNull;
    }
  }
  col_bitmaps_[col_id] = bitmap;
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value) {
  assert(!isVarLenType(schema[col_id].type));
  auto iter = col_based_mems_.find(col_id);
  if (iter != col_based_mems_.end() && iter->second != nullptr) {
    *value = iter->second;
    return KStatus::SUCCESS;
  }
  auto col_len = schema[col_id].size;
  auto col_based_len = col_len * row_data_.size();
  char* col_based_mem = reinterpret_cast<char*>(malloc(col_based_len));
  if (col_based_mem == nullptr) {
    LOG_ERROR("malloc memroy failed.");
    return KStatus::FAIL;
  }
  col_based_mems_[col_id] = col_based_mem;
  if (parser_ == nullptr) {
    parser_ = std::make_unique<TsRawPayloadRowParser>(schema);
  }
  TSSlice value_slice;
  char* cur_offset = col_based_mem;
  for (int i = 0; i < row_data_.size(); i++) {
    auto row = row_data_[i];
    if (!parser_->IsColNull(row->row_data, col_id)) {
      auto ok = parser_->GetColValueAddr(row->row_data, col_id, &value_slice);
      if (!ok) {
        LOG_ERROR("GetColValueAddr failed.");
        return KStatus::FAIL;
      }
      assert(col_len == value_slice.len);
      memcpy(cur_offset, value_slice.data, col_len);
    }
    cur_offset += col_len;
  }
  *value = col_based_mem;
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema,
                                     TSSlice& value) {
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

bool TsMemSegBlock::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) {
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
    TSMemSegRowData* cur_row = reinterpret_cast<TSMemSegRowData*>(buf + +TSMemSegRowData::GetKeyLen());
    memcpy(cur_row, &row, sizeof(TSMemSegRowData));
    cur_row->row_data.data = buf + sizeof(TSMemSegRowData) + TSMemSegRowData::GetKeyLen();
    cur_row->row_data.len = row.row_data.len;
    cur_row->row_idx_in_mem_seg = row_idx_.fetch_add(1);
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

bool TsMemSegment::HasEntityRows(const TsScanFilterParams& filter) {
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  char key[TSMemSegRowData::GetKeyLen() + sizeof(TSMemSegRowData)];
  uint32_t cur_version = 1;
  while (true) {
    TSMemSegRowData* begin = new (key + TSMemSegRowData::GetKeyLen())
        TSMemSegRowData(filter.db_id, filter.table_id, cur_version, filter.entity_id);
    begin->SetData(INT64_MIN, 0, {nullptr, 0});
    begin->GenKey(key);
    iter.Seek(reinterpret_cast<char*>(&key));
    bool scan_over = false;
    while (iter.Valid()) {
      auto cur_row = TSRowDataComparator::decode_key(iter.key());
      assert(cur_row != nullptr);
      if (!cur_row->SameTableId(begin)) {
        scan_over = true;
        break;
      }
      if (cur_row->entity_id > filter.entity_id) {
        cur_version = cur_row->table_version + 1;
        break;
      }
      if (cur_row->entity_id < filter.entity_id) {
        cur_version = cur_row->table_version;
        break;
      }
      if (CheckIfTsInSpan(cur_row->ts, filter.ts_spans_)) {
        return true;
      }
      iter.Next();
    }
    if (scan_over || !iter.Valid()) {
      break;
    }
  }
  return false;
}

bool TsMemSegment::GetEntityRows(const TsBlockItemFilterParams& filter, std::list<TSMemSegRowData*>* rows) {
  rows->clear();
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  char key[TSMemSegRowData::GetKeyLen() + sizeof(TSMemSegRowData)];
  uint32_t cur_version = 1;
  while (true) {
    TSMemSegRowData* begin = new (key + TSMemSegRowData::GetKeyLen())
        TSMemSegRowData(filter.db_id, filter.table_id, cur_version, filter.entity_id);
    begin->SetData(INT64_MIN, 0, {nullptr, 0});
    begin->GenKey(key);
    iter.Seek(reinterpret_cast<char*>(&key));
    bool scan_over = false;
    while (iter.Valid()) {
      auto cur_row = TSRowDataComparator::decode_key(iter.key());
      assert(cur_row != nullptr);
      if (!cur_row->SameTableId(begin)) {
        scan_over = true;
        break;
      }
      if (cur_row->entity_id > filter.entity_id) {
        cur_version = cur_row->table_version + 1;
        break;
      }
      if (cur_row->entity_id < filter.entity_id) {
        cur_version = cur_row->table_version;
        break;
      }
      if (IsTsLsnInSpans(cur_row->ts, cur_row->lsn, filter.spans_)) {
        rows->push_back(cur_row);
      }
      iter.Next();
    }
    if (scan_over || !iter.Valid()) {
      break;
    }
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

KStatus TsMemSegment::GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& blocks, TsEngineSchemaManager* schema_mgr) {
  int re_try_times = 0;
  while (intent_row_num_.load() != written_row_num_.load()) {
    if (++re_try_times % 10 == 0)
      LOG_WARN("TsMemSegment intent_row_num_[%u] != written_row_num_[%u], sleep 1ms. times[%d].",
               intent_row_num_.load(), written_row_num_.load(), re_try_times);
    usleep(1000);
  }
  InlineSkipList<TSRowDataComparator>::Iterator iter(&skiplist_);
  iter.SeekToFirst();

  std::vector<std::unique_ptr<TsMemSegBlock>> mem_blocks;
  TsMemSegBlock* current_memblock = nullptr;

  auto self = shared_from_this();
  if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
    TSMemSegRowData* last_row_data = nullptr;
    for (; iter.Valid(); iter.Next()) {
      TSMemSegRowData* cur_row = TSRowDataComparator::decode_key(iter.key());
      assert(cur_row != nullptr);
      if (last_row_data == nullptr || last_row_data->SameEntityAndTs(cur_row)) {
        last_row_data = cur_row;
        continue;
      }
      if (current_memblock == nullptr || !current_memblock->InsertRow(last_row_data)) {
        auto p = std::make_unique<TsMemSegBlock>(self);
        current_memblock = p.get();
        mem_blocks.push_back(std::move(p));
        current_memblock->InsertRow(last_row_data);
      }
      last_row_data = cur_row;
    }
    if (last_row_data != nullptr) {
      if (current_memblock == nullptr || !current_memblock->InsertRow(last_row_data)) {
        auto p = std::make_unique<TsMemSegBlock>(self);
        current_memblock = p.get();
        mem_blocks.push_back(std::move(p));
        current_memblock->InsertRow(last_row_data);
      }
    }
  } else if (EngineOptions::g_dedup_rule == DedupRule::DISCARD) {
    TSMemSegRowData* last_row_data = nullptr;
    for (; iter.Valid(); iter.Next()) {
      TSMemSegRowData* cur_row = TSRowDataComparator::decode_key(iter.key());
      assert(cur_row != nullptr);
      if (last_row_data == nullptr || !last_row_data->SameEntityAndTs(cur_row)) {
        if (current_memblock == nullptr || !current_memblock->InsertRow(cur_row)) {
          auto p = std::make_unique<TsMemSegBlock>(self);
          current_memblock = p.get();
          mem_blocks.push_back(std::move(p));
          current_memblock->InsertRow(cur_row);
        }
        last_row_data = cur_row;
      }
    }
  } else {  // KEEP
    for (; iter.Valid(); iter.Next()) {
      TSMemSegRowData* cur_row = TSRowDataComparator::decode_key(iter.key());
      assert(cur_row != nullptr);
      if (current_memblock == nullptr || !current_memblock->InsertRow(cur_row)) {
        auto p = std::make_unique<TsMemSegBlock>(self);
        current_memblock = p.get();
        mem_blocks.push_back(std::move(p));
        current_memblock->InsertRow(cur_row);
      }
    }
  }

  for (auto& mem_blk : mem_blocks) {
    auto table_id = mem_blk->GetTableId();
    std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
    auto s = schema_mgr->GetTableSchemaMgr(table_id, table_schema_mgr);
    if (s == FAIL) {
      LOG_ERROR("can not get table schema manager for table_id[%lu].", table_id);
      return s;
    }
    blocks.push_back(std::make_shared<TsBlockSpan>(mem_blk->GetEntityId(), std::move(mem_blk), 0, mem_blk->GetRowNum(),
                                                   table_schema_mgr, 0));
  }
  return SUCCESS;
}

KStatus TsMemSegment::GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                                    std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr, uint32_t scan_version) {
  std::list<kwdbts::TSMemSegRowData*> row_datas;
  bool ok = GetEntityRows(filter, &row_datas);
  if (!ok) {
    LOG_ERROR("GetBlockSpans failed in GetEntityRows.");
    return KStatus::FAIL;
  }
  std::list<std::shared_ptr<TsMemSegBlock>> mem_blocks;
  std::shared_ptr<TsMemSegBlock> cur_blk_item = nullptr;
  auto self = shared_from_this();
  if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
    TSMemSegRowData* last_row_data = nullptr;
    for (auto& row : row_datas) {
      if (last_row_data == nullptr || last_row_data->SameEntityAndTs(row)) {
        last_row_data = row;
        continue;
      }
      if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(last_row_data)) {
        cur_blk_item = std::make_shared<TsMemSegBlock>(self);
        mem_blocks.push_back(cur_blk_item);
        cur_blk_item->InsertRow(last_row_data);
      }
      last_row_data = row;
    }
    if (last_row_data != nullptr) {
      if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(last_row_data)) {
        cur_blk_item = std::make_shared<TsMemSegBlock>(self);
        mem_blocks.push_back(cur_blk_item);
        cur_blk_item->InsertRow(last_row_data);
      }
    }
  } else if (EngineOptions::g_dedup_rule == DedupRule::DISCARD) {
    TSMemSegRowData* last_row_data = nullptr;
    for (auto& row : row_datas) {
      if (last_row_data == nullptr || !last_row_data->SameEntityAndTs(row)) {
        if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(row)) {
          cur_blk_item = std::make_shared<TsMemSegBlock>(self);
          mem_blocks.push_back(cur_blk_item);
          cur_blk_item->InsertRow(row);
        }
        last_row_data = row;
      }
    }
  } else {
    for (auto& row : row_datas) {
      if (cur_blk_item == nullptr || !cur_blk_item->InsertRow(row)) {
        cur_blk_item = std::make_shared<TsMemSegBlock>(self);
        mem_blocks.push_back(cur_blk_item);
        cur_blk_item->InsertRow(row);
      }
    }
  }
  for (auto& mem_blk : mem_blocks) {
    blocks.push_back(make_shared<TsBlockSpan>(filter.vgroup_id, mem_blk->GetEntityId(), mem_blk, 0,
                                              mem_blk->GetRowNum(), tbl_schema_mgr, scan_version));
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
