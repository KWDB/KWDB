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

#include <atomic>
#include <cstdint>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "ts_flush_manager.h"
#include "ts_mem_segment_mgr.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"
#include "ts_vgroup.h"

namespace kwdbts {

TsMemSegment::TsMemSegment(int32_t height) : skiplist_(height) {}

TsMemSegmentManager::TsMemSegmentManager(TsVGroup* vgroup, TsVersionManager* version_manager)
    : vgroup_(vgroup),
      version_manager_(version_manager),
      cur_mem_seg_(TsMemSegment::Create(EngineOptions::mem_segment_max_height)) {
  segment_.push_back(cur_mem_seg_);
}

// WAL CreateCheckPoint call this function to persistent metric datas.
// std::shared_ptr<TsMemSegment> TsMemSegmentManager::SwitchMemSegment() {
//   TsVersionUpdate update;
//   std::shared_ptr<TsMemSegment> ret;
//   {
//     std::unique_lock lock{segment_lock_};
//     ret = cur_mem_seg_;
//     cur_mem_seg_ = TsMemSegment::Create(EngineOptions::mem_segment_max_height);
//     update.AddMemSegment(cur_mem_seg_);
//     segment_.push_back(cur_mem_seg_);
//   }
//   auto row_num = ret->GetRowNum();
//   uint32_t new_heigh = log2(row_num);
//   if (EngineOptions::mem_segment_max_height < new_heigh) {
//     EngineOptions::mem_segment_max_height = new_heigh;
//   }

//   version_manager_->ApplyUpdate(&update);
//   return ret;
// }

bool TsMemSegmentManager::SwitchMemSegment(TsMemSegment* expected_old_mem_seg) {
  {
    std::shared_lock lock(segment_lock_);
    if (cur_mem_seg_.get() != expected_old_mem_seg) {
      return false;
    }
  }
  std::unique_lock lock{segment_lock_};
  if (cur_mem_seg_.get() != expected_old_mem_seg) {
    return false;
  }

  auto row_num = cur_mem_seg_->GetRowNum();
  cur_mem_seg_ = TsMemSegment::Create(EngineOptions::mem_segment_max_height);

  TsVersionUpdate update;
  update.AddMemSegment(cur_mem_seg_);
  segment_.push_back(cur_mem_seg_);
  uint32_t new_heigh = log2(row_num);
  if (EngineOptions::mem_segment_max_height < new_heigh) {
    EngineOptions::mem_segment_max_height = new_heigh;
  }

  version_manager_->ApplyUpdate(&update);
  return true;
}

void TsMemSegmentManager::RemoveMemSegment(const std::shared_ptr<TsMemSegment>& mem_seg) {
  std::unique_lock lock{segment_lock_};
  segment_.remove(mem_seg);
}

bool TsMemSegmentManager::GetMetricSchemaAndMeta(TSTableID table_id, uint32_t version,
                                                 const std::vector<AttributeInfo>** schema, DATATYPE* ts_type,
                                                 LifeTime* lifetime) {
  std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  auto s = vgroup_->GetEngineSchemaMgr()->GetTableSchemaMgr(table_id, schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] schema manager.", table_id);
    return false;
  }
  s = schema_mgr->GetColumnsExcludeDroppedPtr(schema, version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] with version[%u].", table_id, version);
    return false;
  }
  *lifetime = schema_mgr->GetLifeTime();
  *ts_type = schema_mgr->GetTsColDataType();
  return true;
}

KStatus TsMemSegmentManager::PutData(const TSSlice& payload, TSEntityID entity_id, TS_LSN lsn) {
  // first of all, check BG flush job status
  auto bg_status = TsFlushJobPool::GetInstance().GetBackGroundStatus();
  if (bg_status == FAIL) {
    return FAIL;
  }


  auto table_id = TsRawPayload::GetTableIDFromSlice(payload);
  auto table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  // get column info and life time
  const std::vector<AttributeInfo>* schema{nullptr};
  LifeTime life_time{};
  DATATYPE ts_type;
  if (!GetMetricSchemaAndMeta(table_id, table_version, &schema, &ts_type, &life_time)) {
    LOG_ERROR("GetMetricSchemaAndMeta failed.");
    return KStatus::FAIL;
  }
  // calculate acceptable timestamp with life time
  int64_t acceptable_ts = INT64_MIN;
  if (life_time.ts != 0) {
    auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    acceptable_ts = (now.time_since_epoch().count() - life_time.ts) * life_time.precision;
  }

  uint32_t db_id = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(table_id);
  TSMemSegRowData row_data(db_id, table_id, table_version, entity_id);
  TsRawPayload pd(payload, schema);
  uint32_t row_num = pd.GetRowCount();

  auto cur_mem_seg = CurrentMemSegmentAndAllocateRow(row_num);
  timestamp64 max_ts = INT64_MIN;
  for (size_t i = 0; i < row_num; i++) {
    auto row_ts = pd.GetTS(i);
    if (row_ts < acceptable_ts) {
      // TODO(qinlipeng): add reject row_num
      cur_mem_seg->AllocRowNum(-1);
      continue;
    }
    auto p_time = convertTsToPTime(row_ts, ts_type);
    version_manager_->AddPartition(db_id, p_time);

    auto payload_row_data = pd.GetRowData(i);
    row_data.SetData(row_ts, lsn, payload_row_data);
    bool ret = cur_mem_seg->AppendOneRow(row_data);
    if (!ret) {
      LOG_ERROR("failed to AppendOneRow for table [%lu]", row_data.table_id);
      cur_mem_seg->AllocRowNum(0 - (row_num - i));
      return KStatus::FAIL;
    }

    if (row_ts > max_ts) {
      max_ts = row_ts;
    }
  }
  vgroup_->UpdateEntityAndMaxTs(table_id, max_ts, entity_id);
  vgroup_->UpdateEntityLatestRow(entity_id, max_ts);

  if (cur_mem_seg->GetPayloadMemUsage() > EngineOptions::mem_segment_max_size) {
    if (this->SwitchMemSegment(cur_mem_seg.get())) {
      TsFlushJobPool::GetInstance().AddFlushJob(vgroup_, cur_mem_seg);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsMemSegBlock::GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>* schema, TsBitmap& bitmap) {
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

KStatus TsMemSegBlock::GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>* schema, char** value) {
  assert(!isVarLenType((*schema)[col_id].type));
  auto iter = col_based_mems_.find(col_id);
  if (iter != col_based_mems_.end() && iter->second != nullptr) {
    *value = iter->second;
    return KStatus::SUCCESS;
  }
  auto col_len = (*schema)[col_id].size;
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

inline KStatus TsMemSegBlock::GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>* schema,
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

inline bool TsMemSegBlock::IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>* schema) {
  assert(row_data_.size() > row_num);
  if (parser_ == nullptr) {
    parser_ = std::make_unique<TsRawPayloadRowParser>(schema);
  }
  return parser_->IsColNull(row_data_[row_num]->row_data, col_id);
}

bool TsMemSegment::AppendOneRow(TSMemSegRowData& row) {
  auto ok = skiplist_.InsertRowData(row, row_idx_.fetch_add(1));
  if (ok) {
    written_row_num_.fetch_add(1);
    payload_mem_usage_.fetch_add(row.row_data.len, std::memory_order_relaxed);
  } else {
    LOG_ERROR("insert failed. duplicated rows.");
  }
  return  ok;
}

bool TsMemSegment::HasEntityRows(const TsScanFilterParams& filter) {
  SkiplistIterator iter(&skiplist_);
  char key[TSMemSegRowData::GetKeyLen() + sizeof(TSMemSegRowData)];
  uint32_t cur_version = 1;
  while (true) {
    TSMemSegRowData* begin = new (key + TSMemSegRowData::GetKeyLen())
        TSMemSegRowData(filter.db_id_, filter.table_id_, cur_version, filter.entity_id_);
    begin->SetData(INT64_MIN, 0, {nullptr, 0});
    begin->GenKey(key);
    iter.Seek(reinterpret_cast<char*>(&key));
    bool scan_over = false;
    while (iter.Valid()) {
      auto cur_row = skiplist_.ParseKey(iter.key());
      assert(cur_row != nullptr);
      if (!cur_row->SameTableId(begin)) {
        scan_over = true;
        break;
      }
      if (cur_row->entity_id > filter.entity_id_) {
        cur_version = cur_row->table_version + 1;
        break;
      }
      if (cur_row->entity_id < filter.entity_id_) {
        cur_version = cur_row->table_version;
        break;
      }
      if (checkTimestampWithSpans(filter.ts_spans_, cur_row->ts, cur_row->ts)
            == TimestampCheckResult::FullyContained) {
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
  SkiplistIterator iter(&skiplist_);
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
      auto cur_row = skiplist_.ParseKey(iter.key());
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
  SkiplistIterator iter(&skiplist_);
  iter.SeekToFirst();
  while (iter.Valid()) {
    auto cur_row = skiplist_.ParseKey(iter.key());
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
  SkiplistIterator iter(&skiplist_);
  iter.SeekToFirst();
  while (iter.Valid()) {
    auto cur_row = skiplist_.ParseKey(iter.key());
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
  SkiplistIterator iter(&skiplist_);
  iter.SeekToFirst();

  std::vector<std::unique_ptr<TsMemSegBlock>> mem_blocks;
  TsMemSegBlock* current_memblock = nullptr;

  auto self = shared_from_this();
  if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
    TSMemSegRowData* last_row_data = nullptr;
    for (; iter.Valid(); iter.Next()) {
      TSMemSegRowData* cur_row = skiplist_.ParseKey(iter.key());
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
      TSMemSegRowData* cur_row = skiplist_.ParseKey(iter.key());
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
      TSMemSegRowData* cur_row = skiplist_.ParseKey(iter.key());
      assert(cur_row != nullptr);
      if (current_memblock == nullptr || !current_memblock->InsertRow(cur_row)) {
        auto p = std::make_unique<TsMemSegBlock>(self);
        current_memblock = p.get();
        mem_blocks.push_back(std::move(p));
        current_memblock->InsertRow(cur_row);
      }
    }
  }
  std::shared_ptr<TSBlkDataTypeConvert> empty_convert = nullptr;
  for (auto& mem_blk : mem_blocks) {
    auto table_id = mem_blk->GetTableId();
    auto version = mem_blk->GetTableVersion();
    std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr;
    auto s = schema_mgr->GetTableSchemaMgr(table_id, tbl_schema_mgr);
    if (s == FAIL) {
      LOG_ERROR("can not get table schema manager for table_id[%lu].", table_id);
      return s;
    }
    std::shared_ptr<MMapMetricsTable> scan_metric = nullptr;
    s = tbl_schema_mgr->GetMetricSchema(version, &scan_metric);
    if (s != SUCCESS) {
      LOG_ERROR("GetMetricSchema failed. table id [%u], table version [%lu]", version, table_id);
    }
    blocks.push_back(std::make_shared<TsBlockSpan>(0, mem_blk->GetEntityId(), std::move(mem_blk), 0, mem_blk->GetRowNum(),
                                                  empty_convert, version, scan_metric->getSchemaInfoExcludeDroppedPtr()));
  }
  return SUCCESS;
}

KStatus TsMemSegment::GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                                    std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                    std::shared_ptr<MMapMetricsTable>& scan_schema) {
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
  TsBlockSpan* template_blk_span = nullptr;
  for (auto& mem_blk : mem_blocks) {
    std::shared_ptr<TsBlockSpan> cur_span;
    auto s = TsBlockSpan::MakeNewBlockSpan(template_blk_span, filter.vgroup_id, filter.entity_id, mem_blk, 0,
                                  mem_blk->GetRowNum(), scan_schema->GetVersion(),
                                  scan_schema->getSchemaInfoExcludeDroppedPtr(), tbl_schema_mgr, cur_span);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsBlockSpan::GenDataConvertfailed, entity_id=%lu.", filter.entity_id);
        return s;
    }
    template_blk_span = cur_span.get();
    blocks.push_back(std::move(cur_span));
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
