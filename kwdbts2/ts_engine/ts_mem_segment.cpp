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

bool TsMemSegmentManager::FlushOneMemSegment(TsMemSegment* mem_seg) {
  if (!mem_seg->SetFlushing()) {
    LOG_ERROR("cannot set status for mem segment.");
    return false;
  }
  std::unordered_map<TsVGroupPartition*, TsLastSegmentBuilder> builders;
  struct LastRowInfo {
    TSTableID cur_table_id = 0;
    uint32_t database_id = 0;
    uint32_t cur_table_version = 0;
    std::vector<AttributeInfo> info;
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  };
  LastRowInfo last_row_info;
  bool flush_success = true;

  mem_seg->Traversal([&](TSMemSegRowData* tbl) -> bool {
    // 1. get table schema manager.
    if (last_row_info.cur_table_id != tbl->table_id) {
      auto s = vgroup_->GetEngineSchemaMgr()->GetTableSchemaMgr(tbl->table_id, last_row_info.schema_mgr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] schemainfo.", tbl->table_id);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_id = tbl->table_id;
      last_row_info.database_id = vgroup_->GetEngineSchemaMgr()->GetDBIDByTableID(tbl->table_id);
      last_row_info.cur_table_version = 0;
    }
    // 2. get table schema info of certain version.
    if (last_row_info.cur_table_version != tbl->table_version) {
      last_row_info.info.clear();
      auto s = last_row_info.schema_mgr->GetColumnsExcludeDropped(last_row_info.info, tbl->table_version);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot get table[%lu] version[%u] schema info.", tbl->table_id, tbl->table_version);
        flush_success = false;
        return false;
      }
      last_row_info.cur_table_version = tbl->table_version;
    }
    // 3. get partition for metric data. 
    auto partition = vgroup_->GetPartition(last_row_info.database_id, tbl->ts, (DATATYPE)last_row_info.info[0].type);
    auto it = builders.find(partition);
    if (it == builders.end()) {
      std::unique_ptr<TsFile> last_file;
      partition->NewLastFile(&last_file);
      auto result = builders.insert({partition, TsLastSegmentBuilder{vgroup_->GetEngineSchemaMgr(), std::move(last_file)}});
      it = result.first;
    }
    // 4. insert data into segment builder.
    TsLastSegmentBuilder& builder = it->second;
    auto s = builder.PutRowData(tbl->table_id, tbl->table_version, tbl->entity_id, tbl->seqno, tbl->row_data);
    if (s != SUCCESS) {
      LOG_ERROR("PutRowData failed.");
      flush_success = false;
      return false;
    }
    return true;
  });
  if (!flush_success) {
    LOG_ERROR("faile flush memsegment to last segment.");
    return false;
  }

  for (auto& kv : builders) {
    auto s = kv.second.Finalize();
    if (s == FAIL){
      LOG_ERROR("last segment Finalize failed.");
      return false;
    }
    kv.second.Flush();
  }
  mem_seg->SetDeleting();
  return true;
}

// WAL CreateCheckPoint call this function to persistent metric datas.
void TsMemSegmentManager::SwitchMemSegment(std::shared_ptr<TsMemSegment>* segments) {
  segments->reset();
  segment_lock_.lock();
  if (segment_.size() > 0) {
    *segments = segment_.front();
    segment_.push_front(std::make_shared<TsMemSegment>());
  }
  segment_lock_.unlock();
  if (segments->get() != nullptr && (*segments)->SetImm()) {
    // do nothing
  } else {
    LOG_ERROR("can not switch mem segment.");
  }
}

KStatus TsMemSegmentManager::PutData(const TSSlice& payload, TSEntityID entity_id) {
  TSMemSegRowData row_data;
  row_data.table_id = TsRawPayload::GetTableIDFromSlice(payload);
  row_data.table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  row_data.entity_id = entity_id;
  std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
  auto s = vgroup_->GetEngineSchemaMgr()->GetTableSchemaMgr(row_data.table_id, schema_mgr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] schema manager.", row_data.table_id);
    return s;
  }
  std::vector<AttributeInfo> schema;
  s = schema_mgr->GetColumnsExcludeDropped(schema, row_data.table_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot found table [%lu] with version[%u].", row_data.table_id, row_data.table_version);
    return s;
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
  TSSlice ts;
  for (size_t i = 0; i < row_num; i++) {
    row_data.row_data = pd.GetRowData(i);
    pd.GetColValue(i, 0, &ts);
    row_data.ts = KTimestamp(ts.data);
    bool ret = cur_mem_seg->AddPayload(row_data);
    if (!ret) {
      LOG_ERROR("failed to AddPayload for table [%lu]", row_data.table_id);
      cur_mem_seg->AllocRowNum(0 - (row_num - i));
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}


}  //  namespace kwdbts

