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
  if (segments->get() != nullptr && (*segments)->SetImm()) {
    // do nothing
  } else {
    LOG_ERROR("can not switch mem segment.");
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

