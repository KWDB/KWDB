// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "ts_entity_partition.h"
#include "ts_entity_segment.h"

namespace kwdbts {

KStatus TsEntityPartition::Init(std::list<std::shared_ptr<TsMemSegment>>& mems) {
  auto s = SetFilter();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("SetFilter failed.");
    return s;
  }
  AddMemSegment(mems);
  return KStatus::SUCCESS;
}

KStatus TsEntityPartition::GetBlockSpan(std::list<shared_ptr<TsBlockSpan>>* ts_block_spans) {
  ts_block_spans->clear();
  // get block span in mem segment
  for (auto& mem : mems_) {
    auto s = mem->GetBlockSpans(block_data_filter_, *ts_block_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans of mem segment failed.");
      return s;
    }
  }
  if (!skip_file_data_) {
    // get block span in last segment
    std::vector<std::shared_ptr<TsLastSegment>> last_segs = partition_version_->GetAllLastSegments();
    for (auto& last_seg : last_segs) {
      auto s = last_seg->GetBlockSpans(block_data_filter_, *ts_block_spans);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetBlockSpans of mem segment failed.");
        return s;
      }
    }
    // get block span in entity segment
    auto entity_segment = partition_version_->GetEntitySegment();
    if (entity_segment == nullptr) {
      // entity segment not exist
      return KStatus::SUCCESS;
    }
    auto s = entity_segment->GetBlockSpans(block_data_filter_, *ts_block_spans);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetBlockSpans of mem segment failed.");
      return s;
    }
  }
  LOG_DEBUG("reading block span num [%lu]", ts_block_spans->size());
  return KStatus::SUCCESS;
}

KStatus TsEntityPartition::SetFilter() {
  std::list<STDelRange> del_range;
  auto s = partition_version_->GetDelRange(scan_filter_.entity_id, del_range);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelRange failed.");
    return s;
  }
  KwTsSpan partition_span;
  partition_span.begin = convertSecondToPrecisionTS(partition_version_->GetStartTime(), ts_type_);
  partition_span.end = convertSecondToPrecisionTS(partition_version_->GetEndTime(), ts_type_) - 1;
  std::vector<STScanRange> cur_scan_range;
  for (auto& scan : scan_filter_.ts_spans_) {
    KwTsSpan cross_part;
    cross_part.begin = std::max(partition_span.begin, scan.begin);
    cross_part.end = std::min(partition_span.end, scan.end);
    if (cross_part.begin <= cross_part.end) {
      cur_scan_range.push_back(STScanRange(cross_part, {0, scan_lsn_}));
    }
  }
  for (auto& del : del_range) {
    cur_scan_range = LSNRangeUtil::MergeScanAndDelRange(cur_scan_range, del);
  }
  block_data_filter_.spans_ = std::move(cur_scan_range);
  block_data_filter_.db_id = scan_filter_.db_id;
  block_data_filter_.entity_id = scan_filter_.entity_id;
  block_data_filter_.table_id = scan_filter_.table_id;
  return KStatus::SUCCESS;
}

void TsEntityPartition::AddMemSegment(std::list<std::shared_ptr<TsMemSegment>>& mems) {
  KwTsSpan partition_span;
  partition_span.begin = convertSecondToPrecisionTS(partition_version_->GetStartTime(), ts_type_);
  partition_span.end = convertSecondToPrecisionTS(partition_version_->GetEndTime(), ts_type_) - 1;
  TsScanFilterParams mem_filter{scan_filter_.db_id, scan_filter_.table_id, scan_filter_.entity_id, {partition_span}};
  for (auto& mem : mems) {
    if (mem->HasEntityRows(mem_filter)) {
      mems_.push_back(mem);
    }
  }
}

}  // namespace kwdbts
