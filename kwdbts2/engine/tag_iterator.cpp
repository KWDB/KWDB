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

#include "tag_iterator.h"
#include "lt_cond.h"

namespace kwdbts {

bool in(kwdbts::k_uint32 hp, const std::unordered_set<uint32_t>& hps);

KStatus TagPartitionIterator::Next(std::vector<EntityResultIndex>* entity_id_list,
                                     ResultSet* res, k_uint32* count, bool* is_finish) {
  if  (cur_scan_rowid_ > cur_total_row_count_) {
    LOG_DEBUG("fetch tag table %s%s, "
      "cur_scan_rowid[%lu] > cur_total_row_count_[%lu], count: %u",
      m_tag_partition_table_->sandbox().c_str(), m_tag_partition_table_->name().c_str(), cur_scan_rowid_,
      cur_total_row_count_, *count);
    *is_finish = true;
    return(KStatus::SUCCESS);
  }
  *is_finish = false;
  uint32_t fetch_count = *count;
  bool has_data = false;
  size_t start_row = 0;
  size_t row_num = 0;
  ErrorInfo err_info;
  m_tag_partition_table_->startRead();
  for (row_num = cur_scan_rowid_; row_num <= cur_total_row_count_; row_num++) {
    if (fetch_count >= ONE_FETCH_COUNT) {
      LOG_DEBUG("fetch_count[%u] >= ONECE_FETCH_COUNT[%u]", fetch_count,
        ONE_FETCH_COUNT);
      // m_cur_scan_rowid_=row_num;
      goto success_end;
    }
    bool needSkip = false;
    uint32_t hash_point;
    if (!EngineOptions::isSingleNode()) {
      m_tag_partition_table_->getHashpointByRowNum(row_num, &hash_point);
      needSkip = !in(hash_point, hps_);
      LOG_DEBUG("row %lu hashpoint is %u %s in search list", row_num, hash_point, needSkip?"not":"");
    }
    if (!m_tag_partition_table_->isValidRow(row_num) || needSkip) {
      if (has_data) {
        for (int idx = 0; idx < src_version_scan_tags_.size(); idx++) {
          if (src_version_scan_tags_[idx] == INVALID_COL_IDX) {
            Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, row_num - start_row);
            res->push_back(idx, batch);
            continue;
          }
          uint32_t col_idx = src_version_scan_tags_[idx];
          Batch* batch = m_tag_partition_table_->GetTagBatchRecord(start_row, row_num, col_idx,
                                                 result_version_tag_infos_[col_idx], err_info);
          if (err_info.errcode < 0) {
            delete batch;
            LOG_ERROR("GetTagBatchRecord failed.");
            return KStatus::FAIL;
          }
          if (UNLIKELY(batch == nullptr)) {
            LOG_WARN("GetTagBatchRecord result is nullptr, skip this col[%u]", col_idx);
            Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, row_num - start_row);
            res->push_back(idx, batch);
            continue;
          }
          res->push_back(idx, batch);
        }
        has_data = false;
        start_row = row_num + 1;
      }
      continue;
    }
    if (!has_data) {
      start_row = row_num;
      has_data = true;
    }
    if (!EngineOptions::isSingleNode()) {
      m_tag_partition_table_->getHashedEntityIdByRownum(row_num, hash_point, entity_id_list);
    } else {
      m_tag_partition_table_->getEntityIdByRownum(row_num, entity_id_list);
    }
    fetch_count++;
  }  // end for
if (fetch_count == *count) {
  LOG_WARN("no valid record in the tag table %s%s, cur_total_row_count_[%lu], "
      "cur_scan_rowid_[%lu]",
      m_tag_partition_table_->sandbox().c_str(), m_tag_partition_table_->name().c_str(), cur_total_row_count_,
      cur_scan_rowid_);
  m_tag_partition_table_->stopRead();
  *is_finish = true;
  return (KStatus::SUCCESS);
}
success_end:
  if (start_row < row_num) {
    // need start_row < end_row
    for (int idx = 0; idx < src_version_scan_tags_.size(); idx++) {
      if (src_version_scan_tags_[idx] == INVALID_COL_IDX) {
        Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, row_num - start_row);
        res->push_back(idx, batch);
        continue;
      }
      uint32_t col_idx = src_version_scan_tags_[idx];
      Batch* batch = m_tag_partition_table_->GetTagBatchRecord(start_row, row_num, col_idx,
                                      result_version_tag_infos_[col_idx], err_info);
      if (err_info.errcode < 0) {
        delete batch;
        LOG_ERROR("GetTagBatchRecord failed.");
        return KStatus::FAIL;
      }
      if (UNLIKELY(batch == nullptr)) {
        LOG_WARN("GetTagBatchRecord result is nullptr, skip this col[%u]", col_idx);
        Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, row_num - start_row);
        res->push_back(idx, batch);
        continue;
      }
      res->push_back(idx, batch);
    }
  } else {
    LOG_DEBUG("not required to call GetTagBatchRecord on the tag table %s%s, "
      "fetch_count: %u, start_row[%lu] >= row_num[%lu]",
      m_tag_partition_table_->sandbox().c_str(), m_tag_partition_table_->name().c_str(), fetch_count,
      start_row, row_num);
  }
  m_tag_partition_table_->stopRead();
  *count = fetch_count;
  LOG_DEBUG("fatch the tag table %s%s, fetch_count: %u",
    m_tag_partition_table_->sandbox().c_str(), m_tag_partition_table_->name().c_str(), *count);
  cur_scan_rowid_ = row_num;
  *is_finish = (row_num > cur_total_row_count_) ? true : false;
  return (KStatus::SUCCESS);
}

void TagPartitionIterator::Init() {
  m_tag_partition_table_->mutexLock();
  cur_total_row_count_ = m_tag_partition_table_->actual_size();
  m_tag_partition_table_->mutexUnlock();
}

bool in(kwdbts::k_uint32 hp, const std::unordered_set<uint32_t>& hps) {
  return hps.find(hp) != hps.end();
}

}  // namespace kwdbts
