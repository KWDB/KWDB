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
#include "st_group_manager.h"
#include "lt_cond.h"

namespace kwdbts {

bool in(kwdbts::k_uint32 hp, const std::vector<kwdbts::k_uint32>& hps);

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

EntityGroupTagIterator::EntityGroupTagIterator(std::shared_ptr<TsEntityGroup> entity_group, TagTable* tag_bt,
                         uint32_t table_versioin, const std::vector<k_uint32>& scan_tags) :
                         entity_group_(entity_group), tag_bt_(tag_bt),
                         table_version_(table_versioin), scan_tags_(scan_tags) {
    entity_group_->RdDropLock();
}

EntityGroupTagIterator::EntityGroupTagIterator(std::shared_ptr<TsEntityGroup> entity_group, TagTable* tag_bt,
                         uint32_t table_versioin, const std::vector<k_uint32>& scan_tags,
                         const std::vector<uint32_t>& hps) :
                        entity_group_(entity_group), tag_bt_(tag_bt),
                        table_version_(table_versioin), scan_tags_(scan_tags),
                        hps_(hps) {
    entity_group_->RdDropLock();
    #ifdef K_DEBUG
    for (int i =0; i< hps_.size(); i++) {
      LOG_DEBUG("Init EntityGroupTagIterator hashpoints is %u", hps_.at(i));
    }
    #endif
}

EntityGroupTagIterator::~EntityGroupTagIterator() {
  entity_group_->DropUnlock();
  for (size_t idx = 0; idx < tag_partition_iters_.size(); ++idx) {
    delete tag_partition_iters_[idx];
    tag_partition_iters_[idx] = nullptr;
  }
}

KStatus EntityGroupTagIterator::Init() {
  // 1. get all partition tables
  std::vector<TagPartitionTable*> all_part_tables;
  tag_bt_->GetTagPartitionTableManager()->GetAllPartitionTablesLessVersion(all_part_tables, table_version_);
  if (all_part_tables.empty()) {
    LOG_ERROR("tag table version [%u]'s partition table is empty.", table_version_);
    return FAIL;
  }
  // 2. init TagPartitionIterator
  TagVersionObject* result_ver_obj = tag_bt_->GetTagTableVersionManager()->GetVersionObject(table_version_);
  if (nullptr == result_ver_obj) {
      LOG_ERROR("GetTagVersionObject failed, version id: %d", table_version_);
      return FAIL;
  }
  std::vector<uint32_t> result_scan_tags;
  for (const auto& tag_idx : scan_tags_) {
    result_scan_tags.emplace_back(result_ver_obj->getValidSchemaIdxs()[tag_idx]);
  }
  for (const auto& tag_part_ptr : all_part_tables) {
    // get source scan tags
    std::vector<uint32_t> src_scan_tags;
    for (int idx = 0; idx < scan_tags_.size(); ++idx) {
      if (result_scan_tags[idx] >= tag_part_ptr->getIncludeDroppedSchemaInfos().size()) {
        src_scan_tags.push_back(INVALID_COL_IDX);
      } else {
        src_scan_tags.push_back(result_scan_tags[idx]);
      }
    }
    // new TagPartitionIterator
    TagPartitionIterator* tag_part_iter = KNEW TagPartitionIterator(tag_part_ptr, src_scan_tags,
                                                  result_ver_obj->getIncludeDroppedSchemaInfos(), hps_);
    if (nullptr == tag_part_iter) {
      LOG_ERROR("KNEW TagPartitionIterator failed.");
      return FAIL;
    }
    tag_part_iter->Init();
    tag_partition_iters_.push_back(tag_part_iter);
  }
  cur_tag_part_idx_ = 0;
  cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
  return KStatus::SUCCESS;
}
bool in(kwdbts::k_uint32 hp, const std::vector<kwdbts::k_uint32>& hps) {
  for (int i=0; i < hps.size(); i++) {
    if (hps[i] == hp) {
      return true;
    }
  }
  return false;
}

KStatus EntityGroupTagIterator::Next(std::vector<EntityResultIndex>* entity_id_list,
                                     ResultSet* res, k_uint32* count, bool* is_finish) {
  *is_finish = false;
  uint32_t fetch_count = 0;
  KStatus status = KStatus::SUCCESS;
  bool part_iter_finish = false;
  while (fetch_count < ONE_FETCH_COUNT && cur_tag_part_idx_ < tag_partition_iters_.size())  {
    cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
    if (KStatus::SUCCESS != cur_tag_part_iter_->Next(entity_id_list, res, &fetch_count, &part_iter_finish)) {
      LOG_ERROR("failed to get next batch");
      return KStatus::FAIL;
    }
    // each partition is one batch
    if (part_iter_finish) {
      cur_tag_part_idx_++;
      if (fetch_count == 0) {
        // this partition is empty
        continue;
      }
    }
    break;
  }
  *count = fetch_count;
  *is_finish = (cur_tag_part_idx_ >= tag_partition_iters_.size()) ? true : false;
  return KStatus::SUCCESS;
}

KStatus EntityGroupTagIterator::Close() {
  return (KStatus::SUCCESS);
}

TagIterator::~TagIterator() {
  for (uint32_t idx = 0; idx < entitygrp_iters_.size(); idx++) {
    entitygrp_iters_[idx]->Close();
    delete entitygrp_iters_[idx];
    entitygrp_iters_[idx] = nullptr;
  }
  entitygrp_iters_.clear();
}

KStatus TagIterator::Init() {
  if (entitygrp_iters_.empty()) {
    LOG_ERROR("invalid EntityGroupTagIterator");
    return (KStatus::FAIL);
  }
  for (const auto& entitygrp_iter : entitygrp_iters_) {
    if (KStatus::SUCCESS != entitygrp_iter->Init()) {
      LOG_ERROR("EntityGroupTagIterator::Init failed.");
      return KStatus::FAIL;
    }
  }
  cur_idx_ = 0;
  cur_iterator_ = entitygrp_iters_[cur_idx_];
  return KStatus::SUCCESS;
}

KStatus TagIterator::Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) {
  if (cur_idx_ >= entitygrp_iters_.size()) {
    *count = 0;
    return KStatus::SUCCESS;
  }
  uint32_t fetch_count = 0;
  KStatus status = KStatus::SUCCESS;
  bool cur_iter_finish = false;
  while (fetch_count < ONE_FETCH_COUNT && cur_idx_ < entitygrp_iters_.size())  {
    cur_iterator_ = entitygrp_iters_[cur_idx_];
    if (KStatus::SUCCESS != cur_iterator_->Next(entity_id_list, res, &fetch_count, &cur_iter_finish)) {
      LOG_ERROR("failed to get next batch");
      return KStatus::FAIL;
    }
    if (cur_iter_finish) {
      cur_idx_++;
    }
    break;
  }
  *count = fetch_count;
  return KStatus::SUCCESS;
}


KStatus TagIterator::Close() {
  for (uint32_t idx = 0; idx < entitygrp_iters_.size(); idx++) {
    entitygrp_iters_[idx]->Close();
    delete entitygrp_iters_[idx];
    entitygrp_iters_[idx] = nullptr;
  }
  entitygrp_iters_.clear();
  return KStatus::SUCCESS;
}

KStatus EntityGroupMetaIterator::Init() {
  ErrorInfo error_info;
  for (uint32_t i = 1; i <= ebt_manager_->GetMaxSubGroupId(); ++i) {
    TsSubEntityGroup* subgroup = ebt_manager_->GetSubGroup(i, error_info);
    if (!subgroup) {
      continue;
    }
    std::vector<uint32_t> entities = subgroup->GetEntities();
    for (auto& entity : entities) {
      entity_list_.push_back(EntityResultIndex(range_group_id_, entity, i));
    }
    // ebt_manager_->ReleasePartitionTable(subgroup);
  }
  if (entity_list_.empty()) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus EntityGroupMetaIterator::Next(std::vector<EntityResultIndex>* entity_list, k_uint32* count) {
  if (cur_index_ >= entity_list_.size()) {
    return KStatus::SUCCESS;
  }
  size_t fetch_count = 0;
  while (cur_index_ < entity_list_.size()) {
    entity_list->push_back(entity_list_[cur_index_++]);
    if (++fetch_count >= (ONE_FETCH_COUNT - *count)) {
      break;
    }
  }
  *count += fetch_count;
  return KStatus::SUCCESS;
}

MetaIterator::~MetaIterator() {
  for (auto& iter : iters_) {
    delete iter;
    iter = nullptr;
  }
  iters_.clear();
}

KStatus MetaIterator::Init() {
  cur_index_ = 0;
  return SUCCESS;
}

KStatus MetaIterator::Next(std::vector<EntityResultIndex>* entity_list, ResultSet* res, k_uint32* count) {
  if (cur_index_ >= iters_.size()) {
    *count = 0;
    return KStatus::SUCCESS;
  }
  uint32_t fetch_count = 0;
  while (cur_index_ < iters_.size())  {
    EntityGroupMetaIterator* cur_iter = iters_[cur_index_];
    cur_iter->Next(entity_list, &fetch_count);
    if (fetch_count >= ONE_FETCH_COUNT) {
      break;
    }
    ++cur_index_;
  }
  *count = fetch_count;
  LOG_DEBUG("SUCCESS fetch_count: %u ", fetch_count);
  return KStatus::SUCCESS;
}

KStatus MetaIterator::Close() {
  for (auto& iter : iters_) {
    delete iter;
    iter = nullptr;
  }
  iters_.clear();
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
