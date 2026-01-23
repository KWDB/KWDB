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

#include <list>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <memory>
#include <utility>
#include "ts_tag_iterator_v2_impl.h"

namespace kwdbts {

TagIteratorV2Impl::TagIteratorV2Impl(std::shared_ptr<TagTable> tag_bt, uint32_t table_versioin,
                                     const std::vector<k_uint32>& scan_tags)
    : scan_tags_(scan_tags), tag_bt_(tag_bt), table_version_(table_versioin) {}

TagIteratorV2Impl::TagIteratorV2Impl(std::shared_ptr<TagTable> tag_bt, uint32_t table_versioin,
                                     const std::vector<k_uint32>& scan_tags, const std::unordered_set<uint32_t>& hps)
    : scan_tags_(scan_tags), hps_(hps), tag_bt_(tag_bt), table_version_(table_versioin) {}

TagIteratorV2Impl::~TagIteratorV2Impl() {
  for (size_t idx = 0; idx < tag_partition_iters_.size(); ++idx) {
    SafeDeletePointer(tag_partition_iters_[idx]);
  }
}

KStatus TagIteratorV2Impl::Init() {
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

KStatus TagIteratorV2Impl::Next(std::vector<EntityResultIndex>* entity_id_list,
                                     ResultSet* res, k_uint32* count) {
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
  return KStatus::SUCCESS;
}

KStatus TagIteratorV2Impl::NextTag(EntityResultIndex entity_id_list,
                                     ResultSet* res, k_uint32* count) {
  uint32_t fetch_count = 0;
  KStatus status = KStatus::SUCCESS;
  bool part_iter_finish = false;
  while (cur_tag_part_idx_ < tag_partition_iters_.size())  {
    cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
    if (KStatus::SUCCESS != cur_tag_part_iter_->NextTag(entity_id_list, res, &fetch_count, &part_iter_finish)) {
      LOG_ERROR("failed to get next batch");
      return KStatus::FAIL;
    }
    if (fetch_count > 0) {
      break;
    }
    // each partition is one batch
    if (part_iter_finish) {
      cur_tag_part_idx_++;
    }
  }
  *count = fetch_count;
  return KStatus::SUCCESS;
}

KStatus TagIteratorV2Impl::Close() {
  return (KStatus::SUCCESS);
}

TagIteratorByOSN::TagIteratorByOSN(std::shared_ptr<TagTable> tag_bt, uint32_t table_version,
  std::vector<k_uint32>& scan_cols, std::vector<KwOSNSpan>& osn_span) :
  osn_span_(osn_span) {
  tag_bt_ = tag_bt;
  table_version_ = table_version;
  scan_tags_ = scan_cols;
}

KStatus TagIteratorByOSN::Init(const std::unordered_set<uint32_t>& hps,
  std::unordered_map<uint64_t, EntityResultIndex> pkeys) {
  pkeys_status_ = std::move(pkeys);
  hps_ = hps;
  auto s = TagIteratorV2Impl::Init();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TagIteratorV2Impl::Init failed");
    return KStatus::FAIL;
  }
  for (auto p : tag_partition_iters_) {
    p->SetOSNSpan(osn_span_);
  }
  return KStatus::SUCCESS;
}

KStatus TagIteratorByOSN::Next(std::vector<EntityResultIndex>* entity_id_list,
  ResultSet* res, k_uint32* count) {
  auto s = TagIteratorV2Impl::Next(entity_id_list, res, count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TagIteratorV2Impl::Next failed");
    return KStatus::FAIL;
  }
  for (EntityResultIndex& entity : *entity_id_list) {
    uint64_t key = entity.GenUniqueKey();
    auto stat = pkeys_status_.find(key);
    if (stat == pkeys_status_.end()) {
      // current entity has no tag operation between osn range.
      entity.op_with_osn = std::make_shared<OperatorInfoOfRecord>(OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED, 0, 0, 0);
    } else {
      entity.op_with_osn = stat->second.op_with_osn;
    }
  }
  return KStatus::SUCCESS;
}


}  //  namespace kwdbts
