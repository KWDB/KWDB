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
                                     const std::vector<k_uint32>& scan_tags, TS_OSN osn)
    : scan_tags_(scan_tags), tag_bt_(std::move(tag_bt)), table_version_(table_versioin), scan_osn_(osn) {}
TagIteratorV2Impl::TagIteratorV2Impl(std::shared_ptr<TagTable> tag_bt, uint32_t table_versioin,
                                     const std::vector<k_uint32>& scan_tags, std::vector<HashIdSpan>* hps,
                                     TS_OSN scan_osn)
    : scan_tags_(scan_tags), hps_(hps), tag_bt_(std::move(tag_bt)), table_version_(table_versioin), scan_osn_(scan_osn) {}
TagIteratorV2Impl::~TagIteratorV2Impl() {
  // Clean up all partition iterators using smart pointer semantics
  for (auto* iter : tag_partition_iters_) {
    SafeDeletePointer(iter);
  }
  tag_partition_iters_.clear();
}
KStatus TagIteratorV2Impl::Init() {
  // 1. get all partition tables
  std::vector<TagPartitionTable*> all_part_tables;
  auto* partition_mgr = tag_bt_->GetTagPartitionTableManager();
  if (partition_mgr == nullptr) {
    LOG_ERROR("TagIteratorV2Impl::Init: partition manager is null");
    return FAIL;
  }
  partition_mgr->GetAllPartitionTablesLessVersion(all_part_tables, table_version_);
  if (all_part_tables.empty()) {
    LOG_ERROR("TagIteratorV2Impl::Init: tag table version [%u]'s partition table is empty.", table_version_);
    return FAIL;
  }
  // 2. Initialize TagPartitionIterator
  auto* version_mgr = tag_bt_->GetTagTableVersionManager();
  if (version_mgr == nullptr) {
    LOG_ERROR("TagIteratorV2Impl::Init: version manager is null");
    return FAIL;
  }
  TagVersionObject* result_ver_obj = version_mgr->GetVersionObject(table_version_);
  if (nullptr == result_ver_obj) {
    LOG_ERROR("TagIteratorV2Impl::Init: GetVersionObject failed, version id: %d", table_version_);
    return FAIL;
  }
  // Pre-calculate valid schema indexes for all scan tags
  const auto& valid_schema_idxs = result_ver_obj->getValidSchemaIdxs();
  std::vector<uint32_t> result_scan_tags;
  result_scan_tags.reserve(scan_tags_.size());
  for (const auto& tag_idx : scan_tags_) {
    if (tag_idx < valid_schema_idxs.size()) {
      result_scan_tags.emplace_back(valid_schema_idxs[tag_idx]);
    } else {
      result_scan_tags.emplace_back(INVALID_COL_IDX);
      LOG_WARN("TagIteratorV2Impl::Init: tag index %u out of range", tag_idx);
    }
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
      LOG_ERROR("TagIteratorV2Impl::Init: KNEW TagPartitionIterator failed.");
      return FAIL;
    }
    tag_part_iter->Init();
    tag_part_iter->SetOSNSpan({}, scan_osn_);
    tag_partition_iters_.push_back(tag_part_iter);
  }
  if (tag_partition_iters_.empty()) {
    LOG_ERROR("TagIteratorV2Impl::Init: no valid partition iterators created");
    return FAIL;
  }
  cur_tag_part_idx_ = 0;
  cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
  return KStatus::SUCCESS;
}
KStatus TagIteratorV2Impl::Next(std::vector<EntityResultIndex>* entity_id_list,
                                     ResultSet* res, k_uint32* count) {
  uint32_t fetch_count = 0;
  bool part_iter_finish = false;
  while (fetch_count < ONE_FETCH_COUNT && cur_tag_part_idx_ < tag_partition_iters_.size())  {
    cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
    if (KStatus::SUCCESS != cur_tag_part_iter_->Next(entity_id_list, res, &fetch_count, &part_iter_finish)) {
      LOG_ERROR("TagIteratorV2Impl::Next: failed to get next batch from partition %u", cur_tag_part_idx_);
      return KStatus::FAIL;
    }
    // Move to next partition if current one is finished
    if (part_iter_finish) {
      cur_tag_part_idx_++;
      // If no data fetched from this partition, continue to next one
      if (fetch_count == 0) {
        continue;
      }
    }
    // Break if we have data or reached the end
    break;
  }
  *count = fetch_count;
  return KStatus::SUCCESS;
}
KStatus TagIteratorV2Impl::NextTag(const EntityResultIndex& entity_id_list,
                                     ResultSet* res, k_uint32* count) {
  uint32_t fetch_count = 0;
  KStatus status = KStatus::SUCCESS;
  bool part_iter_finish = false;
  // Iterate through partitions until we find data or exhaust all partitions
  while (cur_tag_part_idx_ < tag_partition_iters_.size()) {
    cur_tag_part_iter_ = tag_partition_iters_[cur_tag_part_idx_];
    if (KStatus::SUCCESS != cur_tag_part_iter_->NextTag(entity_id_list, res, &fetch_count, &part_iter_finish)) {
      LOG_ERROR("TagIteratorV2Impl::NextTag: failed to get next batch from partition %u", cur_tag_part_idx_);
      return KStatus::FAIL;
    }
    // Return immediately if we got data
    if (fetch_count > 0) {
      break;
    }
    // Move to next partition if current one is finished
    if (part_iter_finish) {
      cur_tag_part_idx_++;
    }
  }
  *count = fetch_count;
  return KStatus::SUCCESS;
}
KStatus TagIteratorV2Impl::Close() {
  // Reset iterator state for potential reuse
  cur_tag_part_idx_ = 0;
  if (!tag_partition_iters_.empty()) {
    cur_tag_part_iter_ = tag_partition_iters_[0];
  } else {
    cur_tag_part_iter_ = nullptr;
  }
  return KStatus::SUCCESS;
}
TagIteratorByOSN::TagIteratorByOSN(std::shared_ptr<TagTable> tag_bt, uint32_t table_version,
  std::vector<k_uint32>& scan_cols, std::vector<KwOSNSpan>& osn_span) :
  osn_span_(osn_span) {
  tag_bt_ = tag_bt;
  table_version_ = table_version;
  scan_tags_ = scan_cols;
}
KStatus TagIteratorByOSN::Init(std::vector<HashIdSpan>* hps, TS_OSN scan_osn,
  std::unordered_map<uint64_t, EntityResultIndex> pkeys) {
  // Move pkeys to member variable efficiently
  pkeys_status_ = std::move(pkeys);
  hps_ = hps;
  // Call base class initialization
  auto s = TagIteratorV2Impl::Init();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TagIteratorByOSN::Init: TagIteratorV2Impl::Init failed");
    return KStatus::FAIL;
  }
  // Set OSN span for all partition iterators
  for (auto* p : tag_partition_iters_) {
    if (p != nullptr) {
      p->SetOSNSpan(osn_span_, scan_osn);
    } else {
      LOG_ERROR("TagIteratorByOSN::Init: null partition iterator");
    }
  }
  return KStatus::SUCCESS;
}
KStatus TagIteratorByOSN::Next(std::vector<EntityResultIndex>* entity_id_list,
  ResultSet* res, k_uint32* count) {
  auto s = TagIteratorV2Impl::Next(entity_id_list, res, count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TagIteratorByOSN::Next: TagIteratorV2Impl::Next failed");
    return KStatus::FAIL;
  }
  // Enrich entities with OSN operation info
  for (EntityResultIndex& entity : *entity_id_list) {
    uint64_t key = entity.GenUniqueKey();
    auto stat = pkeys_status_.find(key);
    if (stat == pkeys_status_.end()) {
      // Entity has no tag operation in OSN range - use default existed type
      entity.op_with_osn = std::make_shared<OperatorInfoOfRecord>(
          OperatorTypeOfRecord::OP_TYPE_TAG_EXISTED, 0, 0, 0);
    } else {
      // Use existing operation info
      entity.op_with_osn = stat->second.op_with_osn;
    }
  }
  return KStatus::SUCCESS;
}
}  //  namespace kwdbts