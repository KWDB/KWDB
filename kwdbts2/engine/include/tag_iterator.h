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
#pragma once

#include <vector>
#include <memory>
#include <unordered_set>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "ts_time_partition.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_table.h"
#include "ts_common.h"
#include "lg_api.h"
#include "ts_table.h"

namespace kwdbts {
class SubEntityGroupManager;
class BaseEntityIterator {
 public:
  virtual KStatus Init() = 0;
  virtual KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) = 0;
  virtual KStatus Close() = 0;
  virtual ~BaseEntityIterator() = default;
};

class TagPartitionIterator {
 public:
  TagPartitionIterator() = delete;
  explicit TagPartitionIterator(TagPartitionTable* tag_partition_table, const std::vector<k_uint32>& src_scan_tags,
                      const std::vector<TagInfo>& result_tag_infos, const std::unordered_set<kwdbts::k_uint32>& hps) :
                                m_tag_partition_table_(tag_partition_table), src_version_scan_tags_(src_scan_tags),
                                result_version_tag_infos_(result_tag_infos), hps_(hps) {}

  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count, bool* is_finish);

  void Init();

 private:
  TagPartitionTable* m_tag_partition_table_{nullptr};
  size_t cur_total_row_count_{0};
  size_t cur_scan_rowid_{1};
  std::vector<k_uint32> src_version_scan_tags_;
  // std::vector<k_uint32> result_version_scan_tags_;
  std::vector<TagInfo>  result_version_tag_infos_;
  std::unordered_set<uint32_t> hps_;
};

class TsEntityGroup;

class EntityGroupTagIterator {
 public:
  EntityGroupTagIterator() = delete;
  explicit EntityGroupTagIterator(std::shared_ptr<TsEntityGroup> entity_group, TagTable* tag_bt,
                         uint32_t table_versioin, const std::vector<k_uint32>& scan_tags);

  explicit EntityGroupTagIterator(std::shared_ptr<TsEntityGroup> entity_group, TagTable* tag_bt,
                         uint32_t table_versioin, const std::vector<k_uint32>& scan_tags,
                         const std::unordered_set<uint32_t>& hps);

  virtual ~EntityGroupTagIterator();

  KStatus Init();
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count, bool* is_finish);
  KStatus Close();

 private:
  std::vector<k_uint32> scan_tags_;
  std::unordered_set<uint32_t> hps_;
  TagTable* tag_bt_;
  std::shared_ptr<TsEntityGroup> entity_group_;
  std::vector<TagPartitionIterator*> tag_partition_iters_;
  uint32_t table_version_;
  TagPartitionIterator* cur_tag_part_iter_ = nullptr;
  uint32_t cur_tag_part_idx_{0};
};

class TagIterator : public BaseEntityIterator {
 public:
  explicit TagIterator(std::vector<EntityGroupTagIterator*>& tag_grp_iters) : entitygrp_iters_(tag_grp_iters) {}
  ~TagIterator() override;

  KStatus Init() override;
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
  KStatus Close() override;

 private:
  std::vector<EntityGroupTagIterator*> entitygrp_iters_;
  EntityGroupTagIterator* cur_iterator_ = nullptr;
  uint32_t cur_idx_{0};
};

class EntityGroupMetaIterator {
 public:
  EntityGroupMetaIterator() = delete;

  EntityGroupMetaIterator(uint64_t range_group_id, SubEntityGroupManager* ebt_manager) :
                          range_group_id_(range_group_id), ebt_manager_(ebt_manager) {}

  ~EntityGroupMetaIterator() = default;

  KStatus Init();

  KStatus Next(std::vector <EntityResultIndex>* entity_list, k_uint32* count);

 private:
  uint64_t range_group_id_;
  SubEntityGroupManager* ebt_manager_;
  std::vector<EntityResultIndex> entity_list_;
  // mark reading location
  uint32_t cur_index_ = 0;
};

class MetaIterator : public BaseEntityIterator {
 public:
  explicit MetaIterator(std::vector<EntityGroupMetaIterator*>& iters) : iters_(iters) {}
  ~MetaIterator() override;

  KStatus Init() override;
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
  KStatus Close() override;

 private:
  std::vector<EntityGroupMetaIterator*> iters_;
  uint32_t cur_index_ = 0;
};

}  //  namespace kwdbts
