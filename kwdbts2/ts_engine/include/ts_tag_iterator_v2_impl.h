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

#include <memory>
#include <unordered_set>
#include <vector>
#include "kwdb_type.h"
#include "ts_time_partition.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_table.h"
#include "ts_common.h"
#include "lg_api.h"
#include "ts_table.h"
#include "ee_global.h"
#include "tag_iterator.h"

namespace kwdbts {

class TagIteratorV2Impl : public BaseEntityIterator {
 public:
  TagIteratorV2Impl(std::shared_ptr<TagTable> tag_bt, uint32_t table_versioin, const std::vector<k_uint32>& scan_tags);
  TagIteratorV2Impl(std::shared_ptr<TagTable> tag_bt, uint32_t table_versioin, const std::vector<k_uint32>& scan_tags,
                    const std::unordered_set<uint32_t>& hps);
  ~TagIteratorV2Impl() override;

  KStatus Init() override;
  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
  KStatus Close() override;

 private:
  std::vector<k_uint32> scan_tags_;
  std::unordered_set<uint32_t> hps_;
  std::shared_ptr<TagTable> tag_bt_;
  std::vector<TagPartitionIterator*> tag_partition_iters_;
  uint32_t table_version_;
  TagPartitionIterator* cur_tag_part_iter_ = nullptr;
  uint32_t cur_tag_part_idx_{0};
};

}  //  namespace kwdbts
