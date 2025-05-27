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

#include <mmap/mmap_tag_column_table_aux.h>
#include <map>
#include <vector>
#include <string>
#include <unordered_map>
#include <memory>
#include <utility>
#include "ts_table.h"
#include "mmap/mmap_tag_column_table.h"

namespace kwdbts {

// for WALMode::BYRL
class RaftLoggedTsEntityGroup : public TsEntityGroup {
 public:
  RaftLoggedTsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_bt_manager, const string& db_path,
                      const KTableKey& table_id, const RangeGroup& range, const string& tbl_sub_path, uint64_t hash_num);

  ~RaftLoggedTsEntityGroup() override;

  /**
    * @brief Start the checkpoint operation of the current EntityGroup.
    *
    * @return KStatus
    */
  KStatus CreateCheckpoint(kwdbContext_p ctx)  override;
};

// for WALMode::BYRL
class RaftLoggedTsTable : public TsTable {
 public:
  RaftLoggedTsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id);

  ~RaftLoggedTsTable() override;

  KStatus Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups,
               ErrorInfo& err_info = getDummyErrorInfo()) override;

  KStatus CreateCheckpoint(kwdbContext_p ctx) override;

 protected:
  void constructEntityGroup(kwdbContext_p ctx,
                            const RangeGroup& hash_range,
                            const string& range_tbl_sub_path,
                            std::shared_ptr<TsEntityGroup>* entity_group, uint64_t hash_num) override {
    auto t_range = std::make_shared<RaftLoggedTsEntityGroup>(ctx, entity_bt_manager_, db_path_, table_id_, hash_range,
                                                         range_tbl_sub_path, hash_num);
    *entity_group = std::move(t_range);
  }
};

}  // namespace kwdbts
