//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

#pragma once

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "kwdb_type.h"
#include "cm_kwdb_context.h"
#include "libkwdbts2.h"

extern bool cluster_setting_storage_autonomy_enable;

namespace kwdbts {

class TsTable;

struct TableTsStatisticInfo {
  uint64_t insert_rows_per_day;
  uint64_t entity_num;
};

class TsAutonomyApi {
 public:
  static KStatus CollectTsStorageDataInfo(kwdbContext_p ctx, std::shared_ptr<TsTable>& ts_table,
                                          uint64_t& entity_num, uint64_t& insert_rows_per_day,
                                          uint64_t& max_entities_of_last_subgroup, bool open_init = false);

  static TSStatus CollectTsStorageDataInfo(TSEngine* engine, TSTableID table_id, uint64_t* entity_num,
                                           uint64_t* insert_rows_per_day, uint64_t* max_entities_of_last_subgroup);

  static void GetStorageAutonomyResultFromGo(uint32_t& max_blocks_per_segment, uint16_t& max_rows_per_block,
                                             uint64_t tableId, uint16_t max_entities_of_subgroup,
                                             uint64_t partition_interval);

 private:
  static std::unordered_map<uint64_t, TableTsStatisticInfo> table_statistic_infos_;
  static pthread_rwlock_t rw_lock_;
};

}  // namespace kwdbts
