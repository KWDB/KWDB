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

#include "st_config.h"

uint16_t CLUSTER_SETTING_MAX_ROWS_PER_BLOCK = 1000;
bool CLUSTER_SETTING_COUNT_USE_STATISTICS = true;
bool CLUSTER_SETTING_USE_LAST_ROW_OPTIMIZATION = false;

namespace kwdbts {

void GetSegmentConfig(uint32_t& max_blocks_per_segment, uint16_t& max_rows_per_block,
                      uint64_t table_id, uint32_t max_entities_of_subgroup,
                      uint32_t partition_interval) {
#ifndef KWBASE_OSS
  TsConfigAutonomy::GetSegmentConfig(max_blocks_per_segment, max_rows_per_block, table_id,
                                     max_entities_of_subgroup, partition_interval);
#else
  max_rows_per_block = CLUSTER_SETTING_MAX_ROWS_PER_BLOCK;
#endif
}

}  // namespace kwdbts
