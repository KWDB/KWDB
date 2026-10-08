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

#include "ts_autonomy_api.h"
#include <libkwdbts2.h>
#include "cm_func.h"
#include "engine.h"
#include "lg_api.h"
#include "ts_table.h"


bool cluster_storage_autonomy_enable = true;

namespace kwdbts {

std::unordered_map<uint64_t, TableTsStatisticInfo> TsAutonomyApi::table_statistic_infos_ = {};
pthread_rwlock_t TsAutonomyApi::rw_lock_{};

KStatus TsAutonomyApi::CollectTsStorageDataInfo(kwdbContext_p ctx, std::shared_ptr<TsTable>& ts_table, uint64_t& entity_num,
                                                uint64_t& insert_rows_per_day, uint64_t& max_entities_of_last_subgroup,
                                                bool open_init) {
  // If the cluster setting is disabled, return directly
  // if (UNLIKELY(!cluster_storage_autonomy_enable)) {
  //   LOG_INFO("table[%lu] statistic info does not need to updated because it's currently in manual mode",
  //            ts_table->GetTableId());
  //   return KStatus::SUCCESS;
  // }
  //
  // if (open_init) {
  //   ts_table->GetRootTableManager()->GetStatisticInfo(entity_num, insert_rows_per_day);
  //   if (entity_num == 0 || insert_rows_per_day == 0) {
  //     LOG_INFO("table[%lu] CollectTsStorageDataInfo not initialized, entity_num[%lu], insert_rows_per_day[%lu]",
  //              ts_table->GetTableId(), entity_num, insert_rows_per_day);
  //     return KStatus::SUCCESS;
  //   }
  // } else {
  //   // get entity num
  //   KStatus s = ts_table->GetEntityNum(ctx, &entity_num);
  //   if (s == KStatus::FAIL) {
  //     LOG_ERROR("Failed to CollectTsStorageDataInfo of table[%lu], GetEntityNum failed", ts_table->GetTableId());
  //     return KStatus::FAIL;
  //   }
  //
  //   // get insert rows per day
  //   int64_t end_ts = int64_t(now()) * 1000;
  //   int64_t start_ts = end_ts - 86400 * 1000;
  //   KwTsSpan span{start_ts, end_ts};
  //   s = ts_table->GetDataRowNum(ctx, span, &insert_rows_per_day);
  //   if (s == KStatus::FAIL) {
  //     LOG_ERROR("Failed to CollectTsStorageDataInfo of table[%lu], GetDataRowNum failed", ts_table->GetTableId());
  //     return KStatus::FAIL;
  //   }
  //
  //   // get max entities of last subgroup
  //   max_entities_of_last_subgroup = 0;
  //
  //   if (entity_num == 0 || insert_rows_per_day == 0) {
  //     LOG_WARN("CollectTsStorageDataInfo of table[%lu] failed, no data at this time, entity_num[%lu], "
  //              "insert_rows_per_day[%lu]",
  //               ts_table->GetTableId(), entity_num, insert_rows_per_day);
  //     return KStatus::SUCCESS;
  //   }
  //
  //   LOG_INFO("CollectTsStorageDataInfo of table[%lu] succeeded, entity_num[%lu], insert_rows_per_day[%lu], "
  //            "max_entities_of_last_subgroup[%lu]",
  //             ts_table->GetTableId(), entity_num, insert_rows_per_day, max_entities_of_last_subgroup);
  //   ts_table->GetRootTableManager()->SetStatisticInfo(entity_num, insert_rows_per_day);
  // }
  // // update table statistic info
  // pthread_rwlock_wrlock(&rw_lock_);
  // uint64_t table_id = ts_table->GetTableId();
  // auto it = table_statistic_infos_.find(table_id);
  // if (it != table_statistic_infos_.end()) {
  //   it->second.entity_num = entity_num;
  //   it->second.insert_rows_per_day = insert_rows_per_day;
  // } else {
  //   table_statistic_infos_[table_id] = {insert_rows_per_day, entity_num};
  // }
  // pthread_rwlock_unlock(&rw_lock_);
  // LOG_INFO("Collect table[%lu] TsStorageData info succeeded", table_id);
  return KStatus::SUCCESS;
}

TSStatus TsAutonomyApi::CollectTsStorageDataInfo(TSEngine* engine, TSTableID table_id, uint64_t* entity_num,
                                                 uint64_t* insert_rows_per_day, uint64_t* max_entities_of_last_subgroup) {
  // uint64_t max_blocks_per_segment = 0;
  // uint64_t max_rows_per_block = 0;
  // bool status = GetStorageAutonomyResult(&max_blocks_per_segment, &max_rows_per_block, table_id);
  // LOG_INFO("get table[%lu] autonomy result succeed, max_blocks_per_segment: %lu---", table_id, max_blocks_per_segment);

  // kwdbContext_t context;
  // kwdbContext_p ctx_p = &context;
  // KStatus s = InitServerKWDBContext(ctx_p);
  // if (s != KStatus::SUCCESS) {
  //   return ToTsStatus("InitServerKWDBContext Error!");
  // }
  // std::shared_ptr<TsTable> table;
  // s = engine->GetTsTable(ctx_p, table_id, table);
  // if (s != KStatus::SUCCESS) {
  //   LOG_INFO("The current node does not have the table[%lu], skip autonomy CollectTsStorageDataInfo", table_id);
  //   return ToTsStatus("TSTableAutonomy CollectTsStorageDataInfo Error! no table");
  // }
  // s = TsAutonomyApi::CollectTsStorageDataInfo(ctx_p, table, *entity_num, *insert_rows_per_day,
  //                                             *max_entities_of_last_subgroup, false);
  // if (s != KStatus::SUCCESS) {
  //   LOG_ERROR("table[%lu] autonomy CollectTsStorageDataInfo failed", table_id);
  //   return ToTsStatus("TSTableAutonomy CollectTsStorageDataInfo Error! no data");
  // }
  // LOG_INFO("table[%lu] autonomy CollectTsStorageDataInfo succeeded", table_id);
  return kTsSuccess;
}

void TsAutonomyApi::GetStorageAutonomyResultFromGo(uint32_t& max_blocks_per_segment, uint16_t& max_rows_per_block,
                                                   uint64_t tableId, uint16_t max_entities_of_subgroup,
                                                   uint64_t partition_interval) {
  // pthread_rwlock_rdlock(&rw_lock_);
  // GetStorageAutonomyResult(&max_blocks_per_segment, &max_rows_per_block,
  //                          tableId, max_entities_of_subgroup, partition_interval);
  // LOG_INFO("GetStorageAutonomyResultFromGo, max_blocks_per_segment: %u, max_rows_per_block: %u",
  //          max_blocks_per_segment, max_rows_per_block);
  // pthread_rwlock_unlock(&rw_lock_);
}

}  // namespace kwdbts
