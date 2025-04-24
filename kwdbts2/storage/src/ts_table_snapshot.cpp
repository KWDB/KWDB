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

#include <dirent.h>
#include <iostream>
#include <fstream>
#include "ts_table_snapshot.h"
#include "sys_utils.h"
#include "st_logged_entity_group.h"
#include "st_byrl_table.h"

namespace kwdbts {

TsSnapshotProductor::~TsSnapshotProductor() {
  if (subgrp_data_scan_ctx_.sub_grp_data_iter != nullptr) {
    delete subgrp_data_scan_ctx_.sub_grp_data_iter;
    subgrp_data_scan_ctx_.sub_grp_data_iter  = nullptr;
  }
  if (!scan_over_ || status_ != TsSnapshotStatus::SENDING_ALL_SCHEMAS) {
    LOG_WARN("snapshot[%s] not scan over. status: %d.", Print().c_str(), status_);
  }
}

TsSnapshotConsumer::~TsSnapshotConsumer() {
  if (committed_ == 0) {
    LOG_WARN("snapshot[%s] not committed or rollback.", Print().c_str());
  }
}

std::string TsTableEntitiesSnapshot::Print() {
  char mem[256];
  snprintf(mem, sizeof(mem), "type[%hhu] table[%lu] range{hashpoint[%lu - %lu], ts_span[%ld - %ld]}, id [%lu], rows [%lu]",
    snapshot_info_.type, snapshot_info_.table_id,
    snapshot_info_.begin_hash, snapshot_info_.end_hash,
    snapshot_info_.ts_span.begin, snapshot_info_.ts_span.end, snapshot_info_.id, total_rows_);
  return string(mem);
}

KStatus TsTableEntitiesSnapshot::Init(kwdbContext_p ctx, const TsSnapshotInfo& info) {
  snapshot_info_ = info;
  LOG_DEBUG("Initsnapshot begin. info: %s, snapshot id: %lu", Print().c_str(), info.id);
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductor::nextSubGroup(kwdbContext_p ctx, bool &finished) {
  finished = false;
  subgrp_iter_++;
  std::shared_ptr<kwdbts::TsEntityGroup> cur_entity_group = subgrp_data_scan_ctx_.cur_entity_group;
  // if subgroup in current entitygroup is scaned over.
  if (subgrp_iter_ == egrp_iter_->second.end()) {
    egrp_iter_++;
    if (egrp_iter_ == entity_map_.end()) {
      finished = true;
      return KStatus::SUCCESS;
    }
    KStatus s = snapshot_info_.table->GetEntityGroup(ctx, egrp_iter_->first, &cur_entity_group);
    if (s == KStatus::FAIL || cur_entity_group == nullptr) {
      LOG_ERROR("Get entityGroup[%lu] failed during NextSubGroup.", egrp_iter_->first);
      return KStatus::FAIL;
    }
    // entitygroup has at last one subgroup. so we can GetSubGroup directly.
    subgrp_iter_ = egrp_iter_->second.begin();
    assert(subgrp_iter_ != egrp_iter_->second.end());
  }
  ErrorInfo error_info;
  auto cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(subgrp_iter_->first, error_info, false);
  if (error_info.errcode < 0 || cur_sub_group == nullptr) {
    LOG_ERROR("cannot found subgroup in table [%lu]", snapshot_info_.table->GetTableId());
    return KStatus::FAIL;
  }
  subgrp_data_scan_ctx_.cur_entity_group = cur_entity_group;
  subgrp_data_scan_ctx_.cur_sub_group = cur_sub_group;
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductor::getSchemaInfo(kwdbContext_p ctx, uint32_t schema_version, TSSlice* schema) {
  roachpb::CreateTsTable meta;  // Convert according to schema protobuf
  auto ts_table = meta.mutable_ts_table();
  ts_table->set_ts_table_id(snapshot_info_.table->GetTableId());
  ts_table->set_partition_interval(snapshot_info_.table->GetPartitionInterval());
  ts_table->set_ts_version(schema_version);

  // Get table data schema with all columns in storage.
  auto root_bt = snapshot_info_.table->GetMetricsTableMgr();
  std::vector<AttributeInfo> data_schema;
  auto s = root_bt->GetSchemaInfoIncludeDropped(&data_schema, schema_version);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  std::vector<TagInfo> tag_schema_info;
  std::shared_ptr<TsEntityGroup> cur_entity_group;
  s = snapshot_info_.table->GetEntityGroup(ctx, default_entitygroup_id_in_dist_v2, &cur_entity_group);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = cur_entity_group->GetTagSchemaIncludeDroppedByVersion(&tag_schema_info, schema_version);
  if (s != KStatus::SUCCESS) {
    return s;
  }
#ifdef K_DEBUG
  for (const auto& it : tag_schema_info) {
    LOG_DEBUG("getSchemaInfo table_id: %lu version: %u tag_id: %u flag: %u",
         snapshot_info_.table->GetTableId(), schema_version, it.m_id, it.m_flag);
  }
#endif
  // Use data schema and tag schema to construct meta.
  s = snapshot_info_.table->GenerateMetaSchema(ctx, &meta, data_schema, tag_schema_info, schema_version);
  if (s == KStatus::FAIL) {
    LOG_ERROR("generateMetaSchema failed during getSchemaInfo.%s.", Print().c_str());
    return s;
  }

  string meta_str;
  if (!meta.SerializeToString(&meta_str)) {
    LOG_ERROR("SerializeToString failed during getSchemaInfo.%s.", Print().c_str());
    return KStatus::FAIL;
  }
  schema->len = meta_str.size();
  schema->data = reinterpret_cast<char*>(malloc(schema->len));
  memcpy(schema->data, meta_str.data(), schema->len);
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductor::nextVersionSchema(kwdbContext_p ctx, TSSlice* schema) {
  if (schema_versions_.size() == 0) {
    snapshot_info_.table->GetMetricsTableMgr()->GetAllVersion(&schema_versions_);
  }
  if (cur_schema_version_idx_ >= schema_versions_.size()) {
    schema->data = nullptr;
    schema->len = 0;
    return KStatus::SUCCESS;
  }
  int current_version = schema_versions_[cur_schema_version_idx_];
  cur_schema_version_idx_++;
  return getSchemaInfo(ctx, current_version, schema);
}

KStatus TsSnapshotProductor::Init(kwdbContext_p ctx, const TsSnapshotInfo& info) {
  TsTableEntitiesSnapshot::Init(ctx, info);
  std::vector<std::pair<int, EntityResultIndex>> entity_with_tag_rowid;
  KStatus s = info.table->GetEntityIndexWithRowNum(ctx, info.begin_hash, info.end_hash, entity_with_tag_rowid);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not found entity indexs");
    return s;
  }
  for (auto& item : entity_with_tag_rowid) {
    entity_map_[item.second.entityGroupId][item.second.subGroupId].push_back(
                          {item.second.entityId, std::move(TagRowNum{item.second.ts_version, uint32_t(item.first)})});
  }
  egrp_iter_ = entity_map_.begin();
  if (egrp_iter_ == entity_map_.end()) {
    // maybe convert empty range from this node to other.
    LOG_DEBUG("cannot found any entity in table [%lu], moving empty range?", info.table->GetTableId());
    scan_over_ = true;
    status_ = TsSnapshotStatus::SENDING_ALL_SCHEMAS;
    return KStatus::SUCCESS;
  }
  subgrp_iter_ = egrp_iter_->second.begin();
  assert(subgrp_iter_ != egrp_iter_->second.end());
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  s = snapshot_info_.table->GetEntityGroup(ctx, egrp_iter_->first, &cur_entity_group);
  if (s == KStatus::FAIL || cur_entity_group == nullptr) {
    LOG_ERROR("Get entityGroup[%lu] failed during NextSubGroup.", egrp_iter_->first);
    return KStatus::FAIL;
  }
  ErrorInfo error_info;
  auto cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(subgrp_iter_->first, error_info, false);
  if (error_info.errcode < 0 || cur_sub_group == nullptr) {
    LOG_ERROR("cannot found subgroup in table [%lu]", snapshot_info_.table->GetTableId());
    return KStatus::FAIL;
  }
  // generate parameters for iterator
  auto root_bt = snapshot_info_.table->GetMetricsTableMgr();
  // using the latest used schema version.
  using_storage_schema_version_ = root_bt->GetTableVersionOfLatestData();
  s = root_bt->GetSchemaInfoIncludeDropped(&metrics_schema_include_dropped_, using_storage_schema_version_);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = root_bt->GetSchemaInfoExcludeDropped(&pl_metric_attribute_info_, using_storage_schema_version_);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // todo(liangbo01) tag scheam version also need certain version.
  auto result_tag_version_obj = cur_entity_group->GetSubEntityGroupNewTagbt()
                         ->GetTagTableVersionManager()->GetVersionObject(using_storage_schema_version_);
  if (nullptr == result_tag_version_obj) {
    return KStatus::FAIL;
  }
  tag_attribute_info_exclude_dropped_ = result_tag_version_obj->getExcludeDroppedSchemaInfos();
  tag_schema_include_dropped_ = result_tag_version_obj->getIncludeDroppedSchemaInfos();
  for (int tag_idx = 0; tag_idx < tag_attribute_info_exclude_dropped_.size(); ++tag_idx) {
    result_scan_tag_idxs_.emplace_back(result_tag_version_obj->getValidSchemaIdxs()[tag_idx]);
  }

  k_uint32 num_col = pl_metric_attribute_info_.size();
  auto& actual_cols = root_bt->GetIdxForValidCols(using_storage_schema_version_);
  for (int i = 0; i < num_col; i++) {
    kw_scan_cols_.push_back(i);
    ts_scan_cols_.emplace_back(actual_cols[i]);
  }
  ts_spans_.push_back(snapshot_info_.ts_span);
  TsPartitonIteratorParams params{egrp_iter_->first, subgrp_iter_->first, {},
     ts_spans_, kw_scan_cols_, ts_scan_cols_, metrics_schema_include_dropped_};
  params.entity_ids.reserve(subgrp_iter_->second.size());
  for (auto& item : subgrp_iter_->second) {
    params.entity_ids.push_back(item.first);
  }
  // fill context of subgroup scan operation.
  subgrp_data_scan_ctx_.cur_entity_group = cur_entity_group;
  subgrp_data_scan_ctx_.cur_sub_group = cur_sub_group;
  subgrp_data_scan_ctx_.entity_first_batch = {nullptr, 0, false};
  subgrp_data_scan_ctx_.sub_grp_data_iter = new TsSubGroupIteratorEntityBased(cur_sub_group, params);
  subgrp_data_scan_ctx_.cur_block_entity_info = EntityResultIndex(0, 0, 0);
  LOG_DEBUG("create snapshot productor success. with info %s", Print().c_str());
  return KStatus::SUCCESS;
}

/**
 * data struct is :  data_type (2 bytes) + sn(4 bytes) + data
 */
KStatus TsSnapshotProductor::NextData(kwdbContext_p ctx, TSSlice* data) {
  data->len = 0;
  data->data = nullptr;
  TSSlice payload_data{nullptr, 0};
  KStatus s;
  TsSnapshotDataType type;
  switch (status_) {
  case TsSnapshotStatus::SENDING_USING_SCHEMA:
    // first data, we wiil send schema info to desc node. as default rule.
    LOG_DEBUG("sending table schema to desc. size[%lu]", data->len);
    type = TsSnapshotDataType::STORAGE_SCHEMA;
    // need include normal tag index.
    s = getSchemaInfo(ctx, using_storage_schema_version_, &payload_data);
    status_ = TsSnapshotStatus::SENDING_METRIC;
    break;
  case TsSnapshotStatus::SENDING_METRIC:
    LOG_DEBUG("sending data to desc. size[%lu]", data->len);
    s = nextSerializedData(ctx, &payload_data, &type);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("nextSerializedData metric data failed. %s", Print().c_str());
      return s;
    }
    if (payload_data.data == nullptr) {
      status_ = TsSnapshotStatus::SENDING_ALL_SCHEMAS;
    } else {
      break;
    }
  case TsSnapshotStatus::SENDING_ALL_SCHEMAS:
    type = TsSnapshotDataType::STORAGE_SCHEMA;
    s = nextVersionSchema(ctx, &payload_data);
    break;
  default:
    break;
  }
  if (s != SUCCESS) {
    LOG_ERROR("get next data failed.");
    return s;
  }
  if (payload_data.data != nullptr) {
    SnapshotPayloadData cur_payload(type, ++cur_sn_, payload_data);
    *data = cur_payload.GenData();
    free(payload_data.data);
  }
  LOG_DEBUG("snapshot[%s] NextData. size[%lu]", Print().c_str(), data->len);
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductor::nextSubGrpDataIter(kwdbContext_p ctx, bool &finished) {
  finished = false;
  bool is_finished = false;
  KStatus s =  nextSubGroup(ctx, is_finished);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not get next subgroup in entitygroup [%lu]", egrp_iter_->first);
    return s;
  }
  if (is_finished) {
    finished = true;
    return KStatus::SUCCESS;
  }

  TsPartitonIteratorParams params{egrp_iter_->first, subgrp_iter_->first, {},
     ts_spans_, kw_scan_cols_, ts_scan_cols_, metrics_schema_include_dropped_};
  params.entity_ids.reserve(subgrp_iter_->second.size());
  for (auto& item : subgrp_iter_->second) {
    params.entity_ids.push_back(item.first);
  }
  if (subgrp_data_scan_ctx_.sub_grp_data_iter != nullptr) {
    delete subgrp_data_scan_ctx_.sub_grp_data_iter;
    subgrp_data_scan_ctx_.sub_grp_data_iter = nullptr;
  }
  // filled at NextSubGroup
  // subgrp_data_scan_ctx_.cur_entity_group = cur_entity_group;
  // subgrp_data_scan_ctx_.cur_sub_group = cur_sub_group;
  subgrp_data_scan_ctx_.entity_first_batch = {nullptr, 0};
  subgrp_data_scan_ctx_.sub_grp_data_iter =
         new TsSubGroupIteratorEntityBased(subgrp_data_scan_ctx_.cur_sub_group, params);
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductor::nextSerializedData(kwdbContext_p ctx, TSSlice* data, TsSnapshotDataType* type) {
  data->data = nullptr;
  data->len = 0;
  std::list<SnapshotBlockDataInfo> batch_list;
  Defer defer{[&]() {
    for (auto& res : batch_list) {
      delete res.res;
      delete res.blk_item;
    }
  }};
  while (true) {
    if (scan_over_) {
      return KStatus::SUCCESS;
    }
    bool need_change_subgrp;
    KStatus s = getEnoughRows(ctx, &batch_list, &need_change_subgrp);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("next failed during getEnoughRows, subgroup_id[%u]", subgrp_iter_->first);
      return s;
    }
    if (batch_list.size() > 0) {
      s = SerializeData(ctx, batch_list, data, type);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("next failed during Serialize payload Data, subgroup_id[%u]", subgrp_iter_->first);
        return s;
      }
    }
    if (need_change_subgrp) {
      bool all_over = false;
      s = nextSubGrpDataIter(ctx, all_over);
      if (s != KStatus::SUCCESS) {
        free(data->data);
        *data = {nullptr, 0};
        LOG_ERROR("can not find next entity in sub group iterator.");
        return s;
      }
      if (all_over) {
        scan_over_ = true;
      }
    }
    if (data->data != nullptr) {
      LOG_DEBUG("snapshot[%s] get nextdata blocknum[%lu], size[%lu]", Print().c_str(), batch_list.size(), data->len);
      return KStatus::SUCCESS;
    }
  }
  return KStatus::FAIL;
}

KStatus TsSnapshotConsumer::Init(kwdbContext_p ctx, const TsSnapshotInfo& info) {
  KStatus s = TsTableEntitiesSnapshot::Init(ctx, info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsTableEntitiesSnapshot initialize failed.");
    return KStatus::FAIL;
  }
  if (snapshot_info_.table == nullptr) {
    LOG_DEBUG("create consumer snapshot[%s] success. table not exists. we will create one later.", Print().c_str());
    return KStatus::SUCCESS;
  }
  uint64_t row_num_bef = 0;
  s = snapshot_info_.table->GetRangeRowCount(ctx, info.begin_hash, info.end_hash, info.ts_span, &row_num_bef);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("snapshot[%s] GetRangeRowCount failed.", Print().c_str());
    return KStatus::FAIL;
  }
  if (row_num_bef > 0) {
    LOG_WARN("snapshot[%s] range has data rows [%lu], need delete first.", Print().c_str(), row_num_bef);
    s = snapshot_info_.table->DeleteTotalRange(ctx, info.begin_hash, info.end_hash, info.ts_span, 1);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("snapshot[%s] DeleteTotalRange failed.", Print().c_str());
      return KStatus::FAIL;
    }
  }
  s = beginMtrOfEntityGroup(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("beginMtrOfEntityGroup initialize failed.");
    return KStatus::FAIL;
  }
  LOG_DEBUG("create snapshot consumer success. with info %s", Print().c_str());
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumer::beginMtrOfEntityGroup(kwdbContext_p ctx) {
  KStatus s = snapshot_info_.table->GetEntityIndex(ctx, snapshot_info_.begin_hash,
                                    snapshot_info_.end_hash, entities_result_idx_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not get entity index list.");
    return s;
  }
  // default entitygroup use for storing new entities.
  entities_result_idx_.push_back({default_entitygroup_id_in_dist_v2, 0, 0, nullptr});
  // allocate mtr id of all entitygroups , for snapshot writing atomic.
  std::shared_ptr<kwdbts::TsEntityGroup> cur_egrp;
  for (auto& entity : entities_result_idx_) {
    if (entity_grp_mtr_id_.find(entity.entityGroupId) != entity_grp_mtr_id_.end()) {
      continue;
    }
    s = snapshot_info_.table->GetEntityGroup(ctx, entity.entityGroupId, &cur_egrp);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("can not get entitygroup[%lu]", entity.entityGroupId);
      return s;
    }
    auto entity_group = dynamic_cast<LoggedTsEntityGroup*>(cur_egrp.get());
    if (entity_group == nullptr) {
      // LOG_WARN("The TS mini-transaction support is disabled, table id: %lu", snapshot_info_.table->GetTableId())
      // if no open WAL, there is no mtr id.
      break;
    }
    uint64_t mtr_id;
    SnapshotRange range{snapshot_info_.table_id,
                        HashIdSpan{snapshot_info_.begin_hash, snapshot_info_.end_hash},
                        snapshot_info_.ts_span};
    s = entity_group->BeginSnapshotMtr(ctx, entity.entityGroupId, snapshot_info_.id, range, mtr_id);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot begin mtr for entitygroup[%lu]", entity.entityGroupId);
      return s;
    }
    entity_grp_mtr_id_[entity.entityGroupId] = mtr_id;
  }
  LOG_DEBUG("begin mtr for entitygroup num [%lu] success.", entity_grp_mtr_id_.size());
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumer::setMtrOver(kwdbContext_p ctx, bool commit) {
  committed_ = commit ? 1 : -1;
  KStatus s;
#ifdef K_DEBUG
  {
    uint64_t row_num = 0;
    snapshot_info_.table->GetRangeRowCount(ctx, snapshot_info_.begin_hash, snapshot_info_.end_hash,
                                          snapshot_info_.ts_span, &row_num);
    LOG_DEBUG("snapshotmtrover: %s, query range count: %lu", Print().c_str(), row_num);
    if (row_num != total_rows_) {
      LOG_ERROR("snapshotmtrover failed. rows: %lu, snapshot:%lu", row_num, total_rows_);
    }
  }
#endif
  // not open wal, if rollback we need delete data by delete function.
  if (entity_grp_mtr_id_.size() == 0) {
    if (commit) {
      LOG_INFO("flush data for table %lu", snapshot_info_.table->GetTableId());
      return snapshot_info_.table->CreateCheckpoint(ctx);
    }
    HashIdSpan hash_span{snapshot_info_.begin_hash, snapshot_info_.end_hash};
    std::vector<KwTsSpan> ts_spans;
    ts_spans.push_back(snapshot_info_.ts_span);
    uint64_t count;
    std::unordered_map<uint64_t, bool> entity_grps;
    for (auto& entity_grp : entities_result_idx_) {
      if (entity_grps.find(entity_grp.entityGroupId) != entity_grps.end()) {
        continue;
      }
      entity_grps[entity_grp.entityGroupId] = true;
    }
    for (auto& entity_grp_id : entity_grps) {
      snapshot_info_.table->DeleteRangeData(ctx, entity_grp_id.first, hash_span, ts_spans, &count, 0);
    }
    LOG_DEBUG("snapshot[%s] consumer over, result: %s", Print().c_str(), commit ? "success" : "rollback");
    return KStatus::SUCCESS;
  }

  // open wal, using transaction`s  commit and rollback.
  std::shared_ptr<kwdbts::TsEntityGroup> cur_egrp;
  for (auto& egrp : entity_grp_mtr_id_) {
    s = snapshot_info_.table->GetEntityGroup(ctx, egrp.first, &cur_egrp);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("can not get entitygroup[%lu]", egrp.first);
      return s;
    }
    auto entity_group = static_pointer_cast<LoggedTsEntityGroup>(cur_egrp);
    if (entity_group == nullptr) {
      LOG_ERROR("The TS mini-transaction support is disabled, table id: %lu",
                snapshot_info_.table->GetTableId())
      // if no open WAL, there is no mtr id.
      return KStatus::FAIL;
    }
    if (commit) {
      s = entity_group->MtrCommit(ctx, egrp.second);
    } else {
      s = entity_group->MtrRollback(ctx, egrp.second);
    }
  }
  return s;
}

KStatus TsSnapshotConsumer::WriteData(kwdbContext_p ctx, TSSlice data) {
  SnapshotPayloadData cur_data = SnapshotPayloadData::ParseData(data);
  if (cur_sn_ == cur_data.sn) {
    LOG_DEBUG("snapshot[%s] Write duplicate data, data size [%lu]", Print().c_str(), data.len);
    return KStatus::SUCCESS;
  }
  if (cur_sn_ != 0 && cur_sn_ + 1 != cur_data.sn) {
    LOG_DEBUG("snapshot[%s] currnt sn[%u] not relative with paylaod sn[%u]", Print().c_str(), cur_sn_, cur_data.sn);
    return KStatus::FAIL;
  }
  cur_sn_ = cur_data.sn;
  switch (cur_data.type) {
  case TsSnapshotDataType::STORAGE_SCHEMA :
    // need include normal tag index.
    LOG_DEBUG("snapshot[%s] Writeschema, data size [%lu]", Print().c_str(), data.len);
    return updateTableSchema(ctx, cur_data.data_part);
    break;
  case TsSnapshotDataType::PAYLOAD_COL_BASED_DATA :
  case TsSnapshotDataType::BLOCK_ITEM_BASED_DATA :
    LOG_DEBUG("snapshot[%s] Write metric data size [%lu]", Print().c_str(), data.len);
    return WriteMetricData(ctx, cur_data.data_part, cur_data.type);
    break;
  default:
    LOG_ERROR("snapshot[%s] cannot parse payload type [%hhu]", Print().c_str(), cur_data.type);
    break;
  }
  return KStatus::FAIL;
}

KStatus TsSnapshotConsumer::updateTableSchema(kwdbContext_p ctx, TSSlice schema) {
  roachpb::CreateTsTable meta;
  if (!meta.ParseFromString({schema.data, schema.len})) {
    LOG_ERROR("snapshot[%s] Parse schema From String failed during updateTableSchema.", Print().c_str());
    return KStatus::FAIL;
  }
  auto s = snapshot_info_.table->AddSchemaVersion(ctx, &meta, &version_schema_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("snapshot[%s] failed during upper version.", Print().c_str());
    return s;
  }

    LOG_INFO("updateTableSchema TsTable[%lu], starting to create NTAG index.", snapshot_info_.table->GetTableId());
    for (int i = 0; i < meta.index_info_size(); i ++) {
        std::vector<uint32_t> col_ids;
        for (int idx = 0; idx < meta.index_info(i).col_ids_size(); idx++) {
            col_ids.push_back(meta.index_info(i).col_ids(idx));
        }
        s = snapshot_info_.table->createNormalTagIndex(ctx, 0, meta.index_info(i).index_id(), meta.ts_table().ts_version(),
                                        meta.ts_table().ts_version(), col_ids);
        if (s != KStatus::SUCCESS) {
            LOG_ERROR("Failed to create ntag hash index, index id:%d.", meta.index_info(i).index_id())
            return s;
        }
    }
    LOG_INFO("updateTableSchema TsTable[%lu] end.", snapshot_info_.table->GetTableId());


  LOG_DEBUG("updateTableSchema succes..");
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumerByPayload::WriteMetricData(kwdbContext_p ctx, TSSlice data, TsSnapshotDataType type) {
  if (type == TsSnapshotDataType::PAYLOAD_COL_BASED_DATA) {
    return writeDataWithPayload(ctx, data);
  }
  LOG_FATAL("cannot parse payload type of [%hhu] in Payload Type snapshot Consumer.", type);
  return KStatus::FAIL;
}

KStatus TsSnapshotConsumerByPayload::writeDataWithPayload(kwdbContext_p ctx, TSSlice data) {
  // get primary key from payload memory.
  const TSSlice primary_key = Payload::GetPrimaryKeyFromPayload(&data);
  uint64_t cur_egrp_id;
  KStatus s = snapshot_info_.table->GetEntityGrpByPriKey(ctx, primary_key, &cur_egrp_id);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not get entitygroup id by primary key.");
    return s;
  }
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  total_rows_ += Payload::GetRowCountFromPayload(&data);
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  LOG_DEBUG("writeDataWithPayload table_id: %lu ts_version: %u", snapshot_info_.table->GetTableId(),
            Payload::GetTsVersionFromPayload(&data));
  return snapshot_info_.table->PutDataWithoutWAL(ctx, cur_egrp_id , &data, 1, entity_grp_mtr_id_[cur_egrp_id],
                                                 &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, DedupRule::KEEP);
}

KStatus TsSnapshotConsumerByPayload::Init(kwdbContext_p ctx, const TsSnapshotInfo& info) {
  if (TsSnapshotConsumer::Init(ctx, info) != KStatus::SUCCESS) {
    LOG_DEBUG("initilize[%s] failed at payload consumer init.", Print().c_str());
    return KStatus::FAIL;
  }

  LOG_DEBUG("create snapshot payload consumer success. with info %s", Print().c_str());
  return KStatus::SUCCESS;
}

TsSnapshotConsumerByBlock::~TsSnapshotConsumerByBlock() {
  for (auto& tmp : tmp_partition_tables_) {
    ErrorInfo err_info;
    string tmp_path = tmp.second->GetPath();
    tmp.second->remove(true);
    if (!Remove(tmp_path, err_info)) {
      LOG_ERROR("can not remove directory %s, err info: %s", tmp_path.c_str(), err_info.errmsg.c_str());
    }
    delete tmp.second;
  }
  tmp_partition_tables_.clear();
}

KStatus TsSnapshotConsumerByBlock::Init(kwdbContext_p ctx, const TsSnapshotInfo& info) {
  if (TsSnapshotConsumer::Init(ctx, info) != KStatus::SUCCESS) {
    LOG_DEBUG("initilize failed at full block consumer init.[%s]", Print().c_str());
    return KStatus::FAIL;
  }
  LOG_DEBUG("create snapshot full block consumer success. with info %s", Print().c_str());
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumerByBlock::WriteMetricData(kwdbContext_p ctx, TSSlice data, TsSnapshotDataType type) {
  if (data.data == nullptr || data.len == 0) {
    LOG_WARN("writing empty snapshot data. sn [%u]", cur_sn_);
    return KStatus::SUCCESS;
  }
  if (type == TsSnapshotDataType::BLOCK_ITEM_BASED_DATA) {
    return writeBlocks(ctx, data);
  }
  LOG_FATAL("cannot parse payload type of [%hhu] in Payload Type snapshot Consumer.", type);
  return KStatus::FAIL;
}

KStatus TsSnapshotConsumerByBlock::GenEmptyTagForPrimaryKey(kwdbContext_p ctx,
                                                            uint64_t entity_grp_id, TSSlice primary_key) {
  std::vector<AttributeInfo> data_schema;
  RangeGroup range{entity_grp_id, 1};
  std::vector<TagInfo > tag_schema;
  snapshot_info_.table->GetDataSchemaExcludeDropped(ctx, &data_schema);
  snapshot_info_.table->GetTagSchema(ctx, range, &tag_schema);
  PayloadBuilder pl_builder(tag_schema, data_schema);
  for (size_t i = 0; i < tag_schema.size(); i++) {
    // Different tags are stored differently in payload
    if (tag_schema[i].isPrimaryTag()) {
      pl_builder.SetTagValue(i, primary_key.data, primary_key.len);
      break;
    }
  }
  // construct payload
  TSSlice tag_data;
  Defer defer{([&]{
    free(tag_data.data);
  })};
  if (!pl_builder.Build(&tag_data)) {
    LOG_ERROR("Payload build failed when build snapshot, snapshot id[%lu].", snapshot_info_.id);
    return KStatus::FAIL;
  }
  std::shared_ptr<kwdbts::TsEntityGroup> entity_group{nullptr};
  snapshot_info_.table->GetEntityGroup(ctx, entity_grp_id, &entity_group);
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  KStatus s = entity_group->PutDataWithoutWAL(ctx, tag_data, 1, &inc_entity_cnt, &inc_unordered_cnt,
                                              &dedup_result, DedupRule::KEEP);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("tag not found, so create one, created failed.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumerByBlock::CopyBlockToPartitionTable(kwdbContext_p ctx, TsTimePartition* p_table,
                                                              EntityID entity_id, const TsBlockFullData& block) {
  BlockItem *blk_item;
  int err_code = p_table->allocateBlockItem(entity_id, &blk_item, version_schema_->GetVersionNUm());
  if (err_code < 0) {
    LOG_ERROR("cannot allocate new block item");
    return KStatus::FAIL;
  }
  blk_item->CopyMetricMetaInfo(block.block_item);
  blk_item->read_only = true;
  // todo(liangbo01) we will compress segment files that is full, for reserving disk space.
  auto segment_tbl = p_table->getSegmentTable(blk_item->block_id);
  if (nullptr == segment_tbl) {
    LOG_ERROR("cannot open segment table. for block [%u]", blk_item->block_id);
    return KStatus::FAIL;
  }

  err_code = segment_tbl->CopyColBlocks(blk_item, block, snapshot_info_.ts_span);
  if (err_code < 0) {
    LOG_ERROR("failed during CopyColBlocks");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsSnapshotConsumerByBlock::writeBlocks(kwdbContext_p ctx, TSSlice data) {
  SnapshotPayloadDataBlockPart cur_data = SnapshotPayloadDataBlockPart::ParseData(data);
  total_rows_ += cur_data.block_has_max_rows_num * cur_data.res_list->size();
  std::shared_ptr<kwdbts::TsEntityGroup> entity_group{nullptr};
  uint64_t entity_grp_id;
  KStatus s = snapshot_info_.table->GetEntityGrpByPriKey(ctx, cur_data.primary_key, &entity_grp_id);
  snapshot_info_.table->GetEntityGroup(ctx, entity_grp_id, &entity_group);
  EntityID entity_id;
  SubGroupID sub_grp_id;
  bool ret = entity_group->GetSubEntityGroupNewTagbt()->hasPrimaryKey(cur_data.primary_key.data,
                                                            cur_data.primary_key.len, entity_id, sub_grp_id);
  if (!ret) {
    // todo(liangbo01): just for test. when  tag range all over cluster, here will return fail.
    LOG_WARN("cannot found entitygroup for primary key [%s], we will create one with empty tag.",
              cur_data.primary_key.data);
    s = GenEmptyTagForPrimaryKey(ctx, entity_grp_id, cur_data.primary_key);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GenEmptyTagForPrimaryKey failed.");
      return s;
    }
    ret = entity_group->GetSubEntityGroupNewTagbt()->hasPrimaryKey(cur_data.primary_key.data, cur_data.primary_key.len,
                                                                    entity_id, sub_grp_id);
    if (!ret) {
      LOG_ERROR("cannot found entity for primary key [%s].", cur_data.primary_key.data);
      return s;
    }
  }
  ErrorInfo err_info;
  auto sub_grp = entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp_id, err_info);
  if (sub_grp == nullptr) {
    LOG_ERROR("cannot found sugrp %s.", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  const std::vector<AttributeInfo>& data_schema = version_schema_->getSchemaInfoExcludeDropped();
  size_t ts_block_header_size = MMapSegmentTable::GetBlockHeaderSize(cur_data.block_has_max_rows_num,
                                                                      sizeof(data_schema[0].size));
  timestamp64 first_max_ts;

  // todo (liangbo01)  just in case all partition interval is same. and all block max rows is same.
  for (auto& blockitem : *(cur_data.res_list)) {
    char* first_ts_addr = blockitem->col_block_addr[0].data + ts_block_header_size;
    // todo(liangbo01) we will using max min ts in block to check if all rows in bloc belong to same partition later.
    timestamp64 cur_ts = KInt64(first_ts_addr);
    auto p_time = sub_grp->PartitionTime(convertTsToPTime(cur_ts, (DATATYPE)(data_schema[0].type)), first_max_ts);
    std::string key = intToString(entity_grp_id) + "_" + intToString(sub_grp_id) + "_" + convertTsToDirectoryName(p_time);
    std::string tmp_partition_name = key + "_" + intToString(snapshot_info_.id) + "/";
    auto cur_tmp_table = tmp_partition_tables_[key];
    if (cur_tmp_table == nullptr) {
      bool new_one = false;
      auto partition_table = sub_grp->CreateTmpPartitionTable(tmp_partition_name, 1, new_one);
      if (partition_table == nullptr) {
        LOG_ERROR("can not create or open table %s", key.c_str());
        return KStatus::FAIL;
      }
      if (new_one) {
        // write temp directory path into WAL, for WAL undo while restarting service
        auto log_entity_grp = dynamic_cast<LoggedTsEntityGroup*>(entity_group.get());
        if (log_entity_grp != nullptr) {
          s = log_entity_grp->WriteTempDirectoryLog(ctx, entity_grp_mtr_id_[entity_grp_id], partition_table->GetPath());
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("writeblock failed during write to WAL log");
            return s;
          }
        }
      } else {
        LOG_ERROR("this can not hanppend. if happend .need check why?");
        return KStatus::FAIL;
      }
      tmp_partition_tables_[key] = partition_table;
      cur_tmp_table = partition_table;
    }
    s = CopyBlockToPartitionTable(ctx, cur_tmp_table, entity_id, *blockitem);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("CopyBlockToPartitionTable failed.");
      return s;
    }
  }
  return KStatus::SUCCESS;
}
// transcation commit, with WAL of this snapshot commit.
KStatus TsSnapshotConsumerByBlock::WriteCommit(kwdbContext_p ctx) {
  for (auto& tmp : tmp_partition_tables_) {
    std::string p_inof = tmp.first;
    auto p = p_inof.find('_', 0);
    uint64_t entity_grp_id = atol(p_inof.substr(0, p).c_str());
    auto p1 = p_inof.find('_', p + 1);
    SubGroupID sub_grp_id = atol(p_inof.substr(p + 1, p1).c_str());
    auto p2 = p_inof.find('_', p1 + 1);
    auto p_string = p_inof.substr(p1 + 1, p2);
    timestamp64 p_time = convertToTimestamp(p_string);
    std::shared_ptr<kwdbts::TsEntityGroup> entity_group{nullptr};
    snapshot_info_.table->GetEntityGroup(ctx, entity_grp_id, &entity_group);
    if (entity_group == nullptr) {
      LOG_ERROR("GetEntityGroup [%lu] failed", entity_grp_id);
      return KStatus::FAIL;
    }
    ErrorInfo err_info;
    auto sub_grp = entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp_id, err_info);
    if (sub_grp == nullptr) {
      LOG_ERROR("GetSubGroup [%u] failed", sub_grp_id);
      return KStatus::FAIL;
    }
    auto partition_table = sub_grp->GetPartitionTable(p_time, err_info, true);
    Defer defer{[&]() {
      ReleaseTable(partition_table);
    }};
    if (partition_table == nullptr) {
      LOG_ERROR("GetPartitionTable [%s] failed", p_string. c_str());
      return KStatus::FAIL;
    }
    auto ok = partition_table->JoinOtherPartitionTable(tmp.second);
    if (!ok) {
      LOG_ERROR("JoinOtherPartitionTable [%s] failed", p_string. c_str());
      return KStatus::FAIL;
    }
    tmp.second->remove(true);
    if (!Remove(tmp.second->GetPath())) {
      LOG_ERROR("tmp path [%s] remove failed.", tmp.second->GetPath().c_str());
    }
    delete tmp.second;
  }
  tmp_partition_tables_.clear();
  return setMtrOver(ctx, true);
}

// transcation rollback, using WAL rollback function
KStatus TsSnapshotConsumerByBlock::WriteRollback(kwdbContext_p ctx) {
  // clear tmp tables of this snapshot.
  for (auto& tmp : tmp_partition_tables_) {
    ErrorInfo err_info;
    string tmp_path = tmp.second->GetPath();
    tmp.second->remove();
    if (!Remove(tmp_path, err_info)) {
      LOG_ERROR("can not remove directory %s, err info: %s", tmp_path.c_str(), err_info.errmsg.c_str());
      return KStatus::FAIL;
    }
    delete tmp.second;
  }
  tmp_partition_tables_.clear();
  return setMtrOver(ctx, false);
}

int SnapshotFactory::factory_type_ = 1;
TsPayloadSnapshotFactory SnapshotFactory::payload_inst_;
TsFullBlockSnapshotFactory SnapshotFactory::block_inst_;
SnapshotFactory* SnapshotFactory::Get() {
  if (factory_type_ == 1) {
    return &payload_inst_;
  }
  return &block_inst_;
}

}  //  namespace kwdbts
