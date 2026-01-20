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

#include <map>
#include <memory>
#include <list>
#include <utility>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ts_table_v2_impl.h"
#include "kwdb_type.h"
#include "ts_tag_iterator_v2_impl.h"
#include "ts_engine.h"
#include "ts_vgroup.h"
#include "ts_iterator_v2_impl.h"
#include "ts_ts_lsn_span_utils.h"

extern bool g_go_start_service;

namespace kwdbts {

TsTableV2Impl::~TsTableV2Impl() {
  // todo(liangbo01)  no implemented.
  // if (table_dropped_.load()) {
  //   table_schema_mgr_->RemoveAll();
  //   // metric data cannot remove now, clear by vacuum.
  // }
}

void TsTableV2Impl::SetDropped() {
  table_dropped_.store(true);
  table_schema_mgr_->SetDropped();
}

bool TsTableV2Impl::IsDropped() {
  return table_dropped_.load();
}

KStatus TsTableV2Impl::PutData(kwdbContext_p ctx, TsVGroup* v_group, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, TSEntityID entity_id, uint32_t* inc_unordered_cnt,
                          DedupResult* dedup_result, const DedupRule& dedup_rule, bool write_wal) {
  assert(payload_num == 1);
  auto version = TsRawPayload::GetTableVersionFromSlice(*payload);
  const std::vector<AttributeInfo>* metric_schema{nullptr};
  table_schema_mgr_->GetColumnsExcludeDroppedPtr(&metric_schema, version);
  uint8_t payload_data_flag = TsRawPayload::GetRowTypeFromSlice(*payload);
  if (payload_data_flag == DataTagFlag::TAG_ONLY) {
    LOG_DEBUG("tag only. so no need putdata.");
    return KStatus::SUCCESS;
  }
  auto primary_key = TsRawPayload::GetPrimaryKeyFromSlice(*payload);
  auto s = v_group->PutData(ctx, table_schema_mgr_, mtr_id, &primary_key, entity_id, payload, write_wal);
  if (s != KStatus::SUCCESS) {
    // todo(liangbo01) if failed. should we need rollback all inserted data?
    LOG_ERROR("putdata failed. table id[%lu], group id[%u]", table_id_, v_group->GetVGroupID());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::PutData(kwdbContext_p ctx, TsVGroup* v_group, TsRawPayload& p,
                  TSEntityID entity_id, uint64_t mtr_id, uint32_t* inc_unordered_cnt,
                  DedupResult* dedup_result, const DedupRule& dedup_rule, bool write_wal) {
  uint8_t payload_data_flag = p.GetRowType();
  if (payload_data_flag == DataTagFlag::TAG_ONLY) {
    LOG_DEBUG("tag only. so no need putdata.");
    return KStatus::SUCCESS;
  }
  auto primary_key = p.GetPrimaryTag();
  auto payload = p.GetPayload();
  auto version = p.GetTableVersion();
  auto s = v_group->PutData(ctx, table_schema_mgr_, mtr_id, &primary_key, entity_id, &payload, write_wal);
  if (s != KStatus::SUCCESS) {
    // todo(liangbo01) if failed. should we need rollback all inserted data?
    LOG_ERROR("putdata failed. table id[%lu], group id[%u]", table_id_, v_group->GetVGroupID());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::unordered_set<uint32_t> hps,
                                BaseEntityIterator** iter, k_uint32 table_version) {
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  TagIteratorV2Impl* tag_iter;
  if (!EngineOptions::isSingleNode()) {
    tag_iter = new TagIteratorV2Impl(tag_table, table_version, scan_tags, hps);
  } else {
    tag_iter = new TagIteratorV2Impl(tag_table, table_version, scan_tags);
  }
  if (KStatus::SUCCESS != tag_iter->Init()) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}


KStatus TsTableV2Impl::GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                                 const std::vector<uint64_t/*index_id*/> &tags_index_id,
                                 const std::vector<void*> tags,
                                 TSTagOpType op_type,
                                 const std::vector<uint32_t>& scan_tags,
                                 const std::unordered_set<uint32_t> &hps,
                                 std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                                 uint32_t table_version) {
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  if (tag_table->GetEntityIdList(primary_tags, tags_index_id, tags, op_type, scan_tags, hps,
                                    entity_id_list, res, count, table_version) < 0) {
    LOG_ERROR("GetEntityIdList error ");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
                            const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count,
                            uint32_t table_version) {
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  if (tag_table->GetTagList(ctx, entity_id_list, scan_tags, res, count,
                            table_version) < 0) {
    LOG_ERROR("GetTagList error ");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagListByRowNum(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
  const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count,
  uint32_t table_version) {
  *count = 0;
  std::vector<TagPartitionTable*> all_tag_partition_tables;
  auto tag_bt = table_schema_mgr_->GetTagTable();
  tag_bt->GetTagPartitionTableManager()->
    GetAllPartitionTablesLessVersion(all_tag_partition_tables, table_version);

  TagVersionObject* result_ver_obj = tag_bt->GetTagTableVersionManager()->GetVersionObject(table_version);
  if (nullptr == result_ver_obj) {
      LOG_ERROR("GetTagVersionObject failed, version id: %d", table_version);
      return FAIL;
  }
  std::vector<uint32_t> result_scan_tags;
  for (const auto& tag_idx : scan_tags) {
    result_scan_tags.emplace_back(result_ver_obj->getValidSchemaIdxs()[tag_idx]);
  }
  const std::vector<TagInfo>& result_schema = result_ver_obj->getIncludeDroppedSchemaInfos();

  ErrorInfo err_info;
  for (auto& entity : entity_id_list) {
    OperatorInfoOfRecord* opt_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity.op_with_osn.get());
    assert(opt_osn != nullptr);
    assert(opt_osn->row_num > 0);
    auto entity_tag_bt = all_tag_partition_tables[opt_osn->p_tag_id];
    entity_tag_bt->startRead();
    // get source scan tags
    std::vector<uint32_t> src_scan_tags;
    for (int idx = 0; idx < scan_tags.size(); ++idx) {
      if (result_scan_tags[idx] >= entity_tag_bt->getIncludeDroppedSchemaInfos().size()) {
        src_scan_tags.push_back(INVALID_COL_IDX);
      } else {
        src_scan_tags.push_back(result_scan_tags[idx]);
      }
    }

    for (int idx = 0; idx < src_scan_tags.size(); idx++) {
      if (src_scan_tags[idx] == INVALID_COL_IDX) {
        Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, 1);
        res->push_back(idx, batch);
        continue;
      }
      uint32_t col_idx = src_scan_tags[idx];
      Batch* batch = entity_tag_bt->GetTagBatchRecord(opt_osn->row_num, opt_osn->row_num + 1, col_idx,
                                              result_schema[col_idx], err_info);
      if (err_info.errcode < 0) {
        delete batch;
        LOG_ERROR("GetTagBatchRecord failed.");
        return KStatus::FAIL;
      }
      if (UNLIKELY(batch == nullptr)) {
        LOG_WARN("GetTagBatchRecord result is nullptr, skip this col[%u]", col_idx);
        Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, 1);
        res->push_back(idx, batch);
        continue;
      }
      res->push_back(idx, batch);
    }
    *count += 1;
    entity_tag_bt->stopRead();
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetNormalIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter) {
  auto ts_table_iterator = new TsTableIterator();
  KStatus s = KStatus::SUCCESS;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};

  std::shared_ptr<MMapMetricsTable> metric_schema;
  auto ret = table_schema_mgr_->GetMetricSchema(params.table_version, &metric_schema);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("schema version [%u] does not exists", params.table_version);
    return FAIL;
  }

  auto& actual_cols = metric_schema->getIdxForValidCols();
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : params.scan_cols) {
    if (col >= actual_cols.size()) {
      // In the concurrency scenario, after the storage has deleted the column,
      // kwsql sends query again
      LOG_ERROR("GetIterator Error : TsTable no column %d", col);
      s = FAIL;
      return s;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  // Update ts_span
  auto life_time = metric_schema->GetLifeTime();
  if (life_time.ts != 0) {
    int64_t acceptable_ts = INT64_MIN;
    auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    acceptable_ts = now.time_since_epoch().count() - life_time.ts;
    updateTsSpan(acceptable_ts * life_time.precision, params.ts_spans);
  }

  std::map<uint32_t, std::vector<EntityID>> vgroup_ids;
  for (auto& entity : params.entity_ids) {
    vgroup_ids[entity.subGroupId - 1].push_back(entity.entityId);
  }
  std::shared_ptr<TsVGroup> vgroup;
  TSEngineImpl* ts_engine = static_cast<TSEngineImpl*>(ctx->ts_engine);
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = ts_engine->GetTsVGroups();
  for (auto& vgroup_iter : vgroup_ids) {
    if (vgroup_iter.first >= EngineOptions::vgroup_max_num) {
      LOG_ERROR("Invalid vgroup id.[%u]", vgroup_iter.first);
      s = FAIL;
      return s;
    }
    vgroup = (*ts_vgroups)[vgroup_iter.first];
    TsStorageIterator* ts_iter;
    std::shared_ptr<MMapMetricsTable> schema = metric_schema;
    s = vgroup->GetIterator(ctx, params.table_version, vgroup_ids[vgroup_iter.first], params.ts_spans,
                            params.block_filter, params.scan_cols, ts_scan_cols, params.agg_extend_cols,
                            params.scan_agg_types, table_schema_mgr_, schema,
                            &ts_iter, vgroup, params.ts_points, params.reverse, params.sorted);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot create iterator for vgroup[%u].", vgroup_iter.first);
      return s;
    }
    ts_table_iterator->AddEntityIterator(ts_iter);
  }
  LOG_DEBUG("TsTable::GetIterator success.agg: %lu, iter num: %lu",
            params.scan_agg_types.size(), ts_table_iterator->GetIterNumber());
  (*iter) = ts_table_iterator;
  s = KStatus::SUCCESS;
  return s;
}

KStatus TsTableV2Impl::GetOffsetIterator(kwdbContext_p ctx, const IteratorParams &params, TsIterator** iter) {
  std::shared_ptr<MMapMetricsTable> metric_schema;
  auto ret = table_schema_mgr_->GetMetricSchema(params.table_version, &metric_schema);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("schema version [%u] does not exists", params.table_version);
    return FAIL;
  }

  auto& actual_cols = metric_schema->getIdxForValidCols();
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : params.scan_cols) {
    if (col >= actual_cols.size()) {
      // In the concurrency scenario, after the storage has deleted the column, kwsql sends query again
      LOG_ERROR("GetOffsetIterator Error : TsTable no column %d", col);
      return KStatus::FAIL;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  std::unordered_map<uint32_t, std::vector<EntityID>> vgroup_ids;
  std::unordered_map<uint32_t, std::shared_ptr<TsVGroup>> vgroups;
  if (params.entity_ids.empty()) {
    k_uint32 max_vgroup_id = vgroups_.size();
    for (k_uint32 vgroup_id = 1; vgroup_id <= max_vgroup_id; ++vgroup_id) {
      std::shared_ptr<TsVGroup> vgroup = vgroups_[vgroup_id - 1];
      std::vector<EntityID> entities(vgroup->GetMaxEntityID());
      if (entities.empty()) continue;
      std::iota(entities.begin(), entities.end(), 1);
      vgroup_ids[vgroup_id] = entities;
      vgroups[vgroup_id] = vgroup;
    }
  } else {
    for (auto& entity : params.entity_ids) {
      vgroup_ids[entity.subGroupId].push_back(entity.entityId);
      if (!vgroups.count(entity.subGroupId)) {
        vgroups[entity.subGroupId] = vgroups_[entity.subGroupId - 1];
      }
    }
  }

  std::shared_ptr<MMapMetricsTable> schema = metric_schema;

  TsOffsetIteratorV2Impl* ts_iter = new TsOffsetIteratorV2Impl(vgroups, vgroup_ids, params.ts_spans,
                                                               params.scan_cols, ts_scan_cols, table_schema_mgr_,
                                                               schema, params.offset, params.limit);
  KStatus s = ts_iter->Init(params.reverse);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsOffsetIteratorV2Impl Init failed")
    delete ts_iter;
    ts_iter = nullptr;
    return s;
  }
  *iter = ts_iter;
  LOG_DEBUG("TsTableV2Impl::GetOffsetIterator success.");
  return KStatus::SUCCESS;
}

KStatus
TsTableV2Impl::AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column, uint32_t cur_version,
                          uint32_t new_version, string& msg) {
  return table_schema_mgr_->AlterTable(ctx, alter_type, column, cur_version, new_version, msg);
}

KStatus TsTableV2Impl::undoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
      uint32_t cur_version, uint32_t new_version) {
  return table_schema_mgr_->UndoAlterTable(ctx, alter_type, column, cur_version, new_version);
}

KStatus TsTableV2Impl::CheckAndAddSchemaVersion(kwdbContext_p ctx, const KTableKey& table_id, uint64_t version) {
  if (!g_go_start_service) return KStatus::SUCCESS;
  // Check if the version exists instead of checking the current version
  if (IsExistTableVersion(version)) {
    return KStatus::SUCCESS;
  }

  char* error;
  size_t data_len = 0;
  char* data = getTableMetaByVersion(table_id, version, &data_len, &error);
  Defer defer{[&]() { free(data); }};
  if (error != nullptr) {
    LOG_ERROR("getTableMetaByVersion failed. msg: %s", error);
    free(error);
    return KStatus::FAIL;
  }
  roachpb::CreateTsTable meta;
  if (!meta.ParseFromString({data, data_len})) {
    LOG_ERROR("Parse schema From String failed.");
    return KStatus::FAIL;
  }

  ErrorInfo err_info;
  if (table_schema_mgr_->CreateTable(ctx, &meta, meta.ts_table().database_id(), version, err_info) != KStatus::SUCCESS) {
    LOG_ERROR("failed during upper version, err: %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                      const uint32_t cur_version, const uint32_t new_version,
                                      const std::vector<uint32_t/* tag column id*/>& tags) {
    LOG_INFO("CreateNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    if (!table_schema_mgr_->CreateNormalTagIndex(ctx, transaction_id, index_id, cur_version, new_version, tags)) {
        LOG_ERROR("Failed to create normal tag index, table id:%lu, index id:%lu.", this->table_id_, index_id);
        return FAIL;
    }
    LOG_INFO("CreateNormalTagIndex success, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    return SUCCESS;
}


KStatus TsTableV2Impl::TSxClean(kwdbContext_p ctx) {
  table_schema_mgr_->GetTagTable()->GetTagTableVersionManager()->SyncCurrentTableVersion();
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,  const uint32_t cur_version,
                                    const uint32_t new_version, const uint64_t index_id) {
    LOG_INFO("DropNormalTagIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    if (!table_schema_mgr_->DropNormalTagIndex(ctx, transaction_id, cur_version, new_version, index_id)) {
        LOG_ERROR("Failed to drop normal tag index, table id:%lu, index id:%lu.", this->table_id_, index_id);
        return FAIL;
    }
    LOG_INFO("DropNormalTagIndex success, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    return SUCCESS;
}

KStatus TsTableV2Impl::UndoCreateIndex(kwdbContext_p ctx, LogEntry* log) {
  ErrorInfo err_info;
  auto index_log = reinterpret_cast<CreateIndexEntry*>(log);
  uint32_t index_id = index_log->getIndexID();
  uint32_t cur_version = index_log->getCurTsVersion();
  uint32_t new_version = index_log->getNewTsVersion();
  LOG_INFO("UndoCreateHashIndex start, table id:%lu, index id:%u, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  if (!table_schema_mgr_->UndoCreateHashIndex(index_id, cur_version, new_version, err_info)) {
    LOG_ERROR("Failed to UndoCreateHashIndex, table id:%lu, index id:%u.", this->table_id_, index_id);
    return FAIL;
  }
  LOG_INFO("UndoCreateHashIndex success, table id:%lu, index id:%u, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}

KStatus TsTableV2Impl::UndoDropIndex(kwdbContext_p ctx, LogEntry* log) {
  ErrorInfo err_info;
  auto index_log = reinterpret_cast<DropIndexEntry*>(log);
  std::vector<uint32_t> tags;
  for (auto col_id : index_log->getColIDs()) {
    if (col_id < 0) {
      break;
    }
    tags.emplace_back(col_id);
  }
  uint32_t index_id = index_log->getIndexID();
  uint32_t cur_version = index_log->getCurTsVersion();
  uint32_t new_version = index_log->getNewTsVersion();
  LOG_INFO("UndoDropHashIndex start, table id:%lu, index id:%u, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  if (!table_schema_mgr_->UndoDropHashIndex(tags, index_id, cur_version, new_version, err_info)) {
    LOG_ERROR("Failed to UndoDropHashIndex, table id:%lu, index id:%u.", this->table_id_, index_id);
    return FAIL;
  }
  LOG_INFO("UndoDropHashIndex success, table id:%lu, index id:%u, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}

std::vector<uint32_t> TsTableV2Impl::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
    return table_schema_mgr_->GetNTagIndexInfo(ts_version, index_id);
}

KStatus TsTableV2Impl::DeleteEntities(kwdbContext_p ctx,  std::vector<std::string>& primary_tag,
  uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del) {
  *count = 0;
  auto tag_table = table_schema_mgr_->GetTagTable();
  for (auto p_tags : primary_tag) {
    uint32_t v_group_id, entity_id;
    if (!tag_table->hasPrimaryKey(p_tags.data(), p_tags.size(), entity_id, v_group_id)) {
      LOG_INFO("primary key[%s] dose not exist, no need to delete", p_tags.c_str())
      continue;
    }
    std::vector<EntityResultIndex> es{EntityResultIndex(0, entity_id, v_group_id)};
    std::vector<KwTsSpan> ts_spans{{INT64_MIN, INT64_MAX}};
    uint64_t cur_entity_count = 0;
    auto s = GetEntityRowCount(ctx, es, ts_spans, &cur_entity_count);
    if (s != KStatus::SUCCESS) {
      LOG_WARN("GetEntityRowCount failed. vgrp[%u], entity_id[%u]", v_group_id, entity_id);
    }
    *count += cur_entity_count;
    // write WAL and remove tag, if cur_entity_count > 0 remove metric data.
    s = GetVGroupByID(v_group_id)->DeleteEntity(ctx, table_id_, p_tags, entity_id,
                                              &cur_entity_count, mtr_id, osn, user_del);
    if (s != KStatus::SUCCESS) {
      LOG_WARN("DeleteEntity failed. vgrp[%u], entity_id[%u]", v_group_id, entity_id);
    }
    GetVGroupByID(v_group_id)->ResetEntityMaxTs(table_id_, INT64_MAX, entity_id);
    GetVGroupByID(v_group_id)->ResetEntityLatestRow(entity_id, INT64_MAX);
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetRangeRowCount(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
KwTsSpan ts_span, uint64_t* count) {
  HashIdSpan hash_span{begin_hash, end_hash};
  vector<EntityResultIndex> entity_store;
  auto s = GetEntityIdByHashSpan(ctx, hash_span, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityIdByHashSpan failed.");
    return s;
  }
  LOG_INFO("GetEntityIdByHashSpan entity num[%lu]", entity_store.size());
  s = GetEntityRowCount(ctx, entity_store, {ts_span}, count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityRowCount failed.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
KwTsSpan ts_span, uint64_t mtr_id, uint64_t osn) {
  uint64_t row_num_bef = 0;
  uint64_t row_num_aft = 0;
  LOG_INFO("DeleteTotalRange begin. table[%lu] hash[%lu ~ %lu].", table_id_, begin_hash, end_hash);
  #ifdef K_DEBUG
  GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_bef);
  if (ts_span.begin != INT64_MIN || ts_span.end != INT64_MAX) {
    LOG_ERROR("DeleteTotalRange not support range split by timestamp.");
    return KStatus::FAIL;
  }
#endif
  assert(ts_span.begin == INT64_MIN && ts_span.end == INT64_MAX);
  HashIdSpan hash_span{begin_hash, end_hash};
  vector<EntityResultIndex> entity_store;
  uint64_t del_tags;
  // mark all avaiable tag tos delete_by_snapshot.
  auto s = DeleteRangeEntities(ctx, 1, hash_span, &del_tags, 0, osn, false);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("DeleteRangeEntities failed.");
    return s;
  }
  // mark all deleted tags to delete_by_snapshot.
  s = TrasvalAllTagPtable(ctx, [&](TagPartitionTable* entity_tag_bt, size_t vec_idx) -> bool {
    for (int rownum = 1; rownum <= entity_tag_bt->size(); rownum++) {
      bool in_hash_span = true;
      if (!EngineOptions::isSingleNode()) {
        uint32_t tag_hash;
        entity_tag_bt->getHashpointByRowNum(rownum, &tag_hash);
        if (!(hash_span.begin <= tag_hash && tag_hash <= hash_span.end)) {
          in_hash_span = false;
        }
      }
      if (in_hash_span) {
        if (entity_tag_bt->isValidRow(rownum)) {
          auto tag_table = table_schema_mgr_->GetTagTable();
          uint32_t v_group_id, entity_id;
          if (!tag_table->hasPrimaryKey(reinterpret_cast<char*>(entity_tag_bt->record(rownum)),
                entity_tag_bt->primaryTagSize(), entity_id, v_group_id)) {
            LOG_ERROR("DeleteTotalRange failed. current range has new tag insert.");
          }
          TagDataInfo* tag_info = entity_tag_bt->getTagDataInfoByRowNum(rownum);
          LOG_ERROR("DeleteTotalRange failed. vec_idx[%lu], bt_size[%lu]. currow: rownum[%d], op_type[%d], osn[%lu],"
            " target_row[%lu],target_vec[%lu].",
            vec_idx, entity_tag_bt->size(), rownum, tag_info->operate_type, tag_info->osn,
            tag_info->target_row, tag_info->target_ver);
          assert(!entity_tag_bt->isValidRow(rownum));
        }
        if (entity_tag_bt->getTagDataInfoByRowNum(rownum)->operate_type != OperateType::DeleteBySnapshot) {
          TagDataInfo tagInfo{OperateType::DeleteBySnapshot, osn, 0, 0};
          entity_tag_bt->setTagDataInfo(rownum, &tagInfo);
        }
      }
    }
    return true;
  });
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TrasvalAllTagPtable failed.");
    return s;
  }
  #ifdef K_DEBUG
    GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_aft);
  #endif
  LOG_INFO("DeleteTotalRange end. table[%lu] hash[%lu ~ %lu], entity_rows[%lu], metric_rows[%lu-->%lu].",
    table_id_, begin_hash, end_hash, entity_store.size(), row_num_bef, row_num_aft);
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetAvgTableRowSize(kwdbContext_p ctx, uint64_t* row_size) {
  // fixed tuple length of one row.
  size_t row_length = 0;
  const std::vector<AttributeInfo>* schemas{nullptr};
  auto s = table_schema_mgr_->GetColumnsExcludeDroppedPtr(&schemas);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAvgTableRowSize failed. at getting schema.");
    return s;
  }
  for (auto& col : *schemas) {
    if (col.type == DATATYPE::VARSTRING || col.type == DATATYPE::VARBINARY) {
      row_length += col.max_len;
    } else {
      row_length += col.size;
    }
  }
  // todo(liangbo01): make precise estimate if needed.
  *row_size = row_length;
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetDataVolume(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
const KwTsSpan& ts_span, uint64_t* volume) {
  uint64_t row_num = 0;
  auto s = GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDataVolume hash[%lu ~ %lu], ts[%ld ~ %ld] failed.",
      begin_hash, end_hash, ts_span.begin, ts_span.end);
    return s;
  }
  uint64_t row_size = 0;
  s = GetAvgTableRowSize(ctx, &row_size);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDataVolume hash[%lu ~ %lu], ts[%ld ~ %ld] failed.",
      begin_hash, end_hash, ts_span.begin, ts_span.end);
    return s;
  }
  *volume = row_num * row_size;
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetDataVolumeHalfTS(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                            const KwTsSpan& ts_span, timestamp64* half_ts) {
  uint64_t row_num = 0;
  HashIdSpan hash_span{begin_hash, end_hash};
  vector<EntityResultIndex> entity_store;
  auto s = GetEntityIdByHashSpan(ctx, hash_span, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityIdByHashSpan failed.");
    return s;
  }
  s = GetEntityRowCount(ctx, entity_store, {ts_span}, &row_num);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDataVolumeHalfTS hash[%lu ~ %lu], ts[%ld ~ %ld] failed, row_num[%lu].",
      begin_hash, end_hash, ts_span.begin, ts_span.end, row_num);
    return s;
  }
  if (row_num == 0) {
    LOG_INFO("GetDataVolumeHalfTS hash[%lu ~ %lu], ts[%ld ~ %ld] has no rows left, row_num[%lu].",
      begin_hash, end_hash, ts_span.begin, ts_span.end, row_num);
    *half_ts = (ts_span.begin + ts_span.end) / 2;
    return KStatus::SUCCESS;
  }
  std::vector<KwTsSpan> ts_spans{ts_span};
  std::vector<k_uint32> scan_cols{0};
  TsIterator *iter = nullptr;
  Defer defer{[&]() {
    if (iter != nullptr) {
      delete iter;
    }
  }};
  // find half ts by offset iterator.
  uint32_t offset = row_num / 2;
  std::vector<BlockFilter> block_filter;
  std::vector<k_int32> agg_extend_cols;
  std::vector<Sumfunctype> scan_agg_types;
  IteratorParams params = {
      .entity_ids = entity_store,
      .ts_spans = ts_spans,
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = 1,
      .ts_points = {},
      .reverse = false,
      .sorted = false,
      .offset = offset,
      .limit = 1,
  };
  s = GetOffsetIterator(ctx, params, &iter);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetOffsetIterator failed.");
    return s;
  }
  std::list<timestamp64> range_tss;
  uint32_t count;
  ResultSet res;
  res.setColumnNum(1);
  do {
    res.clear();
    s = iter->Next(&res, &count);
    if (KStatus::FAIL == s) {
      LOG_ERROR("TsTableIterator::Next() Failed");
      return s;
    }
    for (size_t i = 0; i < count; i++) {
      range_tss.push_back(KTimestamp(reinterpret_cast<char*>(res.data[0][0]->mem) + i * 8));
    }
  } while (count > 0);
  range_tss.sort();
  uint64_t actual_offset = iter->GetFilterCount();
  uint64_t read_row_num = offset + 1;
  uint64_t range_tss_idx = read_row_num - actual_offset;
  assert(read_row_num > actual_offset);
  assert(range_tss_idx <= range_tss.size());
  auto list_iter = range_tss.begin();
  for (size_t i = 0; i < range_tss_idx - 1; i++) {
    if (list_iter == range_tss.end()) {
      LOG_ERROR("rearch list end.");
      return KStatus::FAIL;
    }
    list_iter++;
  }
  *half_ts = *(list_iter);
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetEntityIdByHashSpan(kwdbContext_p ctx, const HashIdSpan& hash_span,
                                             vector<EntityResultIndex>& entity_store) {
  return TrasvalAllTagPtable(ctx, [&](TagPartitionTable* entity_tag_bt, size_t vec_idx) -> bool {
    for (int rownum = 1; rownum <= entity_tag_bt->size(); rownum++) {
      if (!entity_tag_bt->isValidRow(rownum)) {
        continue;
      }
      if (!EngineOptions::isSingleNode()) {
        uint32_t tag_hash;
        entity_tag_bt->getHashpointByRowNum(rownum, &tag_hash);
        if (hash_span.begin <= tag_hash && tag_hash <= hash_span.end) {
          entity_tag_bt->getHashedEntityIdByRownum(rownum, tag_hash, &entity_store);
        }
      } else {
        entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
      }
    }
    return true;
  });
}

KStatus TsTableV2Impl::getPTagsByHashSpan(kwdbContext_p ctx, const HashIdSpan& hash_span, vector<string>* primary_tags) {
  return TrasvalAllTagPtable(ctx, [&](TagPartitionTable* entity_tag_bt, size_t vec_idx) -> bool {
    for (int rownum = 1; rownum <= entity_tag_bt->size(); rownum++) {
      if (!entity_tag_bt->isValidRow(rownum)) {
        continue;
      }
      if (!EngineOptions::isSingleNode()) {
        uint32_t tag_hash;
        entity_tag_bt->getHashpointByRowNum(rownum, &tag_hash);
        string primary_tag(reinterpret_cast<char*>(entity_tag_bt->record(rownum)),
                                                  entity_tag_bt->primaryTagSize());
        if (hash_span.begin <= tag_hash && tag_hash <= hash_span.end) {
          primary_tags->emplace_back(primary_tag);
        }
      } else {
        string primary_tag(reinterpret_cast<char*>(entity_tag_bt->record(rownum)),
                                                  entity_tag_bt->primaryTagSize());
        primary_tags->emplace_back(primary_tag);
      }
    }
    return true;
  });
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                    uint64_t* count, uint64_t mtr_id, uint64_t osn, bool user_del) {
  *count = 0;
  vector<string> primary_tags;
  auto s = getPTagsByHashSpan(ctx, hash_span, &primary_tags);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getPTagsByHashSpan failed.hash[%lu - %lu]", hash_span.begin, hash_span.end);
    return s;
  }
  if (DeleteEntities(ctx, primary_tags, count, mtr_id, osn, user_del) == KStatus::FAIL) {
    LOG_ERROR("delete entities error")
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                        const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id, uint64_t osn) {
  *count = 0;
  vector<string> primary_tags;
  auto s = getPTagsByHashSpan(ctx, hash_span, &primary_tags);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getPTagsByHashSpan failed.hash[%lu - %lu]", hash_span.begin, hash_span.end);
    return s;
  }
  for (auto p_tags : primary_tags) {
    // Delete the data corresponding to the tag within the time range
    uint64_t entity_del_count = 0;
    KStatus status = DeleteData(ctx, 1, p_tags, ts_spans,  &entity_del_count, mtr_id, osn);
    if (status == KStatus::FAIL) {
      LOG_ERROR("DeleteRangeData failed, delete entity by primary key %s failed", p_tags.c_str());
      return KStatus::FAIL;
    }
    // Update count
    *count += entity_del_count;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteData(kwdbContext_p ctx, uint64_t range_group_id, std::string& primary_tag,
  const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id, uint64_t osn) {
  ErrorInfo err_info;
  auto tag_table = table_schema_mgr_->GetTagTable();
  uint32_t v_group_id, entity_id;
  if (!tag_table->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, v_group_id)) {
    LOG_DEBUG("primary key[%s] dose not exist, no need to delete", primary_tag.c_str());
    return KStatus::SUCCESS;
  }
  if (count != nullptr) {
    std::vector<EntityResultIndex> es{EntityResultIndex(0, entity_id, v_group_id)};
    auto s = GetEntityRowCount(ctx, es, ts_spans, count);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetEntityRowCount failed.");
      return s;
    }
    if (*count == 0) {
      return KStatus::SUCCESS;
    }
  }
  // write WAL and remove metric datas.
  auto s = GetVGroupByID(v_group_id)->DeleteData(ctx, table_id_, primary_tag, entity_id,
                                                ts_spans, count, mtr_id, osn, true);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  timestamp64 max_ts = INT64_MIN;
  for (auto span : ts_spans) {
    max_ts = (max_ts < span.end) ? span.end : max_ts;
  }
  GetVGroupByID(v_group_id)->ResetEntityMaxTs(table_id_, max_ts, entity_id);
  GetVGroupByID(v_group_id)->ResetEntityLatestRow(entity_id, max_ts);
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::CountRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                                       const std::vector<KwTsSpan>& ts_spans, uint64_t* count,
                                       uint64_t mtr_id, uint64_t osn) {
  vector<EntityResultIndex> entity_store;
  auto s = GetEntityIdByHashSpan(ctx, hash_span, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityIdByHashSpan failed.");
    return s;
  }
  s = GetEntityRowCount(ctx, entity_store, ts_spans, count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityRowCount failed.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetEntityRowCount(kwdbContext_p ctx, std::vector<EntityResultIndex>& entity_ids,
const std::vector<KwTsSpan>& ts_spans, uint64_t* row_count) {
  std::vector<k_uint32> scan_cols = {0};
  std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};
  uint32_t table_version = table_schema_mgr_->GetCurrentVersion();
  TsIterator* iter = nullptr;
  Defer defer{[&]() {
    if (iter != nullptr) {
      delete iter;
    }
  }};
  std::vector<BlockFilter> block_filter;
  std::vector<k_int32> agg_extend_cols;
  std::vector<timestamp64> ts_points;
  IteratorParams params = {
      .entity_ids = entity_ids,
      .ts_spans = const_cast<vector<KwTsSpan>&>(ts_spans),
      .block_filter = block_filter,
      .scan_cols = scan_cols,
      .agg_extend_cols = agg_extend_cols,
      .scan_agg_types = scan_agg_types,
      .table_version = table_version,
      .ts_points = ts_points,
      .reverse = false,
      .sorted = false,
      .offset = 0,
      .limit = 0,
  };
  KStatus s = GetNormalIterator(ctx, params, &iter);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityRowCount GetIterator failed.");
    return s;
  }
  *row_count = 0;
  k_uint32 count;
  ResultSet res;
  res.setColumnNum(scan_cols.size());
  for (size_t i = 0; i < entity_ids.size(); i++) {
    res.clear();
    s = iter->Next(&res, &count);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    if (count > 0) {
      assert(count == 1);
      *row_count += *reinterpret_cast<uint64_t*>(res.data[0][0]->mem);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetLastRowEntity(kwdbContext_p ctx, EntityResultIndex& entity_id, timestamp64& entity_last_ts) {
  entity_id = {0, 0, 0};
  entity_last_ts = INT64_MIN;

  for (auto& vgroup : vgroups_) {
    pair<timestamp64, EntityID> cur_last_entity = {INT64_MIN, 0};
    if (vgroup->GetLastRowEntity(ctx, table_schema_mgr_, cur_last_entity) != KStatus::SUCCESS) {
      LOG_ERROR("Vgroup %d GetLastRowEntity failed.", vgroup->GetVGroupID());
      return KStatus::FAIL;
    }
    if (cur_last_entity.second == 0) {
      LOG_WARN("cannot found last row entity for vgroup[%d]", vgroup->GetVGroupID());
      continue;
    }
    if (cur_last_entity.first > entity_last_ts) {
      entity_id.entityGroupId = 1;
      entity_id.subGroupId = vgroup->GetVGroupID();
      entity_id.entityId = cur_last_entity.second;
      entity_last_ts = cur_last_entity.first;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetLastRowBatch(kwdbContext_p ctx, uint32_t table_version, std::vector<uint32_t> scan_cols,
                                       ResultSet* res, k_uint32* count, bool& valid) {
  *count = 0;
  if (0 == EngineOptions::last_cache_max_size) {
    valid = false;
    return KStatus::SUCCESS;
  }
  valid = true;
  timestamp64 entity_max_ts = INT64_MIN;
  EntityResultIndex entity_id = {0, 0, 0};

  for (auto& vgroup : vgroups_) {
    if (0 == vgroup->GetMaxEntityID()) continue;
    // TODO(liumengzhen) problem of multiple tables exists.
    if (!vgroup->isLastRowEntityPayloadValid(table_id_)) {
      valid = false;
      LOG_WARN("last payload is invalid for vgroup[%d]", vgroup->GetVGroupID());
      return KStatus::SUCCESS;
    }
    pair<timestamp64, EntityID> cur_last_entity = {INT64_MIN, 0};
    if (vgroup->GetLastRowEntity(ctx, table_schema_mgr_, cur_last_entity) != KStatus::SUCCESS) {
      LOG_ERROR("Vgroup %d GetLastRowEntity failed.", vgroup->GetVGroupID());
      return KStatus::FAIL;
    }
    if (cur_last_entity.second == 0) {
      LOG_WARN("cannot found last row entity for vgroup[%d]", vgroup->GetVGroupID());
      continue;
    }
    if (cur_last_entity.first > entity_max_ts) {
      entity_id.entityGroupId = 1;
      entity_id.subGroupId = vgroup->GetVGroupID();
      entity_id.entityId = cur_last_entity.second;
      entity_max_ts = cur_last_entity.first;
    }
  }

  if (entity_id.equalsWithoutMem({0, 0, 0})) {
    return KStatus::SUCCESS;
  }

  std::shared_ptr<MMapMetricsTable> schema;
  auto s = table_schema_mgr_->GetMetricSchema(table_version, &schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetMetricSchema failed");
    return KStatus::FAIL;
  }
  vector<uint32_t> actual_cols = schema->getIdxForValidCols();
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : scan_cols) {
    if (col >= actual_cols.size()) {
      LOG_ERROR("query col invalid: col idx %u", col);
      return KStatus::FAIL;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  uint32_t last_vgroup_id = entity_id.subGroupId;
  auto vgroup = GetVGroupByID(last_vgroup_id);
  if (!vgroup->isLastRowEntityPayloadValid(table_id_)) {
    res->clear();
    valid = false;
    LOG_WARN("last payload is invalid for vgroup[%d]", vgroup->GetVGroupID());
    return KStatus::SUCCESS;
  }
  timestamp64 cur_last_ts = INT64_MIN;
  std::shared_ptr<TsRawPayloadRowParser> parser = nullptr;
  bool last_payload_valid = false;
  KStatus ret = vgroup->GetEntityLastRowBatch(entity_id.entityId, table_version, table_schema_mgr_,
                                              schema, parser, {{INT64_MIN, INT64_MAX}}, scan_cols,
                                              cur_last_ts, last_payload_valid, res);
  if (ret != KStatus::SUCCESS) {
    res->clear();
    LOG_ERROR("Vgroup %d GetLastRowBatch failed.", vgroup->GetVGroupID());
    return KStatus::FAIL;
  }
  if (!last_payload_valid) {
    res->clear();
    valid = false;
    LOG_WARN("last payload is invalid for vgroup[%d]", vgroup->GetVGroupID());
    return KStatus::SUCCESS;
  }
  *count = 1;
  res->entity_index = entity_id;
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetColAttributeInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                   AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      attr_info.type = DATATYPE::TIMESTAMP64;
      attr_info.max_len = 3;
      break;
    case roachpb::TIMESTAMP_MICRO:
    case roachpb::TIMESTAMPTZ_MICRO:
      attr_info.type = DATATYPE::TIMESTAMP64_MICRO;
      attr_info.max_len = 6;
      break;
    case roachpb::TIMESTAMP_NANO:
    case roachpb::TIMESTAMPTZ_NANO:
      attr_info.type = DATATYPE::TIMESTAMP64_NANO;
      attr_info.max_len = 9;
      break;
    case roachpb::SMALLINT:
      attr_info.type = DATATYPE::INT16;
      break;
    case roachpb::INT:
      attr_info.type = DATATYPE::INT32;
      break;
    case roachpb::BIGINT:
      attr_info.type = DATATYPE::INT64;
      break;
    case roachpb::FLOAT:
      attr_info.type = DATATYPE::FLOAT;
      break;
    case roachpb::DOUBLE:
      attr_info.type = DATATYPE::DOUBLE;
      break;
    case roachpb::BOOL:
      attr_info.type = DATATYPE::BYTE;
      break;
    case roachpb::CHAR:
      attr_info.type = DATATYPE::CHAR;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::BINARY:
    case roachpb::NCHAR:
      attr_info.type = DATATYPE::BINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::VARCHAR:
      attr_info.type = DATATYPE::VARSTRING;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return KStatus::FAIL;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  if (col.has_name()) {
    strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN - 1);
  }
  attr_info.length = col.storage_len();
  if (!col.nullable()) {
    attr_info.setFlag(AINFO_NOT_NULL);
  }
  if (col.dropped()) {
    attr_info.setFlag(AINFO_DROPPED);
  }
  attr_info.col_flag = static_cast<ColumnFlag>(col.col_type());
  attr_info.version = 1;

  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetMetricColumnInfo(kwdbContext_p ctx, struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  switch (attr_info.type) {
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::TIMESTAMP64_MICRO:
      col.set_storage_type(roachpb::TIMESTAMP_MICRO);
      break;
    case DATATYPE::TIMESTAMP64_NANO:
      col.set_storage_type(roachpb::TIMESTAMP_NANO);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::INVALID:
    default:
    return KStatus::FAIL;
  }

  col.set_column_id(attr_info.id);
  col.set_name(attr_info.name);
  col.set_nullable(true);
  col.set_dropped(false);
  if (!col.has_storage_len()) {
    col.set_storage_len(attr_info.length);
  }
  if (attr_info.isFlag(AINFO_NOT_NULL)) {
    col.set_nullable(false);
  }
  if (attr_info.isFlag(AINFO_DROPPED)) {
    col.set_dropped(true);
  }
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)(attr_info.col_flag));
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagColumnInfo(kwdbContext_p ctx, struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  col.set_storage_len(tag_info.m_length);
  col.set_column_id(tag_info.m_id);
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)((ColumnFlag)tag_info.m_tag_type));
  if (tag_info.isDropped()) {
    col.set_dropped(true);
  }
  switch (tag_info.m_data_type) {
    case DATATYPE::TIMESTAMP64:
      col.set_storage_type(roachpb::TIMESTAMP);
      break;
    case DATATYPE::TIMESTAMP64_MICRO:
      col.set_storage_type(roachpb::TIMESTAMP_MICRO);
      break;
    case DATATYPE::TIMESTAMP64_NANO:
      col.set_storage_type(roachpb::TIMESTAMP_NANO);
      break;
    case DATATYPE::INT16:
      col.set_storage_type(roachpb::SMALLINT);
      break;
    case DATATYPE::INT32:
      col.set_storage_type(roachpb::INT);
      break;
    case DATATYPE::INT64:
      col.set_storage_type(roachpb::BIGINT);
      break;
    case DATATYPE::FLOAT:
      col.set_storage_type(roachpb::FLOAT);
      break;
    case DATATYPE::DOUBLE:
      col.set_storage_type(roachpb::DOUBLE);
      break;
    case DATATYPE::BYTE:
      col.set_storage_type(roachpb::BOOL);
      break;
    case DATATYPE::CHAR:
      col.set_storage_type(roachpb::CHAR);
      break;
    case DATATYPE::BINARY:
      col.set_storage_type(roachpb::BINARY);
      break;
    case DATATYPE::VARSTRING:
      col.set_storage_type(roachpb::VARCHAR);
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      break;
    case DATATYPE::INVALID:
    default:
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GenerateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                    const std::vector<AttributeInfo>& metric_schema,
                                         std::vector<TagInfo>& tag_schema,
                                         uint32_t schema_version) {
  EnterFunc()
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  for (auto col_var : metric_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    KStatus s = GetMetricColumnInfo(ctx, col_var, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate metric Schema", col_var.type);
      Return(s);
    }
  }

  // Traverse tag schema and use tag info to construct metric column info of meta
  for (auto tag_info : tag_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    // XXX Notice: tag_info don't has tag column name,
    KStatus s = GetTagColumnInfo(ctx, tag_info, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate tag Schema", tag_info.m_data_type);
      Return(s);
    }
    // Set storage length.
    if (col->has_storage_len() && col->storage_len() == 0) {
      col->set_storage_len(tag_info.m_size);
    }
  }

  auto tag_infos = table_schema_mgr_->GetTagTable()->GetAllNTagIndexs(schema_version);
  for (auto tag_info : tag_infos) {
    roachpb::NTagIndexInfo* idx_info = meta->add_index_info();
    idx_info->set_index_id(tag_info.first);
    for (auto col_id : tag_info.second) {
      idx_info->add_col_ids(col_id);
    }
  }
  Return(KStatus::SUCCESS);
}

KStatus TsTableV2Impl::GetMetricDelInfoByOSN(kwdbContext_p ctx, const EntityResultIndex& entity_id,
  std::vector<KwOSNSpan>& osn_span, std::vector<KwTsSpan>* del_spans) {
  auto vgroup = GetVGroupByID(entity_id.subGroupId);
  if (vgroup == nullptr) {
    LOG_ERROR("cannot find vgroup [%u].", entity_id.subGroupId);
    return KStatus::FAIL;
  }
  return vgroup->GetDelInfoByOSN(ctx, GetTableId(), entity_id.entityId, osn_span, del_spans);
}

KStatus TsTableV2Impl::GetMetricDelInfoWithOSN(kwdbContext_p ctx, const EntityResultIndex& entity_id,
  list<STDelRange>* del_osns) {
  auto vgroup = GetVGroupByID(entity_id.subGroupId);
  if (vgroup == nullptr) {
    LOG_ERROR("cannot find vgroup [%u].", entity_id.subGroupId);
    return KStatus::FAIL;
  }
  return vgroup->GetDelInfoWithOSN(ctx, GetTableId(), entity_id.entityId, del_osns);
}

KStatus TsTableV2Impl::GetMetricIteratorByOSN(kwdbContext_p ctx, k_uint32 table_version,
  std::vector<k_uint32>& scan_cols,
  std::vector<EntityResultIndex>& entity_ids,
  std::vector<KwOSNSpan>& osn_span, TsIterator** iter) {
  auto ts_table_iterator = new TsTableIterator();
  KStatus s = KStatus::SUCCESS;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};
  std::shared_ptr<MMapMetricsTable> metric_schema;
  auto ret = table_schema_mgr_->GetMetricSchema(table_version, &metric_schema);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("schema version [%u] does not exists", table_version);
    return FAIL;
  }

  auto& actual_cols = metric_schema->getIdxForValidCols();
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : scan_cols) {
    if (col >= actual_cols.size()) {
      LOG_ERROR("GetIterator Error : TsTable no column %d", col);
      s = FAIL;
      return s;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }
  std::map<uint32_t, std::vector<EntityResultIndex>> vgroup_ids;
  for (auto& entity : entity_ids) {
    vgroup_ids[entity.subGroupId - 1].push_back(entity);
  }
  std::shared_ptr<TsVGroup> vgroup;
  TSEngineImpl* ts_engine = static_cast<TSEngineImpl*>(ctx->ts_engine);
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = ts_engine->GetTsVGroups();
  for (auto& vgroup_iter : vgroup_ids) {
    if (vgroup_iter.first >= EngineOptions::vgroup_max_num) {
      LOG_ERROR("Invalid vgroup id.[%u]", vgroup_iter.first);
      s = FAIL;
      return s;
    }
    vgroup = (*ts_vgroups)[vgroup_iter.first];
    TsStorageIterator* ts_iter;
    s = vgroup->GetMetricIteratorByOSN(ctx, vgroup, vgroup_ids[vgroup_iter.first], scan_cols, ts_scan_cols,
      osn_span, table_version, table_schema_mgr_, &ts_iter);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot create iterator for vgroup[%u].", vgroup_iter.first);
      return s;
    }
    ts_table_iterator->AddEntityIterator(ts_iter);
  }
  LOG_DEBUG("GetMetricIteratorByOSN success. iter num: %lu", ts_table_iterator->GetIterNumber());
  (*iter) = ts_table_iterator;
  s = KStatus::SUCCESS;
  return s;
}

KStatus TsTableV2Impl::TrasvalAllTagPtable(kwdbContext_p ctx, std::function<bool(TagPartitionTable*, size_t)> func) {
  std::vector<TagPartitionTable*> all_tag_partition_tables;
  auto tag_bt = table_schema_mgr_->GetTagTable();
  TableVersion cur_tbl_version = tag_bt->GetTagTableVersionManager()->GetCurrentTableVersion();
  tag_bt->GetTagPartitionTableManager()->
    GetAllPartitionTablesLessVersion(all_tag_partition_tables, cur_tbl_version);
  for (size_t i = 0; i < all_tag_partition_tables.size(); i++) {
    auto entity_tag_bt = all_tag_partition_tables[i];
    entity_tag_bt->startRead();
    bool ret = func(entity_tag_bt, i);
    entity_tag_bt->stopRead();
    if (!ret) {
      return KStatus::SUCCESS;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetImagrateTagBySnapshot(kwdbContext_p ctx, HashIdSpan hash_range,
  std::list<EntityResultIndex>* pkeys_status) {
  int valid_tag_num = 0;
  auto s = TrasvalAllTagPtable(ctx, [&](TagPartitionTable* entity_tag_bt, size_t vec_idx) -> bool {
    auto curr_rows = entity_tag_bt->size();
    for (int rownum = 1; rownum <= curr_rows; rownum++) {
      bool is_in_hps = true;
      uint32_t tag_hash;
      if (!EngineOptions::isSingleNode()) {
        entity_tag_bt->getHashpointByRowNum(rownum, &tag_hash);
        if (hash_range.begin > tag_hash || hash_range.end < tag_hash) {
          is_in_hps = false;
        }
      }
      if (is_in_hps) {
        auto tag_info = entity_tag_bt->getTagDataInfoByRowNum(rownum);
        OperatorTypeOfRecord type = OperatorTypeOfRecord::OP_TYPE_UNKNOWN;
        if (tag_info->operate_type == OperateType::Delete) {
          type = OperatorTypeOfRecord::OP_TYPE_TAG_DELETE;
        } else if (tag_info->operate_type == OperateType::Update) {
          type = OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE;
        } else if (tag_info->operate_type == OperateType::Insert) {
          if (entity_tag_bt->isValidRow(rownum)) {
            type = OperatorTypeOfRecord::OP_TYPE_INSERT;
            valid_tag_num++;
          }
        }
        if (type != OperatorTypeOfRecord::OP_TYPE_UNKNOWN) {
          std::vector<kwdbts::EntityResultIndex> entity_id_list;
          if (!EngineOptions::isSingleNode()) {
            entity_tag_bt->getHashedEntityIdByRownum(rownum, tag_hash, &entity_id_list);
          } else {
            entity_tag_bt->getEntityIdByRownum(rownum, &entity_id_list);
          }
          entity_id_list[0].op_with_osn = std::make_shared<OperatorInfoOfRecord>(type, tag_info->osn, vec_idx, rownum);
          pkeys_status->push_back(entity_id_list[0]);
        }
      }
    }
    return true;
  });
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TrasvalAllTagPtable failed.");
    return s;
  }
  LOG_INFO("GetImagrateTagBySnapshot tag total num[%lu], valid num[%d].", pkeys_status->size(), valid_tag_num);
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagRecordInfoByOSN(kwdbContext_p ctx,
  const std::unordered_set<uint32_t> hps,
  std::vector<KwOSNSpan>& osn_span, std::unordered_map<uint64_t, EntityResultIndex>* pkeys_status) {
  return GetTagRecordInfoByOSN(ctx, [&](TagPartitionTable* entity_tag_bt, int row_num) -> bool {
    uint32_t tag_hash;
    entity_tag_bt->getHashpointByRowNum(row_num, &tag_hash);
    if (hps.find(tag_hash) == hps.end()) {
       return false;
    }
    return true;
  }, osn_span, pkeys_status);
}

KStatus TsTableV2Impl::GetTagRecordInfoByOSN(kwdbContext_p ctx,
  std::function<bool(TagPartitionTable* entity_tag_bt, int row_num)> in_span_func,
  std::vector<KwOSNSpan>& osn_span, std::unordered_map<uint64_t, EntityResultIndex>* pkeys_status) {
  std::unordered_map<uint64_t, EntityResultIndex> history_tag_status;
  auto s = TrasvalAllTagPtable(ctx, [&](TagPartitionTable* entity_tag_bt, size_t vec_idx) -> bool {
    for (int rownum = 1; rownum <= entity_tag_bt->size(); rownum++) {
      bool is_in_hps = true;
      if (!EngineOptions::isSingleNode()) {
        if (!in_span_func(entity_tag_bt, rownum)) {
          is_in_hps = false;
        }
      }
      if (is_in_hps) {
        auto tag_info = entity_tag_bt->getTagDataInfoByRowNum(rownum);
        if (tag_info->operate_type == OperateType::Update) {
          std::vector<kwdbts::EntityResultIndex> entity_id_list;
          if (!EngineOptions::isSingleNode()) {
            entity_tag_bt->getHashedEntityIdByRownum(rownum, 0, &entity_id_list);
          } else {
            entity_tag_bt->getEntityIdByRownum(rownum, &entity_id_list);
          }
          uint64_t key = entity_id_list[0].GenUniqueKey();
          if (history_tag_status.find(key) == history_tag_status.end()) {
            history_tag_status[key] = entity_id_list[0];
            history_tag_status[key].op_with_osn =
              std::make_shared<OperatorInfoOfRecord>(
                OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE, tag_info->osn, vec_idx, rownum);
          }
        } else if (IsOsnInSpans(tag_info->osn, osn_span)) {
          OperatorTypeOfRecord type = OperatorTypeOfRecord::OP_TYPE_UNKNOWN;
          if (tag_info->operate_type == OperateType::Delete) {
            type = OperatorTypeOfRecord::OP_TYPE_TAG_DELETE;
          } else if (tag_info->operate_type == OperateType::Insert) {
            if (entity_tag_bt->isValidRow(rownum)) {
              type = OperatorTypeOfRecord::OP_TYPE_INSERT;
            }
          }
          if (type != OperatorTypeOfRecord::OP_TYPE_UNKNOWN) {
            std::vector<kwdbts::EntityResultIndex> entity_id_list;
            if (!EngineOptions::isSingleNode()) {
              entity_tag_bt->getHashedEntityIdByRownum(rownum, 0, &entity_id_list);
            } else {
              entity_tag_bt->getEntityIdByRownum(rownum, &entity_id_list);
            }
            uint64_t key = entity_id_list[0].GenUniqueKey();
            auto iter_tmp = history_tag_status.find(key);
            if (iter_tmp != history_tag_status.end() &&
                type == OperatorTypeOfRecord::OP_TYPE_INSERT) {
              type = OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE;
              assert(reinterpret_cast<OperatorInfoOfRecord*>(iter_tmp->second.op_with_osn.get())->type ==
                    OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE);
            }
            if (pkeys_status->find(key) == pkeys_status->end()) {
              (*pkeys_status)[key] = entity_id_list[0];
              (*pkeys_status)[key].op_with_osn = std::make_shared<OperatorInfoOfRecord>(
                type, tag_info->osn, vec_idx, rownum);
            } else {
              // todo(liangbo01) if one insert tag record readed to pkeys_status,
              //  then this tag record updated, and store at new verison table.
              //  new stored tag record may be readed again.
              // assert(false);
            }
          }
        }
      }
    }
    return true;
  });
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TrasvalAllTagPtable failed.");
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagIteratorByOSN(kwdbContext_p ctx, k_uint32 table_version, std::vector<k_uint32>& scan_cols,
  std::vector<KwOSNSpan>& osn_span,
  const std::unordered_set<uint32_t> hps, BaseEntityIterator** iter) {
  std::unordered_map<uint64_t, EntityResultIndex> pkeys_status;
  auto ret = GetTagRecordInfoByOSN(ctx, hps, osn_span, &pkeys_status);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("GetTagRecordInfoByOSN failed.");
    return ret;
  }
  std::shared_ptr<TagTable> tag_table;
  ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed.");
    return KStatus::FAIL;
  }
  TagIteratorByOSN* tag_iter = new TagIteratorByOSN(tag_table, table_version, scan_cols, osn_span);
  if (KStatus::SUCCESS != tag_iter->Init(hps, std::move(pkeys_status))) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetEntityIdListByOSN(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
            std::vector<KwOSNSpan>& osn_span,
            std::vector<k_uint32>& scan_cols,
            const std::unordered_set<uint32_t> &hps,
            std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
            uint32_t table_version) {
  std::unordered_map<uint64_t, EntityResultIndex> pkeys_status;
  auto ret = GetTagRecordInfoByOSN(ctx, hps, osn_span, &pkeys_status);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("GetTagRecordInfoByOSN failed.");
    return ret;
  }
  std::unordered_map<uint64_t, EntityResultIndex> pkeys_left;
  for (auto idx : pkeys_status) {
    bool found = false;
    for (auto pkey : primary_tags) {
      if (0 == memcmp(idx.second.mem.get(), pkey, idx.second.p_tags_size)) {
        pkeys_left[idx.first] = idx.second;
      }
    }
  }
  std::shared_ptr<TagTable> tag_table;
  ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    LOG_ERROR("GetTagSchema failed.");
    return KStatus::FAIL;
  }
  auto tag_iter = std::make_unique<TagIteratorByOSN>(tag_table, table_version, scan_cols, osn_span);
  if (KStatus::SUCCESS != tag_iter->Init(hps, std::move(pkeys_left))) {
    return KStatus::FAIL;
  }
  uint32_t cur_count;
  while (true) {
    ret = tag_iter->Next(entity_id_list, res, &cur_count);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("tag next failed.");
      return KStatus::FAIL;
    }
    if (cur_count == 0) {
      break;
    }
    *count += cur_count;
  }
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
