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

#include "ts_table_v2_impl.h"
#include "ts_tag_iterator_v2_impl.h"
#include "ts_engine.h"
#include "ts_vgroup.h"
#include "ts_iterator_v2_impl.h"

extern bool g_go_start_service;

namespace kwdbts {

TsTableV2Impl::~TsTableV2Impl() = default;


KStatus TsTableV2Impl::PutData(kwdbContext_p ctx, uint64_t v_group_id, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, uint16_t* entity_id, uint32_t* inc_unordered_cnt,
                          DedupResult* dedup_result, const DedupRule& dedup_rule) {
  assert(payload_num == 1);
  TsRawPayload p{*payload};
  uint8_t payload_data_flag = p.GetRowType();
  if (payload_data_flag == DataTagFlag::TAG_ONLY) {
    LOG_DEBUG("tag only. so no need putdata.");
    return KStatus::SUCCESS;
  }
  auto primary_key = p.GetPrimaryTag();
  auto vgroup = GetVGroupByID(v_group_id);
  assert(vgroup != nullptr);
  auto s = vgroup->PutData(ctx, GetTableId(), mtr_id, &primary_key, KUint64(entity_id), payload, true);
  if (s != KStatus::SUCCESS) {
    // todo(liangbo01) if failed. should we need rollback all inserted data?
    LOG_ERROR("putdata failed. table id[%lu], group id[%lu]", GetTableId(), v_group_id);
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
  auto s = v_group->PutData(ctx, GetTableId(), mtr_id, &primary_key, entity_id, &payload, write_wal);
  if (s != KStatus::SUCCESS) {
    // todo(liangbo01) if failed. should we need rollback all inserted data?
    LOG_ERROR("putdata failed. table id[%lu], group id[%u]", GetTableId(), v_group->GetVGroupID());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::vector<uint32_t> hps,
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
                                 std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                                 uint32_t table_version) {
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  if (tag_table->GetEntityIdList(primary_tags, tags_index_id, tags, op_type, scan_tags,
                                    entity_id_list, res, count, table_version) < 0) {
    LOG_ERROR("GetEntityIdList error ");
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetNormalIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                                   std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                                   std::vector<k_int32> agg_extend_cols, std::vector<Sumfunctype> scan_agg_types,
                                   k_uint32 table_version, TsIterator** iter, std::vector<timestamp64> ts_points,
                                   bool reverse, bool sorted) {
  auto ts_table_iterator = new TsTableIterator();
  KStatus s;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};

  auto& actual_cols = table_schema_mgr_->GetIdxForValidCols(table_version);
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : scan_cols) {
    if (col >= actual_cols.size()) {
      // In the concurrency scenario, after the storage has deleted the column,
      // kwsql sends query again
      LOG_ERROR("GetIterator Error : TsTable no column %d", col);
      return KStatus::FAIL;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
  std::map<uint32_t, std::vector<EntityID>> vgroup_ids;
  for (auto& entity : entity_ids) {
    vgroup_ids[entity.subGroupId - 1].push_back(entity.entityId);
  }
  std::shared_ptr<TsVGroup> vgroup;
  TSEngineV2Impl* ts_engine = static_cast<TSEngineV2Impl*>(ctx->ts_engine);
  std::vector<std::shared_ptr<TsVGroup>>* ts_vgroups = ts_engine->GetTsVGroups();
  for (auto& vgroup_iter : vgroup_ids) {
    if (vgroup_iter.first >= EngineOptions::vgroup_max_num) {
      LOG_ERROR("Invalid vgroup id.[%u]", vgroup_iter.first);
      return s;
    }
    vgroup = (*ts_vgroups)[vgroup_iter.first];
    TsStorageIterator* ts_iter;
    // Update ts_span
    auto life_time = table_schema_mgr_->GetLifeTime();
    if (life_time.ts != 0) {
      int64_t acceptable_ts = INT64_MIN;
      auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
      acceptable_ts = now.time_since_epoch().count() - life_time.ts;
      updateTsSpan(acceptable_ts * life_time.precision, ts_spans);
    }
    s = vgroup->GetIterator(ctx, vgroup_ids[vgroup_iter.first], ts_spans, ts_col_type,
                              scan_cols, ts_scan_cols, agg_extend_cols, scan_agg_types, table_schema_mgr_,
                              table_version, &ts_iter, vgroup, ts_points, reverse, sorted);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot create iterator for vgroup[%u].", vgroup_iter.first);
      return s;
    }
    ts_table_iterator->AddEntityIterator(ts_iter);
  }
  LOG_DEBUG("TsTable::GetIterator success.agg: %lu, iter num: %lu",
              scan_agg_types.size(), ts_table_iterator->GetIterNumber());
  (*iter) = ts_table_iterator;
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetOffsetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                                         vector<KwTsSpan>& ts_spans, std::vector<k_uint32> scan_cols,
                                         k_uint32 table_version, TsIterator** iter, k_uint32 offset, k_uint32 limit,
                                         bool reverse) {
  DATATYPE ts_col_type = table_schema_mgr_->GetTsColDataType();
  auto& actual_cols = table_schema_mgr_->GetIdxForValidCols(table_version);
  std::vector<k_uint32> ts_scan_cols;
  for (auto col : scan_cols) {
    if (col >= actual_cols.size()) {
      // In the concurrency scenario, after the storage has deleted the column, kwsql sends query again
      LOG_ERROR("GetOffsetIterator Error : TsTable no column %d", col);
      return KStatus::FAIL;
    }
    ts_scan_cols.emplace_back(actual_cols[col]);
  }

  std::map<uint32_t, std::vector<EntityID>> vgroup_ids;
  std::map<uint32_t, std::shared_ptr<TsVGroup>> vgroups;
  if (entity_ids.empty()) {
    k_uint32 max_vgroup_id = vgroups_.size();
    for (k_uint32 vgroup_id = 1; vgroup_id <= max_vgroup_id; ++vgroup_id) {
      std::shared_ptr<TsVGroup> vgroup = vgroups_[vgroup_id - 1];
      std::vector<EntityID> entities(vgroup->GetMaxEntityID());
      std::iota(entities.begin(), entities.end(), 1);
      vgroup_ids[vgroup_id] = entities;
      vgroups[vgroup_id] = vgroup;
    }
  } else {
    for (auto& entity : entity_ids) {
      vgroup_ids[entity.subGroupId].push_back(entity.entityId);
      if (!vgroups.count(entity.subGroupId)) {
        vgroups[entity.subGroupId] = vgroups_[entity.subGroupId - 1];
      }
    }
  }

  TsOffsetIteratorV2Impl* ts_iter = new TsOffsetIteratorV2Impl(vgroups, vgroup_ids, ts_spans, ts_col_type,
                                                               ts_scan_cols, table_schema_mgr_,
                                                               table_version, offset, limit);
  KStatus s = ts_iter->Init(reverse);
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
  if (version == GetCurrentTableVersion()) {
    return KStatus::SUCCESS;
  }

  if (table_schema_mgr_->Get(version) != nullptr) {
    return KStatus::SUCCESS;
  }

  char* error;
  size_t data_len = 0;
  char* data = getTableMetaByVersion(table_id, version, &data_len, &error);
  if (error != nullptr) {
    LOG_ERROR("getTableMetaByVersion failed. msg: %s", error);
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

    auto s = table_schema_mgr_->UpdateVersion(cur_version, new_version);
    if (s != KStatus::SUCCESS) {
        LOG_ERROR("Update table version error");
        return s;
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
    auto s = table_schema_mgr_->UpdateVersion(cur_version, new_version);
    if (s != KStatus::SUCCESS) {
        LOG_ERROR("Update table version error");
        return s;
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
  LOG_INFO("UndoCreateHashIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  if (!table_schema_mgr_->UndoCreateHashIndex(index_id, cur_version, new_version, err_info)) {
    LOG_ERROR("Failed to UndoCreateHashIndex, table id:%lu, index id:%lu.", this->table_id_, index_id);
    return FAIL;
  }
  auto s = table_schema_mgr_->RollBack(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBack table version error");
    return s;
  }
  LOG_INFO("UndoCreateHashIndex success, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
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
  LOG_INFO("UndoDropHashIndex start, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  if (!table_schema_mgr_->UndoDropHashIndex(tags, index_id, cur_version, new_version, err_info)) {
    LOG_ERROR("Failed to UndoDropHashIndex, table id:%lu, index id:%lu.", this->table_id_, index_id);
    return FAIL;
  }
  auto s = table_schema_mgr_->RollBack(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("RollBack table version error");
    return s;
  }
  LOG_INFO("UndoDropHashIndex success, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}

std::vector<uint32_t> TsTableV2Impl::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
    return table_schema_mgr_->GetNTagIndexInfo(ts_version, index_id);
}

KStatus TsTableV2Impl::DeleteEntities(kwdbContext_p ctx,  std::vector<std::string>& primary_tag,
  uint64_t* count, uint64_t mtr_id) {
  *count = 0;
  ErrorInfo err_info;
  auto tag_table = table_schema_mgr_->GetTagTable();
  for (auto p_tags : primary_tag) {
    uint32_t v_group_id, entity_id;
    if (!tag_table->hasPrimaryKey(p_tags.data(), p_tags.size(), entity_id, v_group_id)) {
      LOG_INFO("primary key[%s] dose not exist, no need to delete", p_tags.c_str())
      continue;
    }
    if (count != nullptr) {
      std::vector<EntityResultIndex> es{EntityResultIndex(0, entity_id, v_group_id)};
      std::vector<KwTsSpan> ts_spans{{INT64_MIN, INT64_MAX}};
      uint64_t cur_entity_count = 0;
      auto s = GetEntityRowCount(ctx, es, ts_spans, &cur_entity_count);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetEntityRowCount failed.");
        return s;
      }
      *count += cur_entity_count;
    }
    // write WAL and remove metric datas.
    auto s = GetVGroupByID(v_group_id)->DeleteEntity(ctx, table_id_, p_tags, entity_id, count, mtr_id);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    // if any error, end the delete loop and return ERROR to the caller.
    // Delete tag and its index
    tag_table->DeleteTagRecord(p_tags.data(), p_tags.size(), err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                  KwTsSpan ts_span, uint64_t mtr_id) {
  return KStatus::SUCCESS;
}


KStatus TsTableV2Impl::getPTagsByHashSpan(kwdbContext_p ctx, const HashIdSpan& hash_span, vector<string>* primary_tags) {
  std::vector<TagPartitionTable*> all_tag_partition_tables;
  auto tag_bt = table_schema_mgr_->GetTagTable();
  TableVersion cur_tbl_version = tag_bt->GetTagTableVersionManager()->GetCurrentTableVersion();
  tag_bt->GetTagPartitionTableManager()->GetAllPartitionTablesLessVersion(all_tag_partition_tables,
                                                                            cur_tbl_version);
  for (const auto& entity_tag_bt : all_tag_partition_tables) {
    entity_tag_bt->startRead();
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
    entity_tag_bt->stopRead();
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                    uint64_t* count, uint64_t mtr_id) {
  *count = 0;
  vector<string> primary_tags;
  auto s = getPTagsByHashSpan(ctx, hash_span, &primary_tags);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("getPTagsByHashSpan failed.hash[%lu - %lu]", hash_span.begin, hash_span.end);
    return s;
  }
  if (DeleteEntities(ctx, primary_tags, count, mtr_id) == KStatus::FAIL) {
    LOG_ERROR("delete entities error")
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                                const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
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
    KStatus status = DeleteData(ctx, 1, p_tags, ts_spans,  &entity_del_count, mtr_id);
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
                            const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  ErrorInfo err_info;
  auto tag_table = table_schema_mgr_->GetTagTable();
  uint32_t v_group_id, entity_id;
  if (!tag_table->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, v_group_id)) {
    LOG_INFO("primary key[%s] dose not exist, no need to delete", primary_tag.c_str())
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
      LOG_INFO("no valid data, no need add to delete item.");
      return KStatus::SUCCESS;
    }
  }
  // write WAL and remove metric datas.
  auto s = GetVGroupByID(v_group_id)->DeleteData(ctx, table_schema_mgr_->GetTableId(), primary_tag, entity_id,
                                                ts_spans, count, mtr_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsTableV2Impl::GetEntityRowCount(kwdbContext_p ctx, std::vector<EntityResultIndex>& entity_ids,
const std::vector<KwTsSpan>& ts_spans, uint64_t* row_count) {
  std::vector<k_uint32> scan_cols = {0};
  std::vector<Sumfunctype> scan_agg_types = {Sumfunctype::COUNT};
  uint32_t table_version = 1;
  TsIterator* iter = nullptr;
  Defer defer{[&]() { if (iter != nullptr) { delete iter; } }};
  std::vector<timestamp64> ts_points;
  KStatus s = GetNormalIterator(ctx, entity_ids, ts_spans, scan_cols, {}, scan_agg_types, table_version,
                                &iter, ts_points, false, false);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetEntityRowCount GetIterator failed.");
    return s;
  }
  *row_count = 0;
  k_uint32 count;
  bool is_finished = false;
  do {
    ResultSet res{(k_uint32) scan_cols.size()};
    auto s = iter->Next(&res, &count);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    if (count > 0) {
      *row_count += *reinterpret_cast<uint64_t*>(res.data[0][0]->mem);
    }
  } while (count > 0);
  return KStatus::SUCCESS;
}


}  //  namespace kwdbts
