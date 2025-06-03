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
  auto vgroup = vgroups_[v_group_id - 1].get();
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
                                   std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                                   TsIterator** iter, std::vector<timestamp64> ts_points,
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

  std::shared_ptr<TagTable> tag_table;
  s = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    return s;
  }

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
                              scan_cols, ts_scan_cols, scan_agg_types, table_schema_mgr_,
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

KStatus
TsTableV2Impl::AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column, uint32_t cur_version,
                          uint32_t new_version, string& msg) {
  return table_schema_mgr_->AlterTable(ctx, alter_type, column, cur_version, new_version, msg);
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
  auto s = table_schema_mgr_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Update table version error");
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
  auto s = table_schema_mgr_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Update table version error");
    return s;
  }
  LOG_INFO("UndoDropHashIndex success, table id:%lu, index id:%lu, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}

std::vector<uint32_t> TsTableV2Impl::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
    return table_schema_mgr_->GetNTagIndexInfo(ts_version, index_id);
}
}  //  namespace kwdbts
