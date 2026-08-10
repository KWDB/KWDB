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

#include <set>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <list>
#include <unordered_map>
#include "ts_table_del_info.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_compatibility.h"
#include "ts_vgroup.h"
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {
/**
 * OSNDeleteInfo struct
 * 
   __________________________________________________________________________________________________________________________________________________
  |    4    |        4      |       n          |      4       |       n    |       4        |       8      |      8     |       8         |       8         |
  |---------|---------------|------------------|--------------|------------|----------------|--------------|------------|-----------------|-----------------|
  |  type   |  payload len  |  payload data    | OSN Info len | OSN Info   | del range num  | range1 begin | range1 end | range1 osn begin|range1 osn end   |
 * 
 * 
 * type code : 1-tag delete. 2-metric delete
 * 
 * 
 */

TSSlice TsReplicaRangeMigrate::GenData(TSSlice& payload, TSSlice& osn_info, std::list<STDelRange>& dels) {
  size_t mem_len = 4 + 4 + payload.len + 4 + osn_info.len + 4 + 32 * dels.size();
  char* mem = reinterpret_cast<char*>(malloc(mem_len));
  char* offset = mem;
  if (payload.len > 0) {
    KUint32(offset) = STOSNDeleteInfoType::OSN_DELETE_TAG_RECORD;
  } else {
    KUint32(offset) = STOSNDeleteInfoType::OSN_DELETE_METRIC_RANGE;
  }
  offset += 4;
  KUint32(offset) = payload.len;
  offset += 4;
  memcpy(offset, payload.data, payload.len);
  offset += payload.len;
  KUint32(offset) = osn_info.len;
  offset += 4;
  memcpy(offset, osn_info.data, osn_info.len);
  offset += osn_info.len;
  KUint32(offset) = dels.size();
  offset += 4;
  for (const auto& [ts_span, osn_span] : dels) {
    KInt64(offset) = ts_span.begin;
    offset += 8;
    KInt64(offset) = ts_span.end;
    offset += 8;
    KUint64(offset) = osn_span.begin;
    offset += 8;
    KUint64(offset) = osn_span.end;
    offset += 8;
  }
  return TSSlice{mem, mem_len};
}

void TsReplicaRangeMigrate::ParseData(TSSlice data, STOSNDeleteInfoType* type, TSSlice* payload, TSSlice* pkey,
  std::list<STDelRange>* dels) {
  char* offset = data.data;
  *type = (STOSNDeleteInfoType)(KUint32(offset));
  offset += 4;
  payload->len = KUint32(offset);
  offset += 4;
  payload->data = offset;
  offset += payload->len;
  pkey->len = KUint32(offset);
  offset += 4;
  pkey->data = offset;
  offset += pkey->len;
  auto vec_size = KUint32(offset);
  offset += 4;
  for (size_t i = 0; i < vec_size; i++) {
    dels->push_back(STDelRange{{KInt64(offset), KInt64(offset + 8)}, {KUint64(offset + 16), KUint64(offset + 24)}});
    offset += 32;
  }
}

TsReplicaRangeMigrate* TsReplicaRangeMigrate::CreateProducer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e,
  uint32_t v, TS_OSN osn) {
  return new TsRangeMigrateProducer(table, b, e, v, osn);
}
TsReplicaRangeMigrate* TsReplicaRangeMigrate::CreateConsumer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e,
  uint32_t v, TS_OSN osn) {
  return new TsRangeMigrateConsumer(table, b, e, v, osn);
}

KStatus TsRangeMigrateProducer::Init(TS_OSN published_max_osn) {
  published_max_osn_ = published_max_osn;
  auto s = table_->GetImagrateTagBySnapshot(nullptr, {begin_hash_, end_hash_}, scan_osn_, &pkeys_status_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsRangeMigrateProducer init failed at GetImagrateTagBySnapshot.");
    return s;
  }
  pkey_iter_ = pkeys_status_.begin();
  return KStatus::SUCCESS;
}

KStatus TsRangeMigrateConsumer::Init(TS_OSN published_max_osn) {
  published_max_osn_ = published_max_osn;
  std::unordered_map<std::string, std::list<std::list<EntityResultIndex>>> entity_tags;
  auto s = table_->GetReuseTagsForSnapshot(nullptr, {begin_hash_, end_hash_}, &entity_tags);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsRangeMigrateConsumer init failed at GetReuseTagsForSnapshot.");
    return s;
  }
  for (auto& [pkey, tags] : entity_tags) {
    PrimaryKeyEntityInfo info;
    for (auto& entity_tags : tags) {
      TagDataInfo osn_info;
      table_->GetTagOSNInfoByRowNum(nullptr, entity_tags.front(), osn_info);
      info.entity_id_infos_origin[osn_info.osn[0]] = std::move(entity_tags);
    }
    pkey_entity_info_[pkey] = std::move(info);
  }
  return KStatus::SUCCESS;
}

TsReplicaRangeMigrate::~TsReplicaRangeMigrate() {
  LOG_INFO("TsReplicaRangeMigrate end. table[%lu], range[%lu - %lu], total[%u],"
           " valid[%u], ignore[%u], delete range num[%u]. Optional[%s].",
    table_->GetTableId(), begin_hash_, end_hash_, total_tag_row_num_,
    valid_tag_row_num_, ignore_tag_row_num_, del_range_num_,
    optional_msg_.c_str());
}

KStatus TsRangeMigrateProducer::NextMigrateData(kwdbContext_p ctx, TSSlice* data, bool* is_finished) {
  *is_finished = false;
  while (true) {
    if (pkey_iter_ == pkeys_status_.end()) {
      *is_finished = true;
      return KStatus::SUCCESS;
    }
    EntityResultIndex& entity_idx = *pkey_iter_;
    auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_idx.op_with_osn.get());
    assert(op_osn != nullptr);
    auto joint_entity = GenJointEntityID(entity_idx.subGroupId, entity_idx.entityId);
    if (entity_create_osn_.find(joint_entity) == entity_create_osn_.end()) {
      TagDataInfo osn_info;
      auto s = table_->GetTagOSNInfoByRowNum(ctx, entity_idx, osn_info);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetTagOSNInfoByRowNum failed at GenTagPayLoad.");
        return s;
      }
      entity_create_osn_[joint_entity] = osn_info.osn[0];
    }
    if (op_osn->osn < published_max_osn_ && op_osn->type != OperatorTypeOfRecord::OP_TYPE_INSERT) {
      pkey_iter_++;
      continue;
    }
    TSSlice payload{nullptr, 0};
    std::list<STDelRange> del_osns;
    if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_INSERT) {
      // tage type is insert. we should return metric delete info.
      auto s = table_->GetMetricDelInfoWithOSN(ctx, entity_idx, &del_osns);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NextMigrateData failed at GetMetricDelInfoWithOSN.");
        return s;
      }
    }
    del_range_num_ += del_osns.size();
    // if tag is deleted, we need return tag delete info.
    auto s = GenTagPayLoad(ctx, entity_idx, &payload);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("NextMigrateData failed at GenTagPayLoad.");
      return s;
    }
    TsRawPayload::SetOSN(payload, op_osn->osn);
    TsRawPayload::SetHashPoint(payload, entity_idx.hash_point);

    pkey_iter_++;
    if (payload.len != 0 || del_osns.size() != 0) {
      TagDataInfo osn_info;
      auto s = table_->GetTagOSNInfoByRowNum(ctx, entity_idx, osn_info);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetTagOSNInfoByRowNum failed at GenTagPayLoad.");
        return s;
      }
      TSSnapshotOSNInfo sp_osn_info;
      sp_osn_info.magic_num = 0;
      sp_osn_info.op_num = 0;
      for (size_t i = 0; i <= osn_info.operate_idx; i++) {
        // scan_osn_ is max value, so if is true forever.
        if (osn_info.osn[i] <= scan_osn_) {
          sp_osn_info.op_osn[sp_osn_info.op_num] = osn_info.osn[i];
          sp_osn_info.op_types[sp_osn_info.op_num] = osn_info.operate_type[i];
          sp_osn_info.op_num++;
        } else {
          break;
        }
      }
      assert(sp_osn_info.op_num < 3);
      assert(entity_create_osn_.find(joint_entity) != entity_create_osn_.end());
      assert(op_osn->type != OperatorTypeOfRecord::OP_TYPE_INSERT || sp_osn_info.op_num == 1);
      // using last osn loaction store create osn of entity.
      sp_osn_info.op_osn[2] = entity_create_osn_[joint_entity];
      TSSlice sp_osn_info_slice{reinterpret_cast<char*>(&sp_osn_info), sizeof(sp_osn_info)};
      *data = GenData(payload, sp_osn_info_slice, del_osns);
      if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE) {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_UPDATE_TAG_RECORD;
      } else if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE) {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_DELETE_TAG_RECORD;
      } else {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_DELETE_METRIC_RANGE;
        valid_tag_row_num_ += 1;
      }
      total_tag_row_num_ += 1;
      free(payload.data);
      return KStatus::SUCCESS;
    }
  }
  LOG_ERROR("can not run here.");
  return KStatus::FAIL;
}

KStatus TsRangeMigrateProducer::GenTagPayLoad(kwdbContext_p ctx, EntityResultIndex& entity_idx, TSSlice* payload) {
  std::vector<TagInfo> tags_info;
  KStatus s = table_->GetSchemaManager()->GetTagMeta(table_version_, tags_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagMeta failed");
    return KStatus::FAIL;
  }
  std::vector<AttributeInfo> data_schema;
  s = table_->GetSchemaManager()->GetColumnsExcludeDropped(data_schema, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColumnsExcludeDropped failed");
    return KStatus::FAIL;
  }
  std::vector<uint32_t> scan_tags;
  scan_tags.reserve(tags_info.size());
  for (int i = 0; i < tags_info.size(); ++i) {
    scan_tags.push_back(i);
  }
  // init tag iterator
  ResultSet res(scan_tags.size());
  uint32_t count;
  s = table_->GetTagListByRowNum(ctx, {entity_idx}, scan_tags, UINT64_MAX, &res, &count, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagList failed");
    return KStatus::FAIL;
  }
  if (count != 1) {
    LOG_ERROR("GetTagData failed, count=%d", count);
    return KStatus::FAIL;
  }
  TSRowPayloadSparseBuilder build;
  if (!build.Init(tags_info, data_schema, 0, TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE)) {
    LOG_ERROR("TSRowPayloadSparseBuilder init failed");
    return KStatus::FAIL;
  }
  for (size_t i = 0; i < tags_info.size(); i++) {
    bool is_null = false;
    if (!tags_info[i].isPrimaryTag()) {
      s = res.data[i][0]->isNull(0, &is_null);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("tag col value isNull failed");
        return s;
      }
    }
    if (!is_null) {
      if (!tags_info[i].isPrimaryTag() && isVarLenType(tags_info[i].m_data_type)) {
        build.SetTagValue(i, res.data[i][0]->getData(0) + sizeof(uint16_t), res.data[i][0]->getDataLen(0));
      } else {
        int null_bitmap_size = tags_info[i].isPrimaryTag() ? 0 : 1;
        build.SetTagValue(i, reinterpret_cast<char*>(res.data[i][0]->mem) + null_bitmap_size, tags_info[i].m_size);
      }
    }
  }
  if (table_->GetSchemaManager()->IsSparseTable()) {
    OperatorInfoOfRecord* opt_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_idx.op_with_osn.get());
    auto tag_table = table_->GetSchemaManager()->GetTagTable();
    if (!tag_table) {
      LOG_ERROR("GetValidColumns: tag table is null, table id: %lu", table_->GetTableId());
      return KStatus::FAIL;
    }
    assert(opt_osn->row_num > 0);
    auto entity_tag_bt = tag_table->GetTagPartitionTableManager()->GetPartitionTable(opt_osn->p_tag_version);
    if (entity_tag_bt == nullptr) {
      LOG_ERROR("GetPartitionTable[%lu] version[%u] failed.", table_->GetTableId(), opt_osn->p_tag_version);
      return KStatus::FAIL;
    }
    entity_tag_bt->startRead();
    std::vector<std::seed_seq::result_type> valid_columns;
    entity_tag_bt->getValidColumns(opt_osn->row_num, valid_columns);
    entity_tag_bt->stopRead();
    build.SetValidCols(valid_columns);
  }

  if (!build.Build(table_->GetTableId(), table_version_, payload)) {
    LOG_ERROR("TSRowPayloadBuilder build failed");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsRangeMigrateConsumer::ParsePayload(kwdbContext_p ctx, const TSSlice& payload, TsRawPayload** pd) {
  *pd = nullptr;
  auto table_version = TsRawPayload::GetTableVersionFromSlice(payload);
  auto s = table_->CheckAndAddSchemaVersion(ctx, table_->GetTableId(), table_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("table[%lu],CheckAndAddSchemaVersion[%u] init failed.", table_->GetTableId(), table_version);
    return s;
  }

  const std::vector<AttributeInfo> *metric_schema;
  s = table_->GetSchemaManager()->GetColumnsExcludeDroppedPtr(&metric_schema,
    TsRawPayload::GetTableVersionFromSlice(payload));
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get GetColumnsExcludeDroppedPtr id[%ld].", table_->GetTableId());
    return s;
  }
  *pd = new TsRawPayload(metric_schema);
  if (*pd == nullptr) {
    LOG_ERROR("Failed new TsRawPayload id[%ld].", table_->GetTableId());
    return KStatus::FAIL;
  }
  s = (*pd)->ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed parse payload id[%ld].", table_->GetTableId());
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsRangeMigrateConsumer::RMValidPkeyRow(const TSSlice& pkey, std::shared_ptr<TagTable> tag_table) {
  if (!tag_table->hasPrimaryKey(pkey.data, pkey.len)) {
    return KStatus::SUCCESS;
  }
  std::pair<uint64_t, uint64_t> row_info;
  if (!tag_table->GetPrimaryKeyRowInfo(pkey.data, pkey.len, row_info)) {
    LOG_ERROR("Failed get primary key row info.");
    return KStatus::FAIL;
  }
  TagDataInfo data_info;
  auto s = table_->GetTagOSNInfoByRowNum(nullptr, row_info, data_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get tag data info.");
    return KStatus::FAIL;
  }
  std::string pkey_str;
  BinaryToHexStr(pkey, pkey_str);
  LOG_WARN("find valid tag[%lu,%lu], create osn [%lu], drop first, pkey[%s].",
    row_info.first, row_info.second, data_info.osn[0], pkey_str.c_str());

  ErrorInfo err_info;
  std::pair<size_t, size_t> del_row_no;
  auto ret = tag_table->DeleteTagRecord(pkey.data, pkey.len, err_info,
            scan_osn_, OperateType::Ignore, del_row_no);
  if (ret < 0) {
    LOG_ERROR("DeleteTagRecord failed. [%d, %s]", ret, err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}
KStatus TsRangeMigrateConsumer::CoverTagDataInfo(std::pair<uint64_t, uint64_t> row_info,
  TSSnapshotOSNInfo* snap_osn_info) {
  TagDataInfo orig_info;
  orig_info.operate_idx = snap_osn_info->op_num - 1;
  for (size_t i = 0; i < snap_osn_info->op_num; i++) {
    orig_info.osn[i] = snap_osn_info->op_osn[i];
    orig_info.operate_type[i] = snap_osn_info->op_types[i];
  }
  auto s = table_->SetTagOSNInfoByRowNum(nullptr, row_info, orig_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get table id[%ld] SetTagOSNInfoByRowNum[%lu,%lu].",
      table_->GetTableId(), row_info.first, row_info.second);
    return s;
  }
  return KStatus::SUCCESS;
}
KStatus TsRangeMigrateConsumer::WriteMigrateData(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock) {
  STOSNDeleteInfoType type;
  TSSlice payload;
  TSSlice tag_status;
  std::list<STDelRange> dels;
  ParseData(data, &type, &payload, &tag_status, &dels);
  del_range_num_ += dels.size();
  TsRawPayload* p = nullptr;
  Defer defer{[&](){
    delete p;
  }};
  auto s = ParsePayload(ctx, payload, &p);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed parse payload id[%lu].", table_->GetTableId());
    return s;
  }
  auto pkey = p->GetPrimaryTag();
  uint32_t p_hash_point = p->GetHashPoint();
  if (p_hash_point < begin_hash_ || p_hash_point > end_hash_) {
    LOG_ERROR("payload hash point[%u] not in span[%lu,%lu]", p_hash_point, begin_hash_, end_hash_);
    return KStatus::FAIL;
  }

  std::shared_ptr<TagTable> tag_table;
  s = table_->GetSchemaManager()->GetTagSchema(nullptr, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get table id[%ld] tag schema.", table_->GetTableId());
    return s;
  }

  uint32_t hash_point = t1ha1_le(pkey.data, pkey.len);
  tag_lock.WrLock(hash_point);
  Defer defer_1{[&](){
    tag_lock.Unlock(hash_point);
  }};
  // clear valid tag row for current primary key. normal case no need clear.
  s = RMValidPkeyRow(pkey, tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed table id[%ld] rm valid pkey row.", table_->GetTableId());
    return s;
  }
  auto snap_osn_info = reinterpret_cast<TSSnapshotOSNInfo*>(tag_status.data);
  TS_OSN create_osn = snap_osn_info->op_osn[2];
  std::string pkey_str(pkey.data, pkey.len);
  std::pair<uint64_t, uint64_t> row_info{0, 0};
  uint64_t entity_id = 0;
  uint32_t vgroup_id = 0;
  EntityResultIndex entity_idx;
  if (HasHisTagRecord(pkey_str, create_osn, snap_osn_info->op_osn[0], entity_idx)) {
    // reuse existed tag row.
    auto tag_data_info = reinterpret_cast<OperatorInfoOfRecord*>(entity_idx.op_with_osn.get());
    row_info = {tag_data_info->p_tag_version, tag_data_info->row_num};
    reused_tag_row_num_ += 1;
    entity_id = entity_idx.entityId;
    vgroup_id = entity_idx.subGroupId;
  } else {
    // need insert new tag row to save this tag record.
    GetEntityIDVGroupID(pkey_str, create_osn, entity_id, vgroup_id);
    if (tag_table->InsertDeletedTagRecord(*p, vgroup_id, entity_id, 0, OperateType::Invalid, row_info) < 0) {
      LOG_ERROR("Failed InsertTagRecord table id[%ld].", table_->GetTableId());
      return KStatus::FAIL;
    }
    new_tag_row_num_ += 1;
  }
  // cover tag status using imgrated tagdatainfo.
  s = CoverTagDataInfo(row_info, snap_osn_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed cover tag data info.");
    return s;
  }
  if (type == STOSNDeleteInfoType::OSN_DELETE_METRIC_RANGE) {
    valid_tag_row_num_ += 1;
    // need construct tag primary key indexs and set tag row status valid.
    if (tag_table->ReBuildTagRecordIndex(*p, row_info) < 0) {
      LOG_ERROR("Failed ReBuildTagRecordIndex table id[%ld].", table_->GetTableId());
      return KStatus::FAIL;
    }
    // temporarily store delete range info in memory.
    PrimaryKeyEntityInfo& cur_entity_info = pkey_entity_info_[pkey_str];
    assert(cur_entity_info.del_ranges.empty());
    cur_entity_info.del_ranges = std::move(dels);
    cur_entity_info.active_entity_ = {vgroup_id, entity_id};
  }
  return KStatus::SUCCESS;
}

void TsRangeMigrateConsumer::GetEntityIDVGroupID(std::string& pkey, TS_OSN create_osn,
  uint64_t& entity_id, uint32_t& vgroup_id) {
  PrimaryKeyEntityInfo& cur_entity_info = pkey_entity_info_[pkey];
  auto osn_iter = cur_entity_info.entity_id_infos_origin.find(create_osn);
  if (osn_iter != cur_entity_info.entity_id_infos_origin.end()) {
    entity_id = osn_iter->second.front().entityId;
    vgroup_id = osn_iter->second.front().subGroupId;
  } else {
    auto iter = cur_entity_info.new_entity_list.find(create_osn);
    if (iter != cur_entity_info.new_entity_list.end()) {
      vgroup_id = iter->second.first;
      entity_id = iter->second.second;
    } else {
      vgroup_id = GetConsistentVgroupId(pkey.data(), pkey.length(), EngineOptions::vgroup_max_num);
      table_->GetSchemaManager()->AllocateEntityID(vgroup_id, entity_id);
      cur_entity_info.new_entity_list[create_osn] = std::make_pair(vgroup_id, entity_id);
      new_entity_num_ += 1;
    }
  }
}

bool TsRangeMigrateConsumer::HasHisTagRecord(std::string& pkey, TS_OSN create_osn, TS_OSN osn,
  EntityResultIndex& entity_idx) {
  auto iter = pkey_entity_info_.find(pkey);
  if (iter == pkey_entity_info_.end()) {
    return false;
  }
  PrimaryKeyEntityInfo& cur_entity_info = iter->second;
  auto osn_iter = cur_entity_info.entity_id_infos_origin.find(create_osn);
  if (osn_iter == cur_entity_info.entity_id_infos_origin.end()) {
    return false;
  }
  TS_OSN last_osn = 0;
  TagDataInfo data_info;
  std::list<EntityResultIndex>& osn_tag_list = osn_iter->second;
  for (auto& tag_idx : osn_tag_list) {
    auto tag_data_info = reinterpret_cast<OperatorInfoOfRecord*>(tag_idx.op_with_osn.get());
    std::pair<uint64_t, uint64_t> row_info(tag_data_info->p_tag_version, tag_data_info->row_num);
    auto s = table_->GetTagOSNInfoByRowNum(nullptr, row_info, data_info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed get tag data info.");
      return false;
    }
    last_osn = data_info.osn[0];
    if (data_info.osn[0] == osn) {
      entity_idx = tag_idx;
      return true;
    }
  }
  if (last_osn > osn) {
    std::string pkey_str;
    TSSlice p_slice{const_cast<char*>(pkey.data()), pkey.length()};
    BinaryToHexStr(p_slice, pkey_str);
    LOG_WARN("pkey [%s], insert osn [%lu] is not in range [%lu,%lu].", pkey_str.c_str(), osn, create_osn, last_osn);
  }
  return false;
}

KStatus TsRangeMigrateConsumer::CommitMigrate(kwdbContext_p ctx) {
  auto db_id = table_->GetSchemaManager()->GetDbID();
  auto data_type = table_->GetSchemaManager()->GetTsColDataType();
  std::set<std::shared_ptr<const TsPartitionVersion>> partition_syncs;
  for (const auto& pkey : pkey_entity_info_) {
    std::string cur_pkey = pkey.first;
    const PrimaryKeyEntityInfo& cur_entity_info = pkey.second;
    if (cur_entity_info.del_ranges.empty()) {
      continue;
    }
    std::vector<KwTsSpan> tsspans;
    for (auto& [ts_span, osn_span] : cur_entity_info.del_ranges) {
      tsspans.push_back(ts_span);
    }
    auto vgrp_obj = table_->GetVGroupByID(cur_entity_info.active_entity_.first);
    auto partitions = vgrp_obj->CurrentVersion()->GetPartitions(db_id, tsspans, data_type);
    for (auto& partition : partitions) {
      auto s = partition->CoverDelRange(cur_entity_info.active_entity_.second, cur_entity_info.del_ranges);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Failed cover del range.");
        return KStatus::FAIL;
      }
      partition_syncs.insert(partition);
    }
  }
  for (auto& partition : partition_syncs) {
    partition->SyncDelRangeFile();
  }
  return KStatus::SUCCESS;
}

bool STPackageSnapshotData::PackageData(uint32_t package_id, TSTableID tbl_id, uint32_t tbl_version,
  TSSlice& batch_data, uint32_t row_num, TSSlice& del_data, TSSlice* data) {
  size_t data_len = 4 + 8 + 4 + 4 + 4 + 4 + batch_data.len + 4 + del_data.len;
  char* data_with_rownum = reinterpret_cast<char*>(malloc(data_len));
  if (data_with_rownum == nullptr) {
    LOG_ERROR("malloc failed.");
    return false;
  }
  *data = {data_with_rownum, data_len};
  KUint32(data_with_rownum) = package_id;
  data_with_rownum += 4;
  KUint64(data_with_rownum) = tbl_id;
  data_with_rownum += 8;
  KUint32(data_with_rownum) = tbl_version;
  data_with_rownum += 4;
  KUint32(data_with_rownum) = row_num;
  data_with_rownum += 4;
  KUint32(data_with_rownum) = CURRENT_SNAPSHOT_VERSION;
  data_with_rownum += 4;
  KUint32(data_with_rownum) = batch_data.len;
  data_with_rownum += 4;
  memcpy(data_with_rownum, batch_data.data, batch_data.len);
  data_with_rownum += batch_data.len;
  KUint32(data_with_rownum) = del_data.len;
  data_with_rownum += 4;
  memcpy(data_with_rownum, del_data.data, del_data.len);
  return true;
}

bool STPackageSnapshotData::UnpackageData(TSSlice& data, uint32_t& package_id, TSTableID& tbl_id, uint32_t& tbl_version,
  TSSlice& batch_data, uint32_t& row_num, TSSlice& del_data) {
  char* data_with_rownum = data.data;
  package_id = KUint32(data_with_rownum);
  data_with_rownum += 4;
  tbl_id = KUint64(data_with_rownum);
  data_with_rownum += 8;
  tbl_version = KUint32(data_with_rownum);
  data_with_rownum += 4;
  row_num = KUint32(data_with_rownum);
  data_with_rownum += 4;
  uint32_t snapshot_version = KUint32(data_with_rownum);
  if (snapshot_version == CURRENT_SNAPSHOT_VERSION) {
    data_with_rownum += 4;
    batch_data.len = KUint32(data_with_rownum);
    data_with_rownum += 4;
    batch_data.data = data_with_rownum;
    data_with_rownum += batch_data.len;
    del_data.len = KUint32(data_with_rownum);
    data_with_rownum += 4;
    del_data.data = data_with_rownum;
  } else if (snapshot_version == 0) {
    batch_data.data = data_with_rownum;
    batch_data.len = data.len - 20;
  } else if (snapshot_version == 1) {
    data_with_rownum += 4;
    batch_data.len = KUint32(data_with_rownum);
    data_with_rownum += 4;
    batch_data.data = data_with_rownum;
    data_with_rownum += batch_data.len;
    del_data.len = KUint32(data_with_rownum);
    data_with_rownum += 4;
    del_data.data = data_with_rownum;
  } else {
    LOG_ERROR("cannot parse snapshot version.[%u]", snapshot_version);
    return false;
  }
  return true;
}

bool STSnapshotPackageBuilder::AddBatchData(TSSlice& batch_data, uint32_t row_num, TSSlice& del_data) {
  TSSlice data{nullptr, 0};
  bool ok = STPackageSnapshotData::PackageData(package_id_, tbl_id_, tbl_version_, batch_data, row_num, del_data, &data);
  packages_.push_back(data);
  current_package_size_ += data.len;
  return ok;
}

bool STSnapshotPackageBuilder::Package(TSSlice* data) {
  if (packages_.empty()) {
    *data = {nullptr, 0};
    return true;
  }
  size_t data_len = 20 + 4 + 4;
  for (auto& p : packages_) {
    data_len += 4 + p.len;
  }
  char* data_with_rownum = reinterpret_cast<char*>(malloc(data_len));
  if (data_with_rownum == nullptr) {
    LOG_ERROR("malloc failed.");
    return false;
  }
  *data = {data_with_rownum, data_len};
  data_with_rownum += 20;
  KUint32(data_with_rownum) = CURRENT_SNAPSHOT_VERSION;
  data_with_rownum += 4;
  KUint32(data_with_rownum) = packages_.size();
  data_with_rownum += 4;
  for (auto& p : packages_) {
    KUint32(data_with_rownum) = p.len;
    data_with_rownum += 4;
    memcpy(data_with_rownum, p.data, p.len);
    data_with_rownum += p.len;
    free(p.data);
  }
  return true;
}

bool STSnapshotPackageParser::Parser(TSSlice& p_data) {
  assert(p_data.len > 20);
  char* data_with_rownum = p_data.data;
  uint32_t snapshot_version = KUint32(data_with_rownum + 20);
  if (snapshot_version == 1) {
    packages_.push_back(p_data);
  } else if (snapshot_version == 2) {
    data_with_rownum += 24;
    auto package_num = KUint32(data_with_rownum);
    data_with_rownum += 4;
    for (size_t i = 0; i < package_num; i++) {
      auto package_len = KUint32(data_with_rownum);
      packages_.push_back({data_with_rownum + 4, package_len});
      data_with_rownum += 4 + package_len;
      assert(data_with_rownum - p_data.data <= p_data.len);
    }
  }
  cur_package_ = packages_.begin();
  return true;
}

bool STSnapshotPackageParser::NextPackage(uint32_t& package_id, TSTableID& tbl_id, uint32_t& tbl_version,
  TSSlice& batch_data, uint32_t& row_num, TSSlice& del_data) {
  if (cur_package_ == packages_.end()) {
    package_id = 0;
    tbl_id = 0;
    tbl_version = 0;
    batch_data = {nullptr, 0};
    row_num = 0;
    del_data = {nullptr, 0};
    return true;
  }
  auto ok = STPackageSnapshotData::UnpackageData(*cur_package_, package_id, tbl_id, tbl_version,
              batch_data, row_num, del_data);
  if (!ok) {
    LOG_ERROR("UnpackageData failed, last package id [%u].", package_id);
    return false;
  }
  cur_package_++;
  return true;
}

}  // namespace kwdbts
