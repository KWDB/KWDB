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

#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <list>
#include <unordered_map>
#include "ts_table_del_info.h"
#include "ts_compatibility.h"
#include "ts_vgroup.h"

namespace kwdbts {
/**
 * OSNDeleteInfo struct
 * 
   __________________________________________________________________________________________________________________________________________________
  |    4    |        4      |       n          |      4   |    n    |       4        |       8      |      8     |       8         |       8         |
  |---------|---------------|------------------|----------|---------|----------------|--------------|------------|-----------------|-----------------|
  |  type   |  payload len  |  payload data    | pkey len |  pkey   | del range num  | range1 begin | range1 end | range1 osn begin|range1 osn end   |
 * 
 * 
 * type code : 1-tag delete. 2-metric delete
 * 
 */
TSSlice STTableRangeDelAndTagInfo::GenData(TSSlice& payload, TSSlice& pkey, std::list<STDelRange>& dels) {
  size_t mem_len = 4 + 4 + payload.len + 4 + pkey.len + 4 + 32 * dels.size();
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
  KUint32(offset) = pkey.len;
  offset += 4;
  memcpy(offset, pkey.data, pkey.len);
  offset += pkey.len;
  KUint32(offset) = dels.size();
  offset += 4;
  for (auto del : dels) {
    KInt64(offset) = del.ts_span.begin;
    offset += 8;
    KInt64(offset) = del.ts_span.end;
    offset += 8;
    KUint64(offset) = del.osn_span.begin;
    offset += 8;
    KUint64(offset) = del.osn_span.end;
    offset += 8;
  }
  return TSSlice{mem, mem_len};
}

void STTableRangeDelAndTagInfo::ParseData(TSSlice data, STOSNDeleteInfoType* type, TSSlice* payload, TSSlice* pkey,
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

KStatus STTableRangeDelAndTagInfo::Init() {
  auto s = table_->GetImagrateTagBySnapshot(nullptr, {begin_hash_, end_hash_}, &pkeys_status_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("STTableDeleteInfo init failed at GetImagrateTagBySnapshot.");
    return s;
  }
  pkey_iter_ = pkeys_status_.begin();
  return KStatus::SUCCESS;
}

KStatus STTableRangeDelAndTagInfo::GetNextDeleteInfo(kwdbContext_p ctx, TSSlice* data, bool* is_finished) {
  *is_finished = false;
  while (true) {
    if (pkey_iter_ == pkeys_status_.end()) {
      *is_finished = true;
      return KStatus::SUCCESS;
    }
    EntityResultIndex& entity_idx = *pkey_iter_;
    auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(entity_idx.op_with_osn.get());
    assert(op_osn != nullptr);
    TSSlice payload{nullptr, 0};
    std::list<STDelRange> del_osns;
    // if tag is deleted, we need return tag delete info.
    auto s = GenTagPayLoad(ctx, entity_idx, &payload);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetNextDeleteInfo failed at GenTagPayLoad.");
      return s;
    }
    TsRawPayload::SetOSN(payload, op_osn->osn);
    TsRawPayload::SetHashPoint(payload, entity_idx.hash_point);

    if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_INSERT) {
      // tage type is insert. we should return metric delete info.
      auto s = table_->GetMetricDelInfoWithOSN(ctx, entity_idx, &del_osns);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetNextDeleteInfo failed at GetMetricDelInfoWithOSN.");
        return s;
      }
    }
    pkey_iter_++;
    if (payload.len != 0 || del_osns.size() != 0) {
      TSSlice pkey{reinterpret_cast<char*>(entity_idx.mem.get()), entity_idx.p_tags_size};
      *data = GenData(payload, pkey, del_osns);
      if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_UPDATE) {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_UPDATE_TAG_RECORD;
      } else if (op_osn->type == OperatorTypeOfRecord::OP_TYPE_TAG_DELETE) {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_DELETE_TAG_RECORD;
      } else {
        KUint32(data->data) = STOSNDeleteInfoType::OSN_DELETE_METRIC_RANGE;
      }
      free(payload.data);
      return KStatus::SUCCESS;
    }
  }
  LOG_ERROR("can not run here.");
  return KStatus::FAIL;
}

KStatus STTableRangeDelAndTagInfo::GenTagPayLoad(kwdbContext_p ctx, EntityResultIndex& entity_idx, TSSlice* payload) {
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
  for (int i = 0; i < tags_info.size(); ++i) {
    scan_tags.push_back(i);
  }
  // init tag iterator
  ResultSet res(scan_tags.size());
  uint32_t count;
  s = table_->GetTagListByRowNum(ctx, {entity_idx}, scan_tags, &res, &count, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagList failed");
    return KStatus::FAIL;
  }
  if (count != 1) {
    LOG_ERROR("GetTagData failed, count=%d", count);
    return KStatus::FAIL;
  }
  TSRowPayloadBuilder build(tags_info, data_schema, 0);
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
        build.SetTagValue(i, reinterpret_cast<char*>(res.data[i][0]->getVarColData(0)),
          res.data[i][0]->getVarColDataLen(0));
      } else {
        int null_bitmap_size = tags_info[i].isPrimaryTag() ? 0 : 1;
        build.SetTagValue(i, reinterpret_cast<char*>(res.data[i][0]->mem) + null_bitmap_size, tags_info[i].m_size);
      }
    }
  }
  if (!build.Build(table_->GetTableId(), table_version_, payload)) {
    LOG_ERROR("TSRowPayloadBuilder build failed");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus STTableRangeDelAndTagInfo::WriteDeleteTagRecord(kwdbContext_p ctx, TSSlice& payload,
  OperateType type, TsHashRWLatch& tag_lock) {
  assert(payload.len > 0);
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto pkey = p.GetPrimaryTag();
  auto vgroup_id = GetConsistentVgroupId(pkey.data, pkey.len, EngineOptions::vgroup_max_num);
  TsVGroup* vgroup = table_->GetVGroupByID(vgroup_id);
  uint64_t hash_point = t1ha1_le(pkey.data, pkey.len);

  uint32_t entity_id;
  auto iter = pkey_update_idx_.find(std::string(pkey.data, pkey.len));
  if (iter != pkey_update_idx_.end()) {
    assert(vgroup_id == iter->second.subGroupId);
    entity_id = iter->second.entityId;
    pkey_update_idx_.erase(iter);
  } else {
    entity_id = vgroup->AllocateEntityID();
  }

  tag_lock.WrLock(hash_point);
  Defer defer{[&](){
    tag_lock.Unlock(hash_point);
  }};

  std::shared_ptr<TagTable> tag_table;
  s = table_->GetSchemaManager()->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get table id[%ld] tag schema.", table_->GetTableId());
    return s;
  }
  if (tag_table->InsertDeletedTagRecord(p, vgroup_id, entity_id, p.GetOSN(), type) < 0) {
    LOG_ERROR("Failed InsertTagRecord table id[%ld].", table_->GetTableId());
    return KStatus::FAIL;
  }
  // multi delete tags. store max osn of all.
  if (del_tag_osn_[std::string(pkey.data, pkey.len)] < p.GetOSN()) {
    del_tag_osn_[std::string(pkey.data, pkey.len)] = p.GetOSN();
  }

  return KStatus::SUCCESS;
}

KStatus STTableRangeDelAndTagInfo::WriteUpdateTagRecord(kwdbContext_p ctx, TSSlice& payload,
  OperateType type, TsHashRWLatch& tag_lock) {
  assert(payload.len > 0);
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  auto pkey = p.GetPrimaryTag();
  uint32_t entity_id;
  uint32_t vgroup_id = GetConsistentVgroupId(pkey.data, pkey.len, EngineOptions::vgroup_max_num);
  uint64_t hash_point = t1ha1_le(pkey.data, pkey.len);

  tag_lock.WrLock(hash_point);
  Defer defer{[&](){
    tag_lock.Unlock(hash_point);
  }};

  std::shared_ptr<TagTable> tag_table;
  s = table_->GetSchemaManager()->GetTagSchema(ctx, &tag_table);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed get table id[%ld] tag schema.", table_->GetTableId());
    return s;
  }
  assert(!tag_table->hasPrimaryKey(pkey.data, pkey.len, entity_id, vgroup_id));

  auto iter = pkey_update_idx_.find(std::string(pkey.data, pkey.len));
  if (iter == pkey_update_idx_.end()) {
    entity_id = table_->GetVGroupByID(vgroup_id)->AllocateEntityID();
    EntityResultIndex& cur_idx = pkey_update_idx_[std::string(pkey.data, pkey.len)];
    cur_idx.entityId = entity_id;
    cur_idx.subGroupId = vgroup_id;
  } else {
    assert(iter->second.subGroupId == vgroup_id);
    entity_id = iter->second.entityId;
  }
  if (tag_table->InsertDeletedTagRecord(p, vgroup_id, entity_id, p.GetOSN(), type) < 0) {
    LOG_ERROR("Failed InsertTagRecord table id[%ld].", table_->GetTableId());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus STTableRangeDelAndTagInfo::WriteInsertTagRecord(kwdbContext_p ctx, TSSlice& payload,
  OperateType type, TsHashRWLatch& tag_lock) {
  TSSlice pkey = TsRawPayload::GetPrimaryKeyFromSlice(payload);
  std::shared_ptr<TagTable> tag_schema;
  table_->GetSchemaManager()->GetTagSchema(ctx, &tag_schema);
  uint32_t entity_id;
  uint32_t groupid = GetConsistentVgroupId(pkey.data, pkey.len, EngineOptions::vgroup_max_num);
  uint64_t hash_point = t1ha1_le(pkey.data, pkey.len);
  auto iter = pkey_update_idx_.find(std::string(pkey.data, pkey.len));
  if (iter == pkey_update_idx_.end()) {
    entity_id = table_->GetVGroupByID(groupid)->AllocateEntityID();
  } else {
    assert(iter->second.subGroupId == groupid);
    entity_id = iter->second.entityId;
  }
  tag_lock.WrLock(hash_point);
  Defer defer{[&](){
    tag_lock.Unlock(hash_point);
  }};
  if (tag_schema->hasPrimaryKey(pkey.data, pkey.len, entity_id, groupid)) {
    LOG_ERROR("Failed write tag at hasPrimaryKey id[%ld], already exists.", table_->GetTableId());
    return KStatus::FAIL;
  }
  TsRawPayload p;
  auto s = p.ParsePayLoadStruct(payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("ParsePayLoadStruct failed.");
    return s;
  }
  if (tag_schema->InsertTagRecord(p, groupid, entity_id, p.GetOSN(), OperateType::Insert) < 0) {
    LOG_ERROR("InsertTagRecord failed.");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}
KStatus STTableRangeDelAndTagInfo::WriteDelAndTagInfo(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock) {
  STOSNDeleteInfoType type;
  TSSlice payload;
  TSSlice pkey;
  std::list<STDelRange> dels;
  ParseData(data, &type, &payload, &pkey, &dels);
  if (type == STOSNDeleteInfoType::OSN_DELETE_TAG_RECORD) {
    auto s = WriteDeleteTagRecord(ctx, payload, OperateType::Delete, tag_lock);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed table id[%ld] insert delete_tag..", table_->GetTableId());
      return s;
    }
  } else if (type == STOSNDeleteInfoType::OSN_UPDATE_TAG_RECORD) {
    auto s = WriteUpdateTagRecord(ctx, payload, OperateType::Update, tag_lock);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed get table id[%ld] update_tag..", table_->GetTableId());
      return s;
    }
  } else if (type == STOSNDeleteInfoType::OSN_DELETE_METRIC_RANGE) {
    assert(pkey.len > 0);
    assert(payload.len > 0);
    auto s = WriteInsertTagRecord(ctx, payload, OperateType::Insert, tag_lock);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("Failed table id[%ld] insert insert_tag..", table_->GetTableId());
      return s;
    }
    std::string ptag(pkey.data, pkey.len);
    TS_OSN last_del_tag_osn = 0;
    if (del_tag_osn_.find(ptag) != del_tag_osn_.end()) {
      last_del_tag_osn = del_tag_osn_[ptag];
    }
    for (STDelRange& del : dels) {
      // only delte info osn after
      if (del.osn_span.end > last_del_tag_osn) {
        pkey_del_ranges_[ptag].push_back(del);
      }
    }
  } else {
    LOG_ERROR("can not parse this STOSNDeleteInfoType [%u]", type);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus STTableRangeDelAndTagInfo::CommitDeleteInfo(kwdbContext_p ctx) {
  for (auto pkey : pkey_del_ranges_) {
    std::string cur_pkey = pkey.first;
    for (STDelRange& del : pkey.second) {
      auto s = table_->DeleteData(ctx, 1, cur_pkey, {del.ts_span}, nullptr, 0, del.osn_span.end);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Failed DeleteData [%lu].", table_->GetTableId());
        return s;
      }
    }
  }
  return KStatus::SUCCESS;
}

static inline uint64_t EncodeTableID(uint64_t table_id, uint32_t version) {
  table_id &= 0x7FFFFFFF;
  table_id |= (1ULL << 31);
  return (static_cast<uint64_t>(version) << 32) | table_id;
}
static inline std::pair<uint64_t, uint32_t> DecodeTableID(uint64_t table_id) {
  uint64_t table_id_mask = 0x7FFFFFFF;
  uint32_t version = table_id >> 32;
  return {table_id_mask & table_id, version};
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
  tbl_id = tbl_id;
  data_with_rownum += 8;
  tbl_version = KUint32(data_with_rownum);
  data_with_rownum += 4;
  row_num = KUint32(data_with_rownum);
  data_with_rownum += 4;
  uint32_t snapshot_version = KUint32(data_with_rownum);
  if (snapshot_version == 0) {
    batch_data.data = data_with_rownum;
    batch_data.len = data.len - 20;
  } else if (snapshot_version == CURRENT_SNAPSHOT_VERSION) {
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

}  // namespace kwdbts
