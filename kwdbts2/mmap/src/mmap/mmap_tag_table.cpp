// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include <algorithm>
#include "sys_utils.h"
#include "dirent.h"
#include "mmap/mmap_tag_table.h"
#include "mmap/mmap_ptag_hash_index.h"
#include "ts_table.h"

TagTable::TagTable(const std::string& db_path,
                   const std::string& sub_path,
                   uint64_t table_id,
                   int32_t entity_group_id):
                  m_db_path_(db_path), m_tbl_sub_path_(sub_path),
                  m_table_id(table_id), m_entity_group_id_(entity_group_id) {
  m_mutex_ = new KLatch(LATCH_ID_TAG_TABLE_METADATA_MUTEX);
  m_version_mutex_ = new KLatch(LATCH_ID_TAG_TABLE_VERSION_MUTEX);
}

TagTable::~TagTable() {
  delete m_version_mgr_;
  delete m_partition_mgr_;
  delete m_index_;
  delete m_entity_row_index_;
  delete m_mutex_;
  delete m_version_mutex_;
  m_version_mgr_ = nullptr;
  m_partition_mgr_ = nullptr;
  m_index_ = nullptr;
  m_entity_row_index_ = nullptr;
  m_mutex_ = nullptr;
  m_version_mutex_ = nullptr;
}

int TagTable::create(const vector<TagInfo> &schema, uint32_t table_version, ErrorInfo &err_info) {
  LOG_INFO("Create TagTable table_version:%d", table_version)
  // 1. create table version
  m_version_mgr_ = KNEW TagTableVersionManager(m_db_path_, m_tbl_sub_path_, m_table_id);
  if (m_version_mgr_ == nullptr) {
    LOG_ERROR("KNEW TagTableVersionManager failed.");
    return -1;
  }
  auto tag_ver_obj = m_version_mgr_->CreateTagVersionObject(schema, table_version, err_info);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("call CreateTagVersionObject failed, %s ", err_info.errmsg.c_str());
    return -1;
  }
  // 2. create tag partition table
  m_partition_mgr_ = KNEW TagPartitionTableManager(m_db_path_, m_tbl_sub_path_, m_table_id, m_entity_group_id_);
  if (m_partition_mgr_ == nullptr) {
    LOG_ERROR("KNEW TagPartitionTableManager failed.");
    return -1;
  }
  if (m_partition_mgr_->CreateTagPartitionTable(schema, table_version, err_info) < 0) {
    LOG_ERROR("CreateTagPartitionTable failed, %s", err_info.errmsg.c_str());
    return -1;
  }
  // 3. create hash index
  if (initHashIndex(MMAP_CREAT_EXCL, err_info) < 0) {
    LOG_ERROR("create HashIndex failed, %s", err_info.errmsg.c_str());
    return -1;
  }
  // 4. create entity row hash index
  if (initEntityRowHashIndex(MMAP_CREAT_EXCL, err_info) < 0) {
    LOG_ERROR("create EntityRow HashIndex failed, %s", err_info.errmsg.c_str());
    return -1;
  }

  tag_ver_obj->setStatus(TAG_STATUS_READY);
  m_version_mgr_->UpdateNewestTableVersion(table_version);
  m_version_mgr_->SyncCurrentTableVersion();
  return 0;
}

int TagTable::open(ErrorInfo &err_info) {
  if (nullptr == m_version_mgr_) {
    m_version_mgr_ = KNEW TagTableVersionManager(m_db_path_, m_tbl_sub_path_, m_table_id);
    if (m_version_mgr_ == nullptr) {
      LOG_ERROR("KNEW TagTableVersionManager failed.");
      return -1;
    }
  }

  if (nullptr == m_partition_mgr_) {
    m_partition_mgr_ = KNEW TagPartitionTableManager(m_db_path_, m_tbl_sub_path_, m_table_id, m_entity_group_id_);
    if (m_partition_mgr_ == nullptr) {
      LOG_ERROR("KNEW TagPartitionTableManager failed.");
      return -1;
    }
  }
  // 1. load all versions id
  std::vector<TableVersion> all_versions;
  if (loadAllVersions(all_versions, err_info) < 0) {
    return -1;
  }
  // 2. open all object
  for (const auto it : all_versions) {
    // open tag version object
    auto tag_ver_obj = m_version_mgr_->OpenTagVersionObject(it, err_info);
    if (nullptr == tag_ver_obj || !tag_ver_obj->isValid()) {
      // tag version is invalid
      continue;
    }
    // open partition table
    if (m_partition_mgr_->OpenTagPartitionTable(it, err_info) < 0) {
      continue;
    }
    m_version_mgr_->UpdateNewestTableVersion(it);
  }

  // 3. open hash index
  if (initHashIndex(MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    LOG_ERROR("open HashIndex failed. error: %s ", err_info.errmsg.c_str());
    return -1;
  }
  // 4. open entity row hash index
  if (initEntityRowHashIndex(MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    LOG_ERROR("open Entity Row HashIndex failed. error: %s ", err_info.errmsg.c_str());
    return -1;
  }
  m_version_mgr_->SyncCurrentTableVersion();
  return 0;
}

int TagTable::remove(ErrorInfo &err_info) {
  // TsEntityGroup->drop_mutex_ control concurrency,so we don't add ref_cnt mutex to control waiting
  if (m_version_mgr_->RemoveAll(err_info) < 0) {
    LOG_ERROR("call TagTableVersionManager::RemoveAll failed, %s ", err_info.errmsg.c_str());
    return err_info.errcode;
  }
  delete m_version_mgr_;
  m_version_mgr_ = nullptr;
  if (m_partition_mgr_->RemoveAll(err_info) < 0) {
    LOG_ERROR("call TagPartitionTableManager::RemoveAll failed, %s ", err_info.errmsg.c_str());
    return err_info.errcode;
  }
  delete m_partition_mgr_;
  m_partition_mgr_ = nullptr;
  if (m_index_ && (m_index_->clear() < 0)) {
    LOG_ERROR("TagHashIndex remove failed.");
    return -1;
  }
  delete m_index_;
  m_index_ = nullptr;

  if (m_entity_row_index_ && (m_entity_row_index_->remove() < 0)) {
    LOG_ERROR("m_entity_row_index_ remove failed.");
    return -1;
  }
  delete m_entity_row_index_;
  m_entity_row_index_ = nullptr;
  LOG_DEBUG("Tag table remove successed.");
  fs::remove_all((m_db_path_ + m_tbl_sub_path_).c_str());
  return 0;
}

// check ptag exist
bool TagTable::hasPrimaryKey(const char* primary_tag_val, int len, uint32_t& entity_id, uint32_t& sub_group_id) {
  auto ret = m_index_->get(primary_tag_val, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    return false;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_WARN("primary key record's table_version[%u] does not exist.", ret.first);
    return false;
  }
  tag_partition_table->getEntityIdGroupId(ret.second, entity_id, sub_group_id);
  return true;
}

// check ptag exist
bool TagTable::hasPrimaryKey(const char* primary_tag_val, int len) {
  auto ret = m_index_->get(primary_tag_val, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    return false;
  }
  return true;
}

// insert tag record
int TagTable::InsertTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id) {
  // 1. check version
  auto tag_version_object = m_version_mgr_->GetVersionObject(payload.GetTsVersion());
  if (nullptr == tag_version_object) {
    LOG_ERROR("Tag table version[%u] doesnot exist.", payload.GetTsVersion());
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("Tag partition table version[%u] doesnot exist.", tag_partition_version);
    return -1;
  }

  // 2. insert partition data
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, sub_group_id, payload.getHashPoint(),
                                  payload.GetTagAddr(), &row_no) < 0) {
    LOG_ERROR("insert tag partition table[%s/%s] failed. ",
       tag_partition_table->m_tbl_sub_path_.c_str(), tag_partition_table->m_name_.c_str());
    return -1;
  }
  // 3. insert index data
  TSSlice tmp_slice = payload.GetPrimaryTag();
  if (m_index_->insert(tmp_slice.data, tmp_slice.len, tag_partition_version, row_no) < 0) {
    LOG_ERROR("insert hash index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
    return -1;
  }

  // 4. insert normal index data
  // 1) check exist normal index, loop insert
  tag_partition_table->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
    std::vector<TSSlice> index_cols;
    size_t len = 0;
    auto col_ids = ntag_index->getColIDs();
    for (auto col_id : col_ids) {
      uint32_t col_size = tag_partition_table->getTagColSize(col_id);
      uint32_t off = tag_partition_table->getTagColOff(col_id);
      auto col_val = payload.GetNormalTag(off, col_size);
      index_cols.emplace_back(col_val);
      len += col_val.len;
    }
    char index_key[len];
    int num = 0;
    for(auto r:index_cols){
      memcpy(&index_key[num], r.data, r.len);
      num += r.len;
    }
    if (ntag_index->insert(index_key, len, tag_partition_version, row_no) < 0) {
      tag_partition_table->NtagIndexRWMutexUnLock();
      LOG_ERROR("insert hash index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
      return -1;
    }
  }
  tag_partition_table->NtagIndexRWMutexUnLock();

  // 5. insert entity row index
  uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | sub_group_id;
  if (m_entity_row_index_->put(reinterpret_cast<const char *>(&joint_entity_id), sizeof(uint64_t), tag_partition_version, row_no) < 0) {
    LOG_ERROR("insert entity row hash index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
    return -1;
  }

  // 6. set undelete mark
  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

// update tag record
int TagTable::UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info) {
  // 1. delete
  TSSlice tmp_primary_tag = payload.GetPrimaryTag();
  if (this->DeleteTagRecord(tmp_primary_tag.data, tmp_primary_tag.len, err_info) < 0) {
    err_info.errmsg = "delete tag data failed";
    LOG_ERROR("delete tag data failed, error: %s", err_info.errmsg.c_str());
    return err_info.errcode;
  }

  // 2. insert
  if ((err_info.errcode = this->InsertTagRecord(payload, sub_group_id, entity_id)) < 0 ) {
    err_info.errmsg = "insert tag data fail";
    return err_info.errcode;
  }
  return 0;
}

// delete tag record by ptag
int TagTable::DeleteTagRecord(const char *primary_tags, int len, ErrorInfo& err_info) {
  // 1. find
  auto ret = m_index_->get(primary_tags, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    err_info.errmsg = "delete data not found";
    err_info.errcode = -1;
    return -1;
  }
  auto tag_part_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (tag_part_table == nullptr) {
    LOG_ERROR("Tag Partition Table does not exist. table_version: %u", ret.first);
    err_info.errmsg = "delete data not found";
    err_info.errcode = -1;
    return -1;
  }

  // 1. delete normal index record
  auto tag_version_object = m_version_mgr_->GetVersionObject(ret.first);
  if (nullptr == tag_version_object) {
    LOG_ERROR("Tag table version[%u] doesnot exist.", ret.first);
    return -1;
  }
  tag_part_table->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_part_table->getMmapNTagHashIndex()) {
    // ntag_index->getTagColID();
    TSSlice tag_val;
    std::vector<uint32_t> src_scan_tags;
    std::vector<uint32_t> result_scan_tags_idx;
    std::vector<TagInfo> result_scan_tag_infos;
    auto schema_info_exclude_dropped = tag_version_object->getExcludeDroppedSchemaInfos();
    auto col_ids = ntag_index->getColIDs();
    for (auto col_id : col_ids) {
      src_scan_tags.push_back(col_id);
    }
    for (int i = 0; i < src_scan_tags.size(); i++) {
      for (int tag_idx = 0; tag_idx < schema_info_exclude_dropped.size(); tag_idx++) {
        if (schema_info_exclude_dropped[tag_idx].m_id == src_scan_tags[i]) {
          result_scan_tags_idx.emplace_back(tag_version_object->getValidSchemaIdxs()[tag_idx]);
        }
      }
    }

    // get source scan tags
    std::vector<uint32_t> src_scan_tags_idx;
    for (int idx = 0; idx < src_scan_tags.size(); idx++) {
      if (result_scan_tags_idx[idx] >= tag_part_table->getIncludeDroppedSchemaInfos().size()) {
        src_scan_tags_idx.push_back(INVALID_COL_IDX);
      } else {
        src_scan_tags_idx.push_back(result_scan_tags_idx[idx]);
      }
    }
    result_scan_tag_infos = tag_version_object->getIncludeDroppedSchemaInfos();
    kwdbts::ResultSet res;
    tag_part_table->startRead();
    tag_part_table->getColumnsByRownum(ret.second, src_scan_tags_idx, result_scan_tag_infos, &res);
    tag_part_table->stopRead();

    std::vector<void*> tag_cols;
    for (int col = 0; col < res.data.size(); col++) {
      auto per_batch = (TagBatch*)res.data[col].at(0);
      tag_cols.emplace_back(per_batch->getRowAddr(0));
    }

    char index_key[ntag_index->keySize()];
    int num = 0;
    for(int i = 0; i < tag_cols.size(); i++){
      memcpy(&index_key[num], (char*)tag_cols[i], result_scan_tag_infos[src_scan_tags_idx[i]].m_size);
      num += result_scan_tag_infos[src_scan_tags_idx[i]].m_size;
    }
    ntag_index->remove(ret.second, ret.first, index_key, ntag_index->keySize());
  }
  tag_part_table->NtagIndexRWMutexUnLock();

  // 2. delete mark
  uint32_t entity_id{0}, sub_group_id{0};
  tag_part_table->startRead();
  tag_part_table->setDeleteMark(ret.second);
  tag_part_table->getEntityIdGroupId(ret.second, entity_id, sub_group_id);
  tag_part_table->stopRead();

  // 3. delete index record
  auto ret_del = m_index_->remove(primary_tags, len);

  // 4. delete entity row index record
  uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | sub_group_id;
  ret_del = m_entity_row_index_->delete_data(reinterpret_cast<const char *>(&joint_entity_id));
  return 0;
}

// Get the column value of the tag using RowID.
int TagTable::getDataWithRowID(TagPartitionTable* tag_partition, std::pair<TableVersionID, TagPartitionTableRowID> ret,
                               std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                               kwdbts::ResultSet* res, std::vector<uint32_t> &scan_tags, std::vector<uint32_t> &valid_scan_tags,
                               TagVersionObject* tag_version_obj, uint32_t scan_tags_num, bool get_partition) {
  TableVersionID tbl_version = ret.first;
  TagPartitionTableRowID row = ret.second;
  if(get_partition) {
    tag_partition = m_partition_mgr_->GetPartitionTable(tbl_version);
    if (nullptr == tag_partition) {
      LOG_ERROR("GetPartitionTable not found. table_version: %u ", tbl_version);
      return -1;
    }
    // get actual schemas
    valid_scan_tags.clear();
    for (int idx = 0; idx < scan_tags_num; ++idx) {
      if (scan_tags[idx] >= tag_partition->getIncludeDroppedSchemaInfos().size()) {
        valid_scan_tags.push_back(INVALID_COL_IDX);
      } else {
        valid_scan_tags.push_back(scan_tags[idx]);
      }
    }
  }
  tag_partition->startRead();
  if (!EngineOptions::isSingleNode()) {
    uint32_t hps;
    tag_partition->getHashpointByRowNum(row, &hps);
    tag_partition->getHashedEntityIdByRownum(row, hps, entity_id_list);
  } else {
    tag_partition->getEntityIdByRownum(row, entity_id_list);
  }
  int err_code = 0;
  if ((err_code = tag_partition->getColumnsByRownum(
      row, valid_scan_tags,
      tag_version_obj->getIncludeDroppedSchemaInfos(),
      res)) < 0) {
    tag_partition->stopRead();
    return err_code;
  }
  tag_partition->stopRead();
  return err_code;
}

// Query RowID using primary tag indexes.
int TagTable::getRowIDByPTag(const std::vector<void*>& primary_tags,
                             std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &ptag_value) {
  for (int idx = 0; idx < primary_tags.size(); idx++) {
    auto ret = m_index_->get(reinterpret_cast<char *>(primary_tags[idx]), m_index_->keySize());
    if (ret.second == 0) {
      // not found
      LOG_DEBUG("hash index not found.");
      continue;
    }
    ptag_value.emplace_back(ret);
  }
  return 0;
}

// Get the intersection of results.
int TagTable::getIntersectionValue(uint32_t value_size, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> value[],
                                   std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) {
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> array[value_size];
  // The intersection of two result sets is directly evaluated.
  if (value_size == 2) {
    std::set_intersection(value[0].begin(), value[0].end(),
                          value[1].begin(), value[1].end(),
                          std::back_inserter(result));
  } else {
    // When there are multiple result sets, the intersection of the first two results is required,
    // and the resulting value is then intersected with the subsequent result set.
    std::set_intersection(value[0].begin(), value[0].end(),
                          value[1].begin(), value[1].end(),
                          std::back_inserter(array[0]));
    // (tmp-1) is the result of the intersection already obtained.
    int tmp = 1;
    for (int idx = 2; idx < value_size; idx++) {
      if (value[idx].size() != 0) {
        std::set_intersection(array[tmp -1].begin(), array[tmp-1].end(),
                              value[idx].begin(), value[idx].end(),
                              std::back_inserter(array[tmp]));
        tmp++;
      }
    }
    // Record the final intersection result.
    result.insert(result.end(), array[tmp-1].begin(), array[tmp-1].end());
  }
  return 0;
}

// Computes a union of result sets.
int TagTable::getUnionValue(uint32_t value_size, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> value[],
                            std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) {
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> array[value_size];
  if (value_size == 2) {
    // Computes the union of two result sets directly.
    std::set_union(value[0].begin(), value[0].end(),
                          value[1].begin(), value[1].end(),
                          std::back_inserter(result));
  } else {
    // When there are multiple result sets, the first two results need to be combined,
    // and the resulting value is then combined with a subsequent result set.
    std::set_union(value[0].begin(), value[0].end(),
                          value[1].begin(), value[1].end(),
                          std::back_inserter(array[0]));
    // (tmp-1) is the result of the union already obtained.
    int tmp = 1;
    for (int idx = 2; idx < value_size; idx++) {
      if (value[idx].size() != 0) {
        std::set_union(array[tmp - 1].begin(), array[tmp - 1].end(),
                       value[idx].begin(), value[idx].end(),
                       std::back_inserter(array[tmp]));
        tmp++;
      }
    }
    // Record the final union result.
    result.insert(result.end(), array[tmp - 1].begin(), array[tmp - 1].end());
  }
  return 0;
}

// Query RowID using normal tag indexes.
int TagTable::getRowIDByNTag(const std::vector<uint64_t> &tags_index_id, const std::vector<void*> tags,
                   TSTagOpType op_type, TagPartitionTable* tag_part_table,
                   std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result_val) {
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> value[tags_index_id.size()];
  tag_part_table->NtagIndexRWMutexSLock();
  std::vector<MMapNTagHashIndex*> ntag_indexs = tag_part_table->getMmapNTagHashIndex();
  for (int idx = 0; idx < tags_index_id.size(); idx++) {
    for (MMapNTagHashIndex *ntag_hash: ntag_indexs) {
      if (ntag_hash->getIndexID() == tags_index_id[idx]) {
        ntag_hash->get_all(reinterpret_cast<char *>(tags[idx]), ntag_hash->keySize(), value[idx]);
        if(value[idx].size() > 1) {
          sort(value[idx].begin(), value[idx].end());
        }
      }
    }
  }
  tag_part_table->NtagIndexRWMutexUnLock();
  // Generate result sets based on operators.
  if (tags_index_id.size() > 1 && op_type != TSTagOpType::opUnKnow) {
    switch(op_type) {
      case TSTagOpType::opAnd:
        getIntersectionValue(tags_index_id.size(), value, result_val);
        break;
      case TSTagOpType::opOr:
        getUnionValue(tags_index_id.size(), value, result_val);
        break;
    }
  } else { // The unknown result operator or a single result set, returns the result directly.
    for(auto val: value) {
      if (val.size() != 0) {
        result_val.insert(result_val.end(), val.begin(), val.end());
      }
    }
  }
  return 0;
}

// Query tag through the index of the primary tag and normal tag.
int TagTable::GetEntityIdList(const std::vector<void*>& primary_tags, const std::vector<uint64_t/*index_id*/> &tags_index_id,
                              const std::vector<void*> tags, TSTagOpType op_type, const std::vector<uint32_t> &scan_tags,
                              std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                              kwdbts::ResultSet* res, uint32_t* count, uint32_t table_version) {
  TagPartitionTableRowID row;
  uint32_t fetch_count = 0;
  TagVersionObject*   result_tag_version_obj = nullptr;
  TagPartitionTable * result_tag_part_table = nullptr;
  TagPartitionTable*  src_tag_partition_tbl = nullptr;
  TableVersionID last_tbl_version = 0;
  // 1. get result table version scan tags
  result_tag_version_obj = m_version_mgr_->GetVersionObject(table_version);
  if (nullptr == result_tag_version_obj) {
    LOG_ERROR("GetVersionObject not found. table_version: %u ", table_version);
    return -1;
  }
  result_tag_part_table = m_partition_mgr_->GetPartitionTable(result_tag_version_obj->metaData()->m_real_used_version_);
  if (result_tag_part_table == nullptr) {
    LOG_ERROR("GetPartitionTable not found. table_version: %u ", table_version);
    return -1;
  }
  std::vector<uint32_t> result_scan_tags;
  for(const auto& tag_idx : scan_tags) {
    result_scan_tags.emplace_back(result_tag_version_obj->getValidSchemaIdxs()[tag_idx]);
  }
  std::vector<uint32_t> src_scan_tags;
  uint32_t scan_tag_num = scan_tags.size();
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> ptag_value;
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> ntag_value;
  // primary tag index query.
  if (primary_tags.size() > 0) {
    getRowIDByPTag(primary_tags, ptag_value);
  }
  // normal tag index query.
  if (tags_index_id.size() > 0) {
    getRowIDByNTag(tags_index_id, tags, op_type, result_tag_part_table, ntag_value);
  }
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> result_value;
  // Only the Or operations of ptag and normal indexes are supported.
  // The And operations do not occur because of the tagfilter processing logic.
  if ((ptag_value.size() > 0) && (ntag_value.size() == 0)) { // ptag
    result_value.insert(result_value.end(), ptag_value.begin(), ptag_value.end());
  } else if ((ptag_value.size() == 0) && (ntag_value.size() > 0)) { // ntag
    result_value.insert(result_value.end(), ntag_value.begin(), ntag_value.end());
  } else if ((ptag_value.size() > 0) && (ntag_value.size() > 0) && (op_type == 1)) {
    // The union of ptag and ntag.
    sort(ptag_value.begin(), ptag_value.end());
    sort(ntag_value.begin(), ntag_value.end());
    std::set_union(ptag_value.begin(), ptag_value.end(),
                   ntag_value.begin(), ntag_value.end(),
                   std::back_inserter(result_value));
  }

  if (result_value.size() != 0) {
    for (int idx = 0; idx < result_value.size(); idx++) {
      TableVersionID tbl_version = result_value[idx].first;
      bool get_partition = false;
      if (src_tag_partition_tbl == nullptr || tbl_version != last_tbl_version) {
        get_partition = true;
        last_tbl_version = tbl_version;
      }
      int err_code = getDataWithRowID(src_tag_partition_tbl, result_value[idx], entity_id_list, res, result_scan_tags, src_scan_tags, result_tag_version_obj, scan_tag_num, get_partition);
      if (err_code == 0) {
        fetch_count++;
      } else {
        return err_code;
      }
    }
  }
  *count = fetch_count;
  return 0;
}
int TagTable::GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
               const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count, uint32_t table_version) {
  TagPartitionTableRowID row;
  uint32_t fetch_count = 0;
  TagVersionObject*   result_tag_version_obj = nullptr;
  TagPartitionTable*  src_tag_partition_tbl = nullptr;
  TableVersionID last_tbl_version = 0;
  // 1. get result table version scan tags
  result_tag_version_obj = m_version_mgr_->GetVersionObject(table_version);
  if (nullptr == result_tag_version_obj) {
    LOG_ERROR("GetVersionObject not found. table_version: %u ", table_version);
    return -1;
  }
  std::vector<uint32_t> result_scan_tags;
  for(const auto& tag_idx : scan_tags) {
    result_scan_tags.emplace_back(result_tag_version_obj->getValidSchemaIdxs()[tag_idx]);
  }
  std::vector<uint32_t> src_scan_tags;
  for (int idx = 0; idx < entity_id_list.size(); idx++) {
    //2. get entity row index data
    uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id_list[idx].entityId) << 32) | entity_id_list[idx].subGroupId;
    auto ret = m_entity_row_index_->get(reinterpret_cast<const char *>(&joint_entity_id), m_entity_row_index_->keySize());
    if (ret.second == 0) {
      // not found
      LOG_DEBUG("entity row hash index not found.");
      continue;
    }
    TableVersionID tbl_version = ret.first;
    TagPartitionTableRowID row = ret.second;
    if (src_tag_partition_tbl == nullptr || tbl_version != last_tbl_version) {
      src_tag_partition_tbl = m_partition_mgr_->GetPartitionTable(tbl_version);
      if (nullptr == src_tag_partition_tbl) {
        LOG_ERROR("GetPartitionTable not found. table_version: %u ", tbl_version);
        return -1;
      }
      last_tbl_version = tbl_version;
      // get actual schemas
      src_scan_tags.clear();
      for(int i = 0; i < scan_tags.size(); ++i) {
        if (result_scan_tags[i] >= src_tag_partition_tbl->getIncludeDroppedSchemaInfos().size()) {
          src_scan_tags.push_back(INVALID_COL_IDX);
        } else {
          src_scan_tags.push_back(result_scan_tags[i]);
        }
      }
    }
    //3. read data
    src_tag_partition_tbl->startRead();
    // tag column
    int err_code = 0;
    if ((err_code = src_tag_partition_tbl->getColumnsByRownum(
            row, src_scan_tags,
            result_tag_version_obj->getIncludeDroppedSchemaInfos(),
            res)) < 0) {
      src_tag_partition_tbl->stopRead();
      return err_code;
    }
    src_tag_partition_tbl->stopRead();
    fetch_count++;
  }  // end for primary_tags
  *count = fetch_count;
  return 0;
}

// query tag by rownum
int TagTable::GetColumnsByRownumLocked(TableVersion src_table_version, uint32_t src_row_id,
                              const std::vector<uint32_t>& src_tag_idxes,
                              const std::vector<TagInfo>& result_tag_infos,
                              kwdbts::ResultSet* res) {
  auto src_tag_partition_tbl = m_partition_mgr_->GetPartitionTable(src_table_version);
  if (nullptr == src_tag_partition_tbl) {
      LOG_ERROR("GetPartitionTable not found. table_version: %u ", src_table_version);
      return -1;
  }
  //2. read data
  src_tag_partition_tbl->startRead();
  // tag column
  int err_code = 0;
  if ((err_code = src_tag_partition_tbl->getColumnsByRownum(
                                        src_row_id, src_tag_idxes,
                                        result_tag_infos,
                                        res)) < 0) {
    src_tag_partition_tbl->stopRead();
    return err_code;
  }
  src_tag_partition_tbl->stopRead();
  return 0;
}

int TagTable::CalculateSchemaIdxs(TableVersion src_table_version, const std::vector<uint32_t>& result_scan_idxs,
                                   const std::vector<TagInfo>& result_tag_infos,
                                  std::vector<uint32_t>* src_scan_idxs) {
  auto src_tag_version_obj = m_version_mgr_->GetVersionObject(src_table_version);
  if (nullptr == src_tag_version_obj) {
    LOG_ERROR("GetVersionObject not found. table_version: %u ", src_table_version);
    return -1;
  }

  // get actual schemas
  src_scan_idxs->clear();
  for(int idx = 0; idx < result_scan_idxs.size(); ++idx) {
    if (result_scan_idxs[idx] >= src_tag_version_obj->getIncludeDroppedSchemaInfos().size()) {
      src_scan_idxs->push_back(INVALID_COL_IDX);
    } else {
      src_scan_idxs->push_back(result_scan_idxs[idx]);
    }
  }
  return 0;
}

bool isSoftLink(const std::string& path) {
  struct stat fileStat;
  if (lstat(path.c_str(), &fileStat) == -1) {
    LOG_ERROR("Error: Unable to get file status for %s", path.c_str())
    return false;
  }
  return S_ISLNK(fileStat.st_mode);
}

int TagTable::createHashIndex(uint32_t new_version, ErrorInfo &err_info, const std::vector<uint32_t> &tags,
                              uint32_t index_id) {
  TagVersionObject *obj = m_version_mgr_->GetVersionObject(new_version);
  if (nullptr == obj) {
      return -1;
  }
  auto all_schema = obj->getIncludeDroppedSchemaInfos();
  auto schema_info_exclude_dropped = obj->getExcludeDroppedSchemaInfos();
  auto tag_part = GetTagPartitionTableManager()->GetPartitionTable(obj->metaData()->m_real_used_version_);

  std::vector<uint32_t> result_scan_tags_idx;
  std::vector<TagInfo> result_scan_tags_info;
  auto ordered_tags = tags;
  sort(ordered_tags.begin(), ordered_tags.end());
  for (int i = 0; i < ordered_tags.size(); i++) {
    for (int tag_idx = 0; tag_idx < schema_info_exclude_dropped.size(); tag_idx++) {
      if (schema_info_exclude_dropped[tag_idx].m_id == ordered_tags[i]) {
        result_scan_tags_idx.emplace_back(obj->getValidSchemaIdxs()[tag_idx]);
      }
    }
  }

  string index_name = std::to_string(m_table_id) + "_" + to_string(index_id) + ".tag" + ".ht";
  if (access((m_db_path_ + tag_part->m_db_name_ + index_name).c_str(), F_OK) == 0) {
    // file already exist.
    return 0;
  }

  int tags_size = 0;
  for (uint32_t col_id: ordered_tags) {
    for (auto tag_info: all_schema) {
      if (tag_info.m_id == col_id) {
        tags_size += tag_info.m_size;
      }
    }
  }

  MMapNTagHashIndex *mmap_ntag_index = new MMapNTagHashIndex(tags_size, index_id, ordered_tags);
  err_info.errcode = mmap_ntag_index->open(index_name, m_db_path_, tag_part->m_db_name_,
                                           MMAP_CREAT_EXCL, err_info);
  if (err_info.errcode < 0) {
    delete mmap_ntag_index;
    mmap_ntag_index = nullptr;
    err_info.errmsg = "create Hash Index failed.";
    LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), index_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  // todo insert data
  // 1. get all partition tables
  std::vector<TagPartitionTable *> all_part_tables;
  GetTagPartitionTableManager()->GetAllPartitionTablesLessVersion(all_part_tables, new_version);
  if (all_part_tables.empty()) {
    LOG_ERROR("tag table version [%u]'s partition table is empty.", new_version);
    return -1;
  }
  // 2. init TagPartitionIterator
  for (const auto &tag_part_ptr: all_part_tables) {
    tag_part_ptr->startWrite();
    if (tag_part_ptr->metaData().m_ts_version == tag_part->metaData().m_ts_version) {
      tag_part_ptr->NtagIndexRWMutexXLock();
      tag_part_ptr->getMmapNTagHashIndex().emplace_back(mmap_ntag_index);
      tag_part_ptr->NtagIndexRWMutexUnLock();
    } else {
      // symlink
      string new_index_file_name =
              std::to_string(m_table_id) + "_" + to_string(mmap_ntag_index->getIndexID()) + ".tag" + ".ht";
      string new_index_path = tag_part_ptr->m_db_path_ + tag_part_ptr->m_db_name_ + new_index_file_name;
      int errcode = symlink(mmap_ntag_index->realFilePath().c_str(), new_index_path.c_str());
      if (errcode != 0) {
        LOG_ERROR("create hash index symlink failed, errorcode:%d, errno:%d", errcode, errno)
      }
      tag_part_ptr->NtagIndexRWMutexXLock();
      tag_part_ptr->getMmapNTagHashIndex().emplace_back(mmap_ntag_index);
      tag_part_ptr->NtagIndexRWMutexUnLock();
    }

    // get source scan tags
    std::vector<uint32_t> src_scan_tags_idx;
    for (int idx = 0; idx < ordered_tags.size(); idx++) {
      if (result_scan_tags_idx[idx] >= tag_part_ptr->getIncludeDroppedSchemaInfos().size()) {
        src_scan_tags_idx.push_back(INVALID_COL_IDX);
      } else {
        src_scan_tags_idx.push_back(result_scan_tags_idx[idx]);
      }
    }
    // tag partition
    kwdbts::ResultSet res;
    std::vector<TagTableRowID> result_tag_rows;
    size_t need_release_rows = 0;
    //  for get
    for (size_t row_num = 1; row_num <= tag_part_ptr->size(); row_num++) {
      if (tag_part_ptr->isValidRow(row_num)) {
        tag_part_ptr->getColumnsByRownum(row_num, src_scan_tags_idx, all_schema, &res);
        result_tag_rows.emplace_back(row_num);
        need_release_rows++;
      }
      if (need_release_rows > max_rows_per_res || row_num == tag_part_ptr->size()) {
        if (!res.empty()) {
          for (int row = 0; row < result_tag_rows.size(); row++) {
            int continueFlag = 0;
            std::vector<void *> tag_cols;
            for (int col = 0; col < res.data.size(); col++) {
              auto per_batch = (TagBatch *) res.data[col].at(row);
              if (per_batch->data_length_ != 0) {
                tag_cols.emplace_back(per_batch->getRowAddr(0));
              } else {
                continueFlag = 1;
                continue;
              }
            }
            if (continueFlag == 1) { continue; }

            char index_key[tags_size];
            int num = 0;
            for (int i = 0; i < tag_cols.size(); i++) {
              memcpy(&index_key[num], (char *) tag_cols[i], all_schema[src_scan_tags_idx[i]].m_size);
              num += all_schema[src_scan_tags_idx[i]].m_size;
            }

            mmap_ntag_index->insert(index_key, tags_size, tag_part_ptr->metaData().m_ts_version,
                                    result_tag_rows[row]);
          }
        }
        result_tag_rows.clear();
        res.clear();
        need_release_rows = 0;
      }
    }
    tag_part_ptr->stopWrite();
  }
  return 0;
}

int TagTable::dropHashIndex(uint32_t new_version, ErrorInfo &err_info, uint32_t index_id) {
  LOG_INFO("DropHashIndex index_id:%d, new_version:%d", index_id, new_version)
  uint32_t cur_ver = 1;
  // drop the soft link first and then drop the index file.
  MMapNTagHashIndex* index_file = nullptr;
  while (cur_ver <= new_version) {
    TagVersionObject *tag_ver_obj = m_version_mgr_->GetVersionObject(cur_ver);
    if (tag_ver_obj == nullptr) {
      LOG_DEBUG("GetVersionObject not found. table_version: %u ", cur_ver);
      cur_ver++;
      continue;
    }
    TagPartitionTable *part_table = m_partition_mgr_->GetPartitionTable(tag_ver_obj->metaData()->m_real_used_version_);
    if (part_table == nullptr) {
      LOG_ERROR("GetPartitionTable not found. table_version: %u ", cur_ver);
      return -1;
    }

    string index_name = std::to_string(m_table_id) + "_" + to_string(index_id) + ".tag" + ".ht";

    part_table->NtagIndexRWMutexXLock();
    std::vector<MMapNTagHashIndex *> &mmap_ntag_index = part_table->getMmapNTagHashIndex();
    for (auto pos = mmap_ntag_index.begin(); pos < mmap_ntag_index.end(); pos++) {
      if ((*pos) && (*pos)->getIndexID() == index_id) {
        if (isSoftLink(m_db_path_ + part_table->m_db_name_ + index_name)) {
          unlink((m_db_path_ + part_table->m_db_name_ + index_name).c_str());
        } else {
          index_file = *pos;
        }
        mmap_ntag_index.erase(pos);
        break;
      }
      if (pos == mmap_ntag_index.end() && new_version == tag_ver_obj->metaData()->m_real_used_version_) {
        part_table->NtagIndexRWMutexUnLock();
        LOG_INFO("TagHashIndex [%d] can't find. Might Drop yet", index_id);
        return 0;
      }
    }
    part_table->NtagIndexRWMutexUnLock();
    cur_ver++;
  }
  if (index_file != nullptr) {
      if (index_file->clear() < 0) {
          LOG_ERROR("TagHashIndex remove failed.");
          return -1;
      }
  } else {
      LOG_ERROR("TagHashIndex remove failed. Not find index file");
  }

  return 0;
}

int TagTable::addNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info,
                                     const std::vector<uint32_t> &tags, uint32_t index_id, HashIndex idx_flag) {
  LOG_INFO("addNewPartitionVersion table id:%d, new version:%d", this->m_table_id, new_version)

  auto tag_ver_obj = m_version_mgr_->GetVersionObject(new_version);
  if (nullptr != tag_ver_obj) {
      if (!tag_ver_obj->isValid()) {
          // clear files
          cleanPartition(new_version, index_id, err_info);
      } else {
        return 0;
      }
  }

  // 1. create tag version object
  tag_ver_obj = m_version_mgr_->CreateTagVersionObject(schema, new_version, err_info);
  if (nullptr == tag_ver_obj) {
      LOG_ERROR("call CreateTagVersionObject failed, %s ", err_info.errmsg.c_str());
      return -1;
  }

  // get newest version
  TagVersionObject* newest_tag_ver_obj = nullptr;
  uint32_t newest_part_real_ver = 0;
  newest_tag_ver_obj = m_version_mgr_->GetVersionObject(m_version_mgr_->GetNewestTableVersion());
  if (nullptr != newest_tag_ver_obj) {
    newest_part_real_ver = newest_tag_ver_obj->metaData()->m_real_used_version_;
  }

  // 2. create tag partition table
  if (m_partition_mgr_->CreateTagPartitionTable(schema, new_version, err_info, newest_part_real_ver) < 0) {
    LOG_ERROR("CreateTagPartitionTable failed, %s", err_info.errmsg.c_str());
    m_version_mgr_->RollbackTableVersion(new_version, err_info);
    return -1;
  }

  // 3.
  switch (idx_flag) {
    case HashIndex::Create :
    if (!tags.empty() && index_id != 0) {
      if (createHashIndex(new_version, err_info, tags, index_id) < 0) {
        LOG_ERROR("error happening while createHashIndex.");
        m_version_mgr_->RollbackTableVersion(new_version, err_info);
        return -1;
      }
    } else {
      LOG_ERROR("wrong input for createHashIndex.");
      m_version_mgr_->RollbackTableVersion(new_version, err_info);
      return -1;
    }
    break;
    case HashIndex::Drop :
      if (tags.empty() && index_id != 0) {
        if (dropHashIndex(new_version, err_info, index_id) < 0) {
          LOG_ERROR("error happening while dropHashIndex.");
          m_version_mgr_->RollbackTableVersion(new_version, err_info);
          return -1;
        }
      } else {
        LOG_ERROR("wrong input for dropHashIndex.");
        m_version_mgr_->RollbackTableVersion(new_version, err_info);
        return -1;
      }
      break;

    default:
      break;
  }

  tag_ver_obj->setStatus(TAG_STATUS_READY);
  m_version_mgr_->UpdateNewestTableVersion(new_version);
  return 0;
}

int TagTable::AddNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info,
                           const std::vector<roachpb::NTagIndexInfo>& idx_info) {
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  LOG_INFO("AddNewPartitionVersion table id:%d, new version:%d", this->m_table_id, new_version)

  TagVersionObject* tag_ver_obj = m_version_mgr_->GetVersionObject(new_version);
  if (nullptr != tag_ver_obj) {
      if (!tag_ver_obj->isValid()) {
          // clear files
          cleanPartition(new_version, idx_info, err_info);
      } else {
          return 0;
      }
  }

  // 1. create tag version object
  tag_ver_obj = m_version_mgr_->CreateTagVersionObject(schema, new_version, err_info);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("call CreateTagVersionObject failed, %s ", err_info.errmsg.c_str());
    return -1;
  }

  // get newest version
  TagVersionObject* newest_tag_ver_obj = nullptr;
  uint32_t newest_part_real_ver = 0;
  newest_tag_ver_obj = m_version_mgr_->GetVersionObject(m_version_mgr_->GetNewestTableVersion());
  if (nullptr != newest_tag_ver_obj) {
    newest_part_real_ver = newest_tag_ver_obj->metaData()->m_real_used_version_;
  }

  // 2. create tag partition table
  if (m_partition_mgr_->CreateTagPartitionTable(schema, new_version, err_info, newest_part_real_ver) < 0) {
    LOG_ERROR("CreateTagPartitionTable failed, %s", err_info.errmsg.c_str());
    m_version_mgr_->RollbackTableVersion(new_version, err_info);
    return -1;
  }

  // 3.
  if (!idx_info.empty()) {
    for (auto it = idx_info.begin(); it != idx_info.end(); it++) {
      const std::vector<uint32_t> tags (it->col_ids().begin(), it->col_ids().end());
      if (createHashIndex(new_version, err_info, tags, it->index_id()) < 0) {
        LOG_ERROR("error happening while createHashIndex.");
        m_version_mgr_->RollbackTableVersion(new_version, err_info);
        return -1;
      }
    }
  }

  tag_ver_obj->setStatus(TAG_STATUS_READY);
  m_version_mgr_->UpdateNewestTableVersion(new_version);
  return 0;
}

int TagTable::cleanPartition(uint32_t version, const std::vector<roachpb::NTagIndexInfo> ntagidxinfo, ErrorInfo &err_info) {

    string partition_path = m_db_path_ + m_tbl_sub_path_ + "tag" + "_" + std::to_string(version) + "/";

    // find max index id
    uint32_t max_idx_id = 0;
    for (roachpb::NTagIndexInfo info : ntagidxinfo) {
        if (max_idx_id < info.index_id()) {
            max_idx_id = info.index_id();
        }
    }

    // clear index
    uint32_t idx_id = 0;
    string index_path = partition_path + std::to_string(m_table_id) + "_" + to_string(idx_id) + ".tag" + ".ht";
    while (idx_id <= max_idx_id) {
        if (fs::exists(index_path)) {
            if (!isSoftLink(index_path)) {
                dropHashIndex(version, err_info, idx_id);
            }
        }
        idx_id++;
        index_path = partition_path + std::to_string(m_table_id) + "_" + to_string(idx_id) + ".tag" + ".ht";
    }

    // clear memory object
    m_version_mgr_->RollbackTableVersion(version, err_info);
    m_partition_mgr_->RollbackPartitionTableVersion(version, err_info);

    // clear directory
    if (fs::is_directory(partition_path) && !fs::is_empty(partition_path)) {
        fs::remove_all(partition_path);
    }

    // tag version object file
    fs::remove(this->m_db_path_ + this->m_tbl_sub_path_ + std::to_string(m_table_id) + ".tag" + ".mt" + "_" + std::to_string(version));
    return 0;
}

int TagTable::cleanPartition(uint32_t version, const uint32_t drop_index_id, ErrorInfo &err_info) {
    // clear memory object
    m_version_mgr_->RollbackTableVersion(version, err_info);
    m_partition_mgr_->RollbackPartitionTableVersion(version, err_info);

    // clear directory
    string partition_path = m_db_path_ + m_tbl_sub_path_ + "tag" + "_" + std::to_string(version) + "/";
    if (fs::is_directory(partition_path) && !fs::is_empty(partition_path)) {
        fs::remove_all(partition_path);
    }

    // tag version object file
    fs::remove(this->m_db_path_ + this->m_tbl_sub_path_ + std::to_string(m_table_id) + ".tag" + ".mt" + "_" + std::to_string(version));

    // clear index
    dropHashIndex(version, err_info, drop_index_id);
    return 0;
}

int TagTable::AlterTableTag(AlterType alter_type, const AttributeInfo& attr_info,
                    uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  LOG_INFO("AlterTableTag cur_version%d, new_version:%d", cur_version, new_version)
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  // get latest version
  TableVersion latest_version = m_version_mgr_->GetNewestTableVersion();
  TagVersionObject* tag_ver_obj = m_version_mgr_->GetVersionObject(cur_version);
  if (nullptr == tag_ver_obj) {
    LOG_ERROR("tag version [%u] does not exist.", cur_version);
    err_info.errmsg = "tag version " + std::to_string(cur_version) + " does not exist";
    return -1;
  }
  TagInfo tag_schema = {attr_info.id, attr_info.type,
                        static_cast<uint32_t>(attr_info.length), 0,
                        static_cast<uint32_t>(attr_info.size), GENERAL_TAG};
  std::vector<TagInfo> cur_schemas = tag_ver_obj->getIncludeDroppedSchemaInfos();
  int col_idx = tag_ver_obj->getTagColumnIndex(tag_schema);
  switch (alter_type)
  {
    case ADD_COLUMN:
      if (col_idx >= 0 && latest_version >= new_version) {
        LOG_WARN("tag schema already exist")
        return 0;
      }
      cur_schemas.push_back(tag_schema);
      break;

    case DROP_COLUMN:
      if (col_idx < 0) {
        LOG_WARN("tag schema does not exist, column(id %u), table_id = %lu", tag_schema.m_id, m_table_id);
        err_info.errmsg = "tag schema does not exist";
        return -1;
      } else if (latest_version >= new_version) {
        LOG_WARN("new_version [%u] already exist, tag newest_version[%u]",
              new_version, latest_version);
        return 0;
      }
      cur_schemas[col_idx].setFlag(AINFO_DROPPED);
      break;

    case ALTER_COLUMN_TYPE:
      if (col_idx < 0) {
        LOG_ERROR("alter tag type failed: column (id %u) does not exists, table id = %lu",
                  tag_schema.m_id, m_table_id);
        err_info.errmsg = "tag schema does not exist";
        return -1;
      } else if (latest_version == new_version) {
        return 0;
      }
      cur_schemas[col_idx] = tag_schema;
      break;
    default:
      LOG_ERROR("alter type is invaild.");
      return -1;
  }
  // add new partition table
  return addNewPartitionVersion(cur_schemas, new_version, err_info);
}

std::vector<uint32_t> TagTable::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
  TagVersionObject *obj = m_version_mgr_->GetVersionObject(ts_version);
  if (nullptr == obj) {
      return std::vector<uint32_t>{};
  }
  auto tag_part = GetTagPartitionTableManager()->GetPartitionTable(obj->metaData()->m_real_used_version_);
  tag_part->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_part->getMmapNTagHashIndex()) {
    if (ntag_index->getIndexID() == index_id) {
      tag_part->NtagIndexRWMutexUnLock();
      return ntag_index->getColIDs();
    }
  }
  tag_part->NtagIndexRWMutexUnLock();
  return std::vector<uint32_t>{};
}

std::vector<std::pair<uint32_t, std::vector<uint32_t>>> TagTable::GetAllNTagIndexs(uint32_t ts_version) {
  TagVersionObject *obj = m_version_mgr_->GetVersionObject(ts_version);
  if (nullptr == obj) {
      return std::vector<std::pair<uint32_t, std::vector<uint32_t>>>{};
  }
  std::vector<std::pair<uint32_t, std::vector<uint32_t>>> ret;
  auto tag_part = GetTagPartitionTableManager()->GetPartitionTable(obj->metaData()->m_real_used_version_);
  tag_part->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_part->getMmapNTagHashIndex()) {
    ret.emplace_back(std::make_pair(ntag_index->getIndexID(), ntag_index->getColIDs()));
  }
  tag_part->NtagIndexRWMutexUnLock();
  return ret;
}

int TagTable::initHashIndex(int flags, ErrorInfo& err_info) {
  string index_name = std::to_string(m_table_id) + ".tag"+ ".ht";
  m_index_ = new MMapPTagHashIndex(m_partition_mgr_->getPrimarySize());
  err_info.errcode = m_index_->open(index_name, m_db_path_, m_tbl_sub_path_,
                                    flags, err_info);
  if (err_info.errcode < 0) {
    delete m_index_;
    m_index_ = nullptr;
    err_info.errmsg = "create Hash Index failed.";
    LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), index_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  return err_info.errcode;
}

int TagTable::CreateHashIndex(int flags, const std::vector<uint32_t> &tags, uint32_t index_id,
                              const uint32_t cur_version, const uint32_t new_version, ErrorInfo &err_info) {
  LOG_INFO("createNHashIndex index_id:%d, cur_version:%d, new_version:%d", index_id, cur_version, new_version)
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  if (cur_version < new_version) {
    TagVersionObject *cur_obj = m_version_mgr_->GetVersionObject(cur_version);
    if (cur_obj == nullptr) {
        LOG_ERROR("Failed to get tag version id:%d", cur_version)
        return -1;
    }

    TagPartitionTable* tag_part = GetTagPartitionTableManager()->GetPartitionTable(cur_obj->metaData()->m_real_used_version_);
    tag_part->NtagIndexRWMutexSLock();
    for (MMapNTagHashIndex* index : tag_part->getMmapNTagHashIndex()) {
        if (index->getIndexID() == index_id) {
            LOG_ERROR("Normal hash index already exists, index_id:%d", index_id)
            tag_part->NtagIndexRWMutexUnLock();
            return -1;
        }
    }
    tag_part->NtagIndexRWMutexUnLock();

    auto cur_schema = cur_obj->getIncludeDroppedSchemaInfos();
    // 1 for create hash index
    if (addNewPartitionVersion(cur_schema, new_version, err_info, tags, index_id, HashIndex::Create) < 0) {
      LOG_ERROR("Failed to AddNewPartitionVersion while creating hash index.ts_version:%d.err_info:%s",
                cur_version, err_info.errmsg.c_str());
      return -1;
    }
    return 0;
  }
  LOG_ERROR("Failed create hash index table id:%d, index id:%d, cur_version:%d, new_version:%d", m_table_id, index_id,
            cur_version, new_version)
  return -1;
}

int TagTable::UndoCreateHashIndex(uint32_t index_id, uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  // 1. check version is valid
  auto cur_tbl_version = m_version_mgr_->GetCurrentTableVersion();
  if (cur_version < cur_tbl_version) {
    LOG_WARN("Tag rollback version: %u  < committed version: %u ",
             cur_version, cur_tbl_version);
    return 0;
  }
  auto newest_tbl_version = m_version_mgr_->GetNewestTableVersion();
  if (new_version > newest_tbl_version) {
    LOG_ERROR("Tag Table newest table version is %u < rollback version: %u ",
              newest_tbl_version, new_version);
    return -1;
  }

  if (dropHashIndex(new_version, err_info, index_id) < 0) {
    LOG_ERROR("Failed to rollback CreateHashIndex. index_id:%d", index_id)
    return -1;
  }

  LOG_INFO("Rollback tag version begin, new_version: %u to cur_version: %u ", new_version, cur_version);
  m_version_mgr_->UpdateNewestTableVersion(cur_version, false);
  m_version_mgr_->RollbackTableVersion(new_version, err_info);
  m_partition_mgr_->RollbackPartitionTableVersion(new_version, err_info);

  return 0;
}

int TagTable::UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t cur_version,
                                uint32_t new_version, ErrorInfo& err_info) {
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  // 1. check version is valid
  auto cur_tbl_version = m_version_mgr_->GetCurrentTableVersion();
  if (cur_version < cur_tbl_version) {
    LOG_WARN("Tag rollback version: %u  < committed version: %u ",
             cur_version, cur_tbl_version);
    return 0;
  }
  auto newest_tbl_version = m_version_mgr_->GetNewestTableVersion();
  if (new_version > newest_tbl_version) {
    LOG_ERROR("Tag Table newest table version is %u < rollback version: %u ",
              newest_tbl_version, new_version);
    return -1;
  }

  if (dropHashIndex(cur_version, err_info, index_id) < 0){
    LOG_ERROR("Failed to rollback DropHashIndex.")
    return -1;
  }
  if (createHashIndex(cur_version, err_info, tags, index_id) < 0) {
    LOG_ERROR("Failed to rollback DropHashIndex.")
    return -1;
  }

  LOG_INFO("Rollback tag version begin, new_version: %u to cur_version: %u ", new_version, cur_version);
  m_version_mgr_->UpdateNewestTableVersion(cur_version, false);
  m_version_mgr_->RollbackTableVersion(new_version, err_info);
  m_partition_mgr_->RollbackPartitionTableVersion(new_version, err_info);

  return 0;
}

int TagTable::DropHashIndex(uint32_t index_id,  const uint32_t cur_version,
                            const uint32_t new_version, ErrorInfo& err_info) {
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  if (cur_version < new_version) {
    TagVersionObject *cur_obj = m_version_mgr_->GetVersionObject(cur_version);
    if (cur_obj == nullptr) {
      LOG_ERROR("Failed to get tag version id:%d", cur_version)
      return -1;
    }

    TagPartitionTable* tag_part = GetTagPartitionTableManager()->GetPartitionTable(cur_obj->metaData()->m_real_used_version_);
    tag_part->NtagIndexRWMutexSLock();
    auto ntag_index = tag_part->getMmapNTagHashIndex();
    for (int i = 0; i < ntag_index.size(); i++) {
      if (ntag_index[i]->getIndexID() == index_id) {
        break;
      }
      if (i == ntag_index.size() - 1) {
        LOG_ERROR("Normal hash index doesn't exist, index_id:%d", index_id)
        tag_part->NtagIndexRWMutexUnLock();
        return -1;
      }
    }
    tag_part->NtagIndexRWMutexUnLock();

    auto cur_schema = cur_obj->getIncludeDroppedSchemaInfos();
    // 1 for create hash index
    if (addNewPartitionVersion(cur_schema, new_version, err_info, {}, index_id, HashIndex::Drop) < 0) {
      LOG_ERROR("Failed to AddNewPartitionVersion while creating hash index.ts_version:%d.err_info:%s",
                cur_version, err_info.errmsg.c_str());
      return -1;
    }
    return 0;
  }
  LOG_ERROR("Failed drop hash index table id:%d, index id:%d, cur_version:%d, new_version:%d", m_table_id, index_id,
            cur_version, new_version)
  return -1;
}
int TagTable::initPrevEntityRowData(ErrorInfo& err_info) {
  // 1. load all versions id
  std::vector<std::pair<uint32_t, TagPartitionTable*>> all_partition_tbls;
  m_partition_mgr_->GetAllPartitionTables(all_partition_tbls);
  // 2. init data into entity index file
  for (const auto it : all_partition_tbls) {
    auto src_tag_partition_tbl = it.second;
    src_tag_partition_tbl->startRead();
    for (int row = 1; row <= src_tag_partition_tbl->size(); row++) {
      if (src_tag_partition_tbl->isValidRow(row)){
        std::vector<kwdbts::EntityResultIndex> entity_id_list;
        if (!EngineOptions::isSingleNode()) {
          uint32_t hps;
          src_tag_partition_tbl->getHashpointByRowNum(row, &hps);
          src_tag_partition_tbl->getHashedEntityIdByRownum(row, hps, &entity_id_list);
        } else {
          src_tag_partition_tbl->getEntityIdByRownum(row, &entity_id_list);
        }
        for (int idx = 0; idx < entity_id_list.size(); idx++) {
          uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id_list[idx].entityId) << 32) | entity_id_list[idx].subGroupId;
          if (m_entity_row_index_->put(reinterpret_cast<const char *>(&joint_entity_id), sizeof(uint64_t), it.first, row) < 0) {
            LOG_ERROR("insert entity row hash index data failed. table_version: %u row_no: %lu ", it.first, row);
            src_tag_partition_tbl->stopRead();
            return -1;
          }
        }
      }
    }
    src_tag_partition_tbl->stopRead();
  }
  return 0;
}

int TagTable::initEntityRowHashIndex(int flags, ErrorInfo& err_info) {
  string index_name = std::to_string(m_table_id) + ".tag"+ ".et";
  m_entity_row_index_ = new MMapEntityRowHashIndex();
  if (flags & MMAP_OPEN_NORECURSIVE) {
    if (!IsExists(m_db_path_ + m_tbl_sub_path_ + index_name)) {
      flags = MMAP_CREAT_EXCL;
    }
  }
  err_info.errcode = m_entity_row_index_->open(index_name, m_db_path_, m_tbl_sub_path_,
                                    flags, err_info);
  if (err_info.errcode < 0) {
    delete m_entity_row_index_;
    m_entity_row_index_ = nullptr;
    err_info.errmsg = "create EntityRow Hash Index failed.";
    LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), index_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  if (flags & MMAP_CREAT_EXCL) {
    err_info.errcode = initPrevEntityRowData(err_info);
  }
  return err_info.errcode;
}

int TagTable::loadAllVersions(std::vector<TableVersion>& all_versions, ErrorInfo& err_info) {
  string real_path = m_db_path_ + m_tbl_sub_path_;
  // Load all versions of root table
  DIR* dir_ptr = opendir(real_path.c_str());
  if (dir_ptr) {
    string prefix = std::to_string(m_table_id) + ".tag" + ".mt" + '_';
    size_t prefix_len = prefix.length();
    struct dirent* entry;
    while ((entry = readdir(dir_ptr)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
          || entry->d_name[0] == '_') {
        continue;
      }
      std::string full_path = real_path + entry->d_name;
      struct stat file_stat{};
      if (stat(full_path.c_str(), &file_stat) != 0) {
        LOG_ERROR("stat[%s] failed", full_path.c_str());
        err_info.setError(KWENFILE, "stat[" + full_path + "] failed");
        closedir(dir_ptr);
        return -1;
      }
      if (S_ISREG(file_stat.st_mode) &&
          strncmp(entry->d_name, prefix.c_str(), prefix_len) == 0) {
        all_versions.push_back(std::stoi(entry->d_name + prefix_len));
      }
    }
    closedir(dir_ptr);
  } else {
    LOG_ERROR("opendir [%s] failed.", real_path.c_str());
    return -1;
  }
  if (all_versions.empty()) {
    LOG_ERROR("read all tag table version is empty.");
    return -1;
  }
  return 0;
}

// wal
void TagTable::sync_with_lsn(kwdbts::TS_LSN lsn) {
  std::vector<TagPartitionTable*> all_parttables;
  TableVersion cur_tbl_version = this->GetTagTableVersionManager()->GetCurrentTableVersion();
  m_partition_mgr_->GetAllPartitionTablesLessVersion(all_parttables, cur_tbl_version);
  for (const auto& part_table : all_parttables) {
    // add read lock
    (void)part_table->startRead();
    part_table->sync_with_lsn(lsn);
    (void)part_table->stopRead();
  }
  m_index_->setLSN(lsn);
  m_entity_row_index_->setLSN(lsn);
  return ;
}
// snapshot using this sync
void TagTable::sync(int flag) {
  std::vector<TagPartitionTable*> all_parttables;
  TableVersion cur_tbl_version = this->GetTagTableVersionManager()->GetCurrentTableVersion();
  m_partition_mgr_->GetAllPartitionTablesLessVersion(all_parttables, cur_tbl_version);
  for (const auto& part_table : all_parttables) {
    (void)part_table->startRead();
    part_table->sync(flag);
    (void)part_table->stopRead();
  }
  return ;
}

TagTuplePack* TagTable::GenTagPack(const char* primarytag, int len) {
  // 1. search primary tag
  auto ret = m_index_->get(primarytag, len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    LOG_ERROR("GenTagPack does not found primary tag. len: %d", len);
    return nullptr;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return nullptr;
  }

  // 2. gen tag pack
  return tag_partition_table->GenTagPack(ret.second);
}

int TagTable::UndoAlterTagTable(uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  versionMutexLock();
  Defer defer{[&]() { versionMutexUnlock(); }};
  // 1. check version is valid
  auto cur_tbl_version = m_version_mgr_->GetCurrentTableVersion();
  if (cur_version < cur_tbl_version) {
    LOG_WARN("Tag rollback version: %u  < committed version: %u ",
              cur_version, cur_tbl_version);
    return 0;
  }
  auto newest_tbl_version = m_version_mgr_->GetNewestTableVersion();
  if (new_version > newest_tbl_version) {
    LOG_ERROR("Tag Table newest table version is %u < rollback version: %u ",
               newest_tbl_version, new_version);
    return -1;
  }
  LOG_INFO("Rollback tag version begin, new_version: %u to cur_version: %u ", new_version, cur_version);
  m_version_mgr_->UpdateNewestTableVersion(cur_version, false);
  m_version_mgr_->RollbackTableVersion(new_version, err_info);
  m_partition_mgr_->RollbackPartitionTableVersion(new_version, err_info);
  return 0;
}

int TagTable::InsertForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag) {
  // don't do undo when tag table insert successfully
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // no found
    return 0;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return -1;
  }
  ErrorInfo err_info;
  return DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
}

int TagTable::InsertForRedo(uint32_t group_id, uint32_t entity_id,
		    kwdbts::Payload &payload) {
  // 1. search ptag
  TSSlice tmp_slice = payload.GetPrimaryTag();
  auto ret = m_index_->get(tmp_slice.data, tmp_slice.len);
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }
    tag_partition_table->startRead();
    if (!tag_partition_table->isValidRow(ret.second)) {
      tag_partition_table->unsetDeleteMark(ret.second);
    }
    tag_partition_table->stopRead();

    // remove and insert normal index
    tag_partition_table->NtagIndexRWMutexSLock();
    for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
      std::vector<TSSlice> index_cols;
      size_t len = 0;
      auto col_ids = ntag_index->getColIDs();
      for (auto col_id : col_ids) {
          uint32_t col_size = tag_partition_table->getTagColSize(col_id);
          uint32_t off = tag_partition_table->getTagColOff(col_id);
          auto col_val = payload.GetNormalTag(off, col_size);
          index_cols.emplace_back(col_val);
          len += col_val.len;
      }
      char index_key[len];
      int num = 0;
      for(auto r:index_cols){
          strncpy(&index_key[num], r.data, r.len);
          num += r.len;
      }
      ntag_index->remove(ret.second, ret.first, index_key, len);
      if (ntag_index->insert(index_key, len, ret.first, ret.second) < 0) {
          tag_partition_table->NtagIndexRWMutexUnLock();
          LOG_ERROR("InsertForRedo insert remove index data failed. table_version: %u row_no: %lu ", ret.first, ret.second);
          return -1;
      }
    }
    tag_partition_table->NtagIndexRWMutexUnLock();

    return 0;
  }
  // 2. redo insert
  return InsertTagRecord(payload, group_id, entity_id);

}

int TagTable::DeleteForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag_pack) {
    // 1. search primary tag
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  TagPartitionTable* tag_partition_table = nullptr;
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }

    // remove and insert normal index
    tag_partition_table->NtagIndexRWMutexSLock();
    for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
      std::vector<TSSlice> index_cols;
      size_t len = 0;
      auto col_ids = ntag_index->getColIDs();
      for (auto col_id : col_ids) {
          uint32_t col_size = tag_partition_table->getTagColSize(col_id);
          uint32_t off = tag_partition_table->getTagColOff(col_id);
          // "tag_tuple.getTags().data" is tag addr
          std::vector<TagInfo> schemas;
          TagTuplePack tag_tuple(schemas, tag_pack.data, tag_pack.len);
          TSSlice col_val{tag_tuple.getTags().data + off, static_cast<size_t>(col_size)};
          index_cols.emplace_back(col_val);
          len += col_val.len;
      }
      char index_key[len];
      int num = 0;
      for(auto r:index_cols){
          strncpy(&index_key[num], r.data, r.len);
          num += r.len;
      }
      ntag_index->remove(ret.second, ret.first, index_key, len);
      if (ntag_index->insert(index_key, len, ret.first, ret.second) < 0) {
          tag_partition_table->NtagIndexRWMutexUnLock();
          LOG_ERROR("DeleteForUndo insert index data failed. table_version: %u row_no: %lu ", ret.first, ret.second);
          return -1;
      }
    }
    tag_partition_table->NtagIndexRWMutexUnLock();

    tag_partition_table->startRead();
    tag_partition_table->unsetDeleteMark(ret.second);
    tag_partition_table->stopRead();
    return 0;
  }
  // 2. undo for insert tags
  if (tag_pack.data == nullptr) {
    LOG_ERROR("DeleteForUndo tag pack data is nullptr.");
    return -1;
  }
  TableVersion tbl_version = *reinterpret_cast<uint32_t*>(tag_pack.data + TagTuplePack::versionOffset_);
  auto tag_version_object = m_version_mgr_->GetVersionObject(tbl_version);
  if (nullptr == tag_version_object) {
    LOG_ERROR("tag metadata version [%u] object does not exist. ", tbl_version);
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", tag_partition_version);
    return -1;
  }
  uint32_t tag_hash_point = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  std::vector<TagInfo> schemas;
  TagTuplePack tag_tuple(schemas, tag_pack.data, tag_pack.len);

  // 3. insert partition data
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, group_id, tag_hash_point,
                                  tag_tuple.getTags().data, &row_no) < 0) {
    LOG_ERROR("insert tag partition table[%s/%s] failed. ",
       tag_partition_table->m_tbl_sub_path_.c_str(), tag_partition_table->m_name_.c_str());
    return -1;
  }
  // 4. insert index data
  if (m_index_->insert(primary_tag.data, primary_tag.len, tbl_version, row_no) < 0) {
    LOG_ERROR("insert hash index data failed. table_version: %u row_no: %lu ", tbl_version, row_no);
    return -1;
  }

  // insert normal index data
  tag_partition_table->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
      std::vector<TSSlice> index_cols;
      size_t len = 0;
    auto col_ids = ntag_index->getColIDs();
    for (auto col_id : col_ids) {
          uint32_t col_size = tag_partition_table->getTagColSize(col_id);
          uint32_t off = tag_partition_table->getTagColOff(col_id);
          // "tag_tuple.getTags().data" is tag addr
          std::vector<TagInfo> schemas;
          TagTuplePack tag_tuple(schemas, tag_pack.data, tag_pack.len);
          TSSlice col_val{tag_tuple.getTags().data + off, static_cast<size_t>(col_size)};
          index_cols.emplace_back(col_val);
          len += col_val.len;
      }
      char index_key[len];
      int num = 0;
      for(auto r:index_cols){
          strncpy(&index_key[num], r.data, r.len);
          num += r.len;
      }
      if (ntag_index->insert(index_key, len, ret.first, ret.second) < 0) {
          tag_partition_table->NtagIndexRWMutexUnLock();
          LOG_ERROR("DeleteForUndo insert remove index data failed. table_version: %u row_no: %lu ", ret.first, ret.second);
          return -1;
      }
  }
  tag_partition_table->NtagIndexRWMutexUnLock();

  // 5. insert entity row index data
  uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | group_id;
  if (m_entity_row_index_->put(reinterpret_cast<const char *>(&joint_entity_id), sizeof(uint64_t), tbl_version, row_no) < 0) {
    LOG_ERROR("insert entity row hash index data failed. table_version: %u row_no: %lu ", tbl_version, row_no);
    return -1;
  }
  // 6. set undelete mark
  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

int TagTable::DeleteForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, TSSlice& tag_pack) {
  // 1. search primary tag
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first == INVALID_TABLE_VERSION_ID) {
    // not found
    return 0;
  }
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
    return -1;
  }
  // 2. delete data
  TagPartitionTableRowID row_no = ret.second;
  tag_partition_table->startRead();
  tag_partition_table->setDeleteMark(ret.second);
  tag_partition_table->stopRead();

  // 3. delete index record
  auto ret_del = m_index_->remove(primary_tag.data, primary_tag.len);

  // 4.delete normal index
  tag_partition_table->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
    std::vector<TSSlice> index_cols;
    size_t len = 0;
    auto col_ids = ntag_index->getColIDs();
    for (auto col_id : col_ids) {
      uint32_t col_size = tag_partition_table->getTagColSize(col_id);
      uint32_t off = tag_partition_table->getTagColOff(col_id);
      // "tag_tuple.getTags().data" is tag addr
      std::vector<TagInfo> schemas;
      TagTuplePack tag_tuple(schemas, tag_pack.data, tag_pack.len);
      TSSlice col_val{tag_tuple.getTags().data + off, static_cast<size_t>(col_size)};
      index_cols.emplace_back(col_val);
      len += col_val.len;
    }
    char index_key[len];
    int num = 0;
    for(auto r:index_cols){
      strncpy(&index_key[num], r.data, r.len);
      num += r.len;
    }
    ntag_index->remove(ret.second, ret.first, index_key, len);
  }
  tag_partition_table->NtagIndexRWMutexUnLock();

  // 5. delete entity row index record
  uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | group_id;
  ret_del = m_entity_row_index_->delete_data(reinterpret_cast<const char *>(&joint_entity_id));


  return 0;
}

int TagTable::UpdateForRedo(uint32_t group_id, uint32_t entity_id,
                    const TSSlice& primary_tag, kwdbts::Payload &payload) {
  // 1. search primary tag
  TagPartitionTable* tag_partition_table = nullptr;
  TableVersion tbl_version = 0;
  TagPartitionTableRowID tag_row_no = 0;
  auto ret = m_index_->get(primary_tag.data, primary_tag.len);
  if (ret.first != INVALID_TABLE_VERSION_ID) {
    // found
    tag_partition_table = m_partition_mgr_->GetPartitionTable(ret.first);
    if (nullptr == tag_partition_table) {
      LOG_ERROR("primary tag's tag partition table [%u] does not exist.", ret.first);
      return -1;
    }
    // delete mark
    tag_partition_table->startRead();
    tag_partition_table->setDeleteMark(ret.second);
    tag_partition_table->stopRead();
    // delete index data
    auto ret_del = m_index_->remove(primary_tag.data, primary_tag.len);

    // drop normal index
    tag_partition_table->NtagIndexRWMutexSLock();
    for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
      std::vector<TSSlice> index_cols;
      size_t len = 0;
      auto col_ids = ntag_index->getColIDs();
      for (auto col_id : col_ids) {
          uint32_t col_size = tag_partition_table->getTagColSize(col_id);
          uint32_t off = tag_partition_table->getTagColOff(col_id);
          auto col_val = payload.GetNormalTag(off, col_size);
          index_cols.emplace_back(col_val);
          len += col_val.len;
      }
      char index_key[len];
      int num = 0;
      for(auto r:index_cols){
          strncpy(&index_key[num], r.data, r.len);
          num += r.len;
      }
      ntag_index->remove(ret.second, ret.first, index_key, len);
    }
    tag_partition_table->NtagIndexRWMutexUnLock();

    // delete entity row index data
    uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | group_id;
    ret_del = m_entity_row_index_->delete_data(reinterpret_cast<const char *>(&joint_entity_id));
  }
  return InsertTagRecord(payload, group_id, entity_id);
}

int TagTable::UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                    const TSSlice& old_tag) {
  int rc = 0;
  ErrorInfo err_info;
  // 1. check
  if (old_tag.data == nullptr) {
    LOG_ERROR("UpdateForUndo old tag data is nullptr.");
    return -1;
  }
  TableVersion tbl_version = *reinterpret_cast<uint32_t*>(old_tag.data + TagTuplePack::versionOffset_);
  auto tag_version_object = m_version_mgr_->GetVersionObject(tbl_version);
  if (nullptr == tag_version_object) {
    LOG_ERROR("tag metadata version [%u] object does not exist. ", tbl_version);
    return -1;
  }
  TableVersion tag_partition_version = tag_version_object->metaData()->m_real_used_version_;
  auto tag_partition_table = m_partition_mgr_->GetPartitionTable(tag_partition_version);
  if (nullptr == tag_partition_table) {
    LOG_ERROR("primary tag's tag partition table [%u] does not exist.", tag_partition_version);
    return -1;
  }
  // 1. delete
  rc = DeleteTagRecord(primary_tag.data, primary_tag.len, err_info);
  if (rc < 0) {
    return rc;
  }
  uint32_t tag_hash_point = TsTable::GetConsistentHashId(primary_tag.data, primary_tag.len);
  std::vector<TagInfo> schemas;
  TagTuplePack tag_tuple(schemas, old_tag.data, old_tag.len);
  size_t row_no = 0;
  if (tag_partition_table->insert(entity_id, group_id, tag_hash_point, tag_tuple.getTags().data, &row_no) < 0) {
    LOG_ERROR("UpdateForUndo insert tag failed.");
    return -1;
  }
  if (m_index_->insert(primary_tag.data, primary_tag.len, tag_partition_version, row_no) < 0) {
    LOG_ERROR("UpdateForUndo insert hash index failed");
    return -1;
  }

  // normal index insert
  tag_partition_table->NtagIndexRWMutexSLock();
  for (auto ntag_index : tag_partition_table->getMmapNTagHashIndex()) {
    std::vector<TSSlice> index_cols;
    size_t len = 0;
    auto col_ids = ntag_index->getColIDs();
    for (auto col_id : col_ids) {
        uint32_t col_size = tag_partition_table->getTagColSize(col_id);
        uint32_t off = tag_partition_table->getTagColOff(col_id);
        // "tag_tuple.getTags().data" is tag addr
        TSSlice col_val{tag_tuple.getTags().data + off, static_cast<size_t>(col_size)};
        index_cols.emplace_back(col_val);
        len += col_val.len;
    }
    char index_key[len];
    int num = 0;
    for(auto r:index_cols){
        strncpy(&index_key[num], r.data, r.len);
        num += r.len;
    }
    if (ntag_index->insert(index_key, len, tag_partition_version, row_no) < 0) {
        tag_partition_table->NtagIndexRWMutexUnLock();
        LOG_ERROR("UpdateForUndo insert remove index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
        return -1;
    }
  }

  tag_partition_table->NtagIndexRWMutexUnLock();
  // 2. put entity row index record
  uint64_t joint_entity_id = (static_cast<uint64_t>(entity_id) << 32) | group_id;
  if (m_entity_row_index_->put(reinterpret_cast<const char *>(&joint_entity_id), sizeof(uint64_t), tag_partition_version, row_no) < 0) {
    LOG_ERROR("insert entity row hash index data failed. table_version: %u row_no: %lu ", tag_partition_version, row_no);
    return -1;
  }

  tag_partition_table->startRead();
  tag_partition_table->unsetDeleteMark(row_no);
  tag_partition_table->stopRead();
  return 0;
}

TagTable* OpenTagTable(const std::string& db_path, const std::string &dir_path,
                                uint64_t table_id, int32_t entity_group_id, ErrorInfo &err_info) {
  // check path
  std::string new_sub_path = dir_path + "tag/";
  std::string new_dir_path = db_path + new_sub_path;
  if (access(new_dir_path.c_str(), 0)) {
     LOG_ERROR("Directory path %s doesnot exist.", new_dir_path.c_str());
     return nullptr;
  }
  // open tag table
  TagTable* tmp_bt = KNEW TagTable(db_path, new_sub_path, table_id, entity_group_id);
  int err_code = tmp_bt->open(err_info);
  if (err_info.errcode == KWENOOBJ) {
    // table not exists.
    LOG_WARN("the tag table %s%s does not exist", new_sub_path.c_str(), std::to_string(table_id).c_str());
    err_info.clear();
    delete tmp_bt;
    return nullptr;
  }
  if (err_code < 0) {
    // other errors
    LOG_ERROR("failed to open the tag table %s%s, error: %s",
      new_sub_path.c_str(), std::to_string(table_id).c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    return nullptr;
  }
  return tmp_bt;
}

TagTable* CreateTagTable(const std::vector<TagInfo> &tag_schema,
                                   const std::string& db_path, const std::string &dir_path,
                                   uint64_t table_id, int32_t entity_group_id,
                                   uint32_t table_version, ErrorInfo &err_info) {
  // check path
  std::string sub_path = dir_path + "tag/";
  std::string new_dir_path = db_path + sub_path;
  if (access(new_dir_path.c_str(), 0)) {
    if (!MakeDirectory(new_dir_path)) {
      return nullptr;
    }
  } else {
    LOG_ERROR("path: %s already exist.", new_dir_path.c_str());
    return nullptr;
  }
  // create tag table
  TagTable* tmp_bt = KNEW TagTable(db_path, sub_path, table_id, entity_group_id);
  if (tmp_bt == nullptr) {
    LOG_ERROR("create tag table out off memory.");
    return nullptr;
  }
  if (tmp_bt->create(tag_schema, table_version, err_info)< 0) {
    LOG_ERROR("failed to create the tag table %s%lu, error: %s",
        dir_path.c_str(), table_id, err_info.errmsg.c_str());
    delete tmp_bt;
    tmp_bt = nullptr;
  }

  return tmp_bt;
}

int DropTagTable(TagTable* bt, ErrorInfo& err_info) {
  return bt->remove(err_info);
}

int TagPartitionTableManager::Init(ErrorInfo& err_info) {
  return 0;
}

TagPartitionTableManager::~TagPartitionTableManager() {
  wrLock();
  // remove all tables
  for (auto& it : m_partition_tables_) {
    if(it.second) {
      delete it.second;
      it.second = nullptr;
    }
  }
  m_partition_tables_.clear();
  unLock();
}

int TagPartitionTableManager::CreateTagPartitionTable(const std::vector<TagInfo>& schema, uint32_t ts_version,
                                                      ErrorInfo& err_info, uint32_t newest_part_file_version) {
  // 1. check path
  std::string partition_table_path = m_tbl_sub_path_ + "tag" + "_" + std::to_string(ts_version) + "/";
  std::string real_path = m_db_path_ + partition_table_path;
  wrLock();
  // check partition table
  auto part_tbl = m_partition_tables_.find(ts_version);
  if (part_tbl != m_partition_tables_.end()) {
     unLock();
     LOG_WARN("tag partition table version [%u] already exist.", ts_version);
     return 0;
  }
  if (access(real_path.c_str(), 0)) {
    // path does not exist
    if (!MakeDirectory(real_path)) {
      err_info.errcode = -1;
      err_info.errmsg = "create directory failed";
      LOG_ERROR("Create path : %s failed.", real_path.c_str());
      unLock();
      fs::remove_all(real_path.c_str());
      return -1;
    }
  }

  // 2. create partition table
  TagPartitionTable* tmp_bt = new TagPartitionTable();
  if (tmp_bt->open(m_table_prefix_name_, m_db_path_, partition_table_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
      err_info.errcode == KWECORR) {
    tmp_bt->create(schema, m_entity_group_id_, ts_version, err_info);
  }
  if (err_info.errcode < 0) {
    LOG_ERROR("failed to create tag partition table %s%s, error: %s",
        partition_table_path.c_str(), m_table_prefix_name_.c_str(), err_info.errmsg.c_str());
    tmp_bt->setObjectReady();
    tmp_bt->remove();
    delete tmp_bt;
    tmp_bt = nullptr;
    unLock();
    fs::remove_all(real_path.c_str());
    return -1;
  }
  if (m_primary_tag_size_ == 0) {
    m_primary_tag_size_ = tmp_bt->primaryTagSize();
  }

  // Get last version partition table.
  TagPartitionTable* newest_part = nullptr;
  if (newest_part_file_version > 0) {
    auto newest_ver_part = m_partition_tables_.find(newest_part_file_version);
    if (newest_ver_part != m_partition_tables_.end()) {
        newest_part = newest_ver_part->second;
    }
    if (!newest_part) {
      LOG_ERROR("Error happened while finding ts_version [%d] TagPartitionTable", ts_version)
      unLock();
      return -1;
    }
  }

  // update NTag index data.
  tmp_bt->linkNTagHashIndex(m_table_id_, newest_part, err_info);

  // 3. insert into cache
  m_partition_tables_.insert({ts_version, tmp_bt});
  unLock();
  return 0;
}

int TagPartitionTableManager::OpenTagPartitionTable(TableVersion table_version, ErrorInfo& err_info) {
  wrLock();
  auto part_table = m_partition_tables_.find(table_version);
  if (part_table != m_partition_tables_.end()) {
     unLock();
     LOG_DEBUG("TagPartitionTable of version %u was existed.", table_version);
     return 0;
  }
  // set partition table path
  std::string partition_table_path = m_tbl_sub_path_ + "tag" + "_" + std::to_string(table_version) + "/";
  std::string real_path = m_db_path_ + partition_table_path;
  if (access(real_path.c_str(), 0)) {
    // path does not exist
    LOG_DEBUG("The path does not exist. %s ", real_path.c_str());
    unLock();
    return 0;
  }
  TagPartitionTable* tmp_bt = new TagPartitionTable();
  int err_code = tmp_bt->open(m_table_prefix_name_, m_db_path_, partition_table_path, MMAP_OPEN_NORECURSIVE, err_info);
  if (err_code < 0) {
    // other errors
    LOG_ERROR("failed to open the tag table %s%s, error: %s",
      partition_table_path.c_str(), m_table_prefix_name_.c_str(), err_info.errmsg.c_str());
    delete tmp_bt;
    unLock();
    return -1;
  }
  if (m_primary_tag_size_ == 0) {
    m_primary_tag_size_ = tmp_bt->primaryTagSize();
  }
  m_partition_tables_.insert({table_version, tmp_bt});
  unLock();
  return 0;
}

TagPartitionTable* TagPartitionTableManager::GetPartitionTable(uint32_t table_version) {
  rdLock();
  auto part_table = m_partition_tables_.find(table_version);
  if (part_table != m_partition_tables_.end()) {
     unLock();
     return part_table->second;
  }
  unLock();
  LOG_ERROR("Get tag partition table failed. table_version: %u ", table_version);
  return nullptr;
}

void TagPartitionTableManager::GetAllPartitionTablesLessVersion(std::vector<TagPartitionTable*>& tag_part_tables, TableVersion max_table_version) {
  rdLock();
  for (const auto& it : m_partition_tables_) {
    if (it.first <= max_table_version) {
      tag_part_tables.push_back(it.second);
    }
  }
  unLock();
}

void TagPartitionTableManager::GetAllPartitionTables(std::vector<std::pair<uint32_t, TagPartitionTable*>>& tag_part_tables) {
  rdLock();
  for (const auto& it : m_partition_tables_) {
    if (it.second != nullptr) {
      tag_part_tables.push_back(it);
    }
  }
  unLock();
  return ;
}

int TagPartitionTableManager::RemoveAll(ErrorInfo& err_info) {
  wrLock();
  // remove all tables
  for (auto& it : m_partition_tables_) {
    if(it.second) {
      it.second->remove();
      delete it.second;
      it.second = nullptr;
    }
  }
  m_partition_tables_.clear();
  unLock();
  return 0;
}

int TagPartitionTableManager::RollbackPartitionTableVersion(TableVersion need_rollback_version, ErrorInfo& err_info) {
  wrLock();
  auto part_table = m_partition_tables_.find(need_rollback_version);
  if (part_table == m_partition_tables_.end()) {
     unLock();
     LOG_WARN("tag partition table need rollback version [%u] does not exist.", need_rollback_version);
     return 0;
  }
  part_table->second->remove();
  delete part_table->second;
  m_partition_tables_.erase(part_table);
  std::string real_path = m_db_path_ + m_tbl_sub_path_ + "tag" + "_" + std::to_string(need_rollback_version) + "/";
  fs::remove_all(real_path);
  LOG_INFO("Rollback partitionTable version, remove directory: %s", real_path.c_str());
  unLock();
  return 0;
}
