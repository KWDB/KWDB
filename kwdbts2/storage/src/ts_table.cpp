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
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif
#include "engine.h"
#include "mmap/mmap_metrics_table.h"
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_table.h"
#include "perf_stat.h"
#include "sys_utils.h"
#include "lt_cond.h"
#include "ee_global.h"

extern DedupRule g_dedup_rule;
extern bool g_go_start_service;
namespace kwdbts {

TsEntityGroup::TsEntityGroup(kwdbContext_p ctx, MMapRootTableManager*& root_table_manager, const string& db_path,
                             const KTableKey& table_id, const RangeGroup& range, const string& tbl_sub_path) :
    root_bt_manager_(root_table_manager), db_path_(db_path), table_id_(table_id),
    range_(range), tbl_sub_path_(tbl_sub_path) {
  cur_subgroup_id_ = 1;
  mutex_ = new KLatch(LATCH_ID_TSENTITY_GROUP_MUTEX);
  drop_mutex_ = new KRWLatch(RWLATCH_ID_TS_ENTITY_GROUP_DROP_RWLOCK);
}

TsEntityGroup::~TsEntityGroup() {
  if (ebt_manager_ != nullptr) {
    delete ebt_manager_;
    ebt_manager_ = nullptr;
  }
  if (mutex_) {
    delete mutex_;
    mutex_ = nullptr;
  }
  if (drop_mutex_) {
    delete drop_mutex_;
    drop_mutex_ = nullptr;
  }
  delete new_tag_bt_;
  new_tag_bt_ = nullptr;
}

KStatus TsEntityGroup::Create(kwdbContext_p ctx, vector<TagInfo>& tag_schema, uint32_t ts_version) {
  if (ebt_manager_ != nullptr) {
    LOG_ERROR("TsTableRange already OpenInit.")
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  MUTEX_LOCK(mutex_);
  Defer defer{[&]() { MUTEX_UNLOCK(mutex_); }};
  new_tag_bt_ = CreateTagTable(tag_schema, db_path_, tbl_sub_path_, table_id_, range_.range_group_id,
                                  ts_version, err_info);
  if (new_tag_bt_ == nullptr) {
    LOG_ERROR("TsTableRange create tag table error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  ebt_manager_ = new SubEntityGroupManager(root_bt_manager_);
  ebt_manager_->OpenInit(db_path_, tbl_sub_path_, table_id_, err_info);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::OpenInit(kwdbContext_p ctx) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  if (ebt_manager_ != nullptr) {
    LOG_ERROR("TsTableRange already OpenInit.")
    return KStatus::FAIL;
  }

  ErrorInfo err_info;
  // Open Tag table under the range directory
  new_tag_bt_ = OpenTagTable(db_path_, tbl_sub_path_, table_id_, range_.range_group_id, err_info);
  if (err_info.errcode < 0 || new_tag_bt_ == nullptr) {
    LOG_ERROR("TsTableRange OpenTagTable error :%s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  ebt_manager_ = new SubEntityGroupManager(root_bt_manager_);
  ebt_manager_->OpenInit(db_path_, tbl_sub_path_, table_id_, err_info);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::PutEntity(kwdbContext_p ctx, TSSlice& payload, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  KStatus status = SUCCESS;
  ErrorInfo err_info;
  Payload pd(root_bt_manager_, payload);
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  // 1. query tag table
  TSSlice tmp_slice = pd.GetPrimaryTag();
  uint32_t entity_id, subgroup_id;
  if (new_tag_bt_->hasPrimaryKey(tmp_slice.data, tmp_slice.len, entity_id, subgroup_id)) {
    // update
    if (new_tag_bt_->UpdateTagRecord(pd, subgroup_id, entity_id, err_info) < 0) {
      LOG_ERROR("Update tag record failed. error: %s ", err_info.errmsg.c_str());
      releaseTagTable();
      return KStatus::FAIL;
    }
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice* payloads, int length, uint64_t mtr_id,
                               uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                               DedupResult* dedup_result, DedupRule dedup_rule, bool writeWAL) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  for (int i = 0; i < length; i++) {
    // Based on the number of payloads, call PutData repeatedly to write data
    KStatus s = PutData(ctx, payloads[i], 0, inc_entity_cnt, inc_unordered_cnt, dedup_result, dedup_rule);
    if (s == FAIL) return s;
  }
  return SUCCESS;
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice payload) {
  uint16_t inc_entity_cnt = 0;
  uint32_t inc_unordered_cnt = 0;
  DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
  return PutData(ctx, payload, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result, g_dedup_rule);
}

KStatus TsEntityGroup::PutData(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id, uint16_t* inc_entity_cnt,
                               uint32_t* inc_unordered_cnt, DedupResult* dedup_result,
                               DedupRule dedup_rule, bool write_wal) {
  return PutDataWithoutWAL(ctx, payload, mini_trans_id, inc_entity_cnt, inc_unordered_cnt, dedup_result, dedup_rule);
}

KStatus TsEntityGroup::PutDataWithoutWAL(kwdbContext_p ctx, TSSlice payload, TS_LSN mini_trans_id,
                                         uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                                         DedupResult* dedup_result, DedupRule dedup_rule) {
  // If wal is not enabled, this function will be used.
  KStatus status = SUCCESS;
  uint32_t group_id, entity_id;
  ErrorInfo err_info;
  group_id = 1;
  entity_id = 1;
  // Create a Payload object pd to prepare for subsequent writing.
  // The payload object includes both the tag and the payload data.
  // The data to be written can be retrieved using getColumnAddr() and getVarColumnAddr() methods.
  Payload pd(root_bt_manager_, payload);
  pd.dedup_rule_ = dedup_rule;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  Defer defer{[&]() {
    releaseTagTable();
  }};
  // check if lsn is set, in wal-off mode, lsn will not set, we should set lsn to 1 for marking lsn exist.
  TS_LSN pl_lsn = 0;
  if (pd.GetLsn(pl_lsn)) {
    if (pl_lsn == 0) {
      pd.SetLsn(1);
    }
  }
  LOG_DEBUG("PutDataWithoutWAL write rows: %d.", pd.GetRowCount());
  // Query or assign EntityGroupID and EntityID based on the provided payload.
  // Initially, attempt to retrieve the IDs directly from the tag table.
  // If it does not exist, allocate and insert it into the tag table.
  bool new_one;
  if (KStatus::SUCCESS != allocateEntityGroupId(ctx, pd, &entity_id, &group_id, &new_one)) {
    LOG_ERROR("allocateEntityGroupId failed, entity id: %u, group id: %u", entity_id, group_id);
    return KStatus::FAIL;
  }
  if (new_one) {
    ++(*inc_entity_cnt);
  }
  if (pd.GetFlag() == Payload::TAG_ONLY) {
    // Only when a tag is present, do not write data
    return KStatus::SUCCESS;
  }
  if (pd.GetRowCount() > 0) {
    return putDataColumnar(ctx, group_id, entity_id, pd, inc_unordered_cnt, dedup_result);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::putTagData(kwdbContext_p ctx, int32_t groupid, int32_t entity_id, Payload& payload) {
  KWDB_DURATION(StStatistics::Get().put_tag);
  ErrorInfo err_info;
  // 1. Write tag data
  uint8_t payload_data_flag = payload.GetFlag();
  if (payload_data_flag == Payload::DATA_AND_TAG || payload_data_flag == Payload::TAG_ONLY) {
    // tag
    LOG_DEBUG("tag bt insert hashPoint=%hu", payload.getHashPoint());
    err_info.errcode = new_tag_bt_->InsertTagRecord(payload, groupid, entity_id);
  }
  if (err_info.errcode < 0) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::SyncMetricDataVersion() {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  // sync metric schema versions.
  root_bt_manager_->Sync(0, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("root_bt_manager sync failed. %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  // sync metric data.
  ebt_manager_->sync(0);
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetAllPartitions(kwdbContext_p ctx, std::unordered_map<SubGroupID,
                                        std::vector<timestamp64>>* subgrp_partitions) {
  std::vector<void*> primary_tags;
  std::vector<uint32_t> scantags;
  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;
  std::vector<EntityResultIndex> entity_list;
  ResultSet res;
  uint32_t count;
  KStatus s = GetEntityIdList(ctx, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scantags, &entity_list ,
                              &res, &count);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAllPartitionDirs failed at GetEntityIdList.");
    return s;
  }
  KwTsSpan span{INT64_MIN, INT64_MAX};
  ErrorInfo err_info;
  for (auto& entity_index : entity_list) {
    if (entity_index.entityGroupId != range_.range_group_id) {
      // just filter this entitygroup only
      continue;
    }
    if (subgrp_partitions->find(entity_index.subGroupId) != subgrp_partitions->end()) {
      // this subgroup and its parttions already inserted.
      continue;
    }
    const auto& p_bts = ebt_manager_->GetPartitions(span, entity_index.subGroupId, err_info);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTables failed.");
      return KStatus::FAIL;
    }
    (*subgrp_partitions)[entity_index.subGroupId] = p_bts;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::allocateEntityGroupId(kwdbContext_p ctx, Payload& payload, uint32_t* entity_id,
                                            uint32_t* group_id, bool* new_tag) {
  *new_tag = false;
  KWDB_DURATION(StStatistics::Get().alloc_entity);
  ErrorInfo err_info;
  // Attempting to retrieve the group ID and entity ID from the tag table
  // 1.If found, assign a value and return
  // 2.If not found, attempt to lock and query again to prevent concurrent scenarios where the first query fails and
  // the entity is created successfully.
  // 3.If the second search fails, it indicates that no other thread has created the entity. Therefore, use the
  // consistent hashID algorithm to allocate a new ID and write it to the tag table.
  TSSlice tmp_slice = payload.GetPrimaryTag();
  uint32_t entityid, groupid;
  if (new_tag_bt_->hasPrimaryKey(tmp_slice.data, tmp_slice.len, entityid, groupid)) {
    *entity_id = entityid;
    *group_id = groupid;
    return KStatus::SUCCESS;
  }
  {
    // Locking, concurrency control, and the putTagData operation must not be executed repeatedly
    MUTEX_LOCK(mutex_);
    Defer defer{[&]() { MUTEX_UNLOCK(mutex_); }};
    if (new_tag_bt_->hasPrimaryKey(tmp_slice.data, tmp_slice.len, entityid, groupid)) {
      *entity_id = entityid;
      *group_id = groupid;
      return KStatus::SUCCESS;
    }
    // not found
    std::string tmp_str = std::to_string(table_id_);
    uint64_t tag_hash = TsTable::GetConsistentHashId(tmp_str.data(), tmp_str.size());
    std::string primary_tags;
    err_info.errcode = ebt_manager_->AllocateEntity(primary_tags, tag_hash, &groupid, &entityid);
    if (err_info.errcode < 0) {
      return KStatus::FAIL;
    }
    // insert tag table
    if (KStatus::SUCCESS != putTagData(ctx, groupid, entityid, payload)) {
      return KStatus::FAIL;
    }
  }
  *new_tag = true;
  *entity_id = entityid;
  *group_id = groupid;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::putDataColumnar(kwdbContext_p ctx, int32_t group_id, int32_t entity_id,
                                       Payload& payload, uint32_t* inc_unordered_cnt, DedupResult* dedup_result) {
  KWDB_DURATION(StStatistics::Get().put_data);
  KWDB_STAT_ADD(StStatistics::Get().payload_row, payload.GetRowCount());
  ErrorInfo err_info;

  // only used for import data, other scenario will not use discard mode.
  // When importing data, it is necessary to allocate space for bitmap to record discarded rows.
  if (payload.dedup_rule_ == DedupRule::DISCARD
      && dedup_result->payload_num == 1
      && dedup_result->discard_bitmap.data == nullptr) {
    dedup_result->discard_bitmap.len = (payload.GetRowCount() + 7) / 8;
    dedup_result->discard_bitmap.data = reinterpret_cast<char*>(malloc(dedup_result->discard_bitmap.len));
    memset(dedup_result->discard_bitmap.data, 0, dedup_result->discard_bitmap.len);
  }

  // Write data column by column, employing a columnar storage format. Each data column is accompanied by an
  // independent mmap file,
  // necessitating the updating of multiple column files during data writes.
  // After parsing the INSERT statement, the execution module constructs a payload that is stored in a columnar
  // format in memory, which is identical to the physical storage structure.
  // To enhance data writing performance, batch data writing can utilize memory copying methods directly,
  // based on the data column.
  timestamp64 last_p_time = INVALID_TS;
  TsTimePartition* p_bt = nullptr;
  int batch_start = payload.GetStartRowId();
  timestamp64 p_time = INVALID_TS;

  bool all_success = true;
  std::vector<BlockSpan> alloc_spans;
  TsSubEntityGroup* sub_group = ebt_manager_->GetSubGroup(group_id, err_info, false);
  if (err_info.errcode < 0) {
    LOG_ERROR("push_back_payload error: there is no subgroup: %u", group_id);
    return KStatus::FAIL;
  }

  unordered_map<TsTimePartition*, PutAfterProcessInfo*> after_process_info;
  bool payload_dup = false;
  std::unordered_set<timestamp64> dup_set;

  // Examine the timestamp of the first piece of data within the payload to verify the partition being written to
  KTimestamp first_ts_ms = payload.GetTimestamp(payload.GetStartRowId());
  KTimestamp first_ts = convertTsToPTime(first_ts_ms, root_bt_manager_->GetTsColDataType());
  timestamp64 first_max_ts;
  uint32_t hash_point = payload.getHashPoint();
  last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);
  k_int32 row_id = 0;
  LOG_DEBUG("Insert primaryTag is %d ", *(payload.GetPrimaryTag().data));
  // Based on the obtained partition timestamp, determine whether the current payload needs to switch partitions.
  // If necessary, first call push_back_payload to write the current partition data.
  // Then continue to call payloadNextSlice until the traversal of the Payload data is complete.
  while (payloadNextSlice(sub_group, payload, last_p_time, batch_start, &row_id, &p_time)) {
    p_bt = ebt_manager_->GetPartitionTable(last_p_time, group_id, err_info, true);
    if (err_info.errcode < 0) {
      LOG_ERROR("GetPartitionTable error: %s", err_info.errmsg.c_str());
      all_success = false;
      ebt_manager_->ReleasePartitionTable(p_bt);
      break;
    }
    if (!p_bt->isValid()) {
      LOG_WARN("Partition is invalid.");
      err_info.setError(KWEDUPREJECT, "Partition is invalid.");
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    last_p_time = p_time;

    p_bt->RefWritingCount();

    std::vector<BlockSpan> cur_alloc_spans;
    std::vector<MetricRowID> to_deleted_real_rows;

    // push_back_payload is used to allocate space, write data to the partition table, and perform data deduplication
    // and aggregation during this process.
    // The returned cur_alloc_spans contains records of all allocated blocks
    // and the number of rows written to each block.
    // If there is a failure, putAfterProcess will remove the spatial marker for cur_alloc_spans.
    // All rows that need to be deleted (deduplicated) are recorded in the returned to_deleted_real_rows,
    // and recordPutAfterProInfo will process them.
    err_info.errcode = p_bt->push_back_payload(ctx, entity_id, &payload, batch_start, row_id - batch_start,
                                               &cur_alloc_spans, &to_deleted_real_rows, err_info,
                                               inc_unordered_cnt, dedup_result);

    // After handling a partition, record the information to be processed subsequently
    recordPutAfterProInfo(after_process_info, p_bt, cur_alloc_spans, to_deleted_real_rows);

    if (err_info.errcode < 0) {
      LOG_ERROR("push_back_payload error: %s", err_info.errmsg.c_str());
      all_success = false;
      break;
    }
    batch_start = row_id;
  }

  // Writing completed, processing after_process_info recorded earlier
  putAfterProcess(after_process_info, entity_id, all_success);

  if (!all_success) {
    if (err_info.errcode == KWEDUPREJECT) {
      return KStatus::SUCCESS;
    }
    return KStatus::FAIL;
  }
  // update sub_group entity_id.
  {
    timestamp64 max_ts = payload.GetTimestamp(0);
    for (size_t i = 1; i < payload.GetRowCount(); i++) {
      auto cur_ts = payload.GetTimestamp(i);
      if (max_ts < cur_ts) {
        max_ts = cur_ts;
      }
    }
    sub_group->UpdateEntityAndMaxTs(max_ts, entity_id);
  }
  LOG_DEBUG("table[%s] input data num[%d].", this->tbl_sub_path_.c_str(), payload.GetRowCount());
  return KStatus::SUCCESS;
}

void TsEntityGroup::recordPutAfterProInfo(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                                          TsTimePartition* p_bt, std::vector<BlockSpan>& cur_alloc_spans,
                                          std::vector<MetricRowID>& to_deleted_real_rows) {
  if (after_process_info.find(p_bt) != after_process_info.end()) {
    p_bt->UnrefWritingCount();
    ebt_manager_->ReleasePartitionTable(p_bt);
  } else {
    after_process_info[p_bt] = new PutAfterProcessInfo();
  }

  // Update the partition data
  after_process_info[p_bt]->spans.insert(after_process_info[p_bt]->spans.end(),
                                         cur_alloc_spans.begin(), cur_alloc_spans.end());
  after_process_info[p_bt]->del_real_rows.insert(after_process_info[p_bt]->del_real_rows.end(),
                                                 to_deleted_real_rows.begin(), to_deleted_real_rows.end());
}

void TsEntityGroup::putAfterProcess(unordered_map<TsTimePartition*, PutAfterProcessInfo*>& after_process_info,
                                    uint32_t entity_id, bool all_success) {
  for (auto iter : after_process_info) {
    iter.first->publish_payload_space((iter.second)->spans, (iter.second)->del_real_rows, entity_id, all_success);
    iter.first->UnrefWritingCount();
    ebt_manager_->ReleasePartitionTable(iter.first);
    delete iter.second;
  }

  after_process_info.clear();
}

KStatus TsEntityGroup::DeleteRangeData(kwdbContext_p ctx, const HashIdSpan& hash_span, TS_LSN lsn,
                                       const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpans>* del_rows,
                                       uint64_t* count, uint64_t mtr_id, bool evaluate_del) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  // Based on the hash ID range, find all the rows of tag data that need to be deleted.
  vector<string> primary_tags;
  std::vector<TagPartitionTable*> all_tag_partition_tables;
  auto tag_bt = GetSubEntityGroupNewTagbt();
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
          primary_tags.emplace_back(primary_tag);
        }
      } else {
        string primary_tag(reinterpret_cast<char*>(entity_tag_bt->record(rownum)),
                                                  entity_tag_bt->primaryTagSize());
        primary_tags.emplace_back(primary_tag);
      }
    }
    entity_tag_bt->stopRead();
  }

  *count = 0;
  for (const auto& p_tags : primary_tags) {
    // Delete the data corresponding to the tag within the time range
    uint64_t entity_del_count = 0;
    DelRowSpans del_row_spans{p_tags};
    KStatus status = DeleteData(ctx, p_tags, lsn, ts_spans, &del_row_spans.spans, &entity_del_count, mtr_id, evaluate_del);
    if (status == KStatus::FAIL) {
      LOG_ERROR("DeleteRangeData failed, delete entity by primary key %s failed", p_tags.c_str());
      return KStatus::FAIL;
    }
    // Update count
    *count += entity_del_count;
    if (!del_row_spans.spans.empty() && evaluate_del) {
      del_rows->push_back(del_row_spans);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteData(kwdbContext_p ctx, const string& primary_tag, TS_LSN lsn,
                                  const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>* rows,
                                  uint64_t* count, uint64_t mtr_id, bool evaluate_del) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  *count = 0;
  uint32_t entity_id = 0;
  uint32_t subgroup_id = 0;
  // Get subgroup id and entity id based on the primary tag.
  if (false == new_tag_bt_->hasPrimaryKey(const_cast<char*>(primary_tag.c_str()),
                                        primary_tag.size(), entity_id, subgroup_id)) {
    LOG_INFO("primary_tag[%s] does not exist, no need to delete", primary_tag.c_str());
    return KStatus::SUCCESS;
  }
  timestamp64 min_ts = INT64_MAX, max_ts = INT64_MIN;
  for (auto span : ts_spans) {
    min_ts = (min_ts > span.begin) ? span.begin : min_ts;
    max_ts = (max_ts < span.end) ? span.end : max_ts;
  }
  ErrorInfo err_info;
  vector<TsTimePartition*> p_tables =
      ebt_manager_->GetPartitionTables({min_ts, max_ts}, subgroup_id, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("GetPartitionTable error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  TsHashLatch* delete_data_latch = root_bt_manager_->GetDeleteDataLatch();
  delete_data_latch->Lock(entity_id);
  bool del_failed = false;
  for (TsTimePartition* p_bt : p_tables) {
    if (del_failed) {
      ebt_manager_->ReleasePartitionTable(p_bt);
      continue;
    }
    vector<DelRowSpan> delete_rows;
    uint64_t cnt = 0;
    int res = 0;
    if (evaluate_del) {
      res = p_bt->DeleteData(entity_id, 0, ts_spans, &delete_rows, &cnt, err_info, evaluate_del);
    } else {
      res = p_bt->DeleteData(entity_id, 0, ts_spans, nullptr, &cnt, err_info, evaluate_del);
    }
    if (res < 0) {
      del_failed = true;
    }
    if (rows && evaluate_del) {
      for (auto& row_span : delete_rows) {
        row_span.partition_ts = p_bt->minTimestamp();
        rows->emplace_back(row_span);
      }
    }
    (*count) += cnt;
    ebt_manager_->ReleasePartitionTable(p_bt);
  }
  delete_data_latch->Unlock(entity_id);
  if (del_failed) {
    return KStatus::FAIL;
  }
  // check if need reset entity max ts, after data deleted.
  TsSubEntityGroup* subgroup = ebt_manager_->GetSubGroup(subgroup_id, err_info, false);
  if (subgroup != nullptr) {
    subgroup->ResetEntityMaxTs(max_ts, {entity_id});
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  int ret = ebt_manager_->DeleteExpiredData(end_ts, err_info);
  if (ret < 0) {
    LOG_ERROR("SubGroup delete expired data failed, err: %s.", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteEntity(kwdbContext_p ctx, const string& primary_tag, uint64_t* del_cnt, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  uint32_t sub_group_id, entity_id;
  if (!new_tag_bt_->hasPrimaryKey(primary_tag.data(), primary_tag.size(), entity_id, sub_group_id)) {
    return KStatus::FAIL;
  }
  ErrorInfo err_info;
  // Delete tag and its index
  new_tag_bt_->DeleteTagRecord(primary_tag.data(), primary_tag.size(), err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("delete_tag_record error, error msg: %s", err_info.errmsg.c_str())
    return KStatus::FAIL;
  }
  // Delete entities
  TsSubEntityGroup* p_bt = ebt_manager_->GetSubGroup(sub_group_id, err_info, false);
  if (p_bt) {
    p_bt->DeleteEntity(entity_id, 0, del_cnt, err_info);
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteEntities(kwdbContext_p ctx, const std::vector<std::string>& primary_tags,
                                      uint64_t* count, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  *count = 0;
  for (const auto& p_tags : primary_tags) {
    uint32_t sub_group_id, entity_id;
    if (!new_tag_bt_->hasPrimaryKey(p_tags.data(), p_tags.size(), entity_id, sub_group_id)) {
      LOG_INFO("primary key[%s] dose not exist, no need to delete", p_tags.c_str())
      continue;
    }
    uint64_t num = 0;
    KStatus status = DeleteEntity(ctx, p_tags, &num, mtr_id);
    if (status == KStatus::FAIL) {
      LOG_ERROR("Failed to delete entity by primary key %s", p_tags.c_str())
      return KStatus::FAIL;
    }
    *count += num;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::DeleteRangeEntities(kwdbContext_p ctx, const HashIdSpan& hash_span,
                                           uint64_t* count, uint64_t mtr_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  // 1. First, query the primary tags of each record in the tag_bt.
  // 2. Compute the hash ID of the primary tags and check if it falls within the range HashIdSpan.
  //    If it does, proceed with the following steps.
  // 3. Invoke the `DeleteEntities` based on the primary tags to delete the entities.
  // 4. Remove the corresponding data and indexes from tag_bt.
  vector<string> primary_tags;
  std::vector<TagPartitionTable*> all_tag_partition_tables;
  auto tag_bt = GetSubEntityGroupNewTagbt();
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
          primary_tags.emplace_back(primary_tag);
        }
      } else {
        string primary_tag(reinterpret_cast<char*>(entity_tag_bt->record(rownum)),
                                                  entity_tag_bt->primaryTagSize());
        primary_tags.emplace_back(primary_tag);
      }
    }
    entity_tag_bt->stopRead();
  }
  if (DeleteEntities(ctx, primary_tags, count, mtr_id) == KStatus::FAIL) {
    LOG_ERROR("delete entities error")
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                                       const std::vector<uint64_t/*index_id*/> &tags_index_id,
                                       const std::vector<void*> tags,
                                       TSTagOpType op_type,
                                       const std::vector<uint32_t>& scan_tags,
                                       std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                                       uint32_t table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (new_tag_bt_->GetEntityIdList(primary_tags, tags_index_id, tags, op_type, scan_tags, entity_id_list, res, count,
                                   table_version) < 0) {
    LOG_ERROR("GetEntityIdList error ");
    status = KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
           const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count, uint32_t table_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  KStatus status = KStatus::SUCCESS;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (new_tag_bt_->GetTagList(ctx, entity_id_list, scan_tags, res, count, table_version) < 0) {
    LOG_ERROR("GetTagList error ");
    status = KStatus::FAIL;
  }
  releaseTagTable();
  return status;
}

KStatus TsEntityGroup::Drop(kwdbContext_p ctx, bool is_force) {
  RW_LATCH_X_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  ebt_manager_->DropAll(is_force, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsTable::Drop error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  // tag table
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable fail. error: %s ", err_info.errmsg.c_str());
    return KStatus::SUCCESS;
  }
  DropTagTable(new_tag_bt_, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("dropTagTable : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  delete new_tag_bt_;
  new_tag_bt_ = nullptr;
  // delete directory of entity_group
  string group_path = db_path_ + tbl_sub_path_;
  LOG_INFO("remove group %s", group_path.c_str());
  fs::remove_all(group_path.c_str());
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::Compress(kwdbContext_p ctx, const KTimestamp& ts, uint32_t& compressed_num, ErrorInfo& err_info) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ebt_manager_->Compress(ctx, ts, compressed_num, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsEntityGroup::Compress error : %s", err_info.errmsg.c_str());
    return FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::Vacuum(kwdbContext_p ctx, uint32_t ts_version, ErrorInfo &err_info) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ebt_manager_->Vacuum(ctx, ts_version, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsEntityGroup::Vacuum error : %s", err_info.errmsg.c_str());
    return FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::TierMigrate() {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ebt_manager_->TierMigrate();
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::Count(kwdbContext_p ctx, ErrorInfo &err_info) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ebt_manager_->Count(ctx, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsEntityGroup::Count error : %s", err_info.errmsg.c_str());
    return FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetIterator(kwdbContext_p ctx, SubGroupID sub_group_id, vector<uint32_t> entity_ids,
                                   std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                                   std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                                   std::vector<Sumfunctype> scan_agg_types,
                                   uint32_t table_version, TsStorageIterator** iter,
                                   std::shared_ptr<TsEntityGroup> entity_group,
                                   std::vector<timestamp64> ts_points, bool reverse, bool sorted) {
  // TODO(liuwei) update to use read_lsn to fetch Metrics data optimistically.
  // if the read_lsn is 0, ignore the read lsn checking and return all data (it's no WAL support case).
  // TS_LSN read_lsn = GetOptimisticReadLsn();
  TsStorageIterator* ts_iter = nullptr;
  if (scan_agg_types.empty()) {
    if (sorted) {
      ts_iter = new TsSortedRowDataIterator(entity_group, range_.range_group_id, sub_group_id, entity_ids,
                                            ts_spans, ts_col_type, scan_cols, ts_scan_cols, table_version, ASC);
    } else {
      ts_iter = new TsRawDataIterator(entity_group, range_.range_group_id, sub_group_id,
                                      entity_ids, ts_spans, ts_col_type, scan_cols, ts_scan_cols, table_version);
    }
  } else {
    ts_iter = new TsAggIterator(entity_group, range_.range_group_id, sub_group_id, entity_ids, ts_spans,
                                ts_col_type, scan_cols, ts_scan_cols, scan_agg_types, ts_points, table_version);
  }
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetOffsetIterator(kwdbContext_p ctx, std::map<SubGroupID, std::vector<EntityID>> entity_ids,
                                         std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                                         std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                                         uint32_t table_version, TsIterator** iter,
                                         std::shared_ptr<TsEntityGroup> entity_group, k_uint32 offset, k_uint32 limit,
                                         bool reverse) {
  TsOffsetIterator* ts_iter = new TsOffsetIterator(entity_group, range_.range_group_id, entity_ids, ts_spans,
                                                   ts_col_type, scan_cols, ts_scan_cols, table_version, offset, limit);
  KStatus s = ts_iter->Init(reverse);
  if (s != KStatus::SUCCESS) {
    delete ts_iter;
    ts_iter = nullptr;
    return s;
  }
  *iter = ts_iter;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::GetUnorderedDataInfo(kwdbContext_p ctx, const KwTsSpan ts_span, UnorderedDataStats* stats) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo err_info;
  for (uint32_t i = 1; i <= ebt_manager_->GetMaxSubGroupId(); ++i) {
    TsSubEntityGroup* subgroup = ebt_manager_->GetSubGroup(i, err_info);
    if (!subgroup) {
      continue;
    }
    k_uint32 total_entity_cnt = 0;
    k_uint32 unordered_entity_cnt = 0;
    std::vector<uint32_t> entities = subgroup->GetEntities();
    vector<TsTimePartition*> partitions = ebt_manager_->GetPartitionTables(ts_span, i, err_info);
    for (auto entity_id : entities) {
      bool is_in_span = false;
      bool is_disordered = false;
      for (int i = 0; i < partitions.size(); ++i) {
        EntityItem* entity = partitions[i]->getEntityItem(entity_id);
        if (entity->min_ts > ts_span.end || entity->max_ts < ts_span.begin) {
          continue;
        }
        is_in_span = true;
        std::deque<BlockItem*> block_queue;
        partitions[i]->GetAllBlockItems(entity_id, block_queue);
        while (!block_queue.empty()) {
          BlockItem* block = block_queue.front();
          block_queue.pop_front();
          std::shared_ptr<MMapSegmentTable> segment_tbl = partitions[i]->getSegmentTable(block->block_id);
          timestamp64 min_ts = KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MIN));
          timestamp64 max_ts = KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MAX));
          if (ts_span.begin > max_ts || ts_span.end < min_ts) {
            continue;
          }
          bool flag = (ts_span.begin == ts_span.end);
          stats->total_data_rows += flag ? 1 : block->publish_row_count;
          if (block->unordered_flag) {
            is_disordered = true;
            stats->unordered_data_rows += flag ? 1 : block->publish_row_count;
          }
        }
      }
      if (is_in_span) {
        ++total_entity_cnt;
      }
      if (is_disordered) {
        ++unordered_entity_cnt;
      }
    }
    for (auto p_bt : partitions) {
      ebt_manager_->ReleasePartitionTable(p_bt);
    }
    stats->ordered_entity_cnt += (total_entity_cnt - unordered_entity_cnt);
    stats->unordered_entity_cnt += unordered_entity_cnt;
  }
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetTagIterator(kwdbContext_p ctx,
                              std::shared_ptr<TsEntityGroup> entity_group,
                              std::vector<k_uint32>& scan_tags,
                              uint32_t table_version,
                              EntityGroupTagIterator** iter) {
  ErrorInfo err_info;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  EntityGroupTagIterator* entity_group_iter = new EntityGroupTagIterator(entity_group, new_tag_bt_,
                                                  table_version, scan_tags);
  *iter = entity_group_iter;
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetTagIterator(kwdbContext_p ctx,
                            std::shared_ptr<TsEntityGroup> entity_group,
                            std::vector<k_uint32>& scan_tags,
                            uint32_t table_version,
                            EntityGroupTagIterator** iter,
                            const std::vector<uint32_t>& hps) {
  ErrorInfo err_info;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  EntityGroupTagIterator* entitygroupIter = new EntityGroupTagIterator(entity_group, new_tag_bt_,
                                                table_version, scan_tags, hps);
  *iter = entitygroupIter;
  return KStatus::SUCCESS;
}


KStatus
TsEntityGroup::GetMetaIterator(kwdbContext_p ctx, EntityGroupMetaIterator** iter) {
  EntityGroupMetaIterator* meta_iter = new EntityGroupMetaIterator(range_.range_group_id, ebt_manager_);
  if (KStatus::SUCCESS != meta_iter->Init()) {
    delete meta_iter;
    meta_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = meta_iter;
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::TSxClean(kwdbContext_p ctx) {
  ErrorInfo err_info;
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  new_tag_bt_->GetTagTableVersionManager()->SyncCurrentTableVersion();
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::AlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                               uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (new_tag_bt_->AlterTableTag(alter_type, attr_info, cur_version, new_version, err_info) < 0) {
    LOG_ERROR("AlterTableTag failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsEntityGroup::UndoAlterTableTag(kwdbContext_p ctx, uint32_t cur_version,
                                        uint32_t new_version, ErrorInfo& err_info) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  if (getTagTable(err_info) != KStatus::SUCCESS) {
    LOG_ERROR("getTagTable error ");
    return KStatus::FAIL;
  }
  if (new_tag_bt_->undoAlterTagTable(cur_version, new_version, err_info) < 0) {
    LOG_ERROR("AlterTableTag failed. error: %s ", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetColAttributeInfo(kwdbContext_p ctx, const roachpb::KWDBKTSColumn& col,
                                   AttributeInfo& attr_info, bool first_col) {
  switch (col.storage_type()) {
    case roachpb::TIMESTAMP:
    case roachpb::TIMESTAMPTZ:
    case roachpb::DATE:
      if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64;
      }
      attr_info.max_len = 3;
      break;
    case roachpb::TIMESTAMP_MICRO:
    case roachpb::TIMESTAMPTZ_MICRO:
    if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN_MICRO;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64_MICRO;
      }
      attr_info.max_len = 6;
      break;
    case roachpb::TIMESTAMP_NANO:
    case roachpb::TIMESTAMPTZ_NANO:
      if (first_col) {
        attr_info.type = DATATYPE::TIMESTAMP64_LSN_NANO;
      } else {
        attr_info.type = DATATYPE::TIMESTAMP64_NANO;
      }
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
      attr_info.max_len = col.storage_len() - 1;  // because varchar len will +1 when store
      break;
    case roachpb::NVARCHAR:
    case roachpb::VARBINARY:
      attr_info.type = DATATYPE::VARBINARY;
      attr_info.max_len = col.storage_len();
      break;
    case roachpb::SDECHAR:
    case roachpb::SDEVARCHAR:
      attr_info.type = DATATYPE::STRING;
      attr_info.max_len = col.storage_len();
      break;
    default:
      LOG_ERROR("convert roachpb::KWDBKTSColumn to AttributeInfo failed: unknown column type[%d]", col.storage_type());
      return KStatus::FAIL;
  }

  attr_info.size = getDataTypeSize(attr_info);
  attr_info.id = col.column_id();
  strncpy(attr_info.name, col.name().c_str(), COLUMNATTR_LEN);
  attr_info.length = col.storage_len();
  if (!col.nullable()) {
    attr_info.setFlag(AINFO_NOT_NULL);
  }
  if (col.dropped()) {
    attr_info.setFlag(AINFO_DROPPED);
  }
  attr_info.col_flag = (ColumnFlag) col.col_type();
  attr_info.version = 1;

  return KStatus::SUCCESS;
}

KStatus
TsEntityGroup::GetMetricColumnInfo(kwdbContext_p ctx, struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  switch (attr_info.type) {
    case DATATYPE::TIMESTAMP64_LSN:
      col.set_storage_type(roachpb::TIMESTAMPTZ);
      break;
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
      col.set_storage_type(roachpb::TIMESTAMPTZ_MICRO);
      break;
    case DATATYPE::TIMESTAMP64_LSN_NANO:
      col.set_storage_type(roachpb::TIMESTAMPTZ_NANO);
      break;
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
      col.set_storage_len(attr_info.max_len + 1);  // varchar(len) + 1
      break;
    case DATATYPE::VARBINARY:
      col.set_storage_type(roachpb::VARBINARY);
      col.set_storage_len(attr_info.max_len);
      break;
    case DATATYPE::STRING:
      col.set_storage_type(roachpb::SDECHAR);
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

KStatus
TsEntityGroup::GetTagColumnInfo(kwdbContext_p ctx, struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col) {
  col.clear_storage_len();
  col.set_storage_len(tag_info.m_length);
  col.set_column_id(tag_info.m_id);
  col.set_col_type((roachpb::KWDBKTSColumn_ColumnType)((ColumnFlag)tag_info.m_tag_type));
  if (tag_info.isDropped()) {
    col.set_dropped(true);
  }
  switch (tag_info.m_data_type) {
    case DATATYPE::TIMESTAMP64_LSN:
      col.set_storage_type(roachpb::TIMESTAMPTZ);
      break;
    case DATATYPE::TIMESTAMP64_LSN_MICRO:
      col.set_storage_type(roachpb::TIMESTAMPTZ_MICRO);
      break;
    case DATATYPE::TIMESTAMP64_LSN_NANO:
      col.set_storage_type(roachpb::TIMESTAMPTZ_NANO);
      break;
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
    case DATATYPE::STRING:
      col.set_storage_type(roachpb::SDECHAR);
      break;
    case DATATYPE::INVALID:
    default:
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetLastRowEntity(EntityResultIndex& entity_id) {
  timestamp64 entity_max_ts = INT64_MIN;
  entity_id = {0, 0, 0};

  pair<timestamp64, EntityID> cur_last_entity;
  for (auto& e_grp : entity_groups_) {
    std::list<TsSubEntityGroup*> cur_e_grp_sub_grps;
    e_grp.second->GetSubEntityGroupManager()->GetAllSubGroups(cur_e_grp_sub_grps);
    for (auto& sub_grp : cur_e_grp_sub_grps) {
      if (sub_grp->GetLastRowEntity(cur_last_entity) < 0) {
        LOG_ERROR("Subgroup %d GetLastRowEntity failed.", sub_grp->GetID());
        return KStatus::FAIL;
      }
      if (cur_last_entity.second == 0) {
        LOG_WARN("cannot found last row entity for subgrp[%d]", sub_grp->GetID());
        continue;
      }
      if (cur_last_entity.first > entity_max_ts) {
        entity_id.entityGroupId = e_grp.first;
        entity_id.subGroupId = sub_grp->GetID();
        entity_id.entityId = cur_last_entity.second;
      }
    }
  }
  return KStatus::SUCCESS;
}

/// @brief RangeGroupID compute function. new used in hashPoint. RangeGroupID % 65535. hashPoint % 10
/// @param data primaryKey to compute which HashPoint it belongs to
/// @param length how long the primaryKey to compute
/// @return hashPoint ID
uint32_t TsTable::GetConsistentHashId(const char* data, size_t length) {
  // TODO(jiadx): 使用与GO层相同的一致性hashID算法，可能还需要一些方法参数
//  uint64_t hash_id = std::hash<string>()(primary_tags);
  const uint32_t offset_basis = 2166136261;  // 32位offset basis
  const uint32_t prime = 16777619;
  uint32_t hash_val = offset_basis;
  for (int i = 0; i < length; i++) {
    unsigned char b = data[i];
    hash_val *= prime;
    hash_val ^= b;
  }
  return hash_val % HASHPOINT_RANGE;
}

MMapRootTableManager* TsTable::CreateMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                          vector<AttributeInfo>& schema, uint32_t table_version,
                                                          uint64_t partition_interval, ErrorInfo& err_info) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id, partition_interval);
  KStatus s = tmp_bt_manager->CreateRootTable(schema, table_version, err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

MMapRootTableManager* TsTable::OpenMMapRootTableManager(string& db_path, string& tbl_sub_path, KTableKey table_id,
                                                        ErrorInfo& err_info) {
  MMapRootTableManager* tmp_bt_manager = new MMapRootTableManager(db_path, tbl_sub_path, table_id);
  KStatus s = tmp_bt_manager->Init(err_info);
  if (s == KStatus::FAIL) {
    delete tmp_bt_manager;
    tmp_bt_manager = nullptr;
  }
  return tmp_bt_manager;
}

TsTable::TsTable(kwdbContext_p ctx, const string& db_path, const KTableKey& table_id)
    : db_path_(db_path), table_id_(table_id) {
  entity_bt_manager_ = nullptr;
  tbl_sub_path_ = std::to_string(table_id_) + "/";
  db_path_ = db_path_ + "/";
  is_dropped_.store(false);
  entity_groups_mtx_ = new TsTableEntityGrpsRwLatch(RWLATCH_ID_TS_TABLE_ENTITYGRPS_RWLOCK);
  snapshot_manage_mtx_ = new TsTableSnapshotLatch(LATCH_ID_TSTABLE_SNAPSHOT_MUTEX);
}

TsTable::~TsTable() {
  if (is_dropped_) {
    kwdbContext_t context;
    kwdbContext_p ctx = &context;
    KStatus s = InitServerKWDBContext(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("InitServerKWDBContext Error!");
    }
    s = DropAll(ctx);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("DropAll Error!");
    }
  }
  if (entity_bt_manager_) {
    delete entity_bt_manager_;
  }
  if (entity_groups_mtx_) {
    delete entity_groups_mtx_;
    entity_groups_mtx_ = nullptr;
  }
  if (snapshot_manage_mtx_) {
    delete snapshot_manage_mtx_;
    snapshot_manage_mtx_ = nullptr;
  }
}

// Check that the directory name is a numeric
bool IsNumber(struct dirent* dir) {
  for (int i = 0; i < strlen(dir->d_name); ++i) {
    if (!isdigit(dir->d_name[i])) {
      // Iterate over each character and determine if it's a number
      return false;
    }
  }
  return true;
}

KStatus TsTable::Init(kwdbContext_p ctx, std::unordered_map<uint64_t, int8_t>& range_groups, ErrorInfo& err_info) {
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    err_info.setError(KWENOOBJ, "invalid path: " + db_path_ + tbl_sub_path_);
    LOG_WARN("can not access table path [%s].", ((db_path_ + tbl_sub_path_)).c_str())
    return KStatus::FAIL;
  }

  // Load entity bt
  entity_bt_manager_ = OpenMMapRootTableManager(db_path_, tbl_sub_path_, table_id_, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("TsTable Init error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }

  DIR* dir_ptr = opendir((db_path_ + "/" + tbl_sub_path_).c_str());
  if (dir_ptr == nullptr) {
    LOG_ERROR("invalid path : %s", ((db_path_ + tbl_sub_path_)).c_str());
    return KStatus::FAIL;
  }
  // Traverse the range directory
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    struct dirent* group_dir;
    while ((group_dir = readdir(dir_ptr)) != nullptr) {
      if (!IsNumber(group_dir)) {  // Check if it is an entity group directory
        continue;
      }
      int64_t range_group_id = std::atol(group_dir->d_name);
      if (range_group_id < 1) {
        LOG_ERROR("invalid range group : %ld", range_group_id);
      }
      string range_tbl_sub_path = tbl_sub_path_ + group_dir->d_name + "/";
      // hash_range's default type is FOLLOWER when loaded locally
      RangeGroup hash_range{(uint64_t) range_group_id, EntityGroupType::UNINITIALIZED};
      auto it = range_groups.find(range_group_id);
      if (it != range_groups.end()) {
        hash_range.typ = it->second;
      }
      std::shared_ptr<TsEntityGroup> t_range;
      KStatus s = newEntityGroup(ctx, hash_range, range_tbl_sub_path, &t_range);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("TsTableRange OpenInit error : %lu %lu", table_id_, hash_range.range_group_id);
        continue;
      }
      entity_groups_[hash_range.range_group_id] = std::move(t_range);
    }
    closedir(dir_ptr);
  }
  is_dropped_.store(entity_bt_manager_->IsDropped());
  return KStatus::SUCCESS;
}

KStatus TsTable::newEntityGroup(kwdbContext_p ctx, RangeGroup hash_range, const string& range_tbl_sub_path,
                                std::shared_ptr<TsEntityGroup>* ent_group) {
  constructEntityGroup(ctx, hash_range, range_tbl_sub_path, ent_group);
  return (*ent_group)->OpenInit(ctx);
}

KStatus TsTable::Create(kwdbContext_p ctx, vector<AttributeInfo>& metric_schema,
                        uint32_t ts_version, uint64_t partition_interval) {
  if (entity_bt_manager_ != nullptr) {
    LOG_ERROR("Entity Bigtable already exist.");
    return KStatus::FAIL;
  }
  // Check path
  string dir_path = db_path_ + tbl_sub_path_;
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path)) {
      return KStatus::FAIL;
    }
  }

  ErrorInfo err_info;
  // Create entity table
  entity_bt_manager_ = CreateMMapRootTableManager(db_path_, tbl_sub_path_, table_id_, metric_schema, ts_version,
                                                  partition_interval, err_info);
  if (err_info.errcode < 0) {
    LOG_ERROR("createTable fail, table_id[%lu], msg[%s]", table_id_, err_info.errmsg.c_str());
  }

  if (entity_bt_manager_ == nullptr) {
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::CheckAndAddSchemaVersion(kwdbContext_p ctx, const KTableKey& table_id, uint64_t version) {
  if (!g_go_start_service) return KStatus::SUCCESS;
  if (version == entity_bt_manager_->GetCurrentTableVersion()) {
    return KStatus::SUCCESS;
  }

  if (entity_bt_manager_->GetRootTable(version, true) != nullptr) {
    return KStatus::SUCCESS;
  }

  char* error;
  size_t data_len = 0;
  char* data = getTableMetaByVersion(table_id, version, &data_len, &error);
  if (error != nullptr) {
    LOG_ERROR(error);
    return KStatus::FAIL;
  }
  roachpb::CreateTsTable meta;
  if (!meta.ParseFromString({data, data_len})) {
    LOG_ERROR("Parse schema From String failed.");
    return KStatus::FAIL;
  }
  MMapMetricsTable* version_schema;
  if (AddSchemaVersion(ctx, &meta, &version_schema) != KStatus::SUCCESS) {
    LOG_ERROR("failed during upper version.");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataSchemaIncludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema,
                                             uint32_t table_version) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoIncludeDropped(data_schema, table_version);
}

KStatus TsTable::GetDataSchemaExcludeDropped(kwdbContext_p ctx, std::vector<AttributeInfo>* data_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  return entity_bt_manager_->GetSchemaInfoExcludeDropped(data_schema);
}

KStatus TsTable::GetTagSchema(kwdbContext_p ctx, RangeGroup range, std::vector<TagInfo>* tag_schema) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  if (entity_groups_.find(range.range_group_id) == entity_groups_.end()) {
    return KStatus::FAIL;
  }
  return entity_groups_[range.range_group_id]->GetCurrentTagSchemaExcludeDropped(tag_schema);
}

KStatus TsTable::GetTagSchemaIncludeDropped(kwdbContext_p ctx, RangeGroup range, std::vector<TagInfo>* tag_schema,
                                            uint32_t table_version) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  if (entity_groups_.find(range.range_group_id) == entity_groups_.end()) {
    return KStatus::FAIL;
  }
  return entity_groups_[range.range_group_id]->GetTagSchemaIncludeDroppedByVersion(tag_schema, table_version);
}

KStatus TsTable::GenerateMetaSchema(kwdbContext_p ctx, roachpb::CreateTsTable* meta,
                                         std::vector<AttributeInfo>& metric_schema,
                                         std::vector<TagInfo>& tag_schema,
                                         uint32_t schema_version) {
  EnterFunc()
  // Traverse metric schema and use attribute info to construct metric column info of meta.
  for (auto col_var : metric_schema) {
    // meta's column pointer.
    roachpb::KWDBKTSColumn* col = meta->add_k_column();
    KStatus s = TsEntityGroup::GetMetricColumnInfo(ctx, col_var, *col);
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
    KStatus s = TsEntityGroup::GetTagColumnInfo(ctx, tag_info, *col);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColTypeStr[%d] failed during generate tag Schema", tag_info.m_data_type);
      Return(s);
    }
    // Set storage length.
    if (col->has_storage_len() && col->storage_len() == 0) {
      col->set_storage_len(tag_info.m_size);
    }
  }

  auto tag_infos = GetAllNTagIndexs(schema_version);
  for (auto tag_info : tag_infos) {
      roachpb::NTagIndexInfo* idx_info = meta->add_index_info();
      idx_info->set_index_id(tag_info.first);
      for (auto col_id : tag_info.second) {
        idx_info->add_col_ids(col_id);
      }
  }

  Return(KStatus::SUCCESS);
}

KStatus TsTable::CreateEntityGroup(kwdbContext_p ctx, RangeGroup range, vector<TagInfo>& tag_schema,
                                   std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    if (entity_groups_.find(range.range_group_id) != entity_groups_.end()) {
      LOG_ERROR("TableRange already exist : %s", (tbl_sub_path_ + "." + std::to_string(range.range_group_id)).c_str());
      return KStatus::FAIL;
    }
  }

  string range_tbl_sub_path = tbl_sub_path_ + std::to_string(range.range_group_id) + "/";
  if (!MakeDirectory(db_path_ + range_tbl_sub_path)) {
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> t_group;
  constructEntityGroup(ctx, range, range_tbl_sub_path, &t_group);

  KStatus s = t_group->Create(ctx, tag_schema, entity_bt_manager_->GetCurrentTableVersion());
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsTableRange OpenInit error : %ld,%ld", table_id_, range.range_group_id);
    return KStatus::FAIL;
  }
  // init tag version
  // t_group->UpdateTagVersion(entity_bt_manager_->GetCurrentTableVersion());
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  entity_groups_[range.range_group_id] = std::move(t_group);
  *entity_group = entity_groups_[range.range_group_id];
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityGroups(kwdbContext_p ctx, RangeGroups *groups) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  groups->len = entity_groups_.size();
  groups->ranges = static_cast<RangeGroup*>(malloc(sizeof(RangeGroup) * groups->len));
  if (groups->ranges == nullptr) {
    LOG_ERROR("failed malloc RangeGroups");
    return KStatus::FAIL;
  }
  int i = 0;
  for (auto &p : entity_groups_) {
    groups->ranges[i].range_group_id = p.first;
    groups->ranges[i].typ = p.second->HashRange().typ;
    LOG_INFO("range_group_id: %lu, type: %hhd", groups->ranges[i].range_group_id, groups->ranges[i].typ);
    ++i;
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::UpdateEntityGroup(kwdbContext_p ctx, const RangeGroup& range) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(range.range_group_id);
  if (it == entity_groups_.end()) {
    LOG_ERROR("no hash range : %ld", range.range_group_id);
    return KStatus::FAIL;
  }
  it->second->HashRange().typ = range.typ;
  return KStatus::SUCCESS;
}

KStatus
TsTable::GetEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(range_group_id);
  if (it != entity_groups_.end()) {
    *entity_group = it->second;
  } else {
    // s = CreateTableRange(ctx, range, table_range);
    LOG_ERROR("no hash range: %ld", range_group_id);
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::GetEntityGroupByPrimaryKey(kwdbContext_p ctx, const TSSlice& primary_key,
                                            std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  // scan all tags in entitygroup to check if entity in existed entitygroup.
  for (auto& entity_grp : entity_groups_) {
    if (entity_grp.second->GetSubEntityGroupNewTagbt()->hasPrimaryKey(primary_key.data, primary_key.len)) {
      *entity_group = entity_grp.second;
      return KStatus::SUCCESS;
    }
  }
  // if entity no exists in storage. store it to default entitygroup.
  auto iter = entity_groups_.find(default_entitygroup_id_in_dist_v2);
  if (iter != entity_groups_.end()) {
    *entity_group = iter->second;
    return KStatus::SUCCESS;
  }
  LOG_ERROR("can not find entitygroup for primary key %s", primary_key.data);
  return KStatus::FAIL;
}

KStatus
TsTable::GetEntityGroupByHash(kwdbContext_p ctx, uint16_t hashpoint, uint64_t *range_group_id,
                       std::shared_ptr<TsEntityGroup>* entity_group) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  // *range_group_id = getRangeIdByHash(hashpoint);
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto it = entity_groups_.find(*range_group_id);
  if (it != entity_groups_.end()) {
    *entity_group = it->second;
  } else {
    // s = CreateTableRange(ctx, range, table_range);
    LOG_ERROR("no hash range: %ln", range_group_id);
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                         uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                         DedupResult* dedup_result, const DedupRule& dedup_rule) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> entity_grp;
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // entity_groups_: Data from entities in an EntityRangeGroup is stored in an EntityGroup.
    // EntityGroup is a logical unit to persist entities' data, including tag data and measurement data.
    auto it = entity_groups_.find(range_group_id);
    if (it == entity_groups_.end()) {
      LOG_ERROR("no entity group with id: %lu", range_group_id);
      return KStatus::FAIL;
    }
    entity_grp = it->second;
  }
  KStatus s = entity_grp->PutData(ctx, payload, payload_num, mtr_id,
                                  inc_entity_cnt, inc_unordered_cnt, dedup_result, dedup_rule);
  return s;
}

KStatus TsTable::PutDataWithoutWAL(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                                   uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                                   DedupResult* dedup_result, const DedupRule& dedup_rule) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  std::shared_ptr<TsEntityGroup> entity_grp;
  {
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // entity_groups_: Data from entities in an EntityRangeGroup is stored in an EntityGroup.
    // EntityGroup is a logical unit to persist entities' data, including tag data and measurement data.
    auto it = entity_groups_.find(range_group_id);
    if (it == entity_groups_.end()) {
      LOG_ERROR("no entity group with id: %lu", range_group_id);
      return KStatus::FAIL;
    }
    entity_grp = it->second;
  }
  KStatus s = entity_grp->PutData(ctx, *payload, mtr_id, inc_entity_cnt,
                                            inc_unordered_cnt, dedup_result, dedup_rule, false);
  return s;
}

KStatus TsTable::FlushBuffer(kwdbContext_p ctx) {
  // No processing required
  return KStatus::SUCCESS;
}


KStatus TsTable::CreateCheckpoint(kwdbContext_p ctx) {
  // No processing required
  return KStatus::SUCCESS;
}

KStatus TsTable::Recover(kwdbContext_p ctx, const std::map<uint64_t, uint64_t>& applied_indexes) {
  // No processing required
  return KStatus::SUCCESS;
}

KStatus TsTable::GetAllLeaderEntityGroup(kwdbts::kwdbContext_p ctx,
                                         std::vector<std::shared_ptr<TsEntityGroup>>* leader_entity_groups) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  leader_entity_groups->clear();
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    // if (entity_group.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(entity_group.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (entity_group.second->hashRange().typ == EntityGroupType::LEADER) {
      leader_entity_groups->push_back(entity_group.second);
    // }
  }
  return s;
}

KStatus TsTable::DropEntityGroup(kwdbContext_p ctx, uint64_t range_group_id, bool sync) {
  LOG_INFO("DropEntityGroup: range_group_id %lu", range_group_id);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  auto it = entity_groups_.find(range_group_id);
  if (it == entity_groups_.end()) {
    LOG_ERROR("no hash range");
    RW_LATCH_UNLOCK(entity_groups_mtx_);
    return KStatus::FAIL;
  }
  auto entity_group = it->second;
  entity_groups_.erase(it);
  RW_LATCH_UNLOCK(entity_groups_mtx_);

  KStatus s = entity_group->Drop(ctx);
  if (s == KStatus::FAIL) {
    LOG_ERROR("entity group drop failed, range_group_id=%lu", range_group_id);
    RW_LATCH_X_LOCK(entity_groups_mtx_);
    Defer defer{[&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); }};
    entity_groups_[range_group_id] = entity_group;
    return KStatus::FAIL;
  }
  return s;
}

KStatus TsTable::DropAll(kwdbContext_p ctx, bool is_force) {
  LOG_INFO("TsTable::drop table %ld", table_id_);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  std::unordered_map<uint64_t, int8_t> range_groups;
  for (auto & entity_group : entity_groups_) {
    range_groups.insert({entity_group.second->HashRange().range_group_id, entity_group.second->HashRange().typ});
    s = entity_group.second->Drop(ctx, is_force);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange drop failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }
  entity_groups_.clear();
  // clear tbl_ranges, and re-initialize
  if (s != KStatus::SUCCESS) {
    delete entity_bt_manager_;
    entity_bt_manager_ = nullptr;
    Init(ctx, range_groups);
    return s;
  }

  entity_bt_manager_->RemoveAll();
  delete entity_bt_manager_;
  entity_bt_manager_ = nullptr;
  // 删除TsTable目录
  LOG_INFO("TsTable::remove table %ld files", table_id_);
  ::remove((db_path_ + tbl_sub_path_).c_str());
  LOG_INFO("TsTable::drop table %ld over", table_id_);
  return KStatus::SUCCESS;
}

KStatus TsTable::Compress(kwdbContext_p ctx, const KTimestamp& ts, uint32_t& compressed_num, ErrorInfo& err_info) {
  compressed_num = 0;
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    err_info.setError(KWENOOBJ, "table not created");
    return KStatus::FAIL;
  }

  if (ts != INT64_MAX) {
    if (!entity_bt_manager_->TrySetCompressStatus(true)) {
      // If other threads are currently undergoing the compression process,
      // scheduled compression will be skipped to avoid concurrency issues.
      LOG_INFO("table[%lu] is compressing, skip this compression", table_id_);
      return KStatus::SUCCESS;
    }
  } else {
    while (true) {
      // check client ctrl+C first
      if (ctx->relation_ctx != 0 && isCanceledCtx(ctx->relation_ctx)) {
        LOG_INFO("Interrupted, skip compression");
        err_info.setError(KWEOTHER, "interrupted");
        return KStatus::FAIL;
      }
      if (entity_bt_manager_->TrySetCompressStatus(true)) {
        // break the loop and perform compression
        break;
      }
      using std::chrono_literals::operator""s;
      std::this_thread::sleep_for(1s);
    }
  }
  KStatus s = KStatus::SUCCESS;
  std::vector<std::shared_ptr<TsEntityGroup>> compress_entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // get all entity groups
    for (auto& entity_group : entity_groups_) {
      compress_entity_groups.push_back(entity_group.second);
    }
  }

  timestamp64 precision_ts = convertSecondToPrecisionTS(ts, GetRootTableManager()->GetTsColDataType());
  LOG_DEBUG("Compressing table[%lu] , end ts is : %ld. ts: %ld", table_id_, precision_ts, ts);
  for (auto& entity_group : compress_entity_groups) {
    if (!entity_group) {
      continue;
    }
    s = entity_group->Compress(ctx, precision_ts, compressed_num, err_info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange compress failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }

  entity_bt_manager_->SetCompressStatus(false);
  return s;
}

KStatus TsTable::Vacuum(kwdbContext_p ctx, uint32_t ts_version, ErrorInfo& err_info) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    err_info.setError(KWENOOBJ, "table not created");
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  std::vector<std::shared_ptr<TsEntityGroup>> vacuum_entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // get all entity groups
    for (auto& [fst, snd] : entity_groups_) {
      vacuum_entity_groups.push_back(snd);
    }
  }

  for (auto& entity_group : vacuum_entity_groups) {
    if (!entity_group) {
      continue;
    }
    s = entity_group->Vacuum(ctx, ts_version, err_info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange vacuum failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }
  return s;
}

KStatus TsTable::TierMigrate() {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }

  if (!entity_bt_manager_->TrySetMigrateStatus()) {  // avoid migrate repeatedly in a short time
    LOG_INFO("table[%lu] is migrating, skip this schedule", table_id_);
    return KStatus::SUCCESS;
  }

  std::vector<std::shared_ptr<TsEntityGroup>> entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // get all entity groups
    for (auto& entity_group : entity_groups_) {
      entity_groups.push_back(entity_group.second);
    }
  }
  for (auto& entity_group : entity_groups) {
    if (!entity_group) {
      continue;
    }
    entity_group->TierMigrate();
  }
  entity_bt_manager_->ResetMigrateStatus();
  return KStatus::SUCCESS;
}

KStatus TsTable::Count(kwdbContext_p ctx, ErrorInfo& err_info) {
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    err_info.setError(KWENOOBJ, "table not created");
    return KStatus::FAIL;
  }
  KStatus s = KStatus::SUCCESS;
  std::vector<std::shared_ptr<TsEntityGroup>> count_entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    // get all entity groups
    for (auto& [fst, snd] : entity_groups_) {
      count_entity_groups.push_back(snd);
    }
  }

  for (auto& entity_group : count_entity_groups) {
    if (!entity_group) {
      continue;
    }
    s = entity_group->Count(ctx, err_info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsTableRange count failed : %s", tbl_sub_path_.c_str());
      break;
    }
  }
  return s;
}

KStatus TsTable::GetEntityIndex(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                std::vector<EntityResultIndex> &entity_store) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    std::vector<TagPartitionTable*> all_tag_partition_tables;
    auto tag_bt = p.second->GetSubEntityGroupNewTagbt();
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
          if (begin_hash <= tag_hash && tag_hash <= end_hash) {
            entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
          }
        } else {
          entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
        }
      }
      entity_tag_bt->stopRead();
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityGrpByPriKey(kwdbContext_p ctx, const TSSlice& primary_key, uint64_t* entity_grp_idp) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    auto* entity_tag_bt = p.second->GetSubEntityGroupNewTagbt();
    int entity_id;
    SubGroupID sub_grp_id;
    if (entity_tag_bt->hasPrimaryKey(primary_key.data, primary_key.len)) {
      *entity_grp_idp = p.first;
      return KStatus::SUCCESS;
    }
  }
  *entity_grp_idp = default_entitygroup_id_in_dist_v2;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetEntityIndexWithRowNum(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                          std::vector<std::pair<int, EntityResultIndex>> &entity_tag) {
  KStatus s;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto &p : entity_groups_) {
    std::vector<TagPartitionTable*> all_tag_partition_tables;
    auto tag_bt = p.second->GetSubEntityGroupNewTagbt();
    TableVersion cur_tbl_version = tag_bt->GetTagTableVersionManager()->GetCurrentTableVersion();
    tag_bt->GetTagPartitionTableManager()->GetAllPartitionTablesLessVersion(all_tag_partition_tables,
                                                                            cur_tbl_version);
    for (const auto& entity_tag_bt : all_tag_partition_tables) {
      entity_tag_bt->startRead();
      for (int rownum = 1; rownum <= entity_tag_bt->actual_size(); rownum++) {
        if (!entity_tag_bt->isValidRow(rownum)) {
          continue;
        }
        if (!EngineOptions::isSingleNode()) {
          std::vector<EntityResultIndex> entity_store;
          uint32_t tag_hash;
          entity_tag_bt->getHashpointByRowNum(rownum, &tag_hash);
          if (begin_hash <= tag_hash && tag_hash <= end_hash) {
            entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
            entity_store[0].ts_version = entity_tag_bt->metaData().m_ts_version;
            entity_tag.push_back(std::pair<int, EntityResultIndex>(rownum, entity_store[0]));
          }
        } else {
          std::vector<EntityResultIndex> entity_store;
          entity_tag_bt->getEntityIdByRownum(rownum, &entity_store);
          entity_store[0].ts_version = entity_tag_bt->metaData().m_ts_version;
          entity_tag.push_back(std::pair<int, EntityResultIndex>(rownum, entity_store[0]));
        }
      }
    entity_tag_bt->stopRead();
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetAvgTableRowSize(kwdbContext_p ctx, uint64_t* row_size) {
  // fixed tuple length of one row.
  size_t row_length = 0;
  std::vector<AttributeInfo> schemas;
  auto s = entity_bt_manager_->GetSchemaInfoExcludeDropped(&schemas);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  for (auto& col : schemas) {
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

KStatus TsTable::GetDataVolume(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, uint64_t* volume) {
  LOG_DEBUG("GetDataVolume begin. table[%lu] hash[%lu - %lu], time [%ld - %ld]",
            table_id_, begin_hash, end_hash, ts_span.begin, ts_span.end);
  uint64_t row_length = 0;
  KStatus s = GetAvgTableRowSize(ctx, &row_length);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetAvgTableRowSize failed. table [%lu] ", table_id_);
    return s;
  }
  // total row num that  satisfied ts_span and hash span.
  std::vector<EntityResultIndex> entity_store;
  s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }
  size_t total_rows = 0;
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      partitions = cur_sub_group->GetPartitionTables(ts_span, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%lu - %u]", entity_grp.first, sub_grp.first);
          return KStatus::FAIL;
      }
      std::deque<BlockItem*> block_item_queue;
      timestamp64 min_ts, max_ts;
      for (auto& partition : partitions) {
        partition->rdLock();  // control vacuum concurrency
        for (auto cur_entity_id : sub_grp.second) {
          block_item_queue.clear();
          partition->GetAllBlockItems(cur_entity_id, block_item_queue);
          for (auto& block_item : block_item_queue) {
            if (partition->GetBlockMinMaxTS(block_item, &min_ts, &max_ts) &&
                isTimestampWithinSpans({ts_span}, min_ts, max_ts)) {
              total_rows += block_item->publish_row_count - block_item->getDeletedCount();
            }
          }
        }
        partition->unLock();
        ReleaseTable(partition);
      }
    }
  }
  LOG_DEBUG("GetDataVolume end. table[%lu] average row size[%lu], row num [%lu], entities num [%lu]",
            table_id_, row_length, total_rows, entities.size());
  *volume = row_length * total_rows;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataVolumeHalfTS(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                const KwTsSpan& ts_span, timestamp64* half_ts) {
  LOG_DEBUG("GetDataVolumeHalfTS begin. table[%lu] hash[%lu - %lu], time [%ld - %ld]",
              table_id_, begin_hash, end_hash, ts_span.begin, ts_span.end);
  *half_ts = INVALID_TS;
  if (begin_hash != end_hash) {
    LOG_ERROR("can not calculate half timestmap with different hash [%lu - %lu]", begin_hash, end_hash);
    return KStatus::FAIL;
  }
  // total rows of matched entities.
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }

  std::vector<AttributeInfo> data_schema;
  s = GetDataSchemaIncludeDropped(ctx, &data_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get GetDataSchemaIncludeDropped.");
    return s;
  }
  // caclulate all partitions matched entities rows. partitions based on news configure.
  std::map<int64_t, size_t> partitions_rows;
  std::map<int64_t, timestamp64> partitions_mid_ts;
  size_t total_rows = 0;
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;
  auto ts_type = GetRootTableManager()->GetTsColDataType();
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      partitions = cur_sub_group->GetPartitionTables(ts_span, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%lu - %u]", entity_grp.first, sub_grp.first);
          return KStatus::FAIL;
      }
      std::deque<BlockItem*> block_item_queue;
      timestamp64 min_ts, max_ts, max_pt_time;
      for (auto& partition : partitions) {
        partition->rdLock();  // control vacuum concurrency
        uint64_t partition_time = cur_sub_group->PartitionTime(partition->minTimestamp(), max_pt_time);
        TsPartitonIteratorParams param{entity_grp.first, sub_grp.first, sub_grp.second, {ts_span}, {0}, {0}, data_schema};
        TsPartitionIterator pt_iter(partition, param);
        uint32_t partition_row_count = 0;
        while (true) {
          ResultSet res(1);
          uint32_t count = 0;
          if (KStatus::SUCCESS != pt_iter.Next(&res, &count, false, nullptr)) {
            LOG_ERROR("TsPartitonIterator next failed.");
            return KStatus::FAIL;
          }
          if (count == 0) {
            break;
          }
          partition_row_count += count;
        }
        if (partition_row_count > 0) {
          partitions_rows[partition_time] += partition_row_count;
          total_rows += partition_row_count;
          timestamp64 p_min_ts{INT64_MAX}, p_max_ts{INT64_MIN};
          for (auto cur_entity_id : sub_grp.second) {
            auto en_item = partition->getEntityItem(cur_entity_id);
            if (en_item->block_count == 0) {
              continue;
            }
            p_min_ts = std::max(en_item->min_ts, ts_span.begin);
            p_max_ts = std::min(en_item->max_ts, ts_span.end);
            LOG_DEBUG("p_min_ts:%ld, p_max_ts: %ld, partition[%ld-%ld], entity ts[%ld-%ld]", p_min_ts, p_max_ts,
                        partition->minTimestamp(), partition->maxTimestamp(), en_item->min_ts, en_item->max_ts);
            if (p_min_ts >= p_max_ts) {
              continue;
            }
            if (p_min_ts < convertSecondToPrecisionTS(partition->minTimestamp(), ts_type)
                || p_max_ts > convertSecondToPrecisionTS(partition->maxTimestamp(), ts_type)) {
              continue;
            }
            if (partitions_mid_ts.find(partition_time) != partitions_mid_ts.end()) {
              partitions_mid_ts[partition_time] = (partitions_mid_ts[partition_time] + (p_min_ts + p_max_ts) / 2) / 2;
            } else {
              partitions_mid_ts[partition_time] = (p_min_ts + p_max_ts) / 2;
            }
          }
        }
        partition->unLock();
        ReleaseTable(partition);
      }
    }
  }
  if (total_rows == 0) {
    LOG_ERROR("no row left, cannot split. table [%lu]", table_id_);
    return KStatus::FAIL;
  }
  // todo(liangbo01): temp code for debug. will delete later.
  auto it_tmp = partitions_rows.begin();
  for (; it_tmp != partitions_rows.end(); it_tmp++) {
    LOG_DEBUG("partition [%ld] has rows [%lu], midts:%ld.",
              it_tmp->first, it_tmp->second, partitions_mid_ts[it_tmp->first]);
  }
  // caclulte half ts, ts must multiple of partition_interval
  size_t scan_pt_rows = 0;
  int64_t find_min_ts = 0;
  for (auto &iter : partitions_rows) {
    find_min_ts = iter.first;
    if (scan_pt_rows + iter.second > total_rows / 2) {
      *half_ts = convertSecondToPrecisionTS(iter.first, ts_type);
      break;
    } else {
      scan_pt_rows += iter.second;
    }
  }
  // The distribution is severely uneven, using middle ts of partition.
  if (scan_pt_rows * 3 < total_rows || *half_ts < ts_span.begin) {
    *half_ts = partitions_mid_ts[find_min_ts];
  }
  if (!isTimestampInSpans({ts_span}, *half_ts, *half_ts)) {
    LOG_ERROR("GetDataVolumeHalfTS faild. range{%lu/%ld - %lu/%ld}, half ts [%ld]",
              begin_hash, ts_span.begin, end_hash, ts_span.end, *half_ts);
    return KStatus::FAIL;
  }
  LOG_DEBUG("GetDataVolumeHalfTS end. range{%lu/%ld - %lu/%ld}, total_rows:%lu, half ts [%ld]",
            begin_hash, ts_span.begin, end_hash, ts_span.end, total_rows, *half_ts);
  return KStatus::SUCCESS;
}

KStatus TsTable::GetRangeRowCount(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
                                  KwTsSpan ts_span, uint64_t* count) {
  *count = 0;
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> entities;
  for (auto& et : entity_store) {
    entities[et.entityGroupId][et.subGroupId].push_back(et.entityId);
  }
  std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
  TsSubEntityGroup* cur_sub_group = nullptr;
  ErrorInfo err_info;
  std::vector<TsTimePartition*> partitions;

  std::vector<AttributeInfo> data_schema;
  s = GetDataSchemaIncludeDropped(ctx, &data_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get GetDataSchemaIncludeDropped.");
    return s;
  }
  for (auto entity_grp : entities) {
    s = GetEntityGroup(ctx, entity_grp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("cannot get entitygroup [%lu]", entity_grp.first);
      return s;
    }
    for (auto sub_grp : entity_grp.second) {
      err_info.clear();
      cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, entity_grp.first);
        return KStatus::FAIL;
      }
      TsPartitonIteratorParams params{entity_grp.first, sub_grp.first, {},
                                      {ts_span}, {0}, {0}, data_schema};
      for (auto& item : sub_grp.second) {
        params.entity_ids.push_back(item);
      }
      auto cur_entity_iter_ = new TsSubGroupIterator(cur_sub_group, params);
      while (true) {
        ResultSet res(1);
        uint32_t count1;
        cur_entity_iter_->Next(&res, &count1, false, nullptr);
        if (count1 == 0) {
          break;
        }
        *count += count1;
      }
      delete cur_entity_iter_;
    }
  }
  LOG_DEBUG("GetRangeRowCount range{hash[%lu - %lu] ts[%ld - %ld]} row count: %lu.",
            begin_hash, end_hash, ts_span.begin, ts_span.end, *count);
  return KStatus::SUCCESS;
}

KStatus TsTable::DeleteTotalRange(kwdbContext_p ctx, uint64_t begin_hash, uint64_t end_hash,
               KwTsSpan ts_span, uint64_t mtr_id) {
  uint64_t row_num_bef = 0;
  uint64_t row_num_aft = 0;
#ifdef K_DEBUG
  GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_bef);
#endif
  // get matched entities.
  std::vector<EntityResultIndex> entity_store;
  KStatus s = GetEntityIndex(ctx, begin_hash, end_hash, entity_store);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot get entities from table [%lu] ", table_id_);
    return s;
  }
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<EntityID>>> group_ids;
  for (auto& entity : entity_store) {
    group_ids[entity.entityGroupId][entity.subGroupId].push_back(entity.entityId);
  }
  ErrorInfo err_info;
  uint64_t total_del_rows = 0;
  for (auto& egrp : group_ids) {
    std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
    s = GetEntityGroup(ctx, egrp.first, &cur_entity_group);
    if (s != KStatus::SUCCESS) {
      cur_entity_group.reset();
      LOG_ERROR("cannot get entitygroup [%lu]", egrp.first);
      return s;
    }
    for (auto& sub_grp : egrp.second) {
      err_info.clear();
      auto cur_sub_group = cur_entity_group->GetSubEntityGroupManager()->GetSubGroup(sub_grp.first, err_info, false);
      if (err_info.errcode < 0) {
        cur_sub_group = nullptr;
        LOG_ERROR("cannot get subgroup [%u] in entitygroup [%lu]", sub_grp.first, egrp.first);
        return KStatus::FAIL;
      }
      std::vector<TsTimePartition*> partitions;
      partitions = cur_sub_group->GetPartitionTables(ts_span, err_info);
      if (err_info.errcode < 0) {
          LOG_ERROR("cannot get partitions [%u]", sub_grp.first);
          return KStatus::FAIL;
      }
      for (auto& partition : partitions) {
        for (auto& entity_id : sub_grp.second) {
          TsHashLatch* delete_data_latch = entity_bt_manager_->GetDeleteDataLatch();
          delete_data_latch->Lock(entity_id);
          uint64_t del_rows = 0;
          int ret = partition->DeleteData(entity_id, 0, {ts_span}, nullptr, &del_rows, err_info);
          if (err_info.errcode < 0) {
            LOG_ERROR("partition DeleteData failed. egrp [%lu], partition [%ld]",
                        egrp.first, partition->minTimestamp());
            delete_data_latch->Unlock(entity_id);
            break;
          }
          total_del_rows += del_rows;
          delete_data_latch->Unlock(entity_id);
        }
        ReleaseTable(partition);
      }
      // check if need reset entity max ts, after data deleted.
      cur_sub_group->ResetEntityMaxTs(ts_span.end, sub_grp.second);
    }
  }
#ifdef K_DEBUG
  GetRangeRowCount(ctx, begin_hash, end_hash, ts_span, &row_num_aft);
  if (row_num_bef != total_del_rows) {
    LOG_ERROR("DeleteTotalRange failed. rows: %lu, deleted:%lu", row_num_bef, total_del_rows);
  }
#endif
  LOG_DEBUG("DeleteTotalRange range{hash[%lu - %lu] ts[%ld - %ld]} delete rows [%lu], del before:%lu, del after: %lu.",
            begin_hash, end_hash, ts_span.begin, ts_span.end,
            total_del_rows, row_num_bef, row_num_aft);
  // todo(liangbo01): check if entity data is all deleted. if so, we can delete entity tag.
  //  we cannot know deleted data is (1) delted by user. (2)range migrate. so we cannot drop tag info.
  // so we cannot mark entity deleted.
  return KStatus::SUCCESS;
}

KStatus TsTable::ConvertRowTypePayload(kwdbContext_p ctx,  TSSlice payload_row, TSSlice* payload) {
  uint32_t pl_version = Payload::GetTsVsersionFromPayload(&payload_row);
  MMapMetricsTable* root_table = entity_bt_manager_->GetRootTable(pl_version);
  if (root_table == nullptr) {
    LOG_ERROR("table[%lu] cannot found version[%u].", table_id_, pl_version);
    return KStatus::FAIL;
  }
  const std::vector<AttributeInfo>& data_schema = root_table->getSchemaInfoExcludeDropped();
  PayloadStTypeConvert pl(payload_row, data_schema);
  auto s = pl.build(entity_bt_manager_, payload);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("can not parse current row-based payload.");
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::GetNormalIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                                   std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                                   std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                                   TsIterator** iter, std::vector<timestamp64> ts_points,
                                   bool reverse, bool sorted) {
  KWDB_DURATION(StStatistics::Get().get_iterator);
  if (scan_cols.empty()) {
    // LOG_ERROR("TsTable::GetIterator Error : no column");
    // return KStatus::FAIL;
  }
  auto ts_table_iterator = new TsTableIterator();
  KStatus s;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};

  auto& actual_cols = entity_bt_manager_->GetIdxForValidCols(table_version);
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
  DATATYPE ts_col_type = GetRootTableManager()->GetTsColDataType();
  std::map<uint64_t, std::map<SubGroupID, std::vector<EntityID>>> group_ids;
  for (auto& entity : entity_ids) {
    group_ids[entity.entityGroupId][entity.subGroupId].push_back(entity.entityId);
  }
  std::shared_ptr<TsEntityGroup> entity_group;
  for (auto& group_iter : group_ids) {
    s = GetEntityGroup(ctx, group_iter.first, &entity_group);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("can not found entitygroup [%lu].", group_iter.first);
      return s;
    }
    for (auto& sub_group_iter : group_iter.second) {
      TsStorageIterator* ts_iter;
      s = entity_group->GetIterator(ctx, sub_group_iter.first, sub_group_iter.second, ts_spans, ts_col_type,
                                    scan_cols, ts_scan_cols, scan_agg_types, table_version,
                                    &ts_iter, entity_group, ts_points, reverse, sorted);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("cannot create iterator for entitygroup[%lu], subgroup[%u]", group_iter.first, sub_group_iter.first);
        return s;
      }
      ts_table_iterator->AddEntityIterator(ts_iter);
    }
  }
  LOG_DEBUG("TsTable::GetIterator success.agg: %lu, iter num: %lu",
              scan_agg_types.size(), ts_table_iterator->GetIterNumber());
  (*iter) = ts_table_iterator;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetOffsetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                                   vector<KwTsSpan>& ts_spans, std::vector<k_uint32> scan_cols, k_uint32 table_version,
                                   TsIterator** iter, k_uint32 offset, k_uint32 limit, bool reverse) {
  KWDB_DURATION(StStatistics::Get().get_iterator);
  auto actual_cols = entity_bt_manager_->GetIdxForValidCols(table_version);
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
  std::map<SubGroupID, std::vector<EntityID>> subgroup_ids;
  if (entity_ids.empty()) {
    k_uint32 max_subgroup_id = entity_groups_.begin()->second->GetSubGroupNum();
    for (k_uint32 subgroup_id = 1; subgroup_id <= max_subgroup_id; ++subgroup_id) {
      subgroup_ids[subgroup_id] = {};
    }
  } else {
    for (auto& entity : entity_ids) {
      subgroup_ids[entity.subGroupId].push_back(entity.entityId);
    }
  }

  DATATYPE ts_col_type = GetRootTableManager()->GetTsColDataType();
  KStatus s = entity_groups_.begin()->second->GetOffsetIterator(ctx, subgroup_ids, ts_spans, ts_col_type,
                                                                scan_cols, ts_scan_cols, table_version,
                                                                iter, entity_groups_.begin()->second,
                                                                offset, limit, reverse);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("cannot create offset iterator for entitygroup[%lu], subgroup[%u]");
    return s;
  }

  LOG_DEBUG("TsTable::GetOffsetIterator success.");
  return KStatus::SUCCESS;
}

KStatus TsTable::GetIterator(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                             std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                             std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version, TsIterator** iter,
                             std::vector<timestamp64> ts_points, bool reverse, bool sorted, k_uint32 offset,
                             k_uint32 limit) {
  if (offset != 0) {
    return GetOffsetIterator(ctx, entity_ids, ts_spans, scan_cols, table_version, iter, offset, limit, reverse);
  } else {
    return GetNormalIterator(ctx, entity_ids, ts_spans, scan_cols,
                             scan_agg_types, table_version, iter, ts_points, reverse, sorted);
  }
}

KStatus TsTable::GetUnorderedDataInfo(kwdbContext_p ctx, const KwTsSpan ts_span, UnorderedDataStats* stats) {
  for (auto& entity_group : entity_groups_) {
    UnorderedDataStats cur_stats;
    entity_group.second->GetUnorderedDataInfo(ctx, ts_span, &cur_stats);
    *stats += cur_stats;
  }
  return KStatus::SUCCESS;
}

// for multimodel processing, the entity ids have to be in the original order, hence,
// use the following way to get the iterator instead of ordering by entity group
KStatus TsTable::GetIteratorInOrder(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_ids,
                                    std::vector<KwTsSpan> ts_spans, std::vector<k_uint32> scan_cols,
                                    std::vector<Sumfunctype> scan_agg_types, k_uint32 table_version,
                                    TsIterator** iter, std::vector<timestamp64> ts_points,
                                    bool reverse, bool sorted) {
  KWDB_DURATION(StStatistics::Get().get_iterator);
  if (scan_cols.empty()) {
    // LOG_ERROR("TsTable::GetIterator Error : no column");
    // return KStatus::FAIL;
  }
  auto ts_table_iterator = new TsTableIterator();
  KStatus s;
  Defer defer{[&]() {
    if (s == FAIL) {
      delete ts_table_iterator;
      ts_table_iterator = nullptr;
      *iter = nullptr;
    }
  }};

  auto& actual_cols = entity_bt_manager_->GetIdxForValidCols(table_version);
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

  uint64_t entity_group_id = 0;
  uint32_t subgroup_id = 0;
  std::shared_ptr<TsEntityGroup> entity_group;
  std::vector<uint32_t> entities;
  DATATYPE ts_col_type = GetRootTableManager()->GetTsColDataType();
  for (auto& entity : entity_ids) {
    if (entity_group_id == 0 && subgroup_id == 0) {
      entity_group_id = entity.entityGroupId;
      subgroup_id = entity.subGroupId;
      s = GetEntityGroup(ctx, entity.entityGroupId, &entity_group);
      if (s == FAIL) return s;
    }
    if (entity.entityGroupId != entity_group_id || entity.subGroupId != subgroup_id) {
      TsStorageIterator* ts_iter;
      s = entity_group->GetIterator(ctx, subgroup_id, entities, ts_spans, ts_col_type,
                                    scan_cols, ts_scan_cols, scan_agg_types, table_version, &ts_iter, entity_group,
                                    ts_points, reverse, sorted);
      if (s == FAIL) return s;
      ts_table_iterator->AddEntityIterator(ts_iter);

      subgroup_id = entity.subGroupId;
      entities.clear();
    }
    if (entity.entityGroupId != entity_group_id) {
      entity_group_id = entity.entityGroupId;
      entity_group.reset();
      s = GetEntityGroup(ctx, entity.entityGroupId, &entity_group);
      if (s == FAIL) return s;
    }
    entities.emplace_back(entity.entityId);
  }
  if (!entities.empty()) {
    TsStorageIterator* ts_iter;
    s = entity_group->GetIterator(ctx, subgroup_id, entities, ts_spans, ts_col_type,
                                  scan_cols, ts_scan_cols, scan_agg_types, table_version, &ts_iter, entity_group,
                                  ts_points, reverse, sorted);
    if (s == FAIL) return s;
    ts_table_iterator->AddEntityIterator(ts_iter);
  }

  (*iter) = ts_table_iterator;
  return KStatus::SUCCESS;
}

KStatus TsTable::DeleteRangeEntities(kwdbContext_p ctx, const uint64_t& range_group_id, const HashIdSpan& hash_span,
                                     uint64_t* count, uint64_t mtr_id) {
  KStatus s = KStatus::FAIL;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  auto range_group = entity_groups_.find(range_group_id);
  if (range_group == entity_groups_.end()) {
    LOG_ERROR("hash range not found, range_group_id=%lu", range_group_id);
    return s;
  }
  s = range_group->second->DeleteRangeEntities(ctx, hash_span, count, mtr_id);
  return s;
}

KStatus TsTable::DeleteRangeData(kwdbContext_p ctx, uint64_t range_group_id, HashIdSpan& hash_span,
                                 const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsEntityGroup> table_range;
  auto s = GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetEntityGroup failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return s;
  }

  if (table_range) {
    s = table_range->DeleteRangeData(ctx, hash_span, 0, ts_spans, nullptr, count, mtr_id, false);
    if (s == KStatus::FAIL) {
      LOG_ERROR("DeleteRangeData failed, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
                table_id_, range_group_id, hash_span.begin, hash_span.end)
      return s;
    } else {
      LOG_INFO("DeleteRangeData succeed, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
               table_id_, range_group_id, hash_span.begin, hash_span.end);
      return KStatus::SUCCESS;
    }
  } else {
    LOG_ERROR("DeleteRangeData failed, range_group disappear, tableID:%lu, rangeGroupID: %lu, hashSpan[%lu,%lu]",
              table_id_, range_group_id, hash_span.begin, hash_span.end)
    return KStatus::FAIL;
  }
}

KStatus TsTable::DeleteData(kwdbContext_p ctx, uint64_t range_group_id, std::string& primary_tag,
                            const std::vector<KwTsSpan>& ts_spans, uint64_t* count, uint64_t mtr_id) {
  std::shared_ptr<TsEntityGroup> table_range;
  auto s = GetEntityGroup(ctx, range_group_id, &table_range);
  if (s == KStatus::FAIL) {
    LOG_ERROR("GetEntityGroup failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return s;
  }

  if (table_range) {
    s = table_range->DeleteData(ctx, primary_tag, 0, ts_spans, nullptr, count, mtr_id, false);
    if (s == KStatus::FAIL) {
      LOG_ERROR("DeleteData failed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
      return s;
    } else {
      LOG_INFO("DeleteData succeed, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id);
      return KStatus::SUCCESS;
    }
  } else {
    LOG_ERROR("DeleteData failed, range_group disappear, tableID:%lu, rangeGroupID: %lu", table_id_, range_group_id)
    return KStatus::FAIL;
  }
}

KStatus TsTable::DeleteExpiredData(kwdbContext_p ctx, int64_t end_ts) {
  KStatus s = KStatus::FAIL;
  RW_LATCH_X_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  // Traverse through all entity groups to delete expired data
  for (auto& entity_group : entity_groups_) {
    s = entity_group.second->DeleteExpiredData(ctx, end_ts);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("EntityGroup delete expired data failed.");
      return s;
    }
  }
  return s;
}

KStatus TsTable::GetEntityIdList(kwdbContext_p ctx, const std::vector<void*>& primary_tags,
                                 const std::vector<uint64_t/*index_id*/> &tags_index_id,
                                 const std::vector<void*> tags,
                                 TSTagOpType op_type,
                                 const std::vector<uint32_t>& scan_tags,
                                 std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, uint32_t* count,
                                 uint32_t table_version) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    // if (tbl_range.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(tbl_range.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (tbl_range.second->hashRange().typ != EntityGroupType::LEADER) {
    //   // not leader
    //   continue;
    // }
    tbl_range.second->GetEntityIdList(ctx, primary_tags, tags_index_id, tags, op_type, scan_tags, entity_id_list, res,
                                      count, table_version);
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
                                  const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count,
                                  uint32_t table_version) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    if (tbl_range.second->GetTagList(ctx, entity_id_list, scan_tags, res, count, table_version) != KStatus::SUCCESS) {
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::vector<uint32_t> hps,
                                TagIterator** iter, k_uint32 table_version) {
  std::vector<EntityGroupTagIterator*> eg_tag_iters;
  EntityGroupTagIterator* eg_tag_iter = nullptr;

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto tbl_range : entity_groups_) {
    if (!EngineOptions::isSingleNode()) {
      tbl_range.second->GetTagIterator(ctx, tbl_range.second, scan_tags, table_version, &eg_tag_iter, hps);
    } else {
      tbl_range.second->GetTagIterator(ctx, tbl_range.second, scan_tags, table_version, &eg_tag_iter);
    }

    if (!eg_tag_iter) {
      return KStatus::FAIL;
    }
    eg_tag_iters.emplace_back(std::move(eg_tag_iter));
    eg_tag_iter = nullptr;
  }

  TagIterator* tag_iter = new TagIterator(eg_tag_iters);
  if (KStatus::SUCCESS != tag_iter->Init()) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}

KStatus TsTable::GetMetaIterator(kwdbContext_p ctx, MetaIterator** iter, k_uint32 table_version) {
  std::vector<EntityGroupMetaIterator*> iters;
  EntityGroupMetaIterator* eg_meta_iter = nullptr;
  for (const auto tbl_range : entity_groups_) {
    // if (tbl_range.second->hashRange().typ == EntityGroupType::UNINITIALIZED) {
    //   string err_msg = "table[" + std::to_string(table_id_) +
    //                    "] range group[" + std::to_string(tbl_range.first) +
    //                    "] is uninitialized";
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_STATUS, err_msg.c_str());
    //   LOG_ERROR("%s", err_msg.c_str());
    //   return KStatus::FAIL;
    // }
    // if (tbl_range.second->hashRange().typ != EntityGroupType::LEADER) {
    //   // not leader
    //   continue;
    // }
    if (KStatus::SUCCESS != tbl_range.second->GetMetaIterator(ctx, &eg_meta_iter)) {
      continue;
    }
    iters.emplace_back(std::move(eg_meta_iter));
    eg_meta_iter = nullptr;
  }
  MetaIterator* meta_iter = new MetaIterator(iters);
  if (KStatus::SUCCESS != meta_iter->Init()) {
    delete meta_iter;
    meta_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = meta_iter;
  return KStatus::SUCCESS;
}

KStatus TsTable::TSxClean(kwdbContext_p ctx) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& tbl_range : entity_groups_) {
    KStatus s = tbl_range.second->TSxClean(ctx);
    if (s == FAIL) {
      LOG_ERROR("Failed to clean the entity group %lu of table %lu",
                tbl_range.second->HashRange().range_group_id, table_id_)
      return s;
    }
  }

  return SUCCESS;
}

KStatus TsTable::SyncTagTsVersion(uint32_t cur_version, uint32_t new_version) {
  for (auto& entity_group : entity_groups_) {
    if (entity_group.second->SyncTagVersion(cur_version, new_version) < 0) {
      return FAIL;
    }
  }
  return SUCCESS;
}

KStatus TsTable::AddTagSchemaVersion(const std::vector<TagInfo>& schema, uint32_t new_version,
                                     const std::vector<roachpb::NTagIndexInfo>& idx_info) {
  for (auto& entity_group : entity_groups_) {
    if (entity_group.second->AddTagSchemaVersion(schema, new_version, idx_info) < 0) {
      return FAIL;
    }
  }
  return SUCCESS;
}

KStatus TsTable::AlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                            uint32_t cur_version, uint32_t new_version, string& msg) {
  AttributeInfo attr_info;
  KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, *column, attr_info, false);
  if (s != KStatus::SUCCESS) {
    msg = "Unknown column/tag type";
    return s;
  }
  LOG_INFO("AlterTable begin. table_id: %lu alter_type: %hhu cur_version: %u new_version: %u is_general_tag: %d",
          table_id_, alter_type, cur_version, new_version, attr_info.isAttrType(COL_GENERAL_TAG));
  if (alter_type == AlterType::ALTER_COLUMN_TYPE) {
    getDataTypeSize(attr_info);  // update max_len
  }
  if (attr_info.isAttrType(COL_GENERAL_TAG)) {
    s = AlterTableTag(ctx, alter_type, attr_info, cur_version, new_version, msg);
  } else if (attr_info.isAttrType(COL_TS_DATA)) {
    s = AlterTableCol(ctx, alter_type, attr_info, cur_version, new_version, msg);
  }
  LOG_INFO("AlterTable end. table_id: %lu alter_type: %hhu ", table_id_, alter_type);
  return s;
}

KStatus TsTable::AlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                               uint32_t cur_version, uint32_t new_version, string& msg) {
  KStatus s;
  ErrorInfo err_info;
  TagInfo tag_schema = {attr_info.id, attr_info.type,
                        static_cast<uint32_t>(attr_info.length), 0,
                        static_cast<uint32_t>(attr_info.size), GENERAL_TAG};
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
     s = entity_group.second->AlterTableTag(ctx, alter_type, attr_info, cur_version, new_version, err_info);
     if (s != KStatus::SUCCESS) {
        LOG_ERROR("alter tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
        msg = err_info.errmsg;
        return s;
     }
  }
  s = entity_bt_manager_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    msg = "Update table version error";
    return s;
  }
  return SUCCESS;
}

KStatus TsTable::AlterTableCol(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                               uint32_t cur_version, uint32_t new_version, string& msg) {
  ErrorInfo err_info;
  auto col_idx = entity_bt_manager_->GetColumnIndex(attr_info);
  auto latest_version = entity_bt_manager_->GetCurrentTableVersion();
  vector<AttributeInfo> schema;
  KStatus s = entity_bt_manager_->GetSchemaInfoIncludeDropped(&schema, cur_version);
  if (s != KStatus::SUCCESS) {
    msg = "schema version " + to_string(cur_version) + " does not exists";
    return KStatus::FAIL;
  }
  switch (alter_type) {
    case ADD_COLUMN:
      if ((col_idx >= 0) && (latest_version == new_version)) {
        return KStatus::SUCCESS;
      }
      schema.emplace_back(attr_info);
      break;
    case DROP_COLUMN:
      if ((col_idx < 0) && (latest_version == new_version)) {
        return KStatus::SUCCESS;
      }
      schema[col_idx].setFlag(AINFO_DROPPED);
      break;
    case ALTER_COLUMN_TYPE: {
      if (col_idx < 0) {
        LOG_ERROR("alter column type failed: column (id %u) does not exists, table id = %lu", attr_info.id, table_id_);
        msg = "column does not exist";
        return KStatus::FAIL;
      } else if (latest_version == new_version) {
        return KStatus::SUCCESS;
      }
      auto& col_info = schema[col_idx];
      col_info.type = attr_info.type;
      col_info.size = attr_info.size;
      col_info.length = attr_info.length;
      col_info.max_len = attr_info.max_len;
      break;
    }
    default:
      return KStatus::FAIL;
  }
  s = entity_bt_manager_->CreateRootTable(schema, new_version, err_info, cur_version);
  if (s != KStatus::SUCCESS) {
    msg = err_info.errmsg;
    LOG_ERROR("add new version schema failed for alter table: table id = %lu, new_version = %u",  table_id_, new_version);
    return s;
  }
  s = SyncTagTsVersion(cur_version, new_version);

  return s;
}

KStatus TsTable::AddSchemaVersion(kwdbContext_p ctx, roachpb::CreateTsTable* meta, MMapMetricsTable ** version_schema) {
  *version_schema = nullptr;
  KStatus s;
  ErrorInfo err_info;
  auto latest_version = entity_bt_manager_->GetCurrentTableVersion();
  auto upper_version = meta->ts_table().ts_version();
  // skip in case current table has that schema version
  LOG_DEBUG("upper version[%u], latest schema version [%u]", upper_version, latest_version);
  if (upper_version <= latest_version) {
    auto root_table = entity_bt_manager_->GetRootTable(upper_version, true);
    if (root_table != nullptr) {
      *version_schema = root_table;
      return KStatus::SUCCESS;
    }
    LOG_DEBUG("upper version[%u] no exists, need create.", upper_version);
  }

  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    s = TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    // TagInfo struct add member,set value
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                             static_cast<uint32_t>(col_var.length), 0,
                                             static_cast<uint32_t>(col_var.size),
                                             col_var.isAttrType(COL_PRIMARY_TAG) ? PRIMARY_TAG : GENERAL_TAG,
                                             col_var.flag & AINFO_DROPPED}));
    } else {
      metric_schema.push_back(std::move(col_var));
    }
  }
  if (upper_version > latest_version) {
    s = entity_bt_manager_->CreateRootTable(metric_schema, upper_version, err_info);
  } else {
    s = entity_bt_manager_->AddRootTable(metric_schema, upper_version, err_info);
  }
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("add new version schema failed for alter table: table id = %lu, new_version = %u, err_msg: %s",
              table_id_, upper_version, err_info.errmsg.c_str());
    return s;
  }
  *version_schema = entity_bt_manager_->GetRootTable(upper_version, true);
#ifdef K_DEBUG
  for (const auto& it : tag_schema) {
    LOG_DEBUG("AddTagSchemaVersion table_id: %lu upper_version: %u tag_id: %u flag: %u",
       table_id_, upper_version, it.m_id, it.m_flag);
  }
#endif
  std::vector<roachpb::NTagIndexInfo> idx_info;
  for (int i = 0; i < meta->index_info_size(); i++) {
    idx_info.emplace_back(meta->index_info(i));
  }
  s = AddTagSchemaVersion(tag_schema, upper_version);
  return s;
}

KStatus TsTable::UndoCreateIndex(kwdbContext_p ctx, LogEntry* log) {
  KStatus s;
  auto index_log = reinterpret_cast<CreateIndexEntry*>(log);
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    s = entity_group.second->UndoCreateHashIndex(index_log->getIndexID(), index_log->getCurTsVersion(),
                                                 index_log->getNewTsVersion());
    if (s == KStatus::FAIL) {
      return s;
    }
  }
  if (entity_bt_manager_->RollBack(index_log->getCurTsVersion(), index_log->getNewTsVersion()) < 0) {
    LOG_ERROR("undo create index failed: rollback metric table[%lu] version failed, new version[%u]", table_id_,
              index_log->getNewTsVersion());
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::UndoDropIndex(kwdbContext_p ctx, LogEntry* log) {
  KStatus s;
  auto index_log = reinterpret_cast<DropIndexEntry*>(log);
  std::vector<uint32_t> tags;
  for (auto col_id : index_log->getColIDs()) {
    if (col_id < 0) {
      break;
    }
    tags.emplace_back(col_id);
  }
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    s = entity_group.second->UndoDropHashIndex(tags, index_log->getIndexID(), index_log->getCurTsVersion(),
                                               index_log->getNewTsVersion());
    if (s == KStatus::FAIL) {
      return s;
    }
  }
  if (entity_bt_manager_->RollBack(index_log->getCurTsVersion(), index_log->getNewTsVersion()) < 0) {
    LOG_ERROR("undo drop index failed: rollback metric table[%lu] version failed, new version[%u]", table_id_,
              index_log->getNewTsVersion());
    return KStatus::FAIL;
  }

  return KStatus::SUCCESS;
}


KStatus TsTable::UndoAlterTable(kwdbContext_p ctx, LogEntry* log) {
  auto alter_log = reinterpret_cast<DDLAlterEntry*>(log);
  auto alter_type = alter_log->getAlterType();
  switch (alter_type) {
    case AlterType::ADD_COLUMN:
    case AlterType::DROP_COLUMN:
    case AlterType::ALTER_COLUMN_TYPE: {
      auto cur_version = alter_log->getCurVersion();
      auto new_version = alter_log->getNewVersion();
      auto slice = alter_log->getColumnMeta();
      roachpb::KWDBKTSColumn column;
      bool res = column.ParseFromArray(slice.data, slice.len);
      if (!res) {
        LOG_ERROR("Failed to parse the WAL log")
        return KStatus::FAIL;
      }
      if (undoAlterTable(ctx, alter_type, &column, cur_version, new_version) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
    case ALTER_PARTITION_INTERVAL: {
      uint64_t interval = 0;
      memcpy(&interval, alter_log->getData(), sizeof(interval));
      if (AlterPartitionInterval(ctx, interval) == FAIL) {
        return KStatus::FAIL;
      }
      break;
    }
  }

  return KStatus::SUCCESS;
}

KStatus TsTable::undoAlterTable(kwdbContext_p ctx, AlterType alter_type, roachpb::KWDBKTSColumn* column,
                                uint32_t cur_version, uint32_t new_version) {
  AttributeInfo attr_info;
  ErrorInfo err_info;
  KStatus s = TsEntityGroup::GetColAttributeInfo(ctx, *column, attr_info, false);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  if (alter_type == AlterType::ALTER_COLUMN_TYPE) {
    getDataTypeSize(attr_info);  // update max_len
  }

  if (attr_info.isAttrType(COL_GENERAL_TAG)) {
    s = undoAlterTableTag(ctx, alter_type, attr_info, cur_version, new_version);
  } else if (attr_info.isAttrType(COL_TS_DATA)) {
    s = undoAlterTableCol(ctx, cur_version, new_version);
  }

  return s;
}

KStatus TsTable::undoAlterTableTag(kwdbContext_p ctx, AlterType alter_type, const AttributeInfo& attr_info,
                                   uint32_t cur_version, uint32_t new_version) {
  KStatus s;
  ErrorInfo err_info;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    switch (alter_type) {
      case AlterType::ADD_COLUMN:
      case AlterType::DROP_COLUMN:
      case AlterType::ALTER_COLUMN_TYPE:
        s = entity_group.second->UndoAlterTableTag(ctx, cur_version, new_version, err_info);
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("undo add tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
          return s;
        }
        break;
      default:
        return KStatus::FAIL;
    }
  }
  if (entity_bt_manager_->RollBack(cur_version, new_version) < 0) {
    LOG_ERROR("undo alter tag failed: rollback metric table[%lu] version failed, new version[%u]", table_id_, new_version);
    return KStatus::FAIL;
  }
  LOG_INFO("undoAlterTableTag end, table id= %lu cur_version: %u new_version: %u ", table_id_, cur_version, new_version);
  return KStatus::SUCCESS;
}

KStatus TsTable::undoAlterTableCol(kwdbContext_p ctx, uint32_t cur_version, uint32_t new_version) {
  auto s = entity_bt_manager_->RollBack(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  ErrorInfo err_info;
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (const auto& entity_group : entity_groups_) {
    s = entity_group.second->UndoAlterTableTag(ctx, cur_version, new_version, err_info);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("undo add tag failed, table id= %lu, range_group_id = %lu", table_id_, entity_group.first);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsTable::AlterPartitionInterval(kwdbContext_p ctx, uint64_t partition_interval) {
  entity_bt_manager_->SetPartitionInterval(partition_interval);
  return KStatus::SUCCESS;
}

uint64_t TsTable::GetPartitionInterval() {
  return entity_bt_manager_->GetPartitionInterval();
}

void TsTable::SetDropped() {
  is_dropped_.store(true);
  entity_bt_manager_->SetDropped();
}

bool TsTable::IsDropped() {
  return is_dropped_.load();
}

uint64_t beginOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;  // 24 hours * 60 minutes * 60 seconds * 1000 ms
  return timestamp - (timestamp % day_ms);
}

uint64_t endOfDay(uint64_t timestamp) {
  constexpr uint64_t day_ms = 86400000;
  return beginOfDay(timestamp) + day_ms;
}

KStatus TsTable::GetEntityNum(kwdbContext_p ctx, uint64_t* entity_num) {
  *entity_num = 0;
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }

  RW_LATCH_S_LOCK(entity_groups_mtx_);
  for (auto& it : entity_groups_) {
    *entity_num += it.second->GetTagCount();
  }
  RW_LATCH_UNLOCK(entity_groups_mtx_);
  return KStatus::SUCCESS;
}

KStatus TsTable::GetDataRowNum(kwdbContext_p ctx, const KwTsSpan& ts_span, uint64_t* row_num) {
  LOG_INFO("GetDataRowNum begin, table[%lu] time [%ld - %ld]", table_id_, ts_span.begin, ts_span.end);
  if (entity_bt_manager_ == nullptr) {
    LOG_ERROR("TsTable not created : %s", tbl_sub_path_.c_str());
    return KStatus::FAIL;
  }
  *row_num = 0;
  // get all entity group
  std::vector<std::shared_ptr<TsEntityGroup>> entity_groups;
  {
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    for (auto &it : entity_groups_) {
      it.second->RdDropLock();
      entity_groups.push_back(it.second);
    }
    RW_LATCH_UNLOCK(entity_groups_mtx_);
  }
  ErrorInfo err_info;
  timestamp64 block_min_ts = INT64_MAX;
  timestamp64 block_max_ts = INT64_MIN;
  // Traverse all entity groups
  for (int entity_group_idx = 0; entity_group_idx < entity_groups.size(); ++entity_group_idx) {
    auto& entity_group = entity_groups[entity_group_idx];
    Defer defer([&]{entity_group->DropUnlock();});
    auto subgroup_manager = entity_group->GetSubEntityGroupManager();
    // Traverse all subgroups
    for (SubGroupID sub_group_id = 1; sub_group_id <= subgroup_manager->GetMaxSubGroupId(); ++sub_group_id) {
      TsSubEntityGroup* subgroup = subgroup_manager->GetSubGroup(sub_group_id, err_info);
      if (!subgroup) {
        err_info.clear();
        continue;
      }
      auto partition_tables = subgroup->GetPartitionTables(ts_span, err_info);
      if (err_info.errcode < 0) {
        LOG_ERROR("GetPartitionTables failed, error_info: %s", err_info.errmsg.c_str());
        break;
      }
      // Traverse all partitions
      for (auto& partition : partition_tables) {
        usleep(10);
        partition->rdLock();  // control vacuum concurrency
        Defer defer_lock{[&]() { partition->unLock(); }};

        if (!partition || partition->getObjectStatus() != OBJ_READY) {
          err_info.setError(KWEOTHER, "partition table is nullptr or not ready");
          break;
        }
        std::shared_ptr<MMapSegmentTable> segment_table = nullptr;
        for (BLOCK_ID block_id = 1; block_id <= partition->GetMaxBlockID(); ++block_id) {
          BlockItem* block_item = partition->GetBlockItem(block_id);
          // Skip deleted entity
          if (partition->getEntityItem(block_item->entity_id)->is_deleted) {
            continue;
          }
          // Switch segment table
          if (!segment_table || block_id >= segment_table->segment_id() + segment_table->metaData()->block_num_of_segment) {
            segment_table = partition->getSegmentTable(block_item->block_id);
            if (!segment_table) {
              continue;
            }
          }
          // Calculate the number of rows
          timestamp64 min_ts, max_ts;
          TsTimePartition::GetBlkMinMaxTs(block_item, segment_table.get(), min_ts, max_ts);
          timestamp64 intersect_ts = intersectLength(ts_span.begin, ts_span.end, min_ts, max_ts + 1);
          uint32_t count = 1.0L * intersect_ts / (max_ts - min_ts + 1) * block_item->publish_row_count;
          if (intersect_ts > 0 && block_item->publish_row_count > 0 && count == 0) {
            // The estimated data volume is 0, but in cases where there is a very small amount of data,
            // it is considered as one piece of data by default
            count = 1;
          }
          *row_num += count;
          // Calculate the maximum and minimum time range of all blocks
          if (count != 0 && max_ts > block_max_ts) {
            block_max_ts = max_ts;
          }
          if (count != 0 && min_ts < block_min_ts) {
            block_min_ts = min_ts;
          }
        }
      }
      for (auto& partition : partition_tables) {
        if (partition) {
          ReleaseTable(partition);
        }
      }
      if (err_info.errcode < 0) {
        break;
      }
    }
    if (err_info.errcode < 0) {
      break;
    }
  }
  if (err_info.errcode < 0) {
    *row_num = 0;
    return KStatus::FAIL;
  }
  // If the queried data falls within the ts_stpan range (such as in a scenario where data has just been written),
  // it is necessary to estimate the daily data write volume
  if (block_min_ts != INT64_MAX && block_min_ts > ts_span.begin) {
    double ratio = 1.0L * (ts_span.end - block_min_ts) / (ts_span.end - ts_span.begin);
    *row_num = (*row_num) / ratio;
  }

  LOG_INFO("GetDataRowNum succeeded");
  return KStatus::SUCCESS;
}

uint32_t TsTable::GetCurrentTableVersion() {
  return entity_bt_manager_->GetCurrentTableVersion();
}

KStatus TsTable::CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                      const uint32_t cur_version, const uint32_t new_version,
                                      const std::vector<uint32_t/* tag column id*/>& tags) {
  LOG_INFO("CreateNormalTagIndex start, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    if (!entity_group.second->CreateNormalTagIndex(ctx, transaction_id, index_id, cur_version, new_version, tags)) {
      LOG_ERROR("Failed to create normal tag index, table id:%d, index id:%d.", this->table_id_, index_id);
      return FAIL;
    }
  }
  auto s = entity_bt_manager_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Update table version error");
    return s;
  }
  LOG_INFO("CreateNormalTagIndex success, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}

KStatus TsTable::createNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                      const uint32_t cur_version, const uint32_t new_version,
                                      const std::vector<uint32_t/* tag column id*/>& tags) {
    LOG_INFO("snapshot create Normal Tag Index start, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    RW_LATCH_S_LOCK(entity_groups_mtx_);
    Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
    for (auto& entity_group : entity_groups_) {
        if (!entity_group.second->createNormalTagIndex(ctx, transaction_id, index_id, cur_version, new_version, tags)) {
            LOG_ERROR("Failed to create normal tag index, table id:%d, index id:%d.", this->table_id_, index_id);
            return FAIL;
        }
    }
    LOG_INFO("CreateNormalTagIndex success, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
    return SUCCESS;
}

std::vector<uint32_t> TsTable::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    std::vector<uint32_t> ret = entity_group.second->GetNTagIndexInfo(ts_version, index_id);
    return ret;
  }
  return std::vector<uint32_t>();
}

std::vector<std::pair<uint32_t, std::vector<uint32_t>>> TsTable::GetAllNTagIndexs(uint32_t schema_version) {
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    auto ret = entity_group.second->GetAllNTagIndexs(schema_version);
    return ret;
  }
  return std::vector<std::pair<uint32_t, std::vector<uint32_t>>>{};
}

KStatus TsTable::DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,  const uint32_t cur_version,
                                    const uint32_t new_version, const uint64_t index_id) {
  LOG_INFO("DropNormalTagIndex start, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
             this->table_id_, index_id, cur_version, new_version)
  RW_LATCH_S_LOCK(entity_groups_mtx_);
  Defer defer([&]() { RW_LATCH_UNLOCK(entity_groups_mtx_); });
  for (auto& entity_group : entity_groups_) {
    if (!entity_group.second->DropNormalTagIndex(ctx, transaction_id, cur_version, new_version, index_id)) {
        LOG_ERROR("Failed to drop normal tag index, table id:%d, index id:%d.", this->table_id_, index_id);
        return FAIL;
    }
  }
  auto s = entity_bt_manager_->UpdateVersion(cur_version, new_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Update table version error");
    return s;
  }
  LOG_INFO("DropNormalTagIndex success, table id:%d, index id:%d, cur_version:%d, new_version:%d.",
           this->table_id_, index_id, cur_version, new_version)
  return SUCCESS;
}



KStatus TsTable::AlterNormalTagIndex(kwdbContext_p ctx, const uint64_t index_id, const uint64_t transaction_id,
                                     const uint32_t old_version, const uint32_t new_version,
                                     const vector<uint32_t> &new_index_schema) {
    return SUCCESS;
}

KStatus TsEntityGroup::CreateNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                            const uint32_t cur_version, const uint32_t new_version,
                                            const std::vector<uint32_t/* tag column id*/>& tags) {
  RW_LATCH_S_LOCK(drop_mutex_);
  ErrorInfo errorInfo;
  if (getTagTable(errorInfo) != KStatus::SUCCESS) {
      LOG_ERROR("The target TS table is not available.");
      return KStatus::FAIL;
  }
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); releaseTagTable(); }};
  errorInfo.errcode = new_tag_bt_->CreateHashIndex(O_CREAT, tags, index_id, cur_version, new_version, errorInfo);
  if (errorInfo.errcode < 0) {
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsEntityGroup::createNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id, const uint64_t index_id,
                                            const uint32_t cur_version, const uint32_t new_version,
                                            const std::vector<uint32_t/* tag column id*/>& tags) {
    RW_LATCH_S_LOCK(drop_mutex_);
    ErrorInfo errorInfo;
    if (getTagTable(errorInfo) != KStatus::SUCCESS) {
        LOG_ERROR("The target TS table is not available.");
        return KStatus::FAIL;
    }
    Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); releaseTagTable(); }};
    errorInfo.errcode = new_tag_bt_->createHashIndex(new_version, errorInfo, tags, index_id);
    if (errorInfo.errcode < 0) {
        return FAIL;
    }
    return SUCCESS;
}

std::vector<uint32_t> TsEntityGroup::GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  std::vector<uint32_t> ret{};
  ret = new_tag_bt_->GetNTagIndexInfo(ts_version, index_id);
  return ret;
}

std::vector<std::pair<uint32_t, std::vector<uint32_t>>> TsEntityGroup::GetAllNTagIndexs(uint32_t ts_version) {
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  std::vector<std::pair<uint32_t, std::vector<uint32_t>>> ret{};
  ret = new_tag_bt_->GetAllNTagIndexs(ts_version);
  return ret;
}

KStatus TsEntityGroup::DropNormalTagIndex(kwdbContext_p ctx, const uint64_t transaction_id,  const uint32_t cur_version,
                                          const uint32_t new_version, const uint64_t index_id) {
  LOG_INFO("DropNormalTagIndex index_id:%d, cur_version:%d, new_version:%d", index_id, cur_version, new_version)
  RW_LATCH_S_LOCK(drop_mutex_);
  Defer defer{[&]() { RW_LATCH_UNLOCK(drop_mutex_); }};
  ErrorInfo errorInfo;
  errorInfo.errcode = new_tag_bt_->DropHashIndex(index_id, cur_version, new_version, errorInfo);
  if (errorInfo.errcode < 0) {
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsEntityGroup::UndoCreateHashIndex(uint32_t index_id, uint32_t cur_ts_version, uint32_t new_ts_version) {
    // drop hash index
    ErrorInfo err_info;
    int res = new_tag_bt_->UndoCreateHashIndex(index_id, cur_ts_version, new_ts_version, err_info);
    if (res < 0) {
        return KStatus::FAIL;
    }
    return KStatus::SUCCESS;
}

KStatus TsEntityGroup::UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t cur_ts_version,
                                         uint32_t new_ts_version) {
    // drop and create hash index
    ErrorInfo err_info;
    int res = new_tag_bt_->UndoDropHashIndex(tags, index_id, cur_ts_version, new_ts_version, err_info);
    if (res < 0) {
        return KStatus::FAIL;
    }
    return KStatus::SUCCESS;
}

KStatus TsEntityGroup::getTagTable(ErrorInfo& err_info) {
  if (new_tag_bt_ == nullptr) {
    new_tag_bt_ = OpenTagTable(db_path_, tbl_sub_path_, table_id_, range_.range_group_id, err_info);
  }
  if (new_tag_bt_ == nullptr) {
    LOG_ERROR("open tag table error : %s", err_info.errmsg.c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

void TsEntityGroup::releaseTagTable() {
  return;
}

bool TsEntityGroup::payloadNextSlice(TsSubEntityGroup* sub_group, Payload& payload, timestamp64 last_p_time, int start_row,
                                     int32_t* end_row, timestamp64* cur_p_time) {
  // Inspect whether the initial row falls outside the data range
  if (start_row >= payload.GetRowCount()) {
    return false;
  }
  timestamp64 p_time = 0;

  // Search through the rows in the payload to locate the next partition time with a different timestamp
  for (int32_t row_id = start_row; row_id < payload.GetRowCount(); row_id++) {
    KTimestamp cur_ts_ms = payload.GetTimestamp(row_id);
    KTimestamp cur_ts = convertTsToPTime(cur_ts_ms, root_bt_manager_->GetTsColDataType());

    timestamp64 max_ts;

    // Calculate the partition time for the current row and compare it with the previous partition time.
    // If there is a difference, it indicates the need to switch partitions.
    // First, return the information of the current partition to the upper layer for writing,
    // and then continue to search for the next partition.
    p_time = sub_group->PartitionTime(cur_ts, max_ts);
    if (p_time != last_p_time) {
      *cur_p_time = p_time;
      *end_row = row_id;
      return true;  // Locate the next valid partition time
    }
  }

  // If no partition time change is found after traversing all rows, set the last row as the end row
  // and return the current timestamp
  *end_row = payload.GetRowCount();
  *cur_p_time = p_time;
  return true;
}

bool TsEntityGroup::findPartitionPayload(TsSubEntityGroup* sub_group, Payload& payload,
                                         std::multimap<timestamp64, PartitionPayload>* partition_map) {
  // Inspect whether the initial row falls outside the data range
  int start_row = payload.GetStartRowId();
  KTimestamp first_ts_ms = payload.GetTimestamp(payload.GetStartRowId());
  KTimestamp first_ts = convertTsToPTime(first_ts_ms, root_bt_manager_->GetTsColDataType());
  timestamp64 first_max_ts;

  timestamp64 last_p_time = sub_group->PartitionTime(first_ts, first_max_ts);

  if (start_row >= payload.GetRowCount()) {
    return false;
  }
  timestamp64 p_time = 0;
  // Search through the rows in the payload to locate the next partition time with a different timestamp
  for (int32_t row_id = payload.GetStartRowId(); row_id < payload.GetRowCount(); row_id++) {
    KTimestamp cur_ts_ms = payload.GetTimestamp(row_id);
    KTimestamp cur_ts = convertTsToPTime(cur_ts_ms, root_bt_manager_->GetTsColDataType());

    timestamp64 max_ts;
    // Calculate the partition time for the current row and compare it with the previous partition time.
    // If there is a difference, it indicates the need to switch partitions.
    // First, return the information of the current partition to the upper layer for writing,
    // and then continue to search for the next partition.
    p_time = sub_group->PartitionTime(cur_ts, max_ts);
    if (p_time != last_p_time) {
      partition_map->insert({p_time, {start_row, row_id}});
      start_row = row_id;
    }
  }
  // If no partition time change is found after traversing all rows, set the last row as the end row and
  // return the current timestamp

  auto end_row = payload.GetRowCount();
  partition_map->insert({p_time, {start_row, end_row}});
  return true;
}

KStatus TsEntityGroup::FlushBuffer(kwdbContext_p ctx) {
  return SUCCESS;
}

KStatus TsEntityGroup::CreateCheckpoint(kwdbContext_p ctx) {
  return SUCCESS;
}

}  //  namespace kwdbts
