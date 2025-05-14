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

#include <cstdio>
#include <algorithm>
#include <cstring>
#include <atomic>
#include <sys/mman.h>
#include <stdexcept>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
#else
  #include <filesystem>
#endif
#include <st_config.h>

#include "cm_func.h"
#include "dirent.h"
#include "ts_time_partition.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "engine.h"
#include "utils/compress_utils.h"
#include "lg_api.h"
#include "perf_stat.h"
#include "st_tier_manager.h"

int64_t g_vacuum_interval = 3600;

void markDeleted(char* delete_flags, size_t row_index) {
  size_t byte = (row_index - 1) >> 3;
  size_t bit = (row_index - 1) & 7;
  delete_flags[byte] |= (1 << bit);
}

inline bool ReachMetaMaxBlock(BLOCK_ID cur_block_id) {
  return cur_block_id >= INT32_MAX;
}

impl_latch_virtual_func(TsTimePartition, vacuum_query_lock_)

TsTimePartition::~TsTimePartition() {
  meta_manager_.release();
  releaseSegments();
  if (vacuum_query_lock_) {
    delete vacuum_query_lock_;
    vacuum_query_lock_ = nullptr;
  }
  if (vacuum_insert_lock_) {
    delete vacuum_insert_lock_;
    vacuum_insert_lock_ = nullptr;
  }
  if (vacuum_delete_lock_) {
    delete vacuum_delete_lock_;
    vacuum_delete_lock_ = nullptr;
  }
  if (active_segment_lock_) {
    delete active_segment_lock_;
    active_segment_lock_ = nullptr;
  }
  delete m_ref_cnt_mtx_;
  delete m_ref_cnt_cv_;
  m_ref_cnt_cv_ = nullptr;
  m_ref_cnt_mtx_ = nullptr;
}

int TsTimePartition::open(const string& path, const std::string& db_path, const string& tbl_sub_path,
                          int flags, ErrorInfo& err_info) {
  assert(root_table_manager_ != nullptr);
  file_path_ = getTsFilePath(path);
  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  name_ = getTsObjectName(file_path_);

  // load EntityMeta
  if (openBlockMeta(flags, err_info) < 0) {
    return err_info.errcode;
  }

  if (DeleteFlag()) {
    err_info.setError(KWEDROPPEDOBJ, "TsTimePartition[" + tbl_sub_path_ +  "] is deleted");
    return err_info.errcode;
  }

  loadSegments(err_info);

  if (!(flags & O_CREAT) && !isInitialized()) {
    if (meta_manager_.maxTimestamp() > meta_manager_.minTimestamp()) {
      // historical version compatibility
      isInitialized() = true;
    } else {
      err_info.setError(KWEUNINIT, "TsTimePartition[" + tbl_sub_path_ + "] is uninitialized");
      return err_info.errcode;
    }
  }

  setObjectReady();

  return err_info.errcode;
}

int TsTimePartition::loadSegment(BLOCK_ID segment_id, ErrorInfo& err_info) {
  std::shared_ptr<MMapSegmentTable> segment_tbl;
  MMapSegmentTable *tbl = new MMapSegmentTable();
  string file_path = name_ + ".bt";
  if (tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_tbl_sub_path(segment_id),
                MMAP_OPEN_NORECURSIVE, true, err_info) < 0) {
    delete tbl;
    err_info.errmsg = "open segment table failed. " + segment_tbl_sub_path(segment_id);
    err_info.errcode = KWENFILE;
    return err_info.errcode;
  }
  segment_tbl.reset(tbl);
  data_segments_.Insert(segment_id, segment_tbl);
  return 0;
}

std::shared_ptr<MMapSegmentTable> TsTimePartition::reloadSegment(std::shared_ptr<MMapSegmentTable> old_segment,
                                                                 bool lazy_open, ErrorInfo& err_info) {
  std::shared_ptr<MMapSegmentTable> segment_tbl;
  MMapSegmentTable *tbl = new MMapSegmentTable();
  string file_path = name_ + ".bt";
  BLOCK_ID segment_id = old_segment->segment_id();
  if (tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_tbl_sub_path(segment_id),
                MMAP_OPEN_NORECURSIVE, lazy_open, err_info) < 0) {
    delete tbl;
    err_info.errmsg = "open segment table failed. " + segment_tbl_sub_path(segment_id);
    err_info.errcode = KWENFILE;
    old_segment->mutexUnlock();
    return segment_tbl;
  }
  segment_tbl.reset(tbl);
  data_segments_.Insert(segment_id, segment_tbl);
  old_segment->setNotLatestOpened();
  return segment_tbl;
}

int TsTimePartition::loadSegments(ErrorInfo& err_info) {
  releaseSegments();
  string real_path = db_path_ + tbl_sub_path_;
  set<BLOCK_ID> segment_ids;
  BLOCK_ID last_segment_id = 0;
  // Scan and load existing segments
  {
    DIR* dir_ptr = opendir(real_path.c_str());
    if (dir_ptr) {
      struct dirent* entry;
      while ((entry = readdir(dir_ptr)) != nullptr) {
        BLOCK_ID segment_id = 0;
        std::string full_path = real_path + entry->d_name;
        struct stat file_stat{};
        if (stat(full_path.c_str(), &file_stat) != 0) {
          LOG_WARN("stat[%s] failed", full_path.c_str());
          continue;
        }
        if (S_ISREG(file_stat.st_mode)) {
          int dn_len = strlen(entry->d_name);
          dn_len = dn_len - 5;
          if (strcmp(entry->d_name + dn_len, ".sqfs") == 0) {
            string part_name = string(entry->d_name, dn_len);
            segment_id = stoi(part_name);
          }
        } else if (S_ISDIR(file_stat.st_mode)) {
          if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0
              || entry->d_name[0] == '_') {  // _log directory  _tmp directory
            continue;
          }
          segment_id = std::stoi(entry->d_name);
        }
        if (segment_id > 0 && segment_ids.count(segment_id) == 0) {
          int err_code = loadSegment(segment_id, err_info);
          if (err_code < 0) {
            break;
          }
          segment_ids.insert(segment_id);
          if (segment_id > last_segment_id) {
            last_segment_id = segment_id;
          }
        }
      }
      closedir(dir_ptr);
    }
  }

  if (err_info.errcode < 0) {
    releaseSegments();
    return err_info.errcode;
  }

  std::shared_ptr<MMapSegmentTable> last_sta_tbl = nullptr;
  if (!segment_ids.empty()) {
    std::shared_ptr<MMapSegmentTable> segment_table = getSegmentTable(last_segment_id, true);
    if (segment_table && !segment_table->sqfsIsExists()
        && segment_table->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), last_segment_id, name_ + ".bt",
                               db_path_, tbl_sub_path_ + std::to_string(last_segment_id) + "/", MMAP_OPEN_NORECURSIVE,
                               false, err_info) >= 0
        && segment_table->getSegmentStatus() == ActiveSegment) {
      last_sta_tbl = segment_table;
    }
  }

  if (last_sta_tbl != nullptr) {
    active_segment_ = last_sta_tbl;
  }
  return err_info.errcode;
}

int TsTimePartition::init(const vector<AttributeInfo>& schema, int encoding, bool init_data, ErrorInfo& err_info) {
  return err_info.errcode;
}

MMapSegmentTable* TsTimePartition::createSegmentTable(BLOCK_ID segment_id,  uint32_t table_version, ErrorInfo& err_info,
  uint32_t max_rows_per_block, uint32_t max_blocks_per_segment) {
  // Check if the metadata manager has a valid first meta entry.
  if (meta_manager_.GetFirstMeta() == nullptr) {
    err_info.setError(KWEINVALPATH, db_path_ + tbl_sub_path_ + " is not data path.");
    return nullptr;
  }

  if (!IsDiskSpaceEnough(db_path_)) {
    err_info.setError(KWEFREESPCLIMIT);
    return nullptr;
  }

  string segment_sand = segment_tbl_sub_path(segment_id);
  string dir_path = db_path_ + segment_sand;
  // Attempt to access the directory, create it if it doesn't exist.
  if (access(dir_path.c_str(), 0)) {
    if (!MakeDirectory(dir_path, err_info)) {
      return nullptr;
    }
  }
  MMapSegmentTable* segment_tbl = new MMapSegmentTable();

  // Define a cleanup action to handle errors and resource cleanup.
  Defer defer{[&]() {
    if (err_info.errcode < 0) {
      if (err_info.errcode != KWEEXIST) {
        segment_tbl->remove();
      }
      delete segment_tbl;
    }
  }};
  // Prepare file path and attempt to open the segment table with exclusive creation.
  string file_path = name_ + ".bt";
  if (segment_tbl->open(&meta_manager_, segment_id, file_path, db_path_, segment_sand, MMAP_CREAT_EXCL, true, err_info) < 0) {
    return nullptr;
  }

  vector<string> key = {};
  string key_order = "";
  int encoding = ENTITY_TABLE | NO_DEFAULT_TABLE;
  {
    std::vector<AttributeInfo> schema;
    root_table_manager_->GetSchemaInfoIncludeDropped(&schema, table_version);
    int ret = segment_tbl->create(&meta_manager_, schema, table_version, encoding, err_info, max_rows_per_block, max_blocks_per_segment);
    if (ret < 0) {
      return nullptr;
    }
  }
  // reserve files to has enough space storing data
  err_info.errcode = segment_tbl->reserve(segment_tbl->getReservedRows());
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode, segment_sand);
    return nullptr;
  }
  segment_tbl->minTimestamp() = INVALID_TS;
  segment_tbl->maxTimestamp() = INVALID_TS;
  return segment_tbl;
}

int TsTimePartition::openBlockMeta(const int flags, ErrorInfo& err_info) {
  string meta_name = name_ + ".meta";
  // if create segment , we need create meta.0 file first.
  if (!(flags & O_CREAT)) {
    struct stat buff;
    string meta_path = db_path_ + tbl_sub_path_ + meta_name + ".0";
    if (stat(meta_path.c_str(), &buff) != 0) {
      err_info.setError(KWENOOBJ, meta_path);
      return err_info.errcode;
    }
  }

  int ret = meta_manager_.Open(meta_name, db_path_, tbl_sub_path_, true);
  if (ret < 0) {
    err_info.setError(ret, tbl_sub_path_ + meta_name);
    return err_info.errcode;
  }
  return 0;
}

int TsTimePartition::remove(bool exclude_segment) {
  int error_code = 0;
  if (!exclude_segment) {
    data_segments_.Traversal([&](BLOCK_ID id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
      error_code = tbl->remove();
      if (error_code < 0) {
        LOG_ERROR("remove segment[%s] failed!", tbl->GetPath().c_str());
        return false;
      }
      LOG_INFO("remove segment[%s] success!", tbl->GetPath().c_str());
      return true;
    });
  }
  releaseSegments();
  error_code = meta_manager_.remove();
  return error_code;
}

void TsTimePartition::sync(int flags) {
  meta_manager_.sync(flags);
  MUTEX_LOCK(active_segment_lock_);
  data_segments_.Traversal([flags](BLOCK_ID id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->getObjectStatus() == OBJ_READY) {
      tbl->sync(flags);
    }
    return true;
  });
  MUTEX_UNLOCK(active_segment_lock_);
}

bool TsTimePartition::JoinOtherPartitionTable(TsTimePartition* other) {
  MUTEX_LOCK(active_segment_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};
  // mark all segments cannot write. when joining, we cannot write rows.
  if (active_segment_ != nullptr) {
    active_segment_->setSegmentStatus(InActiveSegment);
    active_segment_ = nullptr;
  }
  data_segments_.Traversal([&](BLOCK_ID s_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->sqfsIsExists()) {
      return true;
    }
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                     db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                     false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }
    if (tbl->getSegmentStatus() == ActiveSegment) {
      tbl->setSegmentStatus(InActiveSegment);
    }
    return true;
  });
  auto other_p_header = other->meta_manager_.getEntityHeader();
  auto this_p_header = this->meta_manager_.getEntityHeader();

  std::vector<BLOCK_ID> other_segment_ids;
  other->data_segments_.GetAllKey(&other_segment_ids);
  std::sort(other_segment_ids.begin(), other_segment_ids.end());

  BLOCK_ID current_file_id = this_p_header->cur_block_id + 1;
  for (size_t i = 0; i < other_segment_ids.size(); i++) {
    std::string other_segment_path = other->GetPath() + intToString(other_segment_ids[i]);
    std::string  new_segment_path = this->GetPath() + intToString(this_p_header->cur_block_id + other_segment_ids[i]);
    std::string cmd = "mv " + other_segment_path + " " + new_segment_path;
    ErrorInfo err_info;
    if (!System(cmd, true, err_info)) {
      LOG_ERROR("exec [%s] failed.", cmd.c_str());
      return false;
    }
    auto err_code = loadSegment(this_p_header->cur_block_id + other_segment_ids[i], err_info);
    if (err_code < 0) {
      LOG_ERROR("loadSegment file failed. msg: %s.", err_info.errmsg.c_str());
      return false;
    }
    current_file_id = this_p_header->cur_block_id + other_segment_ids[i];
  }
  BlockItem *new_blk;
  for (size_t i = 1; i <= other_p_header->cur_block_id; i++) {
    auto blk_item = other->GetBlockItem(i);
    this->meta_manager_.AddBlockItem(blk_item->entity_id, &new_blk);
    new_blk->CopyMetricMetaInfo(*blk_item);
    this->meta_manager_.UpdateEntityItem(blk_item->entity_id, new_blk);
  }

  this_p_header->cur_datafile_id = current_file_id;
  return true;
}

size_t TsTimePartition::size(uint32_t entity_id) const {
  return meta_manager_.getEntityItem(entity_id)->row_written;
}

ostream& TsTimePartition::printRecord(uint32_t entity_id, std::ostream& os, size_t row) {
  os << "[ not support ]\n";
  return os;
}

int TsTimePartition::ProcessDuplicateData(kwdbts::Payload* payload, size_t start_in_payload, size_t count,
                                          const BlockSpan span, DedupInfo& dedup_info,
                                          DedupResult* dedup_result, ErrorInfo& err_info, int* deleted_rows) {
  int err_code = 0;
  if (payload->dedup_rule_ == DedupRule::REJECT || payload->dedup_rule_ == DedupRule::DISCARD) {
    for (size_t i = 0; i < span.row_num; i++) {
      // If it is REJECT and there is already duplicate data in the current partition table,
      // insertion is prohibited: mark the duplicate data that has been written to the current payload as deleted and return an error message.
      // If DISCARD is specified, the existing duplicate data in the current payload will be marked for deletion without generating an error message.
      auto cur_ts = KTimestamp(payload->GetColumnAddr(start_in_payload + count + i, 0));
      if (dedup_info.table_real_rows.find(cur_ts) != dedup_info.table_real_rows.end()) {
        if (payload->dedup_rule_ == DedupRule::REJECT) {
          err_code = KWEDUPREJECT;
          err_info.errcode = err_code;
          err_info.errmsg = "dedup_rule_ is reject, and payload has deduplicate ts.";
          return err_code;
        }
        SetDeleted(span.block_item->entity_id);
        span.block_item->setDeleted(span.start_row + i + 1);
        *deleted_rows += 1;
        if (dedup_result->discard_bitmap.len != 0) {
          setRowDeleted(dedup_result->discard_bitmap.data, i + 1);
        }
        dedup_result->dedup_rows += 1;
      }
    }
  }
  // The duplicate data found will be saved in dedup_info.payload_rows,
  // and the rows that need to be deleted during import will be saved in dedup_result.discard_bitmap.
  // need deleted rows in payload, can delete here.
  for (auto kv: dedup_info.payload_rows) {
    if (kv.second.size() > 1) {
      // If the dedup rule is discard, the first record will be retained, and the rest of the date will be deleted,
      // and if it is other mode, the last record will be retained.
      int offset = payload->dedup_rule_ == DedupRule::DISCARD ? 1 : 0;
      for (int i = kv.second.size() - 2 + offset; i >= 0 + offset; i--) {
        if (kv.second[i] >= start_in_payload + count && kv.second[i] < start_in_payload + count + span.row_num) {
          SetDeleted(span.block_item->entity_id);
          span.block_item->setDeleted(span.start_row + (kv.second[i] - start_in_payload - count) + 1);
          *deleted_rows += 1;
          if (!(payload->dedup_rule_ == DedupRule::REJECT || payload->dedup_rule_ == DedupRule::DISCARD)
              || dedup_info.table_real_rows.find(kv.first) == dedup_info.table_real_rows.end()) {
            // only count when this ts is not processed before.
            if (dedup_result->discard_bitmap.len != 0) {
              setRowDeleted(dedup_result->discard_bitmap.data, kv.second[i] + 1);
            }
            dedup_result->dedup_rows += 1;
          }
        }
      }
    }
  }
  return err_code;
}


int64_t TsTimePartition::push_back_payload(kwdbts::kwdbContext_p ctx, uint32_t entity_id, kwdbts::Payload* payload,
                                           size_t start_in_payload, size_t num,
                                           std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
                                           ErrorInfo& err_info, uint32_t* inc_unordered_cnt, DedupResult* dedup_result) {
  KWDB_DURATION(StStatistics::Get().push_payload);
  int err_code = 0;
  err_info.errcode = 0;
  alloc_spans->clear();

  // 1.Calculate the minimum and maximum timestamps for the payload to prepare for subsequent deduplication.
  // If the current payload's timestamp range does not overlap with the entity's, there is no need to deduplicate.
  KTimestamp pl_min = INT64_MAX, pl_max = INT64_MIN;
  for (size_t i = start_in_payload; i < start_in_payload + num; i++) {
    KTimestamp cur_ts = KTimestamp(payload->GetColumnAddr(i, 0));
    if (pl_min > cur_ts) {
      pl_min = cur_ts;
    }
    if (pl_max < cur_ts) {
      pl_max = cur_ts;
    }
  }

  size_t count = 0;
  kwdbts::DedupInfo dedup_info;
  int deleted_rows = 0;
  {
    MUTEX_LOCK(vacuum_insert_lock_);
    CountUpdateTime();
    // 2.Allocate data space for writing to the corresponding entity and save the allocation result to the passed in spans array.
    // If an error occurs during the allocation process, revert the space allocated before and return an error code.
    // If the current segment is full, switch to a new segment.
    err_code = AllocateAllDataSpace(entity_id, num, alloc_spans, payload->GetTsVersion());
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "AllocateSpace failed : " + string(ErrorInfo::errorCodeString(err_code));
      MUTEX_UNLOCK(vacuum_insert_lock_);
      return err_code;
    }
    EntityItem* entity_item = getEntityItem(entity_id);
    // The need_scan_table is used to identify whether there is duplicate data that needs to be scanned for partitions,
    // If the timestamp range of the current payload does not fall within the timestamp range of the current entity,
    // there is no need to scan the entire partition.
    if (pl_min > entity_item->max_ts || entity_item->max_ts == INVALID_TS
        || pl_max < entity_item->min_ts) {
      dedup_info.need_scan_table = false;
    } else {
      dedup_info.need_scan_table = true;
    }
    // During writing, a determination is made as to whether the current payload contains disordered data.
    // If present, entity_item is marked as being disordered.
    // During subsequent queries, a judgment will be made. If the result set is unordered,
    // a secondary HASH aggregation based on AGG SCAN is required.
    if (payload->IsDisordered(start_in_payload, num) ||
        (entity_item->max_ts != INVALID_TS && pl_min < entity_item->max_ts)) {
      entity_item->is_disordered = true;
      // As long as there is data disorder in this partition, it needs to be reorganized
      SetDisordered();
    }
    // Update the maximum and minimum timestamps of entity_items to prepare for subsequent payload deduplication.
    if (pl_max > entity_item->max_ts || entity_item->max_ts == INVALID_TS) {
      entity_item->max_ts = pl_max;
    }
    if (pl_min < entity_item->min_ts || entity_item->min_ts == INVALID_TS) {
      entity_item->min_ts = pl_min;
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }

  int dedup_rows_orgin = dedup_result->dedup_rows;

  // before return, need publish inserted data and update meta datas.
  Defer defer{[&]() {
    if (err_code < 0) {
      // Note the number of rows that failed during import, as it is only used during the import process
      dedup_result->dedup_rows = dedup_rows_orgin + payload->GetRowCount();
    }
    std::vector<size_t> full_block_idx;
    MUTEX_LOCK(vacuum_insert_lock_);
    CountUpdateTime();
    for (size_t i = 0; i < alloc_spans->size(); i++) {
      (*alloc_spans)[i].block_item->publish_row_count += (*alloc_spans)[i].row_num;
      if ((*alloc_spans)[i].block_item->publish_row_count >= (*alloc_spans)[i].block_item->max_rows_in_block) {
        full_block_idx.push_back(i);
      }
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
    for (size_t i = 0; i < full_block_idx.size(); i++) {
      std::shared_ptr<MMapSegmentTable> tbl = getSegmentTable((*alloc_spans)[full_block_idx[i]].block_item->block_id);
      if (tbl == nullptr) {
        LOG_ERROR("getSegmentTable failed, block_id: %u", (*alloc_spans)[full_block_idx[i]].block_item->block_id);
        return;
      }
      const BlockSpan& span = (*alloc_spans)[full_block_idx[i]];
      tbl->updateAggregateResult(span, span.block_item->getDeletedCount() > 0);
    }
  }};

  // 3.Deduplication is performed and the payload is updated according to different deduplication rules (KEEP, REJECT, MERGE).
  // Inspect the payload for any duplicate timestamps and apply various treatments, such as checking for duplicate rows and merging them.
  err_code = updatePayloadUsingDedup(entity_id, (*alloc_spans)[0], payload, start_in_payload, num, dedup_info);
  if (err_code < 0) {
    err_info.errcode = err_code;
    err_info.errmsg = "updatePayloadUsingDedup failed : " + string(ErrorInfo::errorCodeString(err_code));
    return err_code;
  }

  // 4.Retrieve the allocated block_item from alloc_spans and write the data to the pre-allocated space
  // After the writing is completed, corresponding processing (error reporting, data deletion)
  // will be performed according to the deduplication mode (REJECT, DISCARD).
  for (size_t idx = 0; idx < alloc_spans->size(); idx++) {
    const BlockSpan& span = (*alloc_spans)[idx];
    MetricRowID entity_num_node = meta_manager_.GetFileStartRow(span);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(span.block_item->block_id);
    if (!segment_tbl) {
      err_info.errcode = KWENOOBJ;
      err_info.errmsg = "Segment [" + db_path_ + segment_tbl_sub_path(span.block_item->block_id) + "] is null";
      return err_code;
    }
    // Write in a columnar format and update the aggregated results
    err_code = segment_tbl->PushPayload(entity_id, entity_num_node, payload,
                                        start_in_payload + count, span, inc_unordered_cnt, dedup_info);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "PushPayload failed : " + string(ErrorInfo::errorCodeString(err_code));
      return err_code;
    }
    err_code = ProcessDuplicateData(payload, start_in_payload, count, span, dedup_info, dedup_result, err_info, &deleted_rows);
    if (err_code < 0) {
      return err_code;
    }
    count += span.row_num;
  }
  if (payload->dedup_rule_ == DedupRule::OVERRIDE || payload->dedup_rule_ == DedupRule::MERGE) {
    // In the deduplication mode, it is necessary to record the rows that need to be deleted after deduplication for subsequent deletion.
    // return: to be deleted rows in tables.
    for (auto kv : dedup_info.table_real_rows) {
      todo_markdel->insert(todo_markdel->end(), kv.second.begin(), kv.second.end());
    }
  }

  err_info.errcode = err_code;
  return err_code;
}


KStatus TsTimePartition::CurrentLevel(int* level) {
  ErrorInfo err_info;
  return TsTierPartitionManager::GetInstance().GetPartitionCurrentLevel(db_path_ + tbl_sub_path_, level, err_info);
}

bool TsTimePartition::ShouldVacuum(uint32_t ts_version) {
  std::vector<BLOCK_ID> segment_ids;
  data_segments_.GetAllKey(&segment_ids);
  if (segment_ids.empty()) {
    return false;
  }
  if (IsModifiedRecent()) {
    return false;
  }

  bool been_altered = false;  // DDL check
  // only vacuum the partition that it's all segments had been compressed
  for (auto& segment_id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(segment_id);
    if (segment_tbl == nullptr) {
      continue;
    }
    if (segment_tbl->getSegmentStatus() != ImmuSegment) {
      return false;
    }
    auto segment_version = segment_tbl->schemaVersion();
    if (segment_version != ts_version) {
      been_altered = true;
    }
  }

  if (segment_ids.size() > 2) {
    return true;
  }
  if (been_altered) {
    return true;
  }
  return meta_manager_.getEntityHeader()->data_disordered || meta_manager_.getEntityHeader()->data_deleted;
}

KStatus TsTimePartition::PrepareTempPartition(uint32_t max_rows_per_block, uint32_t max_blocks_per_seg,
                                              uint32_t ts_version, TsTimePartition** dest_pt) {
  ErrorInfo err_info;
  std::string tmp_dir = GetPath();
  tmp_dir[tmp_dir.size() - 1] = '_';
  // if tmp_dir exists, remove the garbage files left over from the last vacuum
  if (IsExists(tmp_dir)) {
    LOG_WARN("temp partition [%s] exists, remove it first", tmp_dir.c_str());
    Remove(tmp_dir);
  }
  // Create a temporary partition and segment to store the data after vacuum
  if (!MakeDirectory(tmp_dir, err_info)) {
    LOG_ERROR("couldn't create temporary partition [%s]", tmp_dir.c_str());
    return FAIL;
  }
  *dest_pt = new TsTimePartition(root_table_manager_, meta_manager_.max_entities_per_subgroup);
  std::string tmp_partition_sub_path = tbl_sub_path_ + "/";
  tmp_partition_sub_path[tmp_partition_sub_path.size()-2] = '_';
  (*dest_pt)->open(file_path_, db_path_, tmp_partition_sub_path, MMAP_CREAT_EXCL, err_info);
  if (!err_info.isOK()) {
    delete *dest_pt;
    LOG_ERROR("couldn't open temporary partition table [%s]", tmp_dir.c_str());
    return FAIL;
  }
  // create segment use max_rows_per_block and max_blocks_per_seg
  MMapSegmentTable* segment_tb = (*dest_pt)->createSegmentTable(1, ts_version, err_info,
                                                                max_rows_per_block, max_blocks_per_seg);
  if (segment_tb == nullptr || err_info.errcode < 0) {
    delete *dest_pt;
    LOG_ERROR("createSegmentTable error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  // initialize segment
  segment_tb->setSegmentStatus(ActiveSegment);
  std::shared_ptr<MMapSegmentTable> segment_table(segment_tb);
  (*dest_pt)->data_segments_.Insert(1, segment_table);

  (*dest_pt)->active_segment_ = segment_table;
  (*dest_pt)->meta_manager_.getEntityHeader()->cur_datafile_id = 1;
  return SUCCESS;
}

KStatus TsTimePartition::GetVacuumData(const std::shared_ptr<MMapSegmentTable>& origin_segment_tbl,
                                       BlockItem* cur_block_item, size_t block_start_idx, k_uint32 row_count,
                                       uint32_t ts_version, ResultSet* res) {
  auto& origin_segment_schema = origin_segment_tbl->getSchemaInfo();
  std::vector<AttributeInfo> dst_segment_schema;
  KStatus s = root_table_manager_->GetSchemaInfoExcludeDropped(&dst_segment_schema, ts_version);
  if (s != SUCCESS) {
    LOG_ERROR("Couldn't get schema info of version %u", ts_version);
    return s;
  }
  const std::vector<uint32_t>& valid_cols = root_table_manager_->GetIdxForValidCols(ts_version);

  Batch* b;
  for (int i = 0; i < valid_cols.size(); i++) {
    auto col_idx = valid_cols[i];
    if (col_idx >= 0 && origin_segment_tbl->isColExist(col_idx)) {
      void* bitmap_addr = origin_segment_tbl->getBlockHeader(cur_block_item->block_id, col_idx);
      if (!isVarLenType(dst_segment_schema[i].type)) {
        // not varlen column
        if (origin_segment_schema[col_idx].type != dst_segment_schema[i].type) {
          // convert other types to fixed-length type
          char* value = static_cast<char*>(malloc(dst_segment_schema[i].size * row_count));
          memset(value, 0, dst_segment_schema[i].size * row_count);
          bool need_free_bitmap = false;
          s = ConvertToFixedLen(origin_segment_tbl, value, cur_block_item->block_id,
                                static_cast<DATATYPE>(origin_segment_schema[col_idx].type),
                                static_cast<DATATYPE>(dst_segment_schema[i].type),
                                dst_segment_schema[i].size, block_start_idx, row_count, col_idx, &bitmap_addr, need_free_bitmap);
          if (s != KStatus::SUCCESS) {
            free(value);
            return s;
          }
          b = new Batch(static_cast<void *>(value), row_count, bitmap_addr, block_start_idx, origin_segment_tbl);
          b->is_new = true;
          b->need_free_bitmap = need_free_bitmap;
        } else {
          if (origin_segment_schema[col_idx].size != dst_segment_schema[i].size) {
            if (origin_segment_schema[col_idx].size > dst_segment_schema[i].size) {
              LOG_ERROR("The original column width is greater than the modified column width.");
              return FAIL;
            }
            // convert same fixed-length type to different length
            char* value = static_cast<char*>(malloc(dst_segment_schema[i].size * row_count));
            memset(value, 0, dst_segment_schema[i].size * row_count);
            for (int idx = 0; idx < row_count; idx++) {
              memcpy(value + idx * dst_segment_schema[i].size,
                     origin_segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx + idx - 1, col_idx),
                     origin_segment_schema[col_idx].size);
            }
            b = new Batch(static_cast<void *>(value), row_count, bitmap_addr, block_start_idx, origin_segment_tbl);
            b->is_new = true;
          } else {
            b = new Batch(origin_segment_tbl->columnAddrByBlk(cur_block_item->block_id, block_start_idx - 1, col_idx),
                          row_count, bitmap_addr, block_start_idx, origin_segment_tbl);
          }
        }
      } else {
        // varlen column
        b = new VarColumnBatch(row_count, bitmap_addr, block_start_idx, origin_segment_tbl);
        for (k_uint32 j = 0; j < row_count; ++j) {
          std::shared_ptr<void> data = nullptr;
          bool is_null;
          if (b->isNull(j, &is_null) != KStatus::SUCCESS) {
            delete b;
            b = nullptr;
            return KStatus::FAIL;
          }
          if (is_null) {
            data = nullptr;
          } else {
            if (origin_segment_schema[col_idx].type != dst_segment_schema[i].type) {
              // convert other types to variable length
              data = ConvertToVarLen(origin_segment_tbl, cur_block_item->block_id,
                                     static_cast<DATATYPE>(origin_segment_schema[col_idx].type),
                                     static_cast<DATATYPE>(dst_segment_schema[i].type), block_start_idx + j - 1, col_idx);
            } else {
              data = origin_segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, block_start_idx + j - 1, col_idx);
            }
          }
          b->push_back(data);
        }
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      b = new Batch(bitmap, row_count, bitmap, block_start_idx, origin_segment_tbl);
    }
    res->push_back(i, b);
  }
  return SUCCESS;
}

KStatus
TsTimePartition::WriteVacuumData(TsTimePartition* dest_pt, uint32_t entity_id, ResultSet* res, uint32_t row_count) {
  std::shared_ptr<MMapSegmentTable> dest_segment = dest_pt->getSegmentTable(1);
  if (!dest_segment) {
    LOG_ERROR("Get segment table[%u] failed", 1);
    return FAIL;
  }
  // allocate block item
  std::vector<BlockSpan> dst_spans;
  auto err_code = dest_pt->AllocateAllDataSpace(entity_id, row_count, &dst_spans, dest_segment->schemaVersion());
  if (err_code < 0) {
    LOG_ERROR("AllocateAllDataSpace failed: %s", ErrorInfo::errorCodeString(err_code));
    return FAIL;
  }
  vector<uint32_t> cols_idx = dest_segment->getIdxForValidCols();
  vector<AttributeInfo> cols_info = dest_segment->GetColsInfoWithoutHidden();
  for (int i = 0; i < cols_idx.size(); ++i) {
    const Batch* batch = res->data[i][0];
    bool is_col_not_null = cols_info[i].isFlag(AINFO_NOT_NULL);
    bool has_null = false;
    if (!is_col_not_null) {
      has_null = batch->hasNull();
    }
    if (!isVarLenType(cols_info[i].type)) {
      // fixed length
      uint32_t batch_row_idx = 0;
      uint32_t total_row = 0;
      for (BlockSpan block_span : dst_spans) {
        MetricRowID row_id{block_span.block_item->block_id, block_span.start_row + 1};
        if (i && has_null) {  // timestamp col is not null
          for (int j = 0; j < block_span.row_num; j++) {
            bool is_null = false;
            batch->isNull(batch_row_idx, &is_null);
            if (is_null) {
              dest_segment->setNullBitmap(row_id, cols_idx[i]);
            }
            batch_row_idx++;
            row_id.offset_row++;
          }
        }
        if (batch->mem != nullptr) {
          row_id.offset_row = block_span.start_row + 1;  // reset row_id to first row of block span
          memcpy(dest_segment->columnAddr(row_id, cols_idx[i]), (void*)((char*)batch->mem + total_row * cols_info[i].size),
                 block_span.row_num * cols_info[i].size);
          total_row += block_span.row_num;
        }
      }
    } else {
      int block_span_idx = 0;
      size_t loc = 0;
      int processed_count = 0;  // The number of rows that have been written to the current block span
      uint32_t cur_block_row_id = dst_spans[block_span_idx].start_row;
      // var length data, write one by one
      for (uint32_t batch_row_idx = 0 ; batch_row_idx < row_count; ++batch_row_idx) {
        Defer defer{
          [&]() {
            ++processed_count;
            ++cur_block_row_id;
            if (processed_count >= dst_spans[block_span_idx].row_num) {
              ++block_span_idx;  // the current block is full, switch to next block
              if (block_span_idx < dst_spans.size()) {
                processed_count = 0;
                cur_block_row_id = dst_spans[block_span_idx].start_row;
              }
            }
          }
        };
        if (has_null) {
          bool is_null = false;
          batch->isNull(batch_row_idx, &is_null);
          if (is_null) {
            MetricRowID row_id{dst_spans[block_span_idx].block_item->block_id, cur_block_row_id + 1};
            dest_segment->setNullBitmap(row_id, cols_idx[i]);
            continue;
          }
        }
        char* var_addr = (char*)batch->getVarColData(batch_row_idx);
        uint16_t var_c_len = batch->getVarColDataLen(batch_row_idx);
        MMapStringColumn* dest_str_file = dest_segment->GetStringFile();
        // check string file size
        if (var_c_len + dest_str_file->size() >= dest_str_file->fileLen()) {
          int err_code = 0;
          if (dest_str_file->fileLen() > 1024 * 1024 * 1024) {
            size_t new_size = dest_str_file->size() / 2;
            err_code = dest_str_file->reserve(dest_str_file->size(), 3 * new_size, 2);
          } else {
            err_code = dest_str_file->reserve(dest_str_file->size(), 2 * dest_str_file->fileLen(), 2);
          }
          if (err_code < 0) {
            LOG_ERROR("MMapStringColumn[%s] reserve failed.", dest_str_file->strFile().realFilePath().c_str());
            return FAIL;
          }
        }
        // write data to string file
        if (cols_info[i].type == VARSTRING) {
          loc = dest_str_file->push_back(var_addr, var_c_len);
        } else {
          loc = dest_str_file->push_back_binary(var_addr, var_c_len);
        }
        if (loc == 0) {
          LOG_ERROR("StringFile push bach failed.");
          return FAIL;
        }
        // write string location to column file
        MetricRowID row_id{dst_spans[block_span_idx].block_item->block_id, cur_block_row_id + 1};
        memcpy(dest_segment->columnAddr(row_id, cols_idx[i]), &loc, cols_info[i].size);
      }
    }
  }
  // update aggregate result
  for (BlockSpan block_span : dst_spans) {
    block_span.block_item->publish_row_count += block_span.row_num;
    if (block_span.block_item->publish_row_count >= block_span.block_item->max_rows_in_block) {
      dest_segment->updateAggregateResult(block_span, true);
    } else {
      AggDataAddresses addresses{};
      MetricRowID row_id{block_span.block_item->block_id, block_span.start_row + 1};
      dest_segment->columnAggCalculate(block_span, row_id, 0, addresses, block_span.row_num, false);
      block_span.block_item->max_ts_in_block = KTimestamp(addresses.max);
      block_span.block_item->min_ts_in_block = KTimestamp(addresses.min);
      // Update the maximum and minimum timestamp information of the current segment,
      // which will be used for subsequent compression and other operations
      if (KTimestamp(addresses.max) > dest_segment->maxTimestamp() || dest_segment->maxTimestamp() == INVALID_TS) {
        dest_segment->maxTimestamp() = KTimestamp(addresses.max);
      }
      if (KTimestamp(addresses.min) > dest_segment->minTimestamp() || dest_segment->minTimestamp() == INVALID_TS) {
        dest_segment->minTimestamp() = KTimestamp(addresses.min);
      }
    }
  }
  return SUCCESS;
}

KStatus TsTimePartition::Vacuum(uint32_t ts_version, VacuumStatus &vacuum_result) {
  // check whether partition is being inserted.
  // Partition's ref count has a wider range than lock.
  if (IsWriting()) {
    return SUCCESS;
  }

  std::vector<uint32_t> entities;
  BLOCK_ID max_block_id{0};
  std::vector<BLOCK_ID> segment_ids;
  BLOCK_ID max_segment_id{0};

  // Step 1: Change segment status and get the necessary information
  {
    vacuumLock();
    Defer defer{[&]() { vacuumUnlock(); }};
    if (!ShouldVacuum(ts_version)) {
      return SUCCESS;
    }
    // check not inserting data.
    if (IsWriting()) {
      LOG_WARN("partition[%s] is being inserted, ref count [%d], cannot vacuum now", GetPath().c_str(), writing_count_.load());
      return SUCCESS;
    }

    if (!TrySetExclusiveStatus(ExclusiveStatus::VACUUMING)) {
      LOG_WARN("partition[%s] is being compressed, cannot vacuum now", GetPath().c_str());
      return SUCCESS;
    }
    Defer defer2{[&]() { ResetExclusiveStatus(); }};
    LOG_INFO("Start vacuum in partition [%s]", GetPath().c_str());
    entities = GetEntities();
    // set cancel_vacuum_ flag false, if the data written after unlocking is disordered or deleted, indicates that need to
    // vacuum again, and the flag will be set to true
    cancel_vacuum_ = false;
    data_segments_.GetAllKey(&segment_ids);
    max_segment_id = segment_ids.front();
    max_block_id = meta_manager_.getEntityHeader()->cur_block_id;
    ChangeSegmentStatus();
  }

  // Step 2: Read all valid data of the current entities and record the number of rows
  std::unordered_map<uint32_t, std::deque<BlockSpan>> src_spans;
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back({INT64_MIN, INT64_MAX});
  uint32_t max_block_num = 0;
  uint64_t total_count{0};
  for (uint32_t entity_id : entities) {
    std::deque<BlockSpan> block_spans;
    uint32_t count = GetAllBlockSpans(entity_id, ts_spans, block_spans, max_block_id);
    // Calculate how many blocks are needed for a temporary partition
    max_block_num += (count + CLUSTER_SETTING_MAX_ROWS_PER_BLOCK - 1) / CLUSTER_SETTING_MAX_ROWS_PER_BLOCK;
    src_spans[entity_id] = block_spans;
    total_count += count;
  }
  LOG_INFO("Total record count is [%lu] in partition [%s]", total_count, GetPath().c_str());

  // drop all the segments if all entities have been deleted
  if (max_block_num == 0) {
    vacuumLock();
    Defer defer{[&]() { vacuumUnlock(); }};
    std::vector<BLOCK_ID> new_segment_ids;
    data_segments_.GetAllKey(&new_segment_ids);
    if (IsWriting() || (new_segment_ids[0] > max_segment_id)) {
      LOG_WARN("partition[%s] is being inserted concurrently, ref count [%d], cancel vacuum", GetPath().c_str(), writing_count_.load());
      vacuum_result = VacuumStatus::CANCEL;
      return SUCCESS;
    }

    if (IsSegmentsBusy(segment_ids)) {
      vacuum_result = VacuumStatus::CANCEL;
      return SUCCESS;
    }
    if (DropSegmentDir(segment_ids) < 0) {
      return FAIL;
    }

    // reset all entity item block info.
    for (uint32_t entity_id : entities) {
      auto entity_item = getEntityItem(entity_id);
      if (0 == entity_item->cur_block_id) {
        continue;
      }
      entity_item->block_count = 0;
      entity_item->cur_block_id = 0;
      entity_item->is_deleted = false;
      entity_item->is_disordered = false;
      entity_item->max_ts = INVALID_TS;
      entity_item->min_ts = INVALID_TS;
    }
    for (int i = 1; i <= max_block_id; ++i) {
      BlockItem* block = GetBlockItem(i);
      memset((void*)block, 0, sizeof(BlockItem));
    }

    releaseSegments();
    meta_manager_.getEntityHeader()->cur_block_id = 0;
    vacuum_result = VacuumStatus::FINISH;
    return SUCCESS;
  }

  // Step 3: Create temporary partition.
  TsTimePartition* tmp_pt = nullptr;
  KStatus s = PrepareTempPartition(CLUSTER_SETTING_MAX_ROWS_PER_BLOCK, max_block_num, ts_version, &tmp_pt);
  if (s != SUCCESS) {
    return s;
  }

  std::string tmp_partition_path = tmp_pt->GetPath();
  Defer defer_delete{[&]() {
    delete tmp_pt;
    tmp_pt = nullptr;
    Remove(tmp_partition_path);
  }};

  // Step 4: Traverse all read data and write to the temporary partition
  uint32_t num_col = root_table_manager_->GetIdxForValidCols(ts_version).size();
  for (const auto& iter : src_spans) {
    // Check whether it is needed to cancel this vacuum.
    std::vector<BLOCK_ID> new_segment_ids;
    data_segments_.GetAllKey(&new_segment_ids);
    if (IsWriting() || (new_segment_ids[0] > max_segment_id) || cancel_vacuum_) {
      LOG_WARN("partition[%s] is being inserted concurrently, or there are some data been deleted, cancel vacuum", GetPath().c_str());
      vacuum_result = VacuumStatus::CANCEL;
      return SUCCESS;
    }
    // Traverse every entity's data
    uint32_t entity_id = iter.first;
    auto block_spans = iter.second;
    int64_t entity_row_count = 0;
    // Traverse every block span of this entity
    uint32_t block_cnt = 0;
    for (auto block_span : block_spans) {
      BlockItem* cur_block_item = block_span.block_item;
      uint32_t block_start_row = block_span.start_row;
      uint32_t row_count = block_span.row_num;
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(cur_block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in partition [%s]", cur_block_item->block_id, GetPath().c_str());
        return FAIL;
      }
      ResultSet res{num_col};
      // Step 4.1: Read data for vacuum
      s = GetVacuumData(segment_tbl, cur_block_item, block_start_row + 1, row_count, ts_version, &res);
      if (s != SUCCESS) {
        LOG_ERROR("Get vacuum data failed");
        // Defer would clear data
        return s;
      }
      // Step 4.2: Write into tmp_pt.
      s = WriteVacuumData(tmp_pt, entity_id, &res, row_count);
      if (s != SUCCESS) {
        LOG_ERROR("Write vacuum data failed");
        // Defer will clear data
        return s;
      }
      entity_row_count += row_count;
      ++block_cnt;
      if (block_cnt == 100 && g_vacuum_sleep_time > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(g_vacuum_sleep_time));
        block_cnt = 0;
      }
    }
    EntityItem* tmp_entity_item = tmp_pt->getEntityItem(entity_id);
    EntityItem* origin_entity_item = getEntityItem(entity_id);
    tmp_entity_item->max_ts = origin_entity_item->max_ts;
    tmp_entity_item->min_ts = origin_entity_item->min_ts;
  }
  tmp_pt->minTimestamp() = minTimestamp();
  tmp_pt->maxTimestamp() = maxTimestamp();
  std::shared_ptr<MMapSegmentTable> dest_segment = tmp_pt->getSegmentTable(1);
  dest_segment->setSegmentStatus(InActiveSegment);


  // Step 6: Replace partition's old segments with vacuumed segments
  vacuumLock();
  Defer defer{[&]() { vacuumUnlock(); }};
  std::vector<BLOCK_ID> new_segment_ids;
  data_segments_.GetAllKey(&new_segment_ids);
  if (IsWriting() || (new_segment_ids[0] > max_segment_id) || cancel_vacuum_) {
    LOG_WARN("partition[%s] is being inserted concurrently, or there are some data been deleted, cancel vacuum", GetPath().c_str());
    vacuum_result = VacuumStatus::CANCEL;
    return SUCCESS;
  }

  if (IsSegmentsBusy(segment_ids)) {
    vacuum_result = VacuumStatus::CANCEL;
    return SUCCESS;
  }
  if (DropSegmentDir(segment_ids) < 0) {
    LOG_ERROR("Drop original segments failed");
    return FAIL;
  }
  if (meta_manager_.remove() < 0) {
    LOG_ERROR("Drop original meta files failed");
    return FAIL;
  }
  std::string cmd = "mv " + tmp_partition_path + "* " + GetPath();
  if (!System(cmd)) {
    LOG_ERROR("mv tmp partition dir failed");
    // mv failed, still need to reload meta and segments, not return fail
    vacuum_result = VacuumStatus::FAILED;
  }
  // Step 7: Reload partition & reopen .meta files
  ErrorInfo err_info;
  if (openBlockMeta(MMAP_OPEN_NORECURSIVE, err_info) < 0) {
    LOG_ERROR("Reopen block meta failed, error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  if (loadSegments(err_info) < 0) {
    LOG_ERROR("Reload segments failed, error: %s", err_info.errmsg.c_str());
    return FAIL;
  }
  if (vacuum_result == VacuumStatus::FAILED) {
    return FAIL;
  }
  // update data version
  if (root_table_manager_->GetTableVersionOfLatestData() < ts_version) {
    root_table_manager_->UpdateTableVersionOfLastData(ts_version);
  }
  ResetVacuumFlags();
  vacuum_result = VacuumStatus::FINISH;
  return SUCCESS;
}

// Get segments that require compression processing
std::vector<std::shared_ptr<MMapSegmentTable>> TsTimePartition::GetAllSegmentsForCompressing() {
  vector<std::shared_ptr<MMapSegmentTable>> segment_tables;
  data_segments_.Traversal([&](BLOCK_ID segment_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    // Compressed segment
    if (tbl->sqfsIsExists()) {
      if (tbl->getObjectStatus() == OBJ_READY && tbl->getSegmentStatus() == ImmuSegment) {
        // Segments that have been compressed but not cleaned up from the original data directory
        // need to be cleaned up from the original data directory
        segment_tables.emplace_back(tbl);
      }
      // Compressed and cleaned segments from the original data directory do not take any action
      return true;
    }
    // If the uncompressed segment is not ready,
    // it needs to be reopened and subsequent compression operations should be performed
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                    db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                    false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }
    segment_tables.emplace_back(tbl);
    return true;
  });
  return segment_tables;
}

void TsTimePartition::ImmediateCompress(uint32_t& compressed_num, ErrorInfo& err_info){
  auto segment_tables = GetAllSegmentsForCompressing();
  int num_of_active_segments = 0;
  for (const auto& segment : segment_tables) {
    if (segment->getSegmentStatus() == ActiveSegment) {
      num_of_active_segments++;
      segment->setSegmentStatus(InActiveSegment);
      MUTEX_LOCK(active_segment_lock_);
      if (active_segment_ && active_segment_->segment_id() == segment->segment_id()) {
        active_segment_ = nullptr;
      }
      MUTEX_UNLOCK(active_segment_lock_);
    }
  }

  if (num_of_active_segments != 0) {
    // waiting for unfinished writing threads on active thread.
    using std::chrono_literals::operator""s;
    std::this_thread::sleep_for(1s);
  }

  // compress all the InActiveSegments and make sure they are mounted
  for (const auto& segment_tbl : segment_tables) {
    if (segment_tbl->getSegmentStatus() != InActiveSegment) {
      continue;
    }
    // Sync before compressing
    segment_tbl->sync(MS_SYNC);
    LOG_INFO("MMapSegmentTable[%s] compress start", segment_tbl->GetPath().c_str());
    segment_tbl->setSegmentStatus(ImmuSegment);
    bool ok = compress(db_path_, tbl_sub_path_, std::to_string(segment_tbl->segment_id()),
                       g_mk_squashfs_option.processors_immediate, err_info);
    if (!ok) {
      // If compression fails, restore segment state
      segment_tbl->setSegmentStatus(InActiveSegment);
      LOG_ERROR("MMapSegmentTable[%s] compress failed", segment_tbl->GetPath().c_str());
      return;
    }
    ++compressed_num;
    segment_tbl->setSqfsIsExists();

    // Mount the compressed segment
    if (!isMounted(db_path_ + segment_tbl->tbl_sub_path())) {
      if (!reloadSegment(segment_tbl, false, err_info)) {
        LOG_ERROR("MMapSegmentTable[%s] reload failed", segment_tbl->GetPath().c_str());
        return;
      }
    }
    LOG_INFO("MMapSegmentTable[%s] compress succeeded", segment_tbl->GetPath().c_str());
  }
}

void TsTimePartition::ScheduledCompress(timestamp64 compress_ts_p, uint32_t& compressed_num, ErrorInfo& err_info) {
  auto segment_tables = GetAllSegmentsForCompressing();
  // Traverse the segments that need to be processed
  // There are three types of processing for segments
  //   1.ActiveSegment/ActiveInWriteSegment
  //     Segments that meet compression conditions will be set to inactive
  //   2.InActiveSegment
  //     Attempt to truncate files, compress data, and clean up the original data directory for inactive segments
  //   3.ImmuSegment
  //     Attempting to clean up the original data directory for compressed segments
  auto ts_type = getSchemaInfoExcludeDropped()[0].type;
  for (auto& segment_tbl : segment_tables) {
    switch (segment_tbl->getSegmentStatus()) {
      case ActiveSegment:
        if (convertSecondToPrecisionTS(maxTimestamp(), (DATATYPE)ts_type) >= compress_ts_p) {
          // For segments in partitions where the timestamp of some data is less than compress_ts,
          // if the following two conditions are met, they can be set as inactive segment.
          //   1. the maximum timestamp of the data saved by the segment is less than compress_ts
          //   2. the pre allocated space of the segment is 90% full
          if (segment_tbl->maxTimestamp() < compress_ts_p
              && segment_tbl->size() >= 0.9 * segment_tbl->getReservedRows()) {
            segment_tbl->setSegmentStatus(InActiveSegment);
            MUTEX_LOCK(active_segment_lock_);
            if (active_segment_ && active_segment_->segment_id() == segment_tbl->segment_id()) {
              active_segment_ = nullptr;
            }
            MUTEX_UNLOCK(active_segment_lock_);
          }
        } else {
          // For segments in partitions with all data timestamps less than compress_ts,
          // one of the following two conditions can be met to set the segment to inactive state.
          //   1. segment did not perform data writing within one compression cycle
          //   2. the last write time of segment data is less than or equal to compress_ts
          string ts_file_path = db_path_ + segment_tbl->tbl_sub_path() + name_ + ".0";
          int64_t modify_time = ModifyTime(ts_file_path);  // ts unit: second
          if (modify_time <= now() - g_compress_interval ||
              convertSecondToPrecisionTS(modify_time, (DATATYPE)ts_type) <= compress_ts_p) {
            segment_tbl->setSegmentStatus(InActiveSegment);
            MUTEX_LOCK(active_segment_lock_);
            if (active_segment_ && active_segment_->segment_id() == segment_tbl->segment_id()) {
              active_segment_ = nullptr;
            }
            MUTEX_UNLOCK(active_segment_lock_);
          }
        }
        break;
      case InActiveSegment: {
        // Truncate the data file for the segment.
        // The reason is that there may be unused block items in the segment, which can be removed from the free space
        // if (segment_tbl->truncate() < 0) {
        //   continue;
        // }
        // Compress segment data
        LOG_INFO("MMapSegmentTable[%s] compress start", segment_tbl->GetPath().c_str());
        segment_tbl->setSegmentStatus(ImmuSegment);
        bool ok = compress(db_path_, tbl_sub_path_, std::to_string(segment_tbl->segment_id()),
                           g_mk_squashfs_option.processors_scheduled, err_info);
        if (!ok) {
          // If compression fails, restore segment state
          segment_tbl->setSegmentStatus(InActiveSegment);
          LOG_ERROR("MMapSegmentTable[%s] compress failed", segment_tbl->GetPath().c_str());
          return;
        }
        ++compressed_num;
        segment_tbl->setSqfsIsExists();
        // Check if it is mounted. If it is not, try cleaning up the original data directory before compression.
        // It is necessary to ensure that the original data directory is not used by other threads before cleaning up.
        BLOCK_ID segment_id = segment_tbl->segment_id();
        if (!isMounted(db_path_ + segment_tbl->tbl_sub_path())) {
          if (!reloadSegment(segment_tbl, false, err_info)) {
            LOG_ERROR("MMapSegmentTable[%s] reload failed", segment_tbl->realFilePath().c_str());
            return;
          }
        }
        LOG_INFO("MMapSegmentTable[%s] compress succeeded", segment_tbl->GetPath().c_str());
        break;
      }
      case ImmuSegment: {
        // Check if it is mounted. If it is not, try cleaning up the original data directory before compression.
        // It is necessary to ensure that the original data directory is not used by other threads before cleaning up.
        BLOCK_ID segment_id = segment_tbl->segment_id();
        if (!isMounted(db_path_ + segment_tbl->tbl_sub_path())) {
          if (!reloadSegment(segment_tbl, false, err_info)) {
            LOG_ERROR("MMapSegmentTable[%s] reload failed", segment_tbl->realFilePath().c_str());
            return;
          }
        }
        break;
      }
      default:
        break;
    }
  }
}

bool TsTimePartition::TrySetExclusiveStatus(ExclusiveStatus desired) {
  ExclusiveStatus expected = ExclusiveStatus::NONE;
  if (comp_vacuum_status_.compare_exchange_strong(expected, desired)) {
    return true;
  }
  return false;
}

void TsTimePartition::ResetExclusiveStatus() {
  comp_vacuum_status_.store(ExclusiveStatus::NONE);
}

void TsTimePartition::Compress(const timestamp64& compress_ts, uint32_t& compressed_num, ErrorInfo& err_info) {
  if (!TrySetExclusiveStatus(ExclusiveStatus::COMPRESSING)) {
    return;
  }
  if (compress_ts == INT64_MAX){
    ImmediateCompress(compressed_num, err_info);
  } else {
    ScheduledCompress(compress_ts, compressed_num, err_info);
  }
  ResetExclusiveStatus();
}

int TsTimePartition::Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info) {
  sync(MS_SYNC);
  meta_manager_.sync(MS_SYNC);
  return 0;
}

int TsTimePartition::Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows, ErrorInfo& err_info) {
  sync(MS_SYNC);
  meta_manager_.sync(MS_SYNC);

  std::vector<uint32_t> entities = meta_manager_.getEntities();
  for (auto entity_id: entities) {
    rows.insert(std::make_pair(entity_id, size(entity_id)));
  }

  return 0;
}

int TsTimePartition::DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  *count = 0;
  CountUpdateTime();
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->cur_block_id == 0) {
    deleteEntityItem(entity_id);
    SetDeleted(entity_id);
    return 0;
  }
  meta_manager_.lock();
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  // Delete from current block in reverse order
  while (true) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item->block_id == 0) {
      LOG_WARN("BlockItem[%u, %u] error: No space has been allocated", entity_id, block_item_id)
      continue;
    }
    // Add deleted rows.
    uint32_t block_row_cnt = block_item->publish_row_count - block_item->getDeletedCount();
    *count += block_row_cnt;

    if (block_item->prev_block_id <= 0) {
      break;
    }
    // Point to the previous block
    block_item_id = block_item->prev_block_id;
  }

  // Mark the entity item deleted.
  deleteEntityItem(entity_id);
  // As long as there is data deletion in this partition, it needs to be reorganized
  SetDeleted(entity_id);
  meta_manager_.unlock();
  return 0;
}

int TsTimePartition::DeleteData(uint32_t entity_id, kwdbts::TS_LSN lsn, const std::vector<KwTsSpan>& ts_spans,
                                vector<DelRowSpan>* delete_rows, uint64_t* count,
                                ErrorInfo& err_info, bool evaluate_del) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  // 1. using primary_tag\start\end, get all satisfied rows
  // 2.mark rows delete flag
  CountUpdateTime();
  EntityItem* entity_item = getEntityItem(entity_id);
  BLOCK_ID block_item_id = entity_item->cur_block_id;

  while (block_item_id != 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_WARN("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }

    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    if (!isTimestampInSpans(ts_spans, block_min_ts, block_max_ts)) {
      // blockitem ts span  not cross with ts_spans, no need scan data.
      block_item_id = block_item->prev_block_id;
      continue;
    }

    // scan all data in this blockitem
    DelRowSpan row_span;
    for (k_uint32 row_idx = 1; row_idx <= block_item->alloc_row_count ; ++row_idx) {
      bool has_been_deleted = !segment_tbl->IsRowVaild(block_item, row_idx);
      auto cur_ts = KTimestamp(segment_tbl->columnAddr(block_item->getRowID(row_idx), 0));
      if (has_been_deleted || !isTimestampInSpans(ts_spans, cur_ts, cur_ts)) {
        continue;
      }
      if (!evaluate_del) {
        if (block_item->setDeleted(row_idx) < 0) {
          return -1;
        }
      } else if (delete_rows) {
        markDeleted(row_span.delete_flags, row_idx);
      }
      (*count)++;
      // As long as there is data deletion in this partition, it needs to be reorganized
      SetDeleted(entity_id);
    }
    if (evaluate_del && delete_rows) {
      row_span.blockitem_id = block_item_id;
      delete_rows->emplace_back(row_span);
    }

    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

int TsTimePartition::UndoPut(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t start_row, size_t num,
                             kwdbts::Payload* payload, ErrorInfo& err_info) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  CountUpdateTime();
  size_t p_count = num;
  if (p_count == 0) {
    return 0;
  }
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item == nullptr) {
    LOG_ERROR("EntityItem[%u] error: entity_item is null", entity_id);
    return KWENOOBJ;
  }
  // TODO:(zqh) 创建第二个segment失败，undo put是否正确处理
  if (entity_item->max_ts == INVALID_TS || entity_item->min_ts == INVALID_TS) {
    LOG_WARN("UndoPut: entity_id [%u] has no data inserted", entity_id);
    return 0;
  }
  BLOCK_ID block_item_id = entity_item->cur_block_id;
  auto start_time = payload->GetTimestamp(start_row);
  auto end_time = payload->GetTimestamp(num - 1);
  if (start_time > entity_item->max_ts || end_time < entity_item->min_ts) {
    LOG_WARN("Payload data is not within this entity, entity_id = %u", entity_id);
    return 0;
  }
  // Iterate through all blocks and verify if there is matching data (with identical timestamp and LSN).
  // If such data is found, an undo operation is required to mark the data as deleted.
  while (block_item_id != 0 && p_count > 0) {
    BlockItem* block_item = GetBlockItem(block_item_id);
    if (block_item == nullptr) {
      LOG_ERROR("BlockItem[%u, %u] error: block_item is null", entity_id, block_item_id);
      return KWENOOBJ;
    }
    if (block_item->block_id == 0) {
      LOG_WARN("BlockItem[%u, %u] error: No space has been allocated", entity_id, block_item_id)
      block_item_id = block_item->prev_block_id;
      continue;
    }

    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (segment_tbl == nullptr) {
      LOG_WARN("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }
    if (segment_tbl->getSegmentStatus() >= InActiveSegment) {
      LOG_WARN("Segment [%s] is not ActiveSegment", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      block_item_id = block_item->prev_block_id;
      continue;
    }

    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    if (block_max_ts < start_time || block_min_ts > end_time) {
      block_item_id = block_item->prev_block_id;
      continue;
    }
    if (!isTsWithLSNType((DATATYPE)segment_tbl->getSchemaInfo()[0].type)) {
      LOG_ERROR("The data type in the first column is not TIMESTAMP64_LSN.");
      return KWEDATATYPEMISMATCH;
    }

    // scan all data in blockitem
    k_uint32 row_count = block_item->publish_row_count;
    for (k_uint32 row_idx = 1; row_idx <= row_count; ++row_idx) {
      if (p_count == 0) {
        return 0;
      }
      TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(segment_tbl->columnAddr(
          MetricRowID{block_item->block_id, row_idx}, 0));
      if (ts_lsn->lsn != lsn) {
        continue;
      }
      SetDeleted(entity_id);
      block_item->setDeleted(row_idx);
      block_item->is_agg_res_available = false;
      p_count--;
    }
    block_item_id = block_item->prev_block_id;
  }
  return 0;
}

int TsTimePartition::UndoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn,
                                const vector<DelRowSpan>* rows, ErrorInfo& err_info) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  CountUpdateTime();
  /*
     1. struct BlockItem.rows_delete_flags records the delete flags for 1000 rows in the data block.
        A bit of 1 indicates that the row has been deleted
     2. struct DelRowSpan uniquely locates a block item and records which rows in the data block are deleted
        when DeleteData is called. The deleted row bit is set to 1
     3. undo delete needs to change the bit corresponding to the row that was set to 1 during deletion back to 0
     */
  for (auto row_span : *rows) {
    BlockItem* block_item = GetBlockItem(row_span.blockitem_id);
    for (int i = 0; i < block_item->publish_row_count; i++) {
      if (isRowDeleted(row_span.delete_flags, i + 1)) {
        setRowValid(block_item->rows_delete_flags, i + 1);
      }
    }
    if (block_item->publish_row_count >= block_item->max_rows_in_block) {
      block_item->is_agg_res_available = false;
    }
    ResetRowWritten(entity_id);
  }
  return 0;
}

int TsTimePartition::UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  int err_code = 0;
  EntityItem* entity_item = getEntityItem(entity_id);
  entity_item->is_deleted = false;
  return err_code;
}

int TsTimePartition::RedoPut(kwdbts::kwdbContext_p ctx, uint32_t entity_id, kwdbts::TS_LSN lsn,
                             uint64_t start_row, size_t num, kwdbts::Payload* payload,
                             std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
                             std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map, KTimestamp p_time,
                             ErrorInfo& err_info) {
  int err_code = 0;
  err_info.errcode = 0;
  alloc_spans->clear();
  // Inspect whether the partition table's current state is valid
  if (!isValid()) {
    err_info.errcode = KWENOOBJ;
    return KWENOOBJ;
  }

  // 1.Calculate the minimum and maximum timestamps for the payload to prepare for subsequent deduplication.
  // If the current payload's timestamp range does not overlap with the entity's, there is no need to deduplicate.
  KTimestamp pl_min = INT64_MAX, pl_max = INT64_MIN;
  for (size_t i = start_row; i < start_row + num; i++) {
    KTimestamp cur_ts = KTimestamp(payload->GetColumnAddr(i, 0));
    if (pl_min > cur_ts) {
      pl_min = cur_ts;
    }
    if (pl_max < cur_ts) {
      pl_max = cur_ts;
    }
  }

  size_t count = 0;
  kwdbts::DedupInfo dedup_info;
  int deleted_rows = 0;
  {
    MUTEX_LOCK(vacuum_insert_lock_);
    CountUpdateTime();
    // 2.GetAndAllocateAllDataSpace finds the block corresponding to the data that has been written,
    // rolls it back, writes it to alloc_spans,
    // and then requests the block needed for the data that has not been written, and writes it to alloc_spans.
    err_code = GetAndAllocateAllDataSpace(entity_id, num, start_row, payload, lsn, partition_ts_map, p_time,
                                          alloc_spans);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "AllocateSpace failed : " + string(ErrorInfo::errorCodeString(err_code));
      MUTEX_UNLOCK(vacuum_insert_lock_);
      return err_code;
    }
    EntityItem* entity_item = getEntityItem(entity_id);
    // The need_scan_table is used to identify whether there is duplicate data that needs to be scanned for partitions,
    // If the timestamp range of the current payload does not fall within the timestamp range of the current entity,
    // there is no need to scan the entire partition.
    if (pl_min > entity_item->max_ts || entity_item->max_ts == INVALID_TS
        || pl_max < entity_item->min_ts) {
      dedup_info.need_scan_table = false;
    } else {
      dedup_info.need_scan_table = true;
    }
    // During writing, a determination is made as to whether the current payload contains disordered data.
    // If present, entity_item is marked as being disordered.
    // During subsequent queries, a judgment will be made. If the result set is unordered,
    // a secondary HASH aggregation based on AGG SCAN is required.
    if (payload->IsDisordered(start_row, num) ||
        (entity_item->max_ts != INVALID_TS && pl_min <= entity_item->max_ts)) {
      entity_item->is_disordered = true;
      SetDisordered();
    }
    // Update the maximum and minimum timestamps of entity_items to prepare for subsequent payload deduplication.
    if (pl_max > entity_item->max_ts || entity_item->max_ts == INVALID_TS) {
      entity_item->max_ts = pl_max;
    }
    if (pl_min < entity_item->min_ts || entity_item->min_ts == INVALID_TS) {
      entity_item->min_ts = pl_min;
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }
  DedupResult* dedup_result = new DedupResult();
  // before return, need publish inserted data and update meta datas.
  Defer defer{[&]() {
    delete dedup_result;
    std::vector<size_t> full_block_idx;
    MUTEX_LOCK(vacuum_insert_lock_);
    CountUpdateTime();
    for (size_t i = 0; i < alloc_spans->size(); i++) {
      (*alloc_spans)[i].block_item->publish_row_count = (*alloc_spans)[i].block_item->alloc_row_count;
      std::shared_ptr<MMapSegmentTable> tbl = getSegmentTable((*alloc_spans)[i].block_item->block_id);
      if (tbl == nullptr) {
        LOG_ERROR("getSegmentTable failed, block_id: %u", (*alloc_spans)[i].block_item->block_id);
        return;
      }
      const BlockSpan& span = (*alloc_spans)[i];
      tbl->updateAggregateResult(span, true);
    }
    MUTEX_UNLOCK(vacuum_insert_lock_);
  }};

  // 3.Deduplication is performed and the payload is updated according
  // to different deduplication rules (KEEP, REJECT, MERGE).
  // Inspect the payload for any duplicate timestamps
  // and apply various treatments, such as checking for duplicate rows and merging them.
  err_code = updatePayloadUsingDedup(entity_id, (*alloc_spans)[0], payload, start_row, num, dedup_info);
  if (err_code < 0) {
    err_info.errcode = err_code;
    err_info.errmsg = "updatePayloadUsingDedup failed : " + string(ErrorInfo::errorCodeString(err_code));
    return err_code;
  }

  // 4.Retrieve the allocated block_item from alloc_spans and write the data to the pre-allocated space
  // After the writing is completed, corresponding processing (error reporting, data deletion)
  // will be performed according to the deduplication mode (REJECT, DISCARD).
  for (size_t idx = 0; idx < alloc_spans->size(); idx++) {
    const BlockSpan& span = (*alloc_spans)[idx];
    MetricRowID entity_num_node = meta_manager_.GetFileStartRow(span);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(span.block_item->block_id);
    if (!segment_tbl) {
      err_info.errcode = KWENOOBJ;
      err_info.errmsg = "Segment [" + db_path_ + segment_tbl_sub_path(span.block_item->block_id) + "] is null";
      return err_code;
    }
    // Write in a columnar format and update the aggregated results
    uint32_t inc_unordered_cnt = 0;
    err_code = segment_tbl->PushPayload(entity_id, entity_num_node, payload,
                                        start_row + count, span, &inc_unordered_cnt, dedup_info);
    if (err_code < 0) {
      err_info.errcode = err_code;
      err_info.errmsg = "PushPayload failed : " + string(ErrorInfo::errorCodeString(err_code));
      return err_code;
    }
    err_code = ProcessDuplicateData(payload, start_row, count, span, dedup_info, dedup_result, err_info, &deleted_rows);
    if (err_code < 0) {
      return err_code;
    }
    count += span.row_num;
  }
  if (payload->dedup_rule_ == DedupRule::OVERRIDE || payload->dedup_rule_ == DedupRule::MERGE) {
    // In the deduplication mode, it is necessary to
    // record the rows that need to be deleted after deduplication for subsequent deletion.
    // return: to be deleted rows in tables.
    for (auto kv : dedup_info.table_real_rows) {
      todo_markdel->insert(todo_markdel->end(), kv.second.begin(), kv.second.end());
    }
  }

  err_info.errcode = err_code;
  return err_code;
}

int TsTimePartition::RedoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn,
                                const vector<DelRowSpan>* rows, ErrorInfo& err_info) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  for (auto row_span : *rows) {
    // Retrieve the block item based on its ID
    BlockItem* block_item = GetBlockItem(row_span.blockitem_id);
    for (int i = 0; i < 128; ++i) {
      // Iterate through each bit in the delete flags
      if (row_span.delete_flags[i] == 0) {
        continue;
      }
      block_item->rows_delete_flags[i] |= row_span.delete_flags[i];
    }
    ResetRowWritten(entity_id);
  }

  return 0;
}

int TsTimePartition::GetAllBlockItems(vector<uint32_t>& entity_ids, std::deque<BlockItem*>& block_items) {
  return meta_manager_.GetAllBlockItems(entity_ids, block_items);
}

int TsTimePartition::GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse) {
  return meta_manager_.GetAllBlockItems(entity_id, block_items, reverse);
}

int TsTimePartition::GetCountBlockItems(uint32_t entity_id, BLOCK_ID block_id, std::deque<BlockItem*>& block_items) {
  return meta_manager_.GetCountBlockItems(entity_id, block_id, block_items);
}

int TsTimePartition::AllocateAllDataSpace(uint entity_id, size_t batch_num, std::vector<BlockSpan>* spans,
                                          uint32_t payload_table_version) {
  int err_code = 0;
  size_t left_count = batch_num;
  BlockItem* block_item = nullptr;
  do {
    // Acquire or allocate a BlockItem
    err_code = allocateBlockItem(entity_id, &block_item, payload_table_version);
    if (err_code < 0) {
      // If allocation fails, exit the loop
      break;
    }
    BlockSpan span;
    // Count the number of rows allocated this time and update the span
    span.start_row = block_item->alloc_row_count;
    span.row_num = std::min(left_count, size_t(block_item->max_rows_in_block - block_item->alloc_row_count));
    span.block_item = block_item;
    block_item->alloc_row_count += span.row_num;
    // block_item->publish_row_count = block_item->alloc_row_count;
    spans->push_back(span);

    left_count -= span.row_num;
    // If there are remaining rows, continue the loop
  } while (left_count > 0);

  // rollback all allocated space. mark deleted now.
  if (err_code < 0) {
    for (auto & span : *spans) {
      SetDeleted(entity_id);
      meta_manager_.MarkSpaceDeleted(entity_id, &span);
      span.block_item->publish_row_count += span.row_num;
    }
    spans->clear();
  }
  // Update entity's row_allocated
  auto entity = getEntityItem(entity_id);
  entity->entity_id = entity_id;
  entity->row_allocated += batch_num;

  return err_code;
}

int TsTimePartition::GetAndAllocateAllDataSpace(uint entity_id, size_t batch_num, uint64_t start_row,
                                                kwdbts::Payload* payload, kwdbts::TS_LSN lsn,
                                                std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map,
                                                KTimestamp p_time, std::vector<BlockSpan>* spans) {
  int err_code = 0;
  size_t left_count = batch_num;
  BlockItem* block_item = nullptr;
  BlockSpan span;
  std::deque<BlockItem*> block_items;

  void* ts_begin = payload->GetColumnAddr(start_row, 0);
  timestamp64 start_payload_ts = *(reinterpret_cast<timestamp64*>((intptr_t) (ts_begin)));

  uint32_t pre_block_row = 0;
  BlockItem* pre_block_item = nullptr;

  FindFirstBlockItem(entity_id, lsn, start_payload_ts, block_items, &pre_block_item, &pre_block_row,
                     partition_ts_map, p_time);
  do {
    // If pre_block_item is not null, it indicates that the block_item has already been allocated space,
    // eliminating the need for another allocation. If it is null,
    // it signifies that the previously requested space has been exhausted, necessitating the acquisition of a new one.
    if (pre_block_item != nullptr) {
      batch_num -= std::min(batch_num, size_t(pre_block_item->alloc_row_count));
      pre_block_item->alloc_row_count = pre_block_row;
      pre_block_item->publish_row_count = pre_block_row;
      pre_block_item->is_agg_res_available = false;
      block_item = pre_block_item;
      auto first_row = pre_block_row > 0 ? pre_block_row : 1;
      auto row_count = std::min(left_count, size_t(getMaxRowsInBlock(block_item) - block_item->alloc_row_count));
      setBatchValid(block_item->rows_delete_flags, first_row, row_count);
      ResetRowWritten(entity_id);
    } else {
      err_code = allocateBlockItem(entity_id, &block_item, payload->GetTsVersion());
      if (err_code < 0) {
        // If allocation fails, exit the loop
        break;
      }
      // Count the number of rows allocated this time and update the span
    }
    span.start_row = block_item->alloc_row_count;
    span.row_num = std::min(left_count, size_t(block_item->max_rows_in_block - block_item->alloc_row_count));
    span.block_item = block_item;
    block_item->alloc_row_count += span.row_num;
    // block_item->publish_row_count = block_item->alloc_row_count;
    spans->push_back(span);
    left_count -= span.row_num;
    if (!block_items.empty()) {
      pre_block_item = block_items.front();
      block_items.pop_front();
    } else {
      pre_block_item = nullptr;
    }
    pre_block_row = 0;
    // If there are remaining rows, continue the loop
  } while (left_count > 0);

  // rollback all allocated space. mark deleted now. later, we can reuse this space.
  if (err_code < 0) {
    for (size_t i = 0; i < spans->size(); i++) {
      SetDeleted(entity_id);
      meta_manager_.MarkSpaceDeleted(entity_id, &(spans->at(i)));
      span.block_item->publish_row_count += span.row_num;
    }
    spans->clear();
  }
  partition_ts_map->insert(
      std::make_pair(p_time, MetricRowID{block_item->block_id, span.start_row + span.row_num + 1}));
  // Update entity's row_allocated
  auto entity = getEntityItem(entity_id);
  entity->row_allocated += batch_num;
  return err_code;
}

int TsTimePartition::allocateBlockItem(uint entity_id, BlockItem** blk_item, uint32_t payload_table_version) {
  int err_code = 0;
  BlockItem* block_item = nullptr;
  bool need_add_block_item = false;
  uint32_t new_segment_table_version = payload_table_version;
  uint32_t segment_table_version = payload_table_version;

  auto entity_item = getEntityItem(entity_id);

  // Determines whether a new BlockItem needs to be added.
  if (entity_item->cur_block_id == 0) {
    need_add_block_item = true;
  } else {
    block_item = GetBlockItem(entity_item->cur_block_id);
    // If all the space in the current BlockItem has been allocated or is unavailable, then it is necessary to add a new BlockItem.
    if (isReadOnly(block_item, payload_table_version)) {
      need_add_block_item = true;
    }
  }

  if (need_add_block_item) {
    // Attempts to add a new BlockItem to the metadata.
    err_code = meta_manager_.AddBlockItem(entity_id, &block_item);
    if (err_code < 0) return err_code;

    // Allocates a new data block.
    MUTEX_LOCK(active_segment_lock_);
    Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};
    // Checks if the current segment is already full.
    BLOCK_ID segment_block_num = meta_manager_.getEntityHeader()->cur_block_id
                                 - meta_manager_.getEntityHeader()->cur_datafile_id;
    if (active_segment_) {
      segment_table_version = active_segment_->schemaVersion();
    }
    if (segment_table_version > payload_table_version) {
      new_segment_table_version = segment_table_version;
    }
    uint32_t max_blocks_per_segment = 0;
    if (LIKELY(active_segment_ != nullptr)) {
      max_blocks_per_segment = active_segment_->getBlockMaxNum();
    } else {
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(meta_manager_.getEntityHeader()->cur_block_id - 1);
      if (segment_tbl) {
        max_blocks_per_segment = segment_tbl->getBlockMaxNum();
      }
    }
    if (segment_block_num >= max_blocks_per_segment || segment_table_version < payload_table_version) {
      active_segment_ = nullptr;
    }

    // If there is no active segment currently, or if it's already read-only, then it's necessary to create a new active segment.
    if (active_segment_ == nullptr || active_segment_->getSegmentStatus() >= InActiveSegment) {
      ErrorInfo err_info;
      // Uses the current maximum block ID plus one as the segment ID.
      BLOCK_ID next_segment_id = meta_manager_.getEntityHeader()->cur_block_id;
      MMapSegmentTable* tbl = createSegmentTable(next_segment_id, new_segment_table_version, err_info);
      if (tbl == nullptr || err_info.errcode < 0) {
        meta_manager_.getEntityHeader()->cur_block_id--;
        LOG_ERROR("createSegmentTable error: %s", err_info.errmsg.c_str());
        return err_info.errcode;
      }
      tbl->setSegmentStatus(ActiveSegment);
      std::shared_ptr<MMapSegmentTable> segment_table(tbl);
      data_segments_.Insert(next_segment_id, segment_table);

      active_segment_ = segment_table;
      meta_manager_.getEntityHeader()->cur_datafile_id = next_segment_id;
      root_table_manager_->UpdateTableVersionOfLastData(new_segment_table_version);
    }

    block_item->is_overflow = false;
    block_item->is_agg_res_available = false;
    block_item->max_rows_in_block = active_segment_->getBlockMaxRows();
    block_item->max_ts_in_block = INVALID_TS;
    block_item->min_ts_in_block = INVALID_TS;

    // Updates the metadata of the active segment.
    active_segment_->metaData()->block_num_of_segment++;
    // Checks if the partition table has reached its maximum Block quantity.
    if (ReachMetaMaxBlock(meta_manager_.getEntityHeader()->cur_block_id)) {
      LOG_FATAL("fatal: partition table block num reach max.");
      return KWENOSPC;
    }
    // Updates the corresponding BlockItem information.
    meta_manager_.UpdateEntityItem(entity_id, block_item);
  }
  // Returns the allocated BlockItem.
  *blk_item = block_item;
  return err_code;
}

void TsTimePartition::publish_payload_space(const std::vector<BlockSpan>& alloc_spans,
                                            const std::vector<MetricRowID>& delete_rows, uint32_t entity_id,
                                            bool success) {
  MUTEX_LOCK(vacuum_delete_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(vacuum_delete_lock_); }};
  // All requested space is marked as deleted space.
  CountUpdateTime();
  if (!success) {
    for (auto alloc_span : alloc_spans) {
      SetDeleted(entity_id);
      meta_manager_.MarkSpaceDeleted(entity_id, const_cast<BlockSpan*>(&alloc_span));
    }
  } else {
    for (auto delete_row : delete_rows) {
      if (delete_row.block_id == 0) {
        continue;
      }
      BlockItem* cur_blk_item = meta_manager_.GetBlockItem(delete_row.block_id);
      SetDeleted(entity_id);
      bool is_deleted = false;
      if (cur_blk_item->isDeleted(delete_row.offset_row, &is_deleted) == 0) {
        if (!is_deleted) {
          cur_blk_item->setDeleted(delete_row.offset_row);
        }
      }
    }
  }
}

int TsTimePartition::PrepareDup(kwdbts::DedupRule dedup_rule, uint32_t entity_id,
                                   const std::vector<KwTsSpan>& ts_spans) {
  if (dedup_rule == kwdbts::DedupRule::OVERRIDE) {
    uint64_t cnt = 0;
    ErrorInfo err_info;
    return DeleteData(entity_id, 0, ts_spans, nullptr, &cnt, err_info, false);
  }
  return 0;
}

const size_t PAYLOAD_VARCOLUMN_TUPLE_LEN = 8;

int TsTimePartition::updatePayload(kwdbts::Payload* payload, kwdbts::DedupInfo& dedup_info) {
  if (payload->dedup_rule_ != DedupRule::MERGE) {
    // no need merge.
    return 0;
  }
  const std::vector<AttributeInfo>& schema_info = payload->GetSchemaInfo();
  const std::vector<uint32_t>& payload_valid_cols = payload->GetValidCols();
  for (auto& kv : dedup_info.payload_rows) {
    auto cur_ts = kv.first;
    auto &payload_dedup = kv.second;
    auto merge_row = payload_dedup.back();
    for (size_t column = 0; column < payload_valid_cols.size(); ++column) {
      if (!payload->IsNull(column, merge_row)) {
        // column value is not null, so no need merge from duplicate rows.
        continue;
      }
      if (payload_dedup.size() > 1) {
        // payload has duplicate rows of this ts.
        for (int i = payload_dedup.size() - 1; i >= 0; i--) {
          // find begin from the latest rows.
          if (payload_dedup[i] >= merge_row || payload->IsNull(column, payload_dedup[i])) {
            continue;
          }
          if (schema_info[column].type != DATATYPE::VARSTRING && schema_info[column].type != DATATYPE::VARBINARY) {
            memcpy(payload->GetColumnAddr(merge_row, column),
                  payload->GetColumnAddr(payload_dedup[i], column),
                schema_info[column].size);
          } else {
            memcpy(payload->GetColumnAddr(merge_row, column),
                  payload->GetColumnAddr(payload_dedup[i], column), PAYLOAD_VARCOLUMN_TUPLE_LEN);
          }
          // The merged column now contains values; the bitmap needs to be updated accordingly
          setRowValid(payload->GetNullBitMapAddr(column), merge_row + 1);
          break;
        }
      }
      if (!payload->IsNull(column, merge_row)) {
        continue;
      }
      // find from table entity, start from latest.
      if (dedup_info.table_real_rows.find(cur_ts) != dedup_info.table_real_rows.end()) {
        for (int i = dedup_info.table_real_rows[cur_ts].size() - 1; i >= 0; i--) {
          // The actual row number of the stored data
          MetricRowID dup_row = dedup_info.table_real_rows[cur_ts][i];
          if (dup_row.block_id == 0) {
            continue;
          }
          std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(dup_row.block_id);
          if (segment_tbl == nullptr) {
            LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(dup_row.block_id)).c_str());
            return -1;
          }
          if (segment_tbl->isNullValue(dedup_info.table_real_rows[cur_ts][i], payload_valid_cols[column])) {
            continue;
          }
          // Verify the column types for compatibility, and perform type conversion if they do not match
          if (segment_tbl->getSchemaInfo()[column].type != schema_info[column].type
              || segment_tbl->getSchemaInfo()[column].size != schema_info[column].size) {
            MergeValueInfo merge_value;
            merge_value.attr = segment_tbl->getSchemaInfo()[payload_valid_cols[column]];
            if (merge_value.attr.type == VARSTRING || merge_value.attr.type == VARBINARY) {
              merge_value.value = segment_tbl->varColumnAddr(dedup_info.table_real_rows[cur_ts][i], payload_valid_cols[column]);
            } else {
              void* segment_merge_data = segment_tbl->columnAddr(dedup_info.table_real_rows[cur_ts][i], payload_valid_cols[column]);
              CopyFixedData(static_cast<DATATYPE>(segment_tbl->getSchemaInfo()[payload_valid_cols[column]].type),
                            static_cast<char*>(segment_merge_data), &merge_value.value);
            }
            if (payload->tmp_col_values_4_dedup_merge_.find(column) == payload->tmp_col_values_4_dedup_merge_.end()) {
              payload->tmp_col_values_4_dedup_merge_.insert({column, {}});
            }
            payload->tmp_col_values_4_dedup_merge_[column].insert({merge_row, merge_value});
          } else {
            if (schema_info[column].type != DATATYPE::VARSTRING && schema_info[column].type != DATATYPE::VARBINARY) {
              memcpy(payload->GetColumnAddr(merge_row, column),
                    segment_tbl->columnAddr(dedup_info.table_real_rows[cur_ts][i], payload_valid_cols[column]),
                schema_info[column].size);
            } else {
              MergeValueInfo merge_value;
              merge_value.attr = segment_tbl->getSchemaInfo()[payload_valid_cols[column]];
              merge_value.value = segment_tbl->varColumnAddr(dedup_info.table_real_rows[cur_ts][i], payload_valid_cols[column]);
              if (payload->tmp_col_values_4_dedup_merge_.find(column) == payload->tmp_col_values_4_dedup_merge_.end()) {
                payload->tmp_col_values_4_dedup_merge_.insert({column, {}});
              }
              payload->tmp_col_values_4_dedup_merge_[column].insert({merge_row, merge_value});
            }
          }
          // The merged column now contains values; the bitmap needs to be updated accordingly
          setRowValid(payload->GetNullBitMapAddr(column), merge_row + 1);
          break;
        }
      }
    }
  }
  return 0;
}

int TsTimePartition::updatePayloadUsingDedup(uint32_t entity_id, const BlockSpan& first_span,
                                             kwdbts::Payload* payload, size_t start_in_payload, size_t num,
                                             kwdbts::DedupInfo& dedup_info) {
  int err_code = 0;
  // If the deduplication rule for the payload is KEEP, it will be returned directly
  if (payload->dedup_rule_ == DedupRule::KEEP) {
    return err_code;
  }
  bool ts_with_lsn = isTsWithLSNType((DATATYPE)(payload->GetSchemaInfo()[0].type));

  void* ts_begin = payload->GetColumnAddr(0, 0);
  dedup_info.need_dup = true;
  // Search for data elements that require updating, and handle duplicate data in accordance with the deduplication pattern
  for (int i = 0; i < num; ++i) {
    timestamp64 current_ts = *(reinterpret_cast<timestamp64*>((intptr_t) (ts_begin)
                             + (start_in_payload + i) * payload->GetSchemaInfo()[0].size));
    // If it is in REJECT deduplication mode and duplicate lines are found, an error code will be returned
    if (payload->dedup_rule_ == DedupRule::REJECT && dedup_info.payload_rows.find(current_ts) != dedup_info.payload_rows.end()) {
      LOG_ERROR("payload has duplicated rows. ts %lu", current_ts);
      // payload has duplicated rows. in reject mode, need failed.
      return KWEDUPREJECT;
    }
    // Record the information of duplicate rows
    dedup_info.payload_rows[current_ts].push_back(start_in_payload + i);
  }
  // If there is no need to scan the table, simply return
  if (dedup_info.need_scan_table) {
    // find all duplicate rows.
    // Within the specified BlockSpan range, retrieve unique rows that fall within a specific timestamp range.
    err_code = GetDedupRows(entity_id, first_span, dedup_info, ts_with_lsn);
    if (err_code < 0) {
      return err_code;
    }
  }
  // If the deduplication rule is MERGE and the current payload column is empty,
  // traverse the duplicate data to find the latest duplicate, and update the payload.
  // When updating the payload, it is necessary to check for any type changes in the merge data and perform type conversion.
  // update payload.
  err_code = updatePayload(payload, dedup_info);
  if (err_code < 0) {
    return err_code;
  }
  return err_code;
}

int TsTimePartition::GetDedupRowsInBlk(uint entity_id, BlockItem* block_item, int read_count, kwdbts::DedupInfo& dedup_info, bool has_lsn) {

  for (uint32_t i = 1; i <= read_count; i++) {
    bool is_deleted;
    if (block_item->isDeleted(i, &is_deleted) < 0) {
      LOG_ERROR("BlockItem::isDeleted failed");
      return KWELENLIMIT;
    }
    if (is_deleted) {
      continue;
    }
    MetricRowID real_row = block_item->getRowID(i);

    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (!segment_tbl) {
      LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      return KWENOOBJ;
    }

    // Retrieve the timestamp address of the current row and parse it
    char* ts_addr = reinterpret_cast<char*>(segment_tbl->columnAddr(real_row, 0));
    timestamp64 cur_ts = KTimestamp(ts_addr);
    // If the current timestamp is already present in the deduplication information, record the ID of the row
    if (dedup_info.payload_rows.find(cur_ts) != dedup_info.payload_rows.end()) {
      dedup_info.table_real_rows[cur_ts].push_back(real_row);
    }
  }
  return 0;
}

void TsTimePartition::waitBlockItemDataFilled(uint entity_id, BlockItem* block_item, int read_count, bool has_lsn) {
  time_t begin_time = time(nullptr);
  // If the number of allocated rows equals the number of published rows, return immediately
  if (block_item->alloc_row_count == block_item->publish_row_count) {
    return;
  }
  size_t cur_read_rownum = 1;
  std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
  if (!segment_tbl) {
    LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
    return;
  }
  MetricRowID real_row = block_item->getRowID(cur_read_rownum);
  // When the number of allocated rows in a Block differs from the number of published rows, and the current row's
  // offset_row does not exceed the expected number of rows to be read, a loop check is performed.
  while (block_item->alloc_row_count != block_item->publish_row_count && real_row.offset_row < read_count) {
    bool move_step = false;
    char* ts_addr = reinterpret_cast<char*>(segment_tbl->columnAddr(real_row, 0));
    // If an LSN is present, verify that it is greater than 0; if not, ascertain if the timestamp is greater than 0
    if (has_lsn) {
      uint64_t lsn = KUint64(ts_addr + 8);
      if (lsn > 0) {
        move_step = true;
      }
    } else {
      if (KTimestamp(ts_addr) > 0) {  // timestampe is set, so data is inserted ok.
        move_step = true;
      }
    }
    // If the current row's waiting time exceeds 10 seconds, it is marked as deleted
    if (time(nullptr) - begin_time > 10) {  // check overtime, mark row deleted.
      SetDeleted(entity_id);
      block_item->setDeleted(cur_read_rownum);
      LOG_WARN("check lsn overtime, mark this row deleted. entity:%u, block:%u, row: %u", entity_id,
               real_row.block_id, real_row.offset_row);
      move_step = true;
    }
    // If it is necessary to move to the next row, update the current row number and row ID
    if (move_step) {
      real_row.offset_row++;
      cur_read_rownum++;
    } else {
      // Otherwise, the thread is relinquished to avoid excessive CPU consumption
      std::this_thread::yield();
    }
  }
}

int TsTimePartition::GetDedupRows(uint entity_id, const BlockSpan& first_span, kwdbts::DedupInfo& dedup_info, bool has_lsn) {
  int err_code = 0;
  timestamp64 ts_min = INT64_MAX, ts_max = INT64_MIN;
  // filter blockItem with payload min and max ts.
  for (auto kv : dedup_info.payload_rows) {
    if (kv.first > ts_max) {
      ts_max = kv.first;
    }
    if (kv.first < ts_min) {
      ts_min = kv.first;
    }
  }
  const KwTsSpan& payload_span = {ts_min, ts_max};
  std::deque<BlockItem*> block_items;
  // Get all the block items belonging to the specified entity_id.
  meta_manager_.GetAllBlockItems(entity_id, block_items, true);

  auto has_disorder_row = getEntityItem(entity_id)->is_disordered;

  // Traverse each block item to determine if there are any duplicate records that need to be removed.
  for (size_t i = 0; i < block_items.size(); i++) {
    BlockItem* block_item = block_items[i];
    if (block_item->block_id > first_span.block_item->block_id) {
      continue;
    }
    int read_count = block_item->publish_row_count;
    // If the current block item matches the specified first span, update the number of records read.
    if (block_item == first_span.block_item) {
      read_count = first_span.start_row;
    }
    if (read_count == 0) {
      continue;
    }
    // Wait for the block item to be filled completely.
    waitBlockItemDataFilled(entity_id, block_item, read_count, has_lsn);
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
    if (!segment_tbl) {
      LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
      return KWENOOBJ;
    }
    // Check if the timestamp range of the block item overlaps with the timestamps of the deduplication information.
    timestamp64 block_min_ts, block_max_ts;
    TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
    const KwTsSpan& ts_span = {block_min_ts, block_max_ts};
    if (ts_span.begin > payload_span.end || ts_span.end < payload_span.begin) {
      // no match rows. so no need do anything.
      if (ts_span.end < payload_span.begin && !has_disorder_row) {
        // if entity all row ts is ordered. no need scan before rows.
        return err_code;
      }
    } else {
      // If there are records that can be read from the block item, obtain the deduplication information for these records.
      err_code = GetDedupRowsInBlk(entity_id, block_item, read_count, dedup_info, has_lsn);
    }
    // Encounter an error or find the latest block item, then return the operational results.
    if (err_code < 0) {
      return err_code;
    }
  }
  return err_code;
}

int TsTimePartition::CopyFixedData(DATATYPE old_type, char* old_mem, std::shared_ptr<void>* new_mem) {
  switch (old_type) {
    case DATATYPE::INT16 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int16_t)));
      memcpy(data, old_mem, sizeof(int16_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::INT32 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int32_t)));
      memcpy(data, old_mem, sizeof(int32_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::INT64 : {
      char* data = static_cast<char*>(std::malloc(sizeof(int64_t)));
      memcpy(data, old_mem, sizeof(int64_t));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::FLOAT : {
      char* data = static_cast<char*>(std::malloc(sizeof(float)));
      memcpy(data, old_mem, sizeof(float));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::DOUBLE : {
      char* data = static_cast<char*>(std::malloc(sizeof(double)));
      memcpy(data, old_mem, sizeof(double));
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      char* data = static_cast<char*>(std::malloc(strlen(old_mem) + 1));
      strcpy(data, old_mem);
      std::shared_ptr<void> ptr(data, free);
      *new_mem = ptr;
      break;
    }
    default:
      break;
  }
  return 0;
}

int TsTimePartition::ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size, void* old_mem,
                                             std::shared_ptr<void> old_var_mem, std::shared_ptr<void>* new_mem) {
  ErrorInfo err_info;
  if (!isVarLenType(new_type)) {
    void* temp_new_mem = malloc(new_type_size + 1);
    memset(temp_new_mem, 0, new_type_size + 1);
    if (!isVarLenType(old_type)) {
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, (char*)old_mem, (char*)temp_new_mem, err_info);
      } else {
        err_info.errcode = convertFixedToNum(old_type, new_type, (char*) old_mem, (char*) temp_new_mem, err_info);
      }
      if (err_info.errcode < 0) {
        free(temp_new_mem);
        return err_info.errcode;
      }
    } else {
      uint16_t var_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
      std::string var_value((char*)old_var_mem.get() + MMapStringColumn::kStringLenLen);
      convertStrToFixed(var_value, new_type, (char*) temp_new_mem, var_len, err_info);
    }
    std::shared_ptr<void> ptr(temp_new_mem, free);
    *new_mem = ptr;
  } else {
    if (!isVarLenType(old_type)) {
      auto cur_var_data = convertFixedToVar(old_type, new_type, (char*)old_mem, err_info);
      *new_mem = cur_var_data;
    } else {
      if (old_type == VARSTRING) {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get()) - 1;
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen);
        *reinterpret_cast<uint16_t*>(var_data) = old_len;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      } else {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen + 1));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen + 1);
        *reinterpret_cast<uint16_t*>(var_data) = old_len + 1;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      }
    }
  }
  return 0;
}

string convertFixedToStr(DATATYPE old_type, char* data, ErrorInfo& err_info) {
  std::string res;
  switch (old_type) {
    case DATATYPE::INT16 : {
      res = std::to_string(KInt16(data));
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(data));
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(data));
      break;
    }
    case DATATYPE::FLOAT : {
      std::ostringstream oss;
      oss.clear();
      oss.precision(7);
      oss << KFloat32(data);
      res = oss.str();
      break;
    }
    case DATATYPE::DOUBLE : {
      std::stringstream ss;
      ss << std::setprecision(16) << KDouble64(data);
      res = ss.str();
      break;
    }
    case DATATYPE::CHAR:
    case DATATYPE::BINARY: {
      string cur_s(data, strlen(data));
      res = cur_s;
      break;
    }
    default:
      err_info.setError(KWEPERM, "Incorrect integer value");
      break;
  }
  return res;
}

int TsTimePartition::tryAlterType(const std::string& str, DATATYPE new_type, ErrorInfo& err_info) {
  std::size_t pos{};
  int res = 0;
  std::string incorrect_str = "Incorrect integer value";
  std::string value_str = " '" + str + "'";
  try {
    switch (new_type) {
      case DATATYPE::INT16 : {
        res = std::stoi(str, &pos);
        break;
      }
      case DATATYPE::INT32 : {
        auto value = std::stoi(str, &pos);
        break;
      }
      case DATATYPE::INT64 : {
        auto value = std::stoll(str, &pos);
        break;
      }
      case DATATYPE::FLOAT : {
        incorrect_str = "Incorrect floating value";
        auto value = std::stof(str, &pos);
        break;
      }
      case DATATYPE::DOUBLE : {
        incorrect_str = "Incorrect floating value";
        auto value = std::stod(str, &pos);
        break;
      }
      default:
        break;
    }
  }
  catch (std::invalid_argument const &ex) {
    return err_info.setError(KWEPERM, incorrect_str + value_str);
  }
  catch (std::out_of_range const &ex) {
    return err_info.setError(KWEPERM, "Out of range value" + value_str);
  }
  if (pos < str.size()) {
    return err_info.setError(KWEPERM, incorrect_str + value_str);
  }
  if (new_type == DATATYPE::INT16) {
    if (res > INT16_MAX || res < INT16_MIN) {
      return err_info.setError(KWEPERM, "Out of range value" + value_str);
    }
  }
  return err_info.errcode;
}

int TsTimePartition::FindFirstBlockItem(uint32_t entity_id, kwdbts::TS_LSN lsn, timestamp64 start_payload_ts,
                                        std::deque<BlockItem*>& block_items,
                                        BlockItem** blk_item, uint32_t* block_start_row,
                                        std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map,
                                        KTimestamp p_time) {
  EntityItem* entity_item = getEntityItem(entity_id);
  if (entity_item->is_deleted) {
    return 0;
  }
  if (entity_item->cur_block_id == 0) {
    // This indicates that no data has been written and a normal request to block has been made
    return 0;
  }
  BLOCK_ID pre_block_id = 0;
  auto iter = partition_ts_map->find(p_time);
  if (iter != partition_ts_map->end()) {
    pre_block_id = iter->second.block_id;
  }
  if (start_payload_ts <= entity_item->max_ts && start_payload_ts >= entity_item->min_ts &&
      entity_item->max_ts != INVALID_TS) {
    // Initiate a search from the start of the block queue. If the timestamps and LSNs within a block are identical,
    // it signifies that data has already been written, allowing the reuse of the current block,
    // and the associated starting position should be noted. If no match is found, continue the traversal.
    // If data is retrieved from the map, it implies that this batch of payloads was previously written to
    // the current partition. To prevent retrieving duplicate data,
    // initiate the search from the starting position documented in the map.
    GetAllBlockItems(entity_id, block_items);
    uint32_t offset_row = 1;
    timestamp64 block_min_ts, block_max_ts;
    while (!block_items.empty()) {
      auto block_item = block_items.front();
      block_items.pop_front();
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Segment [%s] is null", (db_path_ + segment_tbl_sub_path(block_item->block_id)).c_str());
        return KWENOOBJ;
      }
      TsTimePartition::GetBlkMinMaxTs(block_item, segment_tbl.get(), block_min_ts, block_max_ts);
      KwTsSpan ts_span = {block_min_ts, block_max_ts};
      if (!(start_payload_ts >= ts_span.begin && start_payload_ts <= ts_span.end) ||
          pre_block_id > block_item->block_id) {
        continue;
      }
      if (pre_block_id == block_item->block_id) {
        offset_row = iter->second.offset_row;
      }
      for (uint32_t j = offset_row; j <= block_item->alloc_row_count; j++) {
        MetricRowID real_row = block_item->getRowID(j);
        TimeStamp64LSN* ts_lsn = reinterpret_cast<TimeStamp64LSN*>(segment_tbl->columnAddr(real_row, 0));
        if (start_payload_ts == ts_lsn->ts64 && lsn == ts_lsn->lsn) {
          *blk_item = block_item;
          *block_start_row = j - 1;
          return 0;
        }
      }
    }
  }
  block_items.clear();
  // There may be a situation where space is requested but no data is written.
  // Determine whether this is the case based on publish_row_count and alloc_row_count.
  GetAllBlockItems(entity_id, block_items);
  while (!block_items.empty()) {
    auto block_item = block_items.front();
    block_items.pop_front();
    if (block_item->alloc_row_count == 0 || block_item->publish_row_count < block_item->alloc_row_count) {
      *blk_item = block_item;
      *block_start_row = block_item->publish_row_count;
      return 0;
    }
  }
  return 0;
}

int TsTimePartition::GetAllBlockSpans(uint32_t entity_id, std::vector<KwTsSpan>& ts_spans,
                                         std::deque<BlockSpan>& block_spans, uint32_t max_block_id, bool reverse) {
  std::deque<BlockItem*> block_queue;
  GetAllBlockItems(entity_id, block_queue);
  EntityItem *entity = getEntityItem(entity_id);

  uint32_t count = 0;
  if (!entity->is_disordered) {
    while (!block_queue.empty()) {
      BlockItem* block_item = block_queue.front();
      block_queue.pop_front();
      std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
      if (segment_tbl == nullptr) {
        LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, GetPath().c_str());
        return -1;
      }

      timestamp64 min_ts, max_ts;
      GetBlkMinMaxTs(block_item, segment_tbl.get(), min_ts, max_ts);
      if (!isTimestampInSpans(ts_spans, min_ts, max_ts) || !block_item->publish_row_count) {
        continue;
      }

      uint32_t first_row = 1;
      bool all_within_spans = isTimestampWithinSpans(ts_spans, min_ts, max_ts);
      for (uint32_t i = 1; i <= block_item->publish_row_count; ++i) {
        MetricRowID row_id = block_item->getRowID(i);
        timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
        if ((!all_within_spans && !CheckIfTsInSpan(cur_ts, ts_spans)) || !segment_tbl->IsRowVaild(block_item, i)) {
          if (i > first_row) {
            reverse ? block_spans.push_front({block_item, first_row - 1, i - first_row}) :
                      block_spans.push_back({block_item, first_row - 1, i - first_row});
            count += i - first_row;
          }
          first_row = i + 1;
          continue;
        }
      }

      if (first_row <= block_item->publish_row_count) {
        reverse ? block_spans.push_front({block_item, first_row - 1, block_item->publish_row_count - first_row + 1}) :
                  block_spans.push_back({block_item, first_row - 1, block_item->publish_row_count - first_row + 1});
        count += block_item->publish_row_count - first_row + 1;
      }
    }
    return count;
  }

  vector<vector<timestamp64>> intervals;
  map<pair<timestamp64, timestamp64>, vector<BlockItem*>> interval_block_map;
  while (!block_queue.empty()) {
    BlockItem* cur_block = block_queue.front();
    block_queue.pop_front();
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(cur_block->block_id);
    timestamp64 min_ts, max_ts;
    GetBlkMinMaxTs(cur_block, segment_tbl.get(), min_ts, max_ts);
    if (isTimestampInSpans(ts_spans, min_ts, max_ts) && cur_block->publish_row_count > 0) {
      if (!interval_block_map.count({min_ts, max_ts})) {
        intervals.push_back({min_ts, max_ts});
      }
      interval_block_map[{min_ts, max_ts}].push_back(cur_block);
    }
  }

  sort(intervals.begin(), intervals.end());
  vector<pair<pair<timestamp64, timestamp64>, vector<BlockItem*>>> block_items;
  for (auto interval : intervals) {
    if (block_items.empty() || interval[0] >= block_items.back().first.second) {
      block_items.push_back({{interval[0], interval[1]}, {interval_block_map[{interval[0], interval[1]}]}});
    } else {
      if (interval[1] > block_items.back().first.second) {
        block_items.back().first.second = interval[1];
      }
      block_items.back().second.insert(block_items.back().second.end(),
                                       interval_block_map[{interval[0], interval[1]}].begin(),
                                       interval_block_map[{interval[0], interval[1]}].end());
    }
  }
  intervals.clear();
  interval_block_map.clear();

  for (auto it : block_items) {
    vector<BlockItem*> blocks = it.second;
    if (blocks.size() == 1 && !blocks[0]->unordered_flag &&
        !hasDeleted(blocks[0]->rows_delete_flags, 1, blocks[0]->publish_row_count)) {
      BlockSpan block_span = BlockSpan{blocks[0], 0, blocks[0]->publish_row_count};
      reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
      count += block_span.row_num;
    } else {
      std::multimap<timestamp64, MetricRowID> ts_order;  // Save every undeleted row_id under each timestamp
      for (auto block : blocks) {
        if (block->block_id > max_block_id) {
          // When vacuum calls this function, the max_block_id passed in indicates that the traversal is over
          // at the end of the block, and the other time, max_block_id = INT32_MAX.
          break;
        }
        std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block->block_id);
        if (segment_tbl == nullptr) {
          LOG_ERROR("Can not find segment use block [%u], in path [%s]", block->block_id, GetPath().c_str());
          return -1;
        }

        bool all_within_spans = isTimestampWithinSpans(ts_spans,
                                   KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MIN)),
                                   KTimestamp(segment_tbl->columnAggAddr(block->block_id, 0, Sumfunctype::MAX)));

        for (uint32_t i = 1; i <= block->publish_row_count; ++i) {
          MetricRowID row_id = block->getRowID(i);
          timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddr(row_id, 0));
          if (all_within_spans || (CheckIfTsInSpan(cur_ts, ts_spans))) {
            if (!segment_tbl->IsRowVaild(block, i)) {
              continue;
            }
            ts_order.insert(std::make_pair(cur_ts, row_id));
          }
        }
      }
      BlockSpan block_span;
      MetricRowID last_row_id;
      for (auto& iter : ts_order) {
        // Convert a time-sorted row_id into a BlockSpan that represents the continuity of the data
        MetricRowID& cur_row_id = iter.second;
        if (last_row_id + 1 != cur_row_id) {
          // When data continuity is interrupted,
          // the data that has just been checked to be continuous is stored and the next check is started
          if (block_span.row_num > 0) {
            reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
            count += block_span.row_num;
          }
          block_span = BlockSpan{GetBlockItem(cur_row_id.block_id), cur_row_id.offset_row - 1, 1};
        } else {
          block_span.row_num++;
        }
        last_row_id = cur_row_id;
      }

      if (block_span.row_num > 0) {
        reverse ? block_spans.push_front(block_span) : block_spans.push_back(block_span);
        count += block_span.row_num;
      }
    }
  }
  return count;
}

void TsTimePartition::ChangeSegmentStatus() {
  // Stop writing of the segment when vacuum
  MUTEX_LOCK(active_segment_lock_);
  Defer defer{[&]() { MUTEX_UNLOCK(active_segment_lock_); }};

  if (active_segment_ != nullptr) {
    active_segment_->setSegmentStatus(InActiveSegment);
    active_segment_ = nullptr;
  }
  // Set all the segments of this partition to InActiveSegment
  data_segments_.Traversal([&](BLOCK_ID s_id, const std::shared_ptr<MMapSegmentTable>& tbl) -> bool {
    if (tbl->sqfsIsExists()) {
      return true;
    }
    // reopen will change status to OBJ_READY, if not do this, setSegmentStatus will crash
    // because TsTableMMapObject::meta_data_ is nullptr
    if (tbl->getObjectStatus() != OBJ_READY) {
      ErrorInfo err_info;
      if (tbl->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), tbl->segment_id(), name_ + ".bt",
                     db_path_, tbl_sub_path_ + std::to_string(tbl->segment_id()) + "/", MMAP_OPEN_NORECURSIVE,
                     false, err_info) < 0) {
        LOG_ERROR("MMapSegmentTable[%s] open failed", tbl->realFilePath().c_str());
        return true;
      }
    }

    if (tbl->getSegmentStatus() < InActiveSegment) {
      tbl->setSegmentStatus(InActiveSegment); // include original active_segment_
    }
    return true;
  });
}

int TsTimePartition::DropSegmentDir(const std::vector<BLOCK_ID>& segment_ids) {
  for (unsigned int segment_id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> segment_table;
    BLOCK_ID key;
    bool ret = data_segments_.Seek(segment_id, key, segment_table);
    if (ret) {
      // try umount segment
      int error_code = segment_table->try_umount();
      if (error_code < 0) {
        LOG_ERROR("try umount segment[%s] failed", segment_table->GetPath().c_str());
        return error_code;
      }
      LOG_INFO("try umount segment[%s] succeed", segment_table->GetPath().c_str());
    }
  }
  for (unsigned int segment_id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> segment_table;
    BLOCK_ID key;
    bool ret = data_segments_.Seek(segment_id, key, segment_table);
    if (ret) {
      // set all blockitem in segment invalid.
      auto block_id = segment_id;
      while (true) {
        std::shared_ptr<MMapSegmentTable> segment_block;
        BLOCK_ID key_block;
        if (data_segments_.Seek(block_id, key_block, segment_block)) {
          if (segment_block.get() != segment_table.get()) {
            break;
          }
        } else {
          LOG_INFO("cannot found segment for block[%u]", block_id);
          break;
        }
        memset(GetBlockItem(block_id), 0, sizeof(BlockItem));
        block_id++;
        if (block_id > meta_manager_.getEntityHeader()->cur_block_id) {
          break;
        }
      }
      // remove segment
      int error_code = segment_table->remove();
      if (error_code < 0) {
        LOG_ERROR("remove segment[%s] failed", segment_table->GetPath().c_str());
        return error_code;
      }
      LOG_INFO("remove segment[%s] succeed", segment_table->GetPath().c_str());
    }
  }
  return 0;
}

KStatus TsTimePartition::GetMigrateFileList(std::vector<std::string>* files) {
  ErrorInfo err_info;
  data_segments_.Traversal([&](BLOCK_ID segment_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->sqfsIsExists()) {
      files->push_back(tbl->getCompressedFilePath());
    } else {
      files->push_back(tbl->GetPath());
    }
    return true;
  });
  auto metas = meta_manager_.GetMetaFileNum();
  for (size_t i = 0; i < metas; i++) {
    files->push_back(GetPath() + name_ + ".meta." + intToString(i));
  }
  return KStatus::SUCCESS;
}

bool TsTimePartition::IsAllSegCompressed() {
  ErrorInfo err_info;
  bool exist_no_compressed = false;
  data_segments_.Traversal([&](BLOCK_ID segment_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (!tbl->sqfsIsExists()) {
      exist_no_compressed = true;
      return false;
    }
    return true;
  });
  return !exist_no_compressed;
}


KStatus TsTimePartition::Migrate(int from_level, int to_level) {
  return KStatus::SUCCESS;
}

bool TsTimePartition::Unmount() {
  // umount all segment.
  ErrorInfo err_info;
  bool failed = false;
  data_segments_.Traversal([&](BLOCK_ID segment_id, std::shared_ptr<MMapSegmentTable> tbl) -> bool {
    if (tbl->close(err_info) < 0) {
      failed = true;
      return false;
    }
    if (!tbl->TryUnmountSqfs(err_info)) {
      failed = true;
      return false;
    }
    return true;
  });
  return !failed;
}

bool TsTimePartition::IsSegmentsBusy(const std::vector<BLOCK_ID>& segment_ids) {
  for (auto id : segment_ids) {
    std::shared_ptr<MMapSegmentTable> value;
    BLOCK_ID key;
    bool res = data_segments_.Seek(id, key, value);
    if (res) {
      if (value.use_count() == 2) {
        continue;
      } else if (value.use_count() > 2) {
        LOG_WARN("segment[%s] is using, cancel vacuum", value->GetPath().c_str());
        return true;
      }
    }
  }
  return false;
}

KStatus TsTimePartition::Count() {
  std::vector<uint32_t> entities = meta_manager_.getEntities();
  for (auto entity_id: entities) {
    TsHashLatch* entity_item_latch = GetEntityItemLatch();
    entity_item_latch->Lock(entity_id);
    Defer defer{[&]() { entity_item_latch->Unlock(entity_id); }};
    EntityItem* entity_item = getEntityItem(entity_id);
    int64_t entity_count = 0;
    std::deque<BlockItem*> block_item_queue_;
    BLOCK_ID entity_cur_block_id = entity_item->cur_block_id;
    BLOCK_ID entity_count_block_id = entity_item->count_block_id;
    if (entity_count_block_id != 0) {
      entity_count = entity_item->row_written;
      meta_manager_.GetCountBlockItems(entity_id, entity_count_block_id, block_item_queue_);
    } else {
      meta_manager_.GetAllBlockItems(entity_id, block_item_queue_);
    }
    while (!block_item_queue_.empty()) {
      BlockItem* block_item = block_item_queue_.front();
      block_item_queue_.pop_front();
      if (block_item->publish_row_count < block_item->max_rows_in_block) {
        std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block_item->block_id);
        if (segment_tbl == nullptr) {
          LOG_ERROR("Can not find segment use block [%u], in path [%s]", block_item->block_id, GetPath().c_str());
          return KStatus::FAIL;
        }
        if (segment_tbl->getSegmentStatus() != ImmuSegment) {
          break;
        }
      }
      entity_count += block_item->getNonNullRowCount();
      entity_count_block_id = block_item->block_id;
    }
    entity_item->row_written = entity_count;
    entity_item->count_block_id = entity_count_block_id;
    block_item_queue_.clear();
  }
  return SUCCESS;
}
