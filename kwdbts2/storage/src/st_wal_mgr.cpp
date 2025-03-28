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

#include "st_wal_mgr.h"
#include "st_wal_internal_logfile_mgr.h"
#include "sys_utils.h"

namespace kwdbts {

WALMgr::WALMgr(const string& db_path, const KTableKey& table_id, uint64_t entity_grp_id, EngineOptions* opt) :
    db_path_(db_path),
    table_id_(table_id),
    entity_grp_id_(entity_grp_id),
    opt_(opt) {
  if (table_id == 0) {
    wal_path_ = db_path_ + "/wal/";
  } else {
    wal_path_ = db_path_ + std::to_string(table_id_) + "/" + std::to_string(entity_grp_id) + "/wal/";
  }

  file_mgr_ = KNEW WALFileMgr(wal_path_, table_id_, opt);
  buffer_mgr_ = KNEW WALBufferMgr(opt, file_mgr_);
  meta_mutex_ = KNEW WALMgrLatch(LATCH_ID_WALMGR_META_MUTEX);
}

WALMgr::~WALMgr() {
  Close();
  if (buffer_mgr_ != nullptr) {
    delete buffer_mgr_;
    buffer_mgr_ = nullptr;
  }

  if (file_mgr_ != nullptr) {
    delete file_mgr_;
    file_mgr_ = nullptr;
  }
  if (meta_mutex_ != nullptr) {
    delete meta_mutex_;
    meta_mutex_ = nullptr;
  }
}

KStatus WALMgr::Create(kwdbContext_p ctx) {
  if (!IsExists(wal_path_)) {
    if (!MakeDirectory(wal_path_)) {
      LOG_ERROR("Failed to create the WAL log directory '%s'", wal_path_.c_str())
      return FAIL;
    }
  }

  KStatus s = initWalMeta(ctx, true);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL metadata.")
    return s;
  }

  TS_LSN current_lsn = FetchCurrentLSN();

  s = file_mgr_->initWalFile(0, current_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL file.")
    return s;
  }

  s = buffer_mgr_->init(current_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL buffer.")
    return s;
  }

  return SUCCESS;
}

KStatus WALMgr::Init(kwdbContext_p ctx) {
  if (!IsExists(wal_path_)) {
    return Create(ctx);
  }

  KStatus s;
  s = initWalMeta(ctx, false);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL metadata.")
    return s;
  }

  TS_LSN current_lsn = FetchCurrentLSN();

  s = file_mgr_->Open(meta_.current_file_no);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to open the WAL file with file num %d", meta_.current_file_no)
    s = file_mgr_->initWalFile(meta_.current_file_no, current_lsn);
    if (s == KStatus::FAIL) {
      LOG_ERROR("Failed to init a WAL file with file num %d", meta_.current_file_no)
      return s;
    }
  }

  s = buffer_mgr_->init(current_lsn);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL buffer with LSN %lu", current_lsn)
    return s;
  }

  return SUCCESS;
}

KStatus WALMgr::writeWALInternal(kwdbContext_p ctx, k_char* wal_log, size_t length, TS_LSN& entry_lsn) {
  TS_LSN lsn_offset = 0;
  KStatus s = buffer_mgr_->writeWAL(ctx, wal_log, length, lsn_offset);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to write the WAL log to WAL buffer, payload length %lu", length)
    size_t wal_file_size = opt_->wal_file_size << 20;
    if (length > wal_file_size) {
      LOG_ERROR("Payload length must less than cluster setting ts.wal.file_size %lu", wal_file_size)
    }
    return s;
  }
  // record current log LSN and update meta_.current_lsn(NEXT LSN)
  entry_lsn = meta_.current_lsn;
  meta_.current_lsn = lsn_offset;
  meta_.current_file_no = file_mgr_->GetCurrentFileNo();

  // TODO(xy): optimize:if WAL LEVEL=SYNC, don't need sync every log to disk, only sync by FLUSH while COMMIT/ROLLBACK.
  if (WALMode(opt_->wal_level) == WALMode::SYNC) {
    if (Flush(ctx) == KStatus::FAIL) {
      LOG_ERROR("Failed to flush the WAL logs on SYNC level, wal length %lu", length)
      return KStatus::FAIL;
    }
  }

  if (eg_ != nullptr && NeedCheckpoint()) {
    LOG_DEBUG("WAL file is full, Force CreateCheckpoint.");
    eg_->CreateCheckpointInternal(ctx);
  }

  return SUCCESS;
}

KStatus WALMgr::WriteWAL(kwdbContext_p ctx, k_char* wal_log, size_t length, TS_LSN& entry_lsn) {
  this->Lock();
  KStatus s = writeWALInternal(ctx, wal_log, length, entry_lsn);
  this->Unlock();
  return (s);
}

KStatus WALMgr::WriteWAL(kwdbContext_p ctx, k_char* wal_log, size_t length) {
  TS_LSN current_lsn = 0;
  return WriteWAL(ctx, wal_log, length, current_lsn);
}

KStatus WALMgr::Flush(kwdbContext_p ctx) {
  KStatus s = buffer_mgr_->flush();
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to flush the WAL logs")
    return s;
  }

  meta_.block_flush_to_disk_lsn = meta_.current_lsn;
  return flushMeta(ctx);
}

KStatus WALMgr::CreateCheckpoint(kwdbContext_p ctx) {
  // 5 update lsn
  meta_.current_checkpoint_no++;
  buffer_mgr_->setHeaderBlockCheckpointInfo(meta_.current_lsn, meta_.current_checkpoint_no);
  meta_.checkpoint_lsn = meta_.current_lsn;
  meta_.checkpoint_file_no = meta_.current_file_no;
  // 6 flush log buffer to disk
  Flush(ctx);

  return SUCCESS;
}

KStatus WALMgr::CreateCheckpointWithoutFlush(kwdbts::kwdbContext_p ctx) {
  meta_.current_checkpoint_no++;
  buffer_mgr_->setHeaderBlockCheckpointInfo(meta_.current_lsn, meta_.current_checkpoint_no);
  meta_.checkpoint_lsn = meta_.current_lsn;
  meta_.checkpoint_file_no = meta_.current_file_no;

  return SUCCESS;
}

KStatus WALMgr::Close() {
    file_mgr_->Close();
    if (meta_file_.is_open()) {
        meta_file_.close();
    }
    return SUCCESS;
}

KStatus WALMgr::Drop() {
  if (wal_path_.length() > 3) {
    if (!IsExists(wal_path_)) {
      return SUCCESS;
    }

    if (!Remove(wal_path_)) {
      return FAIL;
    }
    return SUCCESS;
  }
  return FAIL;
}

void WALMgr::Lock() {
  MUTEX_LOCK(meta_mutex_);
}

void WALMgr::Unlock() {
  MUTEX_UNLOCK(meta_mutex_);
}

KStatus WALMgr::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                               size_t offset, TSSlice prepared_payload) {
  size_t log_len = InsertLogTagsEntry::fixed_length + prepared_payload.len;
  auto* wal_log = InsertLogTagsEntry::construct(WALLogType::INSERT, x_id, WALTableType::TAG, time_partition, offset,
                                                prepared_payload.len, prepared_payload.data);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                               size_t offset, TSSlice primary_tag, TSSlice prepared_payload, TS_LSN& entry_lsn) {
  auto* wal_log = InsertLogMetricsEntry::construct(WALLogType::INSERT, x_id, WALTableType::DATA, time_partition, offset,
                                                   prepared_payload.len, prepared_payload.data, primary_tag.len,
                                                   primary_tag.data);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = InsertLogMetricsEntry::fixed_length + prepared_payload.len + primary_tag.len;
  KStatus status = writeWALInternal(ctx, wal_log, log_len, entry_lsn);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteUpdateWAL(kwdbContext_p ctx, uint64_t x_id, int64_t time_partition,
                               size_t offset, TSSlice new_payload, TSSlice old_payload) {
  size_t log_len = UpdateLogTagsEntry::fixed_length + new_payload.len+ old_payload.len;
  auto* wal_log = UpdateLogTagsEntry::construct(WALLogType::UPDATE, x_id, WALTableType::TAG, time_partition, offset,
                                                new_payload.len, old_payload.len, new_payload.data, old_payload.data);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDeleteMetricsWAL(kwdbContext_p ctx, uint64_t x_id, const string& primary_tag,
                                      const std::vector<KwTsSpan>& ts_spans, vector<DelRowSpan>& row_spans) {
  auto* wal_log = DeleteLogMetricsEntry::construct(WALLogType::DELETE, x_id, WALTableType::DATA, primary_tag.length(),
                                                   0, 0, row_spans.size(), primary_tag.data(), row_spans.data());
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = DeleteLogMetricsEntry::fixed_length + row_spans.size() * sizeof(DelRowSpan) + primary_tag.length();
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDeleteTagWAL(kwdbContext_p ctx, uint64_t x_id, const string& primary_tag,
                                  uint32_t sub_group_id, uint32_t entity_id, TSSlice tag_pack) {
  auto* wal_log = DeleteLogTagsEntry::construct(WALLogType::DELETE, x_id, WALTableType::TAG, sub_group_id, entity_id,
                                                primary_tag.length(), primary_tag.data(), tag_pack.len, tag_pack.data);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = DeleteLogTagsEntry::fixed_length + primary_tag.length() + tag_pack.len;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteCreateIndexWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id, uint32_t index_id,
                                    uint32_t cur_ts_version, uint32_t new_ts_version, std::vector<uint32_t> col_ids) {
  std::array<int32_t , 10> tags{};
  for (int i = 0; i < 10; i++) {
    if (i < col_ids.size()) {
      tags[i] = (int32_t)col_ids[i];
    } else {
      tags[i] = -1;
    }
  }
  auto wal_log = CreateIndexEntry::construct(WALLogType::CREATE_INDEX, x_id, object_id, index_id, cur_ts_version,
                                             new_ts_version, tags);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  KStatus status = WriteWAL(ctx, wal_log, CreateIndexEntry::fixed_length);
  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDropIndexWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id, uint32_t index_id,
                                  uint32_t cur_ts_version, uint32_t new_ts_version, std::vector<uint32_t> col_ids) {
  std::array<int32_t , 10> tags{};
  for (int i = 0; i < 10; i++) {
    if (i < col_ids.size()) {
      tags[i] = (int32_t)col_ids[i];
    } else {
      tags[i] = -1;
    }
  }
  auto wal_log = DropIndexEntry::construct(WALLogType::DROP_INDEX, x_id, object_id, index_id, cur_ts_version,
                                           new_ts_version, tags);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  KStatus status = WriteWAL(ctx, wal_log, DropIndexEntry::fixed_length);
  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteCheckpointWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t tag_offset,
                                   uint32_t range_size, CheckpointPartition* time_partitions, TS_LSN& entry_lsn) {
  auto* wal_log = CheckpointEntry::construct(WALLogType::CHECKPOINT, x_id, meta_.current_checkpoint_no, tag_offset,
                                             range_size, time_partitions);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  uint64_t partition_len = sizeof(CheckpointPartition) * range_size;
  size_t log_len = CheckpointEntry::fixed_length + partition_len;
  KStatus status = writeWALInternal(ctx, wal_log, log_len, entry_lsn);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteCheckpointWAL(kwdbContext_p ctx, uint64_t x_id, TS_LSN& entry_lsn) {
  auto* wal_log = CheckpointEntry::construct(WALLogType::CHECKPOINT, x_id, meta_.current_checkpoint_no, 0, 0, nullptr);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = CheckpointEntry::fixed_length;
  KStatus status = writeWALInternal(ctx, wal_log, log_len, entry_lsn);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteSnapshotWAL(kwdbContext_p ctx, uint64_t x_id, TSTableID tbl_id, uint64_t b_hash,
                                 uint64_t e_hash, KwTsSpan span) {
  auto* wal_log = SnapshotEntry::construct(WALLogType::RANGE_SNAPSHOT, x_id, tbl_id, b_hash, e_hash, span.begin, span.end);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = SnapshotEntry::fixed_length;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WritePartitionTierWAL(kwdbContext_p ctx, uint64_t x_id, std::string link_path, std::string tier_path) {
  auto* wal_log = PartitionTierChangeEntry::construct(WALLogType::PARTITION_TIER_CHANGE, x_id, link_path, tier_path);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = PartitionTierChangeEntry::fixed_length + link_path.length() + tier_path.length() + 2;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteTempDirectoryWAL(kwdbContext_p ctx, uint64_t x_id, std::string path) {
  auto* wal_log = TempDirectoryEntry::construct(WALLogType::SNAPSHOT_TMP_DIRCTORY, x_id, path);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = TempDirectoryEntry::fixed_length + path.length() + 1;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDDLCreateWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id,
                                  roachpb::CreateTsTable* meta, std::vector<RangeGroup>* ranges) {
  auto* wal_log = DDLCreateEntry::construct(WALLogType::DDL_CREATE, x_id, object_id,
                                            meta->ByteSize(), ranges->size(), meta, ranges->data());
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = DDLCreateEntry::fixed_length + meta->ByteSizeLong() + ranges->size() * DDLCreateEntry::range_length;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDDLDropWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id) {
  auto* wal_log = DDLDropEntry::construct(WALLogType::DDL_DROP, x_id, object_id);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = DDLDropEntry::fixed_length;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteDDLAlterWAL(kwdbContext_p ctx, uint64_t x_id, uint64_t object_id, AlterType alter_type,
                                 uint32_t cur_version, uint32_t new_version, TSSlice& column_meta) {
  auto* wal_log = DDLAlterEntry::construct(WALLogType::DDL_ALTER_COLUMN, x_id, object_id, alter_type,
                                           cur_version, new_version, column_meta);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = DDLAlterEntry::fixed_length + column_meta.len;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteMTRWAL(kwdbContext_p ctx, uint64_t x_id, const char* tsx_id, WALLogType log_type) {
  auto* wal_log = MTREntry::construct(log_type, x_id, tsx_id);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = MTREntry::fixed_length;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::WriteTSxWAL(kwdbContext_p ctx, uint64_t x_id, const char* ts_trans_id, WALLogType log_type) {
  auto* wal_log = TTREntry::construct(log_type, x_id, ts_trans_id);
  if (wal_log == nullptr) {
    LOG_ERROR("Failed to construct WAL, insufficient memory")
    return KStatus::FAIL;
  }
  size_t log_len = TTREntry::fixed_length;
  KStatus status = WriteWAL(ctx, wal_log, log_len);

  delete[] wal_log;
  return status;
}

KStatus WALMgr::ReadWALLog(std::vector<LogEntry*>& logs, TS_LSN start_lsn, TS_LSN end_lsn) {
  file_mgr_->Lock();
  KStatus status = buffer_mgr_->readWALLogs(logs, start_lsn, end_lsn);
  file_mgr_->Unlock();
  return status;
}

KStatus WALMgr::ReadWALLogForMtr(uint64_t mtr_trans_id, std::vector<LogEntry*>& logs) {
  file_mgr_->Lock();
  KStatus status = buffer_mgr_->readWALLogs(logs, mtr_trans_id, meta_.current_lsn, mtr_trans_id);
  file_mgr_->Unlock();
  if (status == FAIL) {
    LOG_ERROR("Failed to read the WAL log with transaction id %lu", mtr_trans_id)
    return FAIL;
  }

  return SUCCESS;
}

KStatus WALMgr::ReadWALLogForTSx(char* ts_trans_id, std::vector<LogEntry*>& logs) {
  return SUCCESS;
}

TS_LSN WALMgr::FetchCurrentLSN() const {
  return meta_.current_lsn;
}

TS_LSN WALMgr::FetchFlushedLSN() const {
  return meta_.block_flush_to_disk_lsn;
}

TS_LSN WALMgr::FetchCheckpointLSN() const {
  return meta_.checkpoint_lsn;
}

KStatus WALMgr::initWalMeta(kwdbContext_p ctx, bool is_create) {
  std::string meta_path = wal_path_ + "kwdb_wal.meta";
  if (is_create) {
    meta_file_.open(meta_path, std::ios::in | std::ios::out | std::ios::trunc);
    if (!meta_file_.is_open()) {
      LOG_ERROR("Failed to open the WAL metadata")
      return FAIL;
    }

    // start_lsn is header block size + block header size
    TS_LSN start_lsn = BLOCK_SIZE + LOG_BLOCK_HEADER_SIZE;
    meta_ = {0, start_lsn, start_lsn, 0, 0, start_lsn};

    flushMeta(ctx);

    return SUCCESS;
  }

  meta_file_.open(meta_path, std::ios::in | std::ios::out);
  if (!meta_file_.is_open()) {
    LOG_ERROR("Failed to open the WAL metadata")
    return FAIL;
  }

  streamsize size = sizeof(WALMeta);

  char* buf = KNEW char[size];
  meta_file_.seekg(0, std::ios::beg);

  meta_file_.read(buf, size);
  meta_ = *reinterpret_cast<WALMeta*>(buf);
  delete[] buf;

  return SUCCESS;
}

KStatus WALMgr::flushMeta(kwdbContext_p ctx) {
  streamsize size = sizeof(WALMeta);

  char* buf = KNEW char[size];
  memcpy(buf, &meta_, size);
  meta_file_.seekg(0, std::ios::beg);
  meta_file_.write(buf, size);
  meta_file_.sync();
  delete[] buf;

  return SUCCESS;
}

void WALMgr::CleanUp(kwdbContext_p ctx) {
  this->Lock();
  Flush(ctx);
  file_mgr_->CleanUp(meta_.checkpoint_lsn, meta_.current_lsn);
  this->Unlock();
}

KStatus WALMgr::ResetWAL(kwdbContext_p ctx) {
  LOG_INFO("Cleaning wal meta file tableId: %lu.", table_id_)
  if (!IsExists(wal_path_)) {
    if (!MakeDirectory(wal_path_)) {
      LOG_ERROR("Failed to create the WAL log directory '%s'", wal_path_.c_str())
      return FAIL;
    }
  }

  TS_LSN current_lsn_recover = FetchCurrentLSN();
  WALMeta old_meta = meta_;
  if (meta_file_.is_open()) {
    meta_file_.close();
  }
  string meta_path = wal_path_ + "kwdb_wal.meta";
  if (IsExists(meta_path)) {
    Remove(meta_path);
  }

  KStatus s = initWalMeta(ctx, true);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL metadata.")
    return s;
  }
  meta_ = old_meta;
  meta_.current_file_no = 0;
  meta_.current_checkpoint_no = 0;
  meta_.checkpoint_file_no = 0;

  s = file_mgr_->ResetWALInternal(ctx, current_lsn_recover);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to Reset the WAL files.")
    return s;
  }

  s = file_mgr_->Open(meta_.current_file_no);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to Open the WAL metadata.")
    return s;
  }

  buffer_mgr_->ResetMeta();

  s = buffer_mgr_->init(0);
  if (s == KStatus::FAIL) {
    LOG_ERROR("Failed to initialize the WAL buffer.")
    return s;
  }
  return SUCCESS;
}

bool WALMgr::NeedCheckpoint() {
  return meta_.checkpoint_file_no - meta_.current_file_no == 1 ||
         (meta_.checkpoint_file_no == 0 && meta_.current_file_no == opt_->wal_file_in_group - 1);
}

void WALMgr::SetCheckpointObject(LoggedTsEntityGroup* eg) {
  eg_ = eg;
}

}  // namespace kwdbts
