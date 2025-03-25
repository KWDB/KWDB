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

#include "ts_db_impl.h"
#include <chrono>
#include <iostream>
#include <thread>
#include "db/column_family.h"
#include "db/compaction_iterator.h"
#include "table/block_based_table_factory.h"
#include "table/merging_iterator.h"
#include "ts_vgroup.h"
#include "util/sst_file_manager_impl.h"
#include "ts_payload.h"

namespace rocksdb {

TsDBImpl::TsDBImpl(const DBOptions &options, const std::string &dbname, kwdbts::TsVGroup* ts_vgroup,
                   const bool seq_per_batch, const bool batch_per_txn)
  : DBImpl(options, dbname, seq_per_batch, batch_per_txn), ts_vgroup_(ts_vgroup) {}

Status TsDBImpl::Open(const Options& options, const std::string& dbname, kwdbts::TsVGroup* ts_vgroup, DB** db_ptr) {
  const bool kSeqPerBatch = true;
  const bool kBatchPerTxn = true;
  Status s;
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
    ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle *> handles;

  // SanitizeOptionsByTable
  for (auto cf : column_families) {
    s = cf.options.table_factory->SanitizeOptions(db_options, cf.options);
    if (!s.ok()) {
      return s;
    }
  }

  // ValidateOptions
  for (auto &cfd : column_families) {
    s = CheckCompressionSupported(cfd.options);
    if (s.ok() && db_options.allow_concurrent_memtable_write) {
      s = CheckConcurrentWritesSupported(cfd.options);
    }
    if (s.ok()) {
      s = CheckCFPathsSupported(db_options, cfd.options);
    }
    if (!s.ok()) {
      return s;
    }

    if (cfd.options.ttl > 0) {
      if (db_options.max_open_files != -1) {
        return Status::NotSupported(
          "TTL is only supported when files are always "
          "kept open (set max_open_files = -1). ");
      }
      if (cfd.options.table_factory->Name() !=
          BlockBasedTableFactory().Name()) {
        return Status::NotSupported(
          "TTL is only supported in Block-Based Table format. ");
      }
    }

    if (cfd.options.periodic_compaction_seconds > 0) {
      if (db_options.max_open_files != -1) {
        return Status::NotSupported(
          "Periodic Compaction is only supported when files are always "
          "kept open (set max_open_files = -1). ");
      }
      if (cfd.options.table_factory->Name() !=
          BlockBasedTableFactory().Name()) {
        return Status::NotSupported(
          "Periodic Compaction is only supported in "
          "Block-Based Table format. ");
      }
    }
  }

  if (db_options.db_paths.size() > 4) {
    return Status::NotSupported(
      "More than four DB paths are not supported yet. ");
  }

  if (db_options.allow_mmap_reads && db_options.use_direct_reads) {
    // Protect against assert in PosixMMapReadableFile constructor
    return Status::NotSupported(
      "If memory mapped reads (allow_mmap_reads) are enabled "
      "then direct I/O reads (use_direct_reads) must be disabled. ");
  }

  if (db_options.allow_mmap_writes &&
      db_options.use_direct_io_for_flush_and_compaction) {
    return Status::NotSupported(
      "If memory mapped writes (allow_mmap_writes) are enabled "
      "then direct I/O writes (use_direct_io_for_flush_and_compaction) must "
      "be disabled. ");
  }

  if (db_options.keep_log_file_num == 0) {
    return Status::InvalidArgument("keep_log_file_num must be greater than 0");
  }

  *db_ptr = nullptr;
  handles.clear();

  size_t max_write_buffer_size = 0;
  for (auto cf : column_families) {
    max_write_buffer_size =
      std::max(max_write_buffer_size, cf.options.write_buffer_size);
  }

  TsDBImpl *impl = new TsDBImpl(db_options, dbname, ts_vgroup, !kSeqPerBatch, kBatchPerTxn);
  s = impl->env_->CreateDirIfMissing(impl->immutable_db_options_.wal_dir);
  if (s.ok()) {
    std::vector<std::string> paths;
    for (auto &db_path : impl->immutable_db_options_.db_paths) {
      paths.emplace_back(db_path.path);
    }
    for (auto &cf : column_families) {
      for (auto &cf_path : cf.options.cf_paths) {
        paths.emplace_back(cf_path.path);
      }
    }
    for (auto &path : paths) {
      s = impl->env_->CreateDirIfMissing(path);
      if (!s.ok()) {
        break;
      }
    }

    // For recovery from NoSpace() error, we can only handle
    // the case where the database is stored in a single path
    if (paths.size() <= 1) {
      impl->error_handler_.EnableAutoRecovery();
    }
  }

  if (!s.ok()) {
    delete impl;
    return s;
  }

  s = impl->CreateArchivalDirectory();
  if (!s.ok()) {
    delete impl;
    return s;
  }
  impl->mutex_.Lock();
  // Handles create_if_missing, error_if_exists
  s = impl->Recover(column_families);
  if (s.ok()) {
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    log::Writer *new_log = nullptr;
    const size_t preallocate_block_size =
      impl->GetWalPreallocateBlockSize(max_write_buffer_size);
    s = impl->CreateWAL(new_log_number, 0, preallocate_block_size, &new_log);
    if (s.ok()) {
      InstrumentedMutexLock wl(&impl->log_write_mutex_);
      impl->logfile_number_ = new_log_number;
      assert(new_log != nullptr);
      impl->logs_.emplace_back(new_log_number, new_log);
    }

    if (s.ok()) {
      // set column family handles
      for (auto cf : column_families) {
        auto cfd = impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
        if (cfd != nullptr) {
          handles.push_back(
            new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
          impl->NewThreadStatusCfInfo(cfd);
        } else {
          if (db_options.create_missing_column_families) {
            // missing column family, create it
            ColumnFamilyHandle *handle;
            impl->mutex_.Unlock();
            s = impl->CreateColumnFamily(cf.options, cf.name, &handle);
            impl->mutex_.Lock();
            if (s.ok()) {
              handles.push_back(handle);
            } else {
              break;
            }
          } else {
            s = Status::InvalidArgument("Column family not found: ", cf.name);
            break;
          }
        }
      }
    }
    if (s.ok()) {
      SuperVersionContext sv_context(/* create_superversion */ true);
      for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
        impl->InstallSuperVersionAndScheduleWork(cfd, &sv_context, *cfd->GetLatestMutableCFOptions());
      }
      sv_context.Clean();
      if (impl->two_write_queues_) {
        impl->log_write_mutex_.Lock();
      }
      impl->alive_log_files_.push_back(
        DBImpl::LogFileNumberSize(impl->logfile_number_));
      if (impl->two_write_queues_) {
        impl->log_write_mutex_.Unlock();
      }
      impl->DeleteObsoleteFiles();
      s = impl->directories_.GetDbDir()->Fsync();
    }
  }

  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      if (cfd->ioptions()->compaction_style == kCompactionStyleFIFO) {
        auto *vstorage = cfd->current()->storage_info();
        for (int i = 1; i < vstorage->num_levels(); ++i) {
          int num_files = vstorage->NumLevelFiles(i);
          if (num_files > 0) {
            s = Status::InvalidArgument(
              "Not all files are at level 0. Cannot "
              "open with FIFO compaction style.");
            break;
          }
        }
      }
      if (!cfd->mem()->IsSnapshotSupported()) {
        impl->is_snapshot_supported_ = false;
      }
      if (cfd->ioptions()->merge_operator != nullptr &&
          !cfd->mem()->IsMergeOperatorSupported()) {
        s = Status::InvalidArgument(
          "The memtable of column family %s does not support merge operator "
          "its options.merge_operator is non-null",
          cfd->GetName().c_str());
      }
      if (!s.ok()) {
        break;
      }
    }
  }
  Status persist_options_status;
  if (s.ok()) {
    // Persist RocksDB Options before scheduling the compaction.
    // The WriteOptionsFile() will release and lock the mutex internally.
    persist_options_status = impl->WriteOptionsFile(
      false /*need_mutex_lock*/, false /*need_enter_write_thread*/);

    *db_ptr = impl;
    impl->opened_successfully_ = true;
    impl->MaybeScheduleFlushOrCompaction();
  }
  impl->mutex_.Unlock();

  auto sfm = static_cast<SstFileManagerImpl *>(
    impl->immutable_db_options_.sst_file_manager.get());
  if (s.ok() && sfm) {
    // Notify SstFileManager about all sst files that already exist in
    // db_paths[0] and cf_paths[0] when the DB is opened.
    std::vector<std::string> paths;
    paths.emplace_back(impl->immutable_db_options_.db_paths[0].path);
    for (auto &cf : column_families) {
      if (!cf.options.cf_paths.empty()) {
        paths.emplace_back(cf.options.cf_paths[0].path);
      }
    }
    // Remove duplicate paths.
    std::sort(paths.begin(), paths.end());
    paths.erase(std::unique(paths.begin(), paths.end()), paths.end());
    for (auto &path : paths) {
      std::vector<std::string> existing_files;
      impl->immutable_db_options_.env->GetChildren(path, &existing_files);
      for (auto &file_name : existing_files) {
        uint64_t file_number;
        FileType file_type;
        std::string file_path = path + "/" + file_name;
        if (ParseFileName(file_name, &file_number, &file_type) &&
            file_type == kTableFile) {
          sfm->OnAddFile(file_path);
        }
      }
    }

    // Reserve some disk buffer space. This is a heuristic - when we run out
    // of disk space, this ensures that there is atleast write_buffer_size
    // amount of free space before we resume DB writes. In low disk space
    // conditions, we want to avoid a lot of small L0 files due to frequent
    // WAL write failures and resultant forced flushes
    sfm->ReserveDiskBuffer(max_write_buffer_size,
                           impl->immutable_db_options_.db_paths[0].path);
  }

  if (s.ok()) {
    ROCKS_LOG_HEADER(impl->immutable_db_options_.info_log, "DB pointer %p",
                     impl);
    LogFlush(impl->immutable_db_options_.info_log);
    // If the assert above fails then we need to FlushWAL before returning
    // control back to the user.
    if (!persist_options_status.ok()) {
      s = Status::IOError(
        "DB::Open() failed --- Unable to persist Options file",
        persist_options_status.ToString());
    }
  }
  if (s.ok()) {
    impl->StartTimedTasks();
  }
  if (!s.ok()) {
    for (auto *h : handles) {
      delete h;
    }
    handles.clear();
    delete impl;
    *db_ptr = nullptr;
  }

  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }
  return s;
}

bool parseKey(const std::string& key, uint32_t& db_id, TSTableID& table_id, TSEntityID& entity_id, KTimestamp& start_ts) {
  std::stringstream ss(key);
  std::string item;
  std::vector<std::string> tokens;

  // 分割字符串
  while (std::getline(ss, item, '/')) {
    tokens.push_back(item);
  }

// 检查是否正好有4个部分
  if (tokens.size() == 4) {
    db_id = std::stoi(tokens[0]);
    table_id = std::stoll(tokens[1]);
    entity_id = std::stoll(tokens[2]);
    start_ts = std::stoll(tokens[3]);
    return true;
  } else {
    return false;
  }
}

Status TsDBImpl::FlushMemTableToOutputFile(rocksdb::ColumnFamilyData *cfd, const MutableCFOptions &mutable_cf_options,
                                   bool *made_progress, JobContext *job_context,
                                   SuperVersionContext *superversion_context,
                                   std::vector<SequenceNumber> &snapshot_seqs,
                                   SequenceNumber earliest_write_conflict_snapshot,
                                   SnapshotChecker *snapshot_checker, LogBuffer *log_buffer,
                                   Env::Priority thread_pri) {
  mutex_.AssertHeld();
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());
  // pick memtable
  MemTable* mems;
  auto base_version = cfd->current();
  {
    base_version->Ref();  // it is likely that we do not need this reference
    cfd->imm()->PickOneMemtableToFlush(nullptr, &mems);
    if (mems == nullptr) {
      return Status::OK();
    }
    MemTable *m = mems;
    m->GetEdits()->SetPrevLogNumber(0);
    // SetLogNumber(log_num) indicates logs with number smaller than log_num
    // will no longer be picked up for recovery.
    m->GetEdits()->SetLogNumber(mems->GetNextLogNumber());
    m->GetEdits()->SetColumnFamily(cfd->GetID());
  }
  Status s;
  if (logfile_number_ > 0 &&
      versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1) {
    // If there are more than one column families, we need to make sure that
    // all the log files except the most recent one are synced. Otherwise if
    // the host crashes after flushing and before WAL is persistent, the
    // flushed SST may contain data from write batches whose updates to
    // other column families are missing.
    // SyncClosedLogs() may unlock and re-lock the db_mutex.
    s = SyncClosedLogs(job_context);
  }

  mutex_.Unlock();
  // memtables and range_del_iters store internal iterators over each data
  // memtable and its associated range deletion memtable, respectively, at
  // corresponding indexes.
  std::vector<InternalIterator*> memtables;
  ReadOptions ro;
  ro.total_order_seek = true;
  Arena arena;
  memtables.push_back(mems->NewIterator(ro, &arena));

  // build ScopeArenaIterator
  ScopedArenaIterator iter(
    NewMergingIterator(&cfd->internal_comparator(), &memtables[0],
  static_cast<int>(memtables.size()), &arena));

  iter->SeekToFirst();
  kwdbts::TsVGroup::TsPartitionedFlush flush{this->ts_vgroup_, iter.get()};
  s = flush.FlushFromMem();
  // iter->SeekToFirst();
  // for (; iter->Valid(); iter->Next()) {
  //   const Slice &key = iter->user_key();
  //   const Slice &payload = iter->value();
  //   FullKey full_key;
  //   ParseFullKey(iter->key(), &full_key);
  //   // TODO(limeng04): flush payload to LastSegment
  //   std::cout << full_key.user_key.ToString() << " -> " << full_key.sequence << " -> " << full_key.type
  //             << " -> " << payload.ToString() << std::endl;
  // }
  mutex_.Lock();
  base_version->Unref();
  if (s.ok() &&
      (shutting_down_.load(std::memory_order_acquire) || cfd->IsDropped())) {
    s = Status::ShutdownInProgress(
        "Database shutdown or Column family drop during flush");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    s = cfd->imm()->TryInstallMemtableFlushResults(
        cfd, mutable_cf_options, {mems}, &logs_with_prep_tracker_, versions_.get(), &mutex_, 1,
        &job_context->memtables_to_free, directories_.GetDbDir(), log_buffer);
  } else {
    cfd->imm()->RollbackMemtableFlush({mems}, 1);
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       mutable_cf_options);
    if (made_progress) {
      *made_progress = true;
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress()) {
    Status new_bg_error = s;
    error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
  }
  return s;
}

}  // namespace rocksdb
