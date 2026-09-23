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
#pragma once
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "sys_utils.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_filename.h"
#include "ts_io.h"
#include "ts_sliceguard.h"

namespace kwdbts {

// Per-db max_entity_id file layout:
//   [magic u32][version u32][n_vgroup u32][max_entity_id u64] * n_vgroup
constexpr uint32_t kMaxEntityIdFileMagic = 0x4449454D;  // "MEID" (little-endian)
constexpr uint32_t kMaxEntityIdFileVersion = 1;
constexpr uint64_t kMaxEntityIdFileHeaderSize = 12;

class TsEngineSchemaManager;
class TsDBSchema {
  // Entity IDs start from 1; next_entity_id_ holds the next id to hand out.
  // alignas(64) pads each counter to its own cache line to avoid false sharing.
  class alignas(64) EntityIDAllocator {
    std::atomic<TSEntityID> next_entity_id_{1};

   public:
    EntityIDAllocator() = default;
    explicit EntityIDAllocator(TSEntityID eid) : next_entity_id_{eid} {}

    TSEntityID AllocateEntityID() { return next_entity_id_.fetch_add(1, std::memory_order_relaxed); }
    TSEntityID GetMaxEntityID() const { return next_entity_id_.load(std::memory_order_relaxed) - 1; }

    void SetMaxEntityID(TSEntityID max_eid) { next_entity_id_.store(max_eid + 1, std::memory_order_relaxed); }
  };

 private:
  std::vector<EntityIDAllocator> entity_id_allocators_;
  uint32_t db_id_{0};
  fs::path root_path_;
  std::mutex persist_m_;

  KStatus Load() {
    auto counter_file_path = root_path_ / "max_entity_id";

    if (!fs::exists(counter_file_path)) {
      return SUCCESS;
    }

    std::unique_ptr<TsSequentialReadFile> counter_file;
    auto s = TsFIOEnv::GetInstance().NewSequentialReadFile(counter_file_path, &counter_file);
    if (s == FAIL) {
      LOG_ERROR("failed to open max_entity_id file [%s] of db %u", counter_file_path.c_str(), db_id_);
      return FAIL;
    }

    auto file_size = counter_file->GetFileSize();

    if (file_size < kMaxEntityIdFileHeaderSize) {
      LOG_ERROR("max_entity_id file [%s] of db %u is truncated: size %lu",
                counter_file_path.c_str(), db_id_, static_cast<uint64_t>(file_size));
      return FAIL;
    }

    TsSliceGuard slice;
    s = counter_file->Read(file_size, &slice);
    if (s == FAIL) {
      LOG_ERROR("failed to read max_entity_id file [%s] of db %u", counter_file_path.c_str(), db_id_);
      return FAIL;
    }

    auto data = slice.AsSlice();
    uint32_t magic;
    uint32_t version;
    uint32_t n_vgroup;
    GetFixed32(&data, &magic);
    GetFixed32(&data, &version);
    GetFixed32(&data, &n_vgroup);

    if (magic != kMaxEntityIdFileMagic) {
      LOG_ERROR("max_entity_id file [%s] of db %u has bad magic 0x%x, expected 0x%x",
                counter_file_path.c_str(), db_id_, magic, kMaxEntityIdFileMagic);
      return FAIL;
    }
    if (version != kMaxEntityIdFileVersion) {
      LOG_ERROR("max_entity_id file [%s] of db %u has unsupported version %u, supported %u",
                counter_file_path.c_str(), db_id_, version, kMaxEntityIdFileVersion);
      return FAIL;
    }

    // Changing vgroup_max_num across restarts is not supported; enforce it
    // here so a mismatched file fails cleanly instead of indexing out of
    // bounds below.
    if (n_vgroup != entity_id_allocators_.size()) {
      LOG_ERROR("max_entity_id file of db %u has %u vgroups, but configured vgroup_max_num is %lu",
                db_id_, n_vgroup, entity_id_allocators_.size());
      return FAIL;
    }

    if (file_size != kMaxEntityIdFileHeaderSize + 8 * static_cast<uint64_t>(n_vgroup)) {
      LOG_ERROR("max_entity_id file [%s] of db %u has inconsistent size %lu for %u vgroups",
                counter_file_path.c_str(), db_id_, static_cast<uint64_t>(file_size), n_vgroup);
      return FAIL;
    }

    for (uint32_t i = 0; i < n_vgroup; i++) {
      uint64_t max_eid;
      GetFixed64(&data, &max_eid);
      entity_id_allocators_[i].SetMaxEntityID(max_eid);  // set max entity id for each vgroup
    }
    return SUCCESS;
  }

  KStatus Create() { return TsFIOEnv::GetInstance().NewDirectory(root_path_); }

  // Flush the directory entry created by the rename in Persist(). Without
  // this, a crash can lose the renamed file while the vg.mei removal (or an
  // earlier persist) already hit disk — permanently losing the dropped-table
  // id protection. Gated by force_sync_file like the file Sync itself.
  KStatus SyncDir(const fs::path& dir) {
    if (!EngineOptions::force_sync_file) {
      return SUCCESS;
    }
    int fd = open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
      LOG_ERROR("failed to open dir [%s] for sync of db %u", dir.c_str(), db_id_);
      return FAIL;
    }
    KStatus s = SUCCESS;
    if (fsync(fd) != 0) {
      LOG_ERROR("failed to sync dir [%s] of db %u", dir.c_str(), db_id_);
      s = FAIL;
    }
    close(fd);
    return s;
  }

 public:
  KStatus LoadOrCreate() {
    if (fs::exists(root_path_)) {
      return Load();
    }
    return Create();
  }
  KStatus Init() { return LoadOrCreate(); }
  KStatus Init(const std::map<int, uint32_t>& vg_max_eid_map) {
    auto s = Init();  // load counters from files in db
    if (s == FAIL) {
      LOG_ERROR("failed to load persisted entity id counters of db %u", db_id_);
      return FAIL;
    }
    for (auto [vgid, max_eid] : vg_max_eid_map) {
      auto current_max_eid = entity_id_allocators_[vgid - 1].GetMaxEntityID();
      if (current_max_eid < max_eid) {
        entity_id_allocators_[vgid - 1].SetMaxEntityID(max_eid);
      }
    }
    return SUCCESS;
  }

  KStatus Persist() {
    std::unique_lock lk{persist_m_};
    std::string filename = "max_entity_id";
    auto tmp_path = root_path_ / TempFileName(filename);
    std::unique_ptr<TsAppendOnlyFile> file;
    auto s = TsFIOEnv::GetInstance().NewAppendOnlyFile(tmp_path, &file);
    if (s == FAIL) {
      LOG_ERROR("failed to create temp max_entity_id file [%s] of db %u", tmp_path.c_str(), db_id_);
      return FAIL;
    }
    TsBufferBuilder tmp_buffer;
    PutFixed32(&tmp_buffer, kMaxEntityIdFileMagic);
    PutFixed32(&tmp_buffer, kMaxEntityIdFileVersion);
    PutFixed32(&tmp_buffer, static_cast<uint32_t>(entity_id_allocators_.size()));
    for (auto& allocator : entity_id_allocators_) {
      PutFixed64(&tmp_buffer, allocator.GetMaxEntityID());
    }
    s = file->Append(tmp_buffer.AsStringView());
    if (s == FAIL) {
      LOG_ERROR("failed to write max_entity_id file [%s] of db %u", tmp_path.c_str(), db_id_);
      return FAIL;
    }
    s = file->Sync();
    if (s == FAIL) {
      LOG_ERROR("failed to sync max_entity_id file [%s] of db %u", tmp_path.c_str(), db_id_);
      return FAIL;
    }
    std::error_code ec;
    fs::rename(tmp_path, root_path_ / filename, ec);
    if (ec) {
      LOG_ERROR("failed to rename max_entity_id file of db %u: %s", db_id_, ec.message().c_str());
      return FAIL;
    }
    // Make the renamed directory entry durable before callers act on the
    // persist (e.g. the vg.mei migration removing the legacy file).
    return SyncDir(root_path_);
  }

  TsDBSchema(const std::string& schema_root_path, uint32_t db_id)
      : entity_id_allocators_(EngineOptions::vgroup_max_num), db_id_(db_id) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%05u", db_id_);
    root_path_ = fs::path(schema_root_path) / buf;
  }

  uint32_t GetDBID() const { return db_id_; }
  TSEntityID DBAllocateEntityID(uint32_t vgroup_id) {
    assert(vgroup_id != 0 && vgroup_id <= EngineOptions::vgroup_max_num);
    return entity_id_allocators_[vgroup_id - 1].AllocateEntityID();
  }

  TSEntityID GetMaxEntityID(uint32_t vgroup_id) {
    assert(vgroup_id != 0 && vgroup_id <= EngineOptions::vgroup_max_num);
    return entity_id_allocators_[vgroup_id - 1].GetMaxEntityID();
  }

  // Raise per-vgroup counters to at least the legacy vg.mei values (written
  // by the old vgroup-global allocator). Used only by the one-shot vg.mei
  // migration at startup.
  void ApplyLegacyMaxEntityIds(const std::vector<uint32_t>& legacy_max_ids) {
    size_t n = std::min(legacy_max_ids.size(), entity_id_allocators_.size());
    for (size_t i = 0; i < n; i++) {
      if (entity_id_allocators_[i].GetMaxEntityID() < legacy_max_ids[i]) {
        entity_id_allocators_[i].SetMaxEntityID(legacy_max_ids[i]);
      }
    }
  }
};

class TsDBSchemaManager {
 private:
  mutable std::shared_mutex mutex_;
  std::unordered_map<uint32_t, std::shared_ptr<TsDBSchema>> db_schemas_;
  fs::path root_path_;
  std::map<uint32_t /*db_id*/, std::map<int /*vgroup_id*/, uint32_t>> max_entity_id_map_;

 public:
  explicit TsDBSchemaManager(const std::string& schema_root_path)
      : root_path_{fs::path(schema_root_path) / "db"} {}

  KStatus Init(TsEngineSchemaManager* schema_mgr) {
    auto s = LoadCountersFromTagTable(schema_mgr);
    if (s == FAIL) {
      LOG_ERROR("failed to load entity id counters from tag tables");
      return FAIL;
    }
    s = TsFIOEnv::GetInstance().NewDirectory(root_path_);
    if (s == FAIL) {
      LOG_ERROR("failed to create db schema directory [%s]", root_path_.c_str());
      return FAIL;
    }
    s = ResetCounters();
    if (s == FAIL) {
      LOG_ERROR("failed to recover per-db entity id counters");
      return FAIL;
    }
    return SUCCESS;
  }

  KStatus GetOrCreateDatabases(uint32_t db_id, std::shared_ptr<TsDBSchema>& db_schema) {
    {
      std::shared_lock lk(mutex_);
      auto it = db_schemas_.find(db_id);
      if (it != db_schemas_.end()) {
        db_schema = it->second;
        return SUCCESS;
      }
    }
    std::unique_lock lk(mutex_);
    auto it = db_schemas_.find(db_id);
    if (it != db_schemas_.end()) {
      db_schema = it->second;
      return SUCCESS;
    }
    db_schema = std::make_shared<TsDBSchema>(root_path_, db_id);

    if (db_schema->Init() != SUCCESS) {
      LOG_ERROR("failed to init db schema of db %u", db_id);
      return FAIL;
    }
    db_schemas_[db_id] = db_schema;
    return SUCCESS;
  }

  std::shared_ptr<TsDBSchema> GetDatabase(uint32_t db_id) {
    std::shared_lock lk(mutex_);
    auto it = db_schemas_.find(db_id);
    if (it != db_schemas_.end()) {
      return it->second;
    }
    return nullptr;
  }

  void GetAllDbSchema(std::vector<std::shared_ptr<TsDBSchema>>& db_schemas) const {
    db_schemas.clear();
    std::shared_lock lk(mutex_);
    db_schemas.reserve(db_schemas_.size());
    for (const auto& pair : db_schemas_) {
      db_schemas.push_back(pair.second);
    }
  }

  KStatus LoadCountersFromTagTable(TsEngineSchemaManager* schema_mgr);

  // One-shot migration of the legacy vg.mei per-vgroup max ids. The legacy
  // max has no db dimension, so it becomes a per-vgroup floor for every db
  // that may hold pre-upgrade data: dbs already known here (from the tag
  // scan) plus dbs found in any vgroup's partition list (covers dbs whose
  // tables were all dropped). Max-based merging keeps this idempotent; the
  // caller removes vg.mei only after this returns SUCCESS.
  KStatus MigrateLegacyVgMei(const std::vector<uint32_t>& legacy_max_ids,
                             const std::set<uint32_t>& dbs_with_data) {
    std::set<uint32_t> dbs = dbs_with_data;
    {
      std::shared_lock lk(mutex_);
      for (const auto& [db_id, db_schema] : db_schemas_) {
        dbs.insert(db_id);
      }
    }
    for (auto db_id : dbs) {
      std::shared_ptr<TsDBSchema> db_schema;
      if (GetOrCreateDatabases(db_id, db_schema) == FAIL) {
        LOG_ERROR("vg.mei migration: failed to get db schema of db %u", db_id);
        return FAIL;
      }
      db_schema->ApplyLegacyMaxEntityIds(legacy_max_ids);
      if (db_schema->Persist() == FAIL) {
        LOG_ERROR("vg.mei migration: failed to persist entity id counters of db %u", db_id);
        return FAIL;
      }
    }
    return SUCCESS;
  }

  // Startup snapshot of the tag-table scan (db -> vgroup -> max live entity
  // id), filled by LoadCountersFromTagTable before the vg.mei migration runs.
  // Used to bound the last-row cache pre-population: ids above this belong to
  // dropped entities with no tag, which no query can ever reference.
  const std::map<uint32_t, std::map<int, uint32_t>>& GetTagScanMaxEntityIds() const {
    return max_entity_id_map_;
  }

  KStatus ResetCounters() {
    for (const auto& [db_id, vg_max_eid_map] : max_entity_id_map_) {
      std::shared_ptr<TsDBSchema> db_schema;
      auto s = GetOrCreateDatabases(db_id, db_schema);
      if (s == FAIL) {
        LOG_ERROR("failed to get db schema of db %u", db_id);
        return FAIL;
      }
      s = db_schema->Init(vg_max_eid_map);
      if (s == FAIL) {
        LOG_ERROR("failed to apply tag-table max entity ids for db %u", db_id);
        return FAIL;
      }
    }
    return SUCCESS;
  }
};
}  // namespace kwdbts
