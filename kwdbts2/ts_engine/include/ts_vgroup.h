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

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "kwdb_type.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "ts_engine_schema_manager.h"
#include "ts_mmap_env.h"
#include "ts_vgroup_partition.h"
#include "ts_mem_segment_mgr.h"
#include "st_wal_mgr.h"

namespace kwdbts {

// Interval class, left close right open -> [start , end)
// 简化
struct TimeInterval {
  int64_t start, end;

 public:
  explicit TimeInterval(int64_t start, int64_t end) : start(start), end(end) { assert(end >= start); }
  bool operator<(const TimeInterval& rhs) const { return this->end < rhs.end; }
  bool operator>(const TimeInterval& rhs) const { return this->end > rhs.end; }
  int64_t GetInterval() const { return end - start; }

  // For Debug
  TimeInterval operator&(const TimeInterval& rhs) const {
    if (start > rhs.end || end < rhs.start) {
      return TimeInterval{0, 0};
    }
    int64_t newstart = 0, newend = 0;
    newstart = start > rhs.start ? start : rhs.start;
    newend = end < rhs.end ? end : rhs.end;
    return TimeInterval{newstart, newend};
  }
};

class TsVGroup;
class PartitionManager {
 private:
  std::unordered_map<int, std::unique_ptr<TsVGroupPartition>> partitions_;
  TsVGroup* vgroup_;
  int database_id_;
  uint64_t interval_;

 public:
  PartitionManager(TsVGroup* vgroup, int database_id, uint64_t interval)
      : vgroup_(vgroup), database_id_(database_id), interval_(interval) {}
  TsVGroupPartition* Get(int64_t timestamp, bool create_if_not_exist);
  void SetInterval(int64_t interval) { interval_ = interval; }
};

/**
 * table group used for organizing a series of table(super table of device).
 * in current time vgroup is same as database
 */
// const pointer
class TsVGroup {
 private:
  uint32_t vgroup_id_;
  TsEngineSchemaManager* schema_mgr_{nullptr};
  //  <database_id, Manager>
  std::map<uint32_t, std::unique_ptr<PartitionManager>> partitions_;

  TsMemSegmentManager mem_segment_mgr_;

  std::filesystem::path path_;

  uint64_t entity_counter_;

  mutable std::mutex mutex_;

  MMapFile* config_file_;

  rocksdb::DB* db_ = nullptr;

  static TsEnv env_;

  EngineOptions engine_options_;

  std::unique_ptr<WALMgr> wal_manager_ = nullptr;

 public:
  TsVGroup() = delete;

  TsVGroup(const EngineOptions& engine_options, uint32_t vgroup_id, TsEngineSchemaManager* schema_mgr);

  ~TsVGroup();

  KStatus Init(kwdbContext_p ctx);

  KStatus CreateTable(kwdbContext_p ctx, const KTableKey& table_id, roachpb::CreateTsTable* meta);

  inline TsVGroupPartition* GetPartition(uint32_t database_id, timestamp64 ts, DATATYPE ts_type) {
    auto p_time = convertTsToPTime(ts, ts_type);
    return GetPartition(database_id, p_time);
  }

  __attribute__((visibility("hidden"))) KStatus PutData(kwdbContext_p ctx, TSTableID table_id,
                                                        TSEntityID entity_id, TSSlice* payload);

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;

  uint32_t AllocateEntityID();

  uint32_t GetMaxEntityID() const;

  TsEngineSchemaManager* GetEngineSchemaMgr() {
    return schema_mgr_;
  }

  WALMgr* GetWALManager() {
    return wal_manager_.get();
  }

  // flush all mem segment data into last segment.
  KStatus Flush() {
    std::shared_ptr<TsMemSegment> imm_segment;
    mem_segment_mgr_.SwitchMemSegment(&imm_segment);
    return FlushImmSegment(imm_segment);
  }

  void SwitchMemSegment(std::shared_ptr<TsMemSegment>* imm_segment) {
    mem_segment_mgr_.SwitchMemSegment(imm_segment);
  }

  KStatus FlushImmSegment(const std::shared_ptr<TsMemSegment>& segment);
  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice prepared_payload);

  KStatus WriteInsertWAL(kwdbContext_p ctx, uint64_t x_id, TSSlice primary_tag, TSSlice prepared_payload);

  KStatus GetIterator(kwdbContext_p ctx, vector<uint32_t> entity_ids,
                      std::vector<KwTsSpan> ts_spans, DATATYPE ts_col_type,
                      std::vector<k_uint32> scan_cols, std::vector<k_uint32> ts_scan_cols,
                      std::vector<Sumfunctype> scan_agg_types,
                      std::shared_ptr<TsTableSchemaManager> table_schema_mgr,
                      uint32_t table_version, TsStorageIterator** iter,
                      std::shared_ptr<TsVGroup> vgroup,
                      std::vector<timestamp64> ts_points, bool reverse, bool sorted);

  rocksdb::DB* GetDB();

  uint32_t GetVGroupID();

  TsEngineSchemaManager* GetSchemaMgr() const;

 private:
  // p_time should generated by function convertTsToPTime.
  TsVGroupPartition* GetPartition(uint32_t database_id, timestamp64 p_time);

  int saveToFile(uint32_t new_id) const;

 public:
  class TsPartitionedFlush {
   public:
    TsPartitionedFlush() = delete;
    TsPartitionedFlush(TsVGroup*, rocksdb::InternalIterator*);
    rocksdb::Status FlushFromMem();

   private:
    TsVGroup* vgroup_;
    rocksdb::InternalIterator* iter_;
  };
};


}  // namespace kwdbts
