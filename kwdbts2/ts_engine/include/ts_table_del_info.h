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

#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <list>
#include <utility>
#include <unordered_map>
#include "cm_kwdb_context.h"
#include "engine.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_common.h"
#include "ts_table_v2_impl.h"
#include "ts_hash_latch.h"

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
// to be Compatible with lower verion, this struct can add paramter at last. this using from snapshot version 2.
struct TSSnapshotOSNInfo {
  uint64_t magic_num;
  uint64_t op_osn[3];
  uint8_t  op_types[3];
  uint8_t op_num;
  uint8_t reserved[4];
};

enum STOSNDeleteInfoType : uint32_t {
  OSN_DELETE_TAG_RECORD = 1,
  OSN_UPDATE_TAG_RECORD = 2,
  OSN_DELETE_METRIC_RANGE = 3,
};

class TsReplicaRangeMigrate {
 protected:
  std::shared_ptr<TsTableImpl> table_;
  uint64_t begin_hash_;
  uint64_t end_hash_;
  uint32_t table_version_;
  TS_OSN scan_osn_{UINT64_MAX};
  uint32_t dbid_;
  TS_OSN published_max_osn_{UINT64_MAX};
  uint32_t total_tag_row_num_ = 0;
  uint32_t valid_tag_row_num_ = 0;
  uint32_t ignore_tag_row_num_ = 0;
  uint32_t del_range_num_ = 0;
  std::string optional_msg_;

 public:
  TsReplicaRangeMigrate(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e, uint32_t v, TS_OSN osn) :
    table_(table), begin_hash_(b), end_hash_(e), table_version_(v), scan_osn_(osn),
    dbid_(table_->GetSchemaManager()->GetDbID()) {}

  virtual ~TsReplicaRangeMigrate();
  virtual KStatus Init(TS_OSN published_max_osn) = 0;
  virtual KStatus NextMigrateData(kwdbContext_p ctx, TSSlice* data, bool* is_finished) = 0;
  virtual KStatus WriteMigrateData(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock) = 0;
  virtual KStatus CommitMigrate(kwdbContext_p ctx) = 0;
  static TsReplicaRangeMigrate* CreateProducer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e,
    uint32_t v, TS_OSN osn);
  static TsReplicaRangeMigrate* CreateConsumer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e,
    uint32_t v, TS_OSN osn);
  // generate OSNDeleteInfo data.
  TSSlice GenData(TSSlice& payload, TSSlice& pkey, std::list<STDelRange>& dels);
  // parse OSNDeleteInfo data.
  void ParseData(TSSlice data, STOSNDeleteInfoType* type, TSSlice* payload, TSSlice* pkey,
    std::list<STDelRange>* dels);
};


class TsRangeMigrateProducer : public TsReplicaRangeMigrate {
 private:
  std::list<kwdbts::EntityResultIndex> pkeys_status_;
  std::list<kwdbts::EntityResultIndex>::iterator pkey_iter_;
  std::unordered_map<uint64_t, TS_OSN> entity_create_osn_;  // uint64_t joint_entity_id
  std::unordered_map<std::string, kwdbts::EntityResultIndex> pkey_last_row_;

 public:
  TsRangeMigrateProducer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e, uint32_t v, TS_OSN osn) :
    TsReplicaRangeMigrate(table, b, e, v, osn) {}
  ~TsRangeMigrateProducer() override {}
  KStatus Init(TS_OSN published_max_osn) override;
  KStatus NextMigrateData(kwdbContext_p ctx, TSSlice* data, bool* is_finished) override;
  KStatus WriteMigrateData(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock) override {
    LOG_ERROR("TsRangeMigrateProducer is not supported");
    return KStatus::FAIL;
  }
  KStatus CommitMigrate(kwdbContext_p ctx) override {
    LOG_ERROR("TsRangeMigrateProducer is not supported");
    return KStatus::FAIL;
  }

 private:
  // generate payload only with tag info.
  KStatus GenTagPayLoad(kwdbContext_p ctx, EntityResultIndex& entity_idx, TSSlice* payload);
  uint64_t GenJointEntityID(uint32_t vgroup_id, uint64_t entity_id) {
    return (static_cast<uint64_t>(vgroup_id) << 32) | entity_id;
  }
};

class TsRangeMigrateConsumer : public TsReplicaRangeMigrate {
 private:
struct PrimaryKeyEntityInfo {
  std::map<TS_OSN, std::list<EntityResultIndex>> entity_id_infos_origin;
  std::list<STDelRange> del_ranges;
  std::map<TS_OSN, std::pair<uint32_t, uint64_t>> new_entity_list;
  std::pair<uint32_t, uint64_t> active_entity_{0, 0};
};

 private:
  std::unordered_map<std::string, PrimaryKeyEntityInfo> pkey_entity_info_;
  uint64_t reused_tag_row_num_ = 0;
  uint64_t new_tag_row_num_ = 0;
  uint64_t new_entity_num_ = 0;

 public:
  TsRangeMigrateConsumer(std::shared_ptr<TsTableImpl> table, uint64_t b, uint64_t e, uint32_t v, TS_OSN osn) :
    TsReplicaRangeMigrate(table, b, e, v, osn) {}
  ~TsRangeMigrateConsumer() override {
    char buff[128];
    snprintf(buff, sizeof(buff), "new allocated entity num: %lu, reused tag row num: %lu, new tag row num: %lu.",
             new_entity_num_, reused_tag_row_num_, new_tag_row_num_);
    optional_msg_ = std::string(buff);
  }
  KStatus Init(TS_OSN published_max_osn) override;
  KStatus NextMigrateData(kwdbContext_p ctx, TSSlice* data, bool* is_finished) override {
    LOG_ERROR("TsRangeMigrateConsumer is not supported");
    return KStatus::FAIL;
  }

  KStatus WriteMigrateData(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock) override;
  KStatus CommitMigrate(kwdbContext_p ctx) override;

 private:
  KStatus RMValidPkeyRow(const TSSlice& pkey, std::shared_ptr<TagTable> tag_table);
  bool HasHisTagRecord(std::string& pkey, TS_OSN create_osn, TS_OSN osn, EntityResultIndex& entity_idx);
  KStatus ParsePayload(kwdbContext_p ctx, const TSSlice& payload, TsRawPayload** pd);
  KStatus CoverTagDataInfo(std::pair<uint64_t, uint64_t> row_info, TSSnapshotOSNInfo* snap_osn_info);
  void GetEntityIDVGroupID(std::string& pkey, TS_OSN create_osn, uint64_t& entity_id, uint32_t& vgroup_id);
};

/**
 * snapshot struct  SNAPSHOT_VERSION = 1
 * 
   ________________________________________________________________________________________________________________________
  |      4    |    8     |       4          |     4     |       4        |      4    |    n       |       4      |      n   | 
  |-----------|----------|------------------|-----------|----------------|-----------|------------|--------------|----------|
  |package id | table id |  table version   | batch num |snapshot version| batch len | batch data | del data len | del data |
 * 
 * 
 * type code : 1-tag delete. 2-metric delete
 * 
 *
 */
class STPackageSnapshotData {
 public:
  static bool PackageData(uint32_t package_id, TSTableID tbl_id, uint32_t tbl_version,
    TSSlice& batch_data, uint32_t row_num, TSSlice& del_data, TSSlice* data);

  static bool UnpackageData(TSSlice& data, uint32_t& package_id, TSTableID& tbl_id, uint32_t& tbl_version,
    TSSlice& batch_data, uint32_t& row_num, TSSlice& del_data);
};

/**
 * snapshot struct  SNAPSHOT_VERSION = 2
 *
   _____________________________________________________________________________________________________________________________
  |     20    |       4          |       4        |      4       |    n       |       4      |      n   |
  |-----------|------------------|----------------|--------------|------------|--------------|----------|
  | reserverd | snapshot version | package number | package1 len | package1   | package2 len | package2 |
 *
 * package struct is SNAPSHOT_VERSION = 1.
 *
 */
class STSnapshotPackageBuilder {
 private:
  uint32_t package_id_;
  TSTableID tbl_id_;
  uint32_t tbl_version_;
  uint32_t current_package_size_{0};
  std::list<TSSlice> packages_;

 public:
  STSnapshotPackageBuilder(uint32_t package_id, TSTableID tbl_id, uint32_t tbl_version) :
    package_id_(package_id), tbl_id_(tbl_id), tbl_version_(tbl_version) {}

  inline bool OverMinThreshold() {
    return current_package_size_ >= SNAPSHOT_MIN_PACKAGE_SIZE;
  }

  bool AddBatchData(TSSlice& batch_data, uint32_t row_num, TSSlice& del_data);

  bool Package(TSSlice* data);
};

class STSnapshotPackageParser {
 private:
  std::list<TSSlice> packages_;
  std::list<TSSlice>::iterator cur_package_;

 public:
  STSnapshotPackageParser() {}

  bool Parser(TSSlice& p_data);

  bool NextPackage(uint32_t& package_id, TSTableID& tbl_id, uint32_t& tbl_version,
    TSSlice& batch_data, uint32_t& row_num, TSSlice& del_data);
};

}  // namespace kwdbts
