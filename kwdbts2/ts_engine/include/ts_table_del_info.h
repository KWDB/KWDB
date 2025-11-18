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

#include <memory>
#include <string>
#include <vector>
#include <list>
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
   ____________________________________________________________________________________________________________________________
  |    4    |        4      |       n          |      4   |    n    |       4        |       8      |      8     |  8         |
  |---------|---------------|------------------|----------|---------|----------------|--------------|------------|------------|
  |  type   |  payload len  |  payload data    | pkey len |  pkey   | del range num  | range1 begin | range1 end | range1 osn |
 * 
 * 
 * type code : 1-tag delete. 2-metric delete
 * 
 */
enum STOSNDeleteInfoType : uint32_t {
  OSN_DELETE_TAG_RECORD = 1,
  OSN_UPDATE_TAG_RECORD = 2,
  OSN_DELETE_METRIC_RANGE = 3,
};


class STTableRangeDelAndTagInfo {
 private:
  std::shared_ptr<TsTableV2Impl> table_;
  uint64_t begin_hash_;
  uint64_t end_hash_;
  uint32_t table_version_;
  std::list<kwdbts::EntityResultIndex> pkeys_status_;
  std::list<kwdbts::EntityResultIndex>::iterator pkey_iter_;
  std::unordered_map<std::string, TS_OSN> del_tag_osn_;
  std::unordered_map<std::string, std::list<STDelRange>> pkey_del_ranges_;
  std::unordered_map<std::string, EntityResultIndex> pkey_update_idx_;

 public:
  STTableRangeDelAndTagInfo(std::shared_ptr<TsTableV2Impl> table, uint64_t b, uint64_t e, uint32_t v) :
    table_(table), begin_hash_(b), end_hash_(e), table_version_(v) {}

  KStatus Init();
  // generate OSNDeleteInfo data.
  TSSlice GenData(TSSlice& payload, TSSlice& pkey, std::list<STDelRange>& dels);
  // parse OSNDeleteInfo data.
  void ParseData(TSSlice data, STOSNDeleteInfoType* type, TSSlice* payload, TSSlice* pkey,
    std::list<STDelRange>* dels);
  // get next batch datas.
  KStatus GetNextDeleteInfo(kwdbContext_p ctx, TSSlice* data, bool* is_finished);
  // generate payload only with tag info.
  KStatus GenTagPayLoad(kwdbContext_p ctx, EntityResultIndex& entity_idx, TSSlice* payload);

  KStatus WriteDelAndTagInfo(kwdbContext_p ctx, TSSlice& data, TsHashRWLatch& tag_lock);

  KStatus WriteDeleteTagRecord(kwdbContext_p ctx, TSSlice& payload, OperateType type, TsHashRWLatch& tag_lock);
  KStatus WriteUpdateTagRecord(kwdbContext_p ctx, TSSlice& payload, OperateType type, TsHashRWLatch& tag_lock);
  KStatus WriteInsertTagRecord(kwdbContext_p ctx, TSSlice& payload, OperateType type, TsHashRWLatch& tag_lock);

  KStatus CommitDeleteInfo(kwdbContext_p ctx);
};

    // package_id + table_id + table_version + row_num + data

/**
 * snapshot struct
 * 
   ____________________________________________________________________________________________________________
  |      4    |       8    |       4          |     4     |      4    |    n       |       4      |      n   | 
  |-----------|------------|------------------|-----------|-----------|------------|--------------|----------|
  |package id |  table_id  |  table version   | batch num | batch len | batch data | del data len | del data |
 * 
 * 
 * type code : 1-tag delete. 2-metric delete
 * 
 */
class STPackageSnapshotData {
 public:
  static bool PackageData(uint32_t package_id, TSTableID tbl_id, uint32_t tbl_version,
    TSSlice& batch_data, uint32_t row_num, TSSlice& del_data, TSSlice* data);

  static void UnpackageData(TSSlice& data, uint32_t& package_id, TSTableID& tbl_id, uint32_t& tbl_version,
    TSSlice& batch_data, uint32_t& row_num, TSSlice& del_data);
};

}  // namespace kwdbts
