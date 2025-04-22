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
#include <memory>
#include <string>
#include <cstdio>

#include "ts_block_segment.h"
#include "ts_lastsegment_manager.h"
#include "ts_engine_schema_manager.h"

namespace kwdbts {

class TsVGroupPartition {
 private:
  int database_id_;
  int64_t start_, end_;

  std::filesystem::path path_;

  std::unique_ptr<TsBlockSegment> blk_segment_;
  TsLastSegmentManager last_segment_mgr_;
  std::unique_ptr<KRWLatch> partition_mtx_;

  TsEngineSchemaManager* schema_mgr_;

 public:
  TsVGroupPartition(std::filesystem::path root, int database_id, TsEngineSchemaManager* schema_mgr,
                    int64_t start, int64_t end);

  ~TsVGroupPartition();

  KStatus Open();
  // compact data from last segment to block segment. compact one block data every time.
  KStatus Compact();

  KStatus NewLastSegmentFile(std::unique_ptr<TsFile>* last_segment, uint32_t *ver);
  void PublicLastSegment(uint32_t file_number);

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;

  int GetDBId() const { return database_id_; }

  int64_t StartTs() const { return start_; }

  int64_t EndTs() const { return end_; }

  TsEngineSchemaManager* GetSchemaMgr() { return schema_mgr_; }

  TsBlockSegment* GetBlockSegment() { return blk_segment_.get(); }

  TsLastSegmentManager* GetLastSegmentMgr() { return &last_segment_mgr_; }

  KStatus AppendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                               uint32_t col_num, uint32_t row_num, timestamp64 max_ts, timestamp64 min_ts,
                               TSSlice block_data, TSSlice block_agg);
};


}  // namespace kwdbts
