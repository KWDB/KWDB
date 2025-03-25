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
#include <vector>
#include <cstdio>

#include "ts_common.h"
#include "payload.h"
#include "ts_block_segment.h"
#include "ts_last_segment_manager.h"
#include "ts_metric_block.h"

namespace kwdbts {

class TsVGroupPartition {
 private:
  int database_id_;
  int64_t start_, end_;

  std::filesystem::path path_;

  std::unique_ptr<TsBlockSegment> blk_segment_;
  TsLastSegmentManager last_segments_;
  std::unique_ptr<KRWLatch> partition_mtx_;


 public:
  TsVGroupPartition(std::filesystem::path root, int database_id, int64_t start, int64_t end);

  ~TsVGroupPartition();

  KStatus Open();
  // compact data from last segment to block segment. compact one block data every time.
  KStatus Compact(TSTableID table_id, TSEntityID entity_id, const std::vector<AttributeInfo>& schema,
                  uint32_t table_version, uint32_t num, const std::vector<TsBlockColData>& col_datas);
  KStatus AppendToBlockSegment();

  KStatus FlushToLastSegment(const std::string& piece);

  KStatus NewLastFile(std::unique_ptr<TsFile>* file) { return last_segments_.NewLastFile(file); }

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;
  KStatus appendToBlockSegment(uint32_t table_version, TSSlice block_data, TSSlice block_agg,
                               uint32_t row_num, TSEntityID entity_id);

  int64_t StartTs() const { return start_; }

  int64_t EndTs() const { return end_; }

 private:
  KStatus appendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                               TSSlice block_data, TSSlice block_agg, uint32_t row_num);
};

}  // namespace kwdbts
