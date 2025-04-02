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
  TsLastSegmentManager last_segment_mgr_;
  std::unique_ptr<KRWLatch> partition_mtx_;

  TsEngineSchemaManager* schema_mgr_;

  // compact thread flag
  bool open_compact_thread_{false};
  // Id of the compact thread
  KThreadID compact_thread_id_{0};
  // Conditional variable
  std::condition_variable cv_;
  // Mutexes for condition variables
  std::mutex cv_mutex_;

 public:
  TsVGroupPartition(std::filesystem::path root, int database_id, TsEngineSchemaManager* schema_mgr,
                    int64_t start, int64_t end, bool open_compact_thread = true);

  ~TsVGroupPartition();

  KStatus Open();
  // compact data from last segment to block segment. compact one block data every time.
  KStatus Compact(int thread_num = 5);

  KStatus NewLastSegment(std::unique_ptr<TsLastSegment>* last_segment);
  void PublicLastSegment(std::unique_ptr<TsLastSegment>&& last_segment);

  std::filesystem::path GetPath() const;

  std::string GetFileName() const;

  int64_t StartTs() const { return start_; }

  int64_t EndTs() const { return end_; }

  TsEngineSchemaManager* GetSchemaMgr() { return schema_mgr_; }

  KStatus AppendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                               TSSlice block_data, TSSlice block_agg, uint32_t row_num);                      

 protected:
  // Thread scheduling executes compact tasks to clean up items that require erasing.
  void compactRoutine(void* args);
  // Initialize compact thread.
  void initCompactThread();
  // Close compact thread.
  void closeCompactThread();
};


}  // namespace kwdbts
