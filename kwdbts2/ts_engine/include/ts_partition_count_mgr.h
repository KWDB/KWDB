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
#include <vector>
#include <string>
#include "libkwdbts2.h"
#include "ts_io.h"
#include "ts_file_vector_index.h"

namespace kwdbts {

struct TsCountStatsFileHeader {
  uint8_t file_version;
  uint64_t version_num;
  TS_OSN max_osn;
  char reserverd[8];
};
static_assert(sizeof(TsCountStatsFileHeader) == 32, "wrong size of FileHeader, please check TsEntityCountHeader.");

struct TsEntityCountStats {
  TSTableID table_id;
  TSEntityID entity_id;
  timestamp64 min_ts;
  timestamp64 max_ts;
  uint64_t valid_count;
  bool is_count_valid;
  char reserverd[17];
};
static_assert(sizeof(TsEntityCountStats) == 64, "wrong size of TsEntityCountStat, please check TsEntityCountStat.");

struct CountStatMetaInfo {
  uint64_t file_number;
  std::vector<TsEntityCountStats> flush_infos;
};

class TsPartitionEntityCountManager {
 private:
  std::string path_;
  TsMMapAllocFile mmap_alloc_;
  TsCountStatsFileHeader* header_{nullptr};
  // get offset of first index node.
  VectorIndexForFile<uint64_t> index_;
  bool delete_after_free = false;

 public:
  explicit TsPartitionEntityCountManager(std::string path);
  ~TsPartitionEntityCountManager();
  KStatus Open();
  KStatus SetCountStatsHeader(uint8_t file_version, uint64_t version_num, TS_OSN max_osn);
  KStatus AddEntityCountStats(TsEntityCountStats& info);
  KStatus SetEntityCountStats(TsEntityCountStats& info);
  KStatus GetCountStatsHeader(TsCountStatsFileHeader& file_header);
  KStatus GetEntityCountStats(TsEntityCountStats& stats);
  void MarkDelete() { delete_after_free = true; }
  std::string FilePath() const { return path_; }

  KStatus Sync() { return mmap_alloc_.Sync(); }
  void DropAll();
  KStatus Reset();

 private:
  KStatus updateEntityCount(TsEntityCountStats* header, TsEntityCountStats* info);
};

}  // namespace kwdbts
