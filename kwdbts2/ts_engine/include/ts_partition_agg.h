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
#include "ts_common.h"
#include "ts_io.h"


namespace kwdbts {

struct TsAggStatsFileHeader {
  TSEntityID max_entity_id;
  uint64_t version_num;
  TS_OSN max_osn;
  char reserved[16];
};
static_assert(sizeof(TsAggStatsFileHeader) == 40, "wrong size of TsAggStatsFileHeader, please check TsAggStatsFileHeader");

struct TsEntityAggStats {
  TSTableID table_id;
  TSEntityID entity_id;
  uint32_t table_version;
  timestamp64 min_ts;
  timestamp64 max_ts;
  TS_OSN max_osn;
  uint64_t agg_offset;
  uint64_t agg_len;
  char reserved[16];
};
static_assert(sizeof(TsEntityAggStats) == 80, "wrong size of TsEntityAggStats, please check TsEntityAggStats");

class TsPartitionAggEntityItemFile {
 public:
  explicit TsPartitionAggEntityItemFile(const fs::path& partition_path);
  ~TsPartitionAggEntityItemFile();
  KStatus Open();
  KStatus Close();
  KStatus GetPartitionAggHeader(TsAggStatsFileHeader& header);
  KStatus SetEntityAggStats(TsEntityAggStats& stats);
  KStatus GetEntityAggStats(TsEntityAggStats& stats);

 private:
  fs::path file_path_;
  TsMMapAllocFile file_;
  TsAggStatsFileHeader* header_{nullptr};
  VectorIndexForFile<uint64_t> index_;
};

class TsPartitionAggFile {
 public:
  TsPartitionAggFile() = default;
  explicit TsPartitionAggFile(TsIOEnv* env, const fs::path& path) : io_env_(env), file_path_(path) {}

  ~TsPartitionAggFile() {}

  KStatus Open();
  KStatus ReadAggData(uint64_t offset, TsSliceGuard* data, size_t len);

 private:
  TsIOEnv* io_env_{nullptr};
  fs::path file_path_;
  std::unique_ptr<TsRandomReadFile> r_file_{nullptr};
};

class TsPartitionAggFileBuilder {
 public:
  explicit TsPartitionAggFileBuilder(TsIOEnv* env, const fs::path& partition_path);

  ~TsPartitionAggFileBuilder() {}

  KStatus Open();
  KStatus Close();
  KStatus AppendAggBlock(const TSSlice& agg, uint64_t* offset);

 private:
  TsIOEnv* io_env_{nullptr};
  fs::path file_path_;
  // uint64_t file_number_;
  std::unique_ptr<TsAppendOnlyFile> w_file_{nullptr};
  // size_t file_size_ = 0;
};


class TsPartitionAggCalculator {
 public:
  explicit TsPartitionAggCalculator(TsIOEnv* io_env, const fs::path& path);
  ~TsPartitionAggCalculator();
  KStatus Open();
  KStatus Close();
  KStatus GetPartitionAggHeader(TsAggStatsFileHeader& header);
  KStatus AppendEntityAgg(const TSSlice& agg, TsEntityAggStats& stats);
  KStatus GetEntityAggStats(TsEntityAggStats& stats);

 private:
  TsIOEnv* io_env_{nullptr};
  // PartitionIdentifier partition_id;
  fs::path partition_path_;
  std::unique_ptr<TsPartitionAggEntityItemFile> entity_item_file_{nullptr};
  std::unique_ptr<TsPartitionAggFileBuilder> agg_builder_{nullptr};
};

class TsPartitionAggReader {
 public:
  explicit TsPartitionAggReader(TsIOEnv* io_env, const fs::path& path);
  ~TsPartitionAggReader();
  KStatus Open();
  KStatus GetPartitionAggHeader(TsAggStatsFileHeader& header);
  KStatus GetPartitionAggStats(TsEntityAggStats& stats);
  KStatus GetPartitionAgg(TSEntityID entity_id, TsSliceGuard& agg);

 private:
  TsIOEnv* io_env_{nullptr};
  // PartitionIdentifier partition_id;
  fs::path partition_path_;
  std::unique_ptr<TsPartitionAggEntityItemFile> entity_item_file_{nullptr};
  std::unique_ptr<TsPartitionAggFile> agg_file_{nullptr};
};
}  // namespace kwdbts
