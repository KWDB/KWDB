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
#include <vector>
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

struct TsEntityPartitionAggIndex {
  TSTableID table_id = 0;
  TSEntityID entity_id = 0;
  uint32_t table_version = 0;
  timestamp64 min_ts = 0;
  timestamp64 max_ts = 0;
  TS_OSN max_osn = 0;
  uint64_t agg_offset = 0;
  uint64_t agg_len = 0;
  char reserved[16] = {0};
};
static_assert(sizeof(TsEntityPartitionAggIndex) == 80, "wrong size of TsEntityAggStats, please check TsEntityAggStats");

struct TsPartitionAggFooter {
  uint64_t entity_agg_stats_idx_offset = 0;
  uint64_t max_entity_id = 0;
  uint64_t file_version = 0;
  uint64_t magic_number = 0;
  char reserved[16] = {0};
};
static_assert(sizeof(TsPartitionAggFooter) == 48, "wrong size of TsPartitionAggFooter, please check TsPartitionAggFooter");


class TsPartitionAggBuilder {
 public:
  explicit TsPartitionAggBuilder(TsIOEnv* io_env, const fs::path& path, uint32_t max_entity_id);
  ~TsPartitionAggBuilder();
  KStatus Open();
  KStatus Close();
  KStatus AppendEntityAgg(const TSSlice& agg, TsEntityPartitionAggIndex& stats);
  KStatus Finalize();
  KStatus GetEntityAggIndex(TsEntityPartitionAggIndex& stats);

 private:
  TsIOEnv* io_env_{nullptr};
  fs::path file_path_;
  std::unique_ptr<TsAppendOnlyFile> w_file_{nullptr};
  std::vector<TsEntityPartitionAggIndex> entity_index_buffer_;
  TsBufferBuilder buffer_builder_;
  uint32_t max_entity_id_{0};
};

class TsPartitionAggReader {
 public:
  explicit TsPartitionAggReader(TsIOEnv* io_env, fs::path path);
  ~TsPartitionAggReader();
  KStatus Open();
  KStatus Reload();
  KStatus GetPartitionAggIndex(TsEntityPartitionAggIndex& stats);
  KStatus GetPartitionAgg(TSEntityID entity_id, TsSliceGuard& agg);
  void MarkDelete() { delete_after_free_ = true; }

 private:
  bool ready_{false};
  bool delete_after_free_{false};
  TsIOEnv* io_env_{nullptr};
  fs::path file_path_;
  std::unique_ptr<TsRandomReadFile> r_file_{nullptr};
  TsPartitionAggFooter footer_;
  TsSliceGuard footer_guard_;
  TsSliceGuard entity_index_data_;
  std::vector<TsEntityPartitionAggIndex> agg_index_buffer_;
};
}  // namespace kwdbts
