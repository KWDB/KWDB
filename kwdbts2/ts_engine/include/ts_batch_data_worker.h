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

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <list>
#include <utility>
#include "ts_block_span_sorted_iterator.h"
#include "ts_common.h"
#include "ts_const.h"
#include "ts_entity_segment_builder.h"
#include "ts_version.h"

namespace kwdbts {
class TsBatchDataWorker {
 private:
  uint64_t job_id_;

 protected:
  bool is_finished_ = false;

 public:
  explicit TsBatchDataWorker(uint64_t job_id) : job_id_(job_id) {}
  TsBatchDataWorker() {}

  uint64_t GetJobId() const { return job_id_; }

  virtual KStatus Init(kwdbContext_p ctx) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Read(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Write(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Finish(kwdbContext_p ctx) {
    is_finished_ = true;
    return KStatus::SUCCESS;
  }

  virtual void Cancel(kwdbContext_p ctx) {
    is_finished_ = true;
  }
};

class TSEngineV2Impl;
class TsReadBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineV2Impl* ts_engine_;
  TSTableID table_id_;
  uint32_t table_version_;
  KwTsSpan ts_span_;
  KwTsSpan actual_ts_span_;

  DATATYPE ts_col_type_;
  std::queue<std::pair<uint32_t, uint32_t>> vgroup_entity_ids_;
  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;
  std::shared_ptr<TsBlockSpanSortedIterator> block_spans_iterator_ = nullptr;

  std::string cur_block_span_data_;
  uint32_t n_cols_ = 0;

  uint32_t ts_block_span_len_offset = 0;

  KStatus GetTagData(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span);

  KStatus GetTsBlockSpanInfo(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span);

  KStatus NextBlockSpansIterator();

 public:
  TsReadBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id, uint32_t table_version, KwTsSpan ts_span,
                        uint64_t job_id, std::queue<std::pair<uint32_t, uint32_t>> vgroup_entity_ids);

  KStatus Init(kwdbContext_p ctx) override;

  static std::string GenKey(TSTableID table_id, uint32_t table_version, uint64_t begin_hash,
                            uint64_t end_hash, KwTsSpan ts_span);

  KStatus Read(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) override;
};

class TsWriteBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineV2Impl* ts_engine_;
  TSTableID table_id_;
  uint32_t table_version_;

  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;

  std::unordered_map<uint64_t, TS_LSN> vgroups_lsn_;

  std::map<PartitionIdentifier, std::shared_ptr<TsEntitySegmentBuilder>> entity_segment_builders_;

  KStatus GetTagPayload(TSSlice* data, std::shared_ptr<TsRawPayload>& payload_only_tag);

  KStatus UpdateLSN(uint32_t vgroup_id, TSSlice* input, std::string& result);

 public:
  TsWriteBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id, uint32_t table_version, uint64_t job_id);

  KStatus Init(kwdbContext_p ctx) override;

  KStatus Write(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) override;

  KStatus Finish(kwdbContext_p ctx) override;

  void Cancel(kwdbContext_p ctx) override;
};

}  // namespace kwdbts
