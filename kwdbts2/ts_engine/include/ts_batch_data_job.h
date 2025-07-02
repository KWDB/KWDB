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

namespace kwdbts {
class TsBatchDataJob {
 private:
  uint64_t job_id_;

 public:
  explicit TsBatchDataJob(uint64_t job_id) : job_id_(job_id) {}
  ~TsBatchDataJob() {}

  virtual KStatus Read(TSSlice* data, int32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Write(TSSlice* data, int32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual void Finish() {}

  virtual void Cancel() {}
};

class TSEngineV2Impl;
class TsReadBatchDataJob : public TsBatchDataJob {
 private:
  TSEngineV2Impl* ts_engine_;
  TSTableID table_id_;
  uint32_t table_version_;
  uint64_t begin_hash_;
  uint64_t end_hash_;
  KwTsSpan ts_span_;
  std::vector<std::pair<uint64_t, uint64_t>> entity_ids_;
  uint64_t cur_entity_idx_ = 0;
  TsBlockSpanSortedIterator* cur_entity_block_spans_ = nullptr;
  std::vector<std::shared_ptr<TsVGroup>>* vgroups_ = nullptr;

 public:
  TsReadBatchDataJob(TSEngineV2Impl* ts_engine, TSTableID table_id, uint32_t table_version,
                     uint64_t begin_hash, uint64_t end_hash, KwTsSpan ts_span, uint64_t job_id,
                     std::vector<std::pair<uint64_t, uint64_t>> entity_ids);

  KStatus Read(TSSlice* data, int32_t* row_num) override;

  void Finish() override;

  void Cancel() override;
};

class TsWriteBatchDataJob : public TsBatchDataJob {
 private:
  TSEngine* ts_engine_;
  TSTableID table_id_;
  uint32_t table_version_;

 public:
  TsWriteBatchDataJob(TSEngine* ts_engine, TSTableID table_id, uint32_t table_version, uint64_t job_id)
    : TsBatchDataJob(job_id), ts_engine_(ts_engine), table_id_(table_id), table_version_(table_version) {}

  KStatus Write(TSSlice* data, int32_t* row_num) override;

  void Finish() override;

  void Cancel() override;
};

}  // namespace kwdbts
