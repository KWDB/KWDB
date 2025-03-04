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
#include <map>
#include <vector>
#include <queue>

#include "ee_aggregate_op.h"
#include "ee_data_container.h"

namespace kwdbts {

/**
 * @brief
 *        PostAggScanOperator
 * @author
 */
class PostAggScanOperator : public HashAggregateOperator {
 public:
  PostAggScanOperator(TsFetcherCollection* collection, BaseOperator* input, TSAggregatorSpec* spec,
                                  TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  PostAggScanOperator(const PostAggScanOperator&, BaseOperator* input, int32_t processor_id);

  ~PostAggScanOperator() override {
    SafeDeleteArray(agg_output_col_info_);
  }

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

  KStatus ResolveAggFuncs(kwdbContext_p ctx) override;

  void CalculateAggOffsets() override;

 protected:
  void ResolveGroupByCols(kwdbContext_p ctx) override;

  KStatus accumulateRows(kwdbContext_p ctx) override;

  KStatus getAggResults(kwdbContext_p ctx, DataChunkPtr& chunk) override;

//  void CalculateInputOffsets();

  inline DataChunkPtr constructAggResults(k_uint32 capacity) {
    // Initialize the agg output buffer.
    auto agg_results_ = std::make_unique<DataChunk>(agg_output_col_info_, agg_output_col_num_, capacity);
    if (agg_results_->Initialize() != true) {
      agg_results_ = nullptr;
    }
    return agg_results_;
  }

  KStatus initDiskSink();

 protected:
  static const k_uint64 POST_AGG_SCAN_MAX_MEM_BUFFER_SIZE = BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE;
  std::queue<DataChunkPtr> processed_chunks_;
  bool pass_agg_{true};
  k_uint64 agg_result_counter_{0};

  std::map<k_uint32, k_uint32> agg_source_target_col_map_;
  ColumnInfo* agg_output_col_info_{nullptr};
  k_int32 agg_output_col_num_{0};
  DataContainerPtr disk_sink_;
};

}  // namespace kwdbts
