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

#include <set>
#include <memory>
#include <map>
#include <vector>
#include <queue>
#include <limits>
#include <string>
#include <ctime>

#include "kwdb_type.h"
#include "ee_scan_op.h"
#include "ee_aggregate_parser.h"
#include "ee_aggregate_func.h"

namespace kwdbts {

// AggTableScanOperator is used by agg op
class AggTableScanOperator : public TableScanOperator {
 public:
  AggTableScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, PostProcessSpec* post,
                       TABLE* table, int32_t processor_id)
      : TableScanOperator(collection, spec, post, table, processor_id),
        aggregation_spec_(spec->aggregator()),
        aggregation_post_(spec->aggregatorpost()) {}

  AggTableScanOperator(const AggTableScanOperator& other, int32_t processor_id)
      : TableScanOperator(other, processor_id),
        aggregation_spec_(other.aggregation_spec_),
        aggregation_post_(other.aggregation_post_),
        has_post_agg_(other.has_post_agg_),
        agg_limit_{other.agg_limit_} {}

  ~AggTableScanOperator() override {
    SafeDeleteArray(agg_output_col_info_);
    SafeDeletePointer(agg_param_);
    if (agg_num_ > 0 && agg_renders_) {
      free(agg_renders_);
      agg_renders_ = nullptr;
    }

    for (auto field : agg_output_fields_) {
      SafeDeletePointer(field)
    }
    SafeFreePointer(group_cols_);
    SafeFreePointer(agg_source_target_col_map_);
    agg_num_ = 0;
    for (auto func : funcs_) {
      SafeDeletePointer(func);
    }
  };

  // resolve spec
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  // call Next for get data
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  // clone the operator for parallel
  BaseOperator* Clone() override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_AGG_SCAN;}

  // add data to trunk struct
  KStatus AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch);

  // process group col
  k_bool ProcessGroupCols(k_int32& target_row, RowBatch* row_batch,
                          GroupByColumnInfo* group_by_cols,
                          KTimestampTz& time_bucket, IChunk *chunk, k_uint32 row);
  KStatus FinishProcess(kwdbContext_p ctx);
  // resolve agg func
  KStatus ResolveAggFuncs(kwdbContext_p ctx);

  KStatus getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk);

  [[nodiscard]] bool hasTimeBucket() const {
    return interval_seconds_ != 0;
  }
  // timebucket
  void extractTimeBucket(Field** readers, k_uint32 render_num) {
    for (k_uint32 i = 0; i < render_num; ++i) {
      Field* field = readers[i];
      auto time_bucket_field = dynamic_cast<FieldFuncTimeBucket*>(field);
      if (time_bucket_field != nullptr) {
        interval_seconds_ = time_bucket_field->interval_seconds_;
        year_bucket_ = time_bucket_field->year_bucket_;
        timezone_ = time_bucket_field->time_zone_;
        // col_idx_ = time_bucket_field->get_num();
        col_idx_ = i;
      }
    }
  }

  // construct agg info
  inline void constructAggResults() {
    // initialize the agg output buffer.
    current_data_chunk_ = std::make_unique<DataChunk>(agg_output_col_info_, agg_output_col_num_);
    if (current_data_chunk_->Initialize() != true) {
      current_data_chunk_ = nullptr;
      return;
    }
    current_data_chunk_->SetAllNull();
  }

  template<typename T>
  void processGroupByColumn(char* source_ptr, char* target_ptr, uint32_t target_col,
                            bool is_dest_null, GroupByColumnInfo *group_by_cols, bool& is_new_group, k_int32 col_index) {
    if constexpr (std::is_same_v<T, std::string>) {
      auto source_str = String(source_ptr);
      k_uint32 len = source_str.length();
      if (is_dest_null) {
        is_new_group = true;
      } else {
        auto string_val_ptr = target_ptr + STRING_WIDE;
        k_uint16 typ_len = *static_cast<k_uint16*>(static_cast<void*>(target_ptr));
        auto target_str = String(string_val_ptr, typ_len);
        if (source_str.compare(target_str) != 0) {
          is_new_group = true;
        }
      }
      group_by_cols[col_index] = {target_col, source_ptr, len};
    } else {
      if (is_dest_null) {
        is_new_group = true;
      } else {
        T src_val = *reinterpret_cast<T*>(source_ptr);
        T dest_val = *reinterpret_cast<T*>(target_ptr);

        if constexpr (std::is_same_v<T, std::float_t> || std::is_same_v<T, std::double_t>) {
          if (std::abs(src_val - dest_val) < std::numeric_limits<double>::epsilon()) {
            is_new_group = true;
          }
        } else {
          if (src_val != dest_val) {
            is_new_group = true;
          }
        }
      }
      group_by_cols[col_index] = {target_col, source_ptr, sizeof(T)};
    }
  }

  void SetHasPostAgg(bool post_agg) { has_post_agg_ = post_agg; }

 private:
  k_uint32 col_idx_{0};
  k_int64 interval_seconds_{0};
  k_bool year_bucket_{false};
  k_int8 timezone_{0};

  // the list of input column's type
  std::vector<roachpb::DataType> data_types_;
  TsAggregateParser *agg_param_{nullptr};

  // group cols
  k_uint32* group_cols_{nullptr};
  k_uint32  group_cols_size_{0};

  // agg cols
  std::vector<k_uint32> agg_cols_;
  std::vector<k_uint32> normal_cols_;
  // storage agg funcs
  std::vector<AggregateFunc *> funcs_;

  // Aggregate spec
  std::vector<TSAggregatorSpec_Aggregation> aggregations_;
  const TSAggregatorSpec& aggregation_spec_;
  const PostProcessSpec& aggregation_post_;
  k_uint32* agg_source_target_col_map_{nullptr};

  Field** agg_renders_{nullptr};  // agg operator projection column
  k_uint32 agg_num_{0};           // the count of agg projection column

  std::vector<Field*> agg_output_fields_;  // the output field of agg operator
  ColumnInfo* agg_output_col_info_{nullptr};        // construct agg output col
  k_int32 agg_output_col_num_{0};
  bool is_resolve_datachunk_{false};

  // used to save if the current row is a new group based on the input groupby information.
  GroupByMetadata group_by_metadata_;
  bool disorder_{false};  // it is disorder if only group by timebucket
  bool has_post_agg_{false};
  k_uint32 agg_limit_{0};
  k_int64 max_ts_{INT64_MAX};
};

}  //  namespace kwdbts
