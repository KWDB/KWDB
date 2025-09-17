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

#include "ee_aggregate_func.h"
#include "ee_aggregate_parser.h"
#include "ee_base_op.h"
#include "ee_combined_group_key.h"
#include "ee_data_chunk.h"
#include "ee_data_container.h"
#include "ee_global.h"
#include "ee_hash_table.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {
// Base Agg OP
class BaseAggregator : public BaseOperator {
 public:
  BaseAggregator(TsFetcherCollection* collection, TSAggregatorSpec* spec, PostProcessSpec* post,
                 TABLE* table, int32_t processor_id);

  BaseAggregator(const BaseAggregator&, int32_t processor_id);

  virtual ~BaseAggregator();

  /*
    Inherited from BseIterator's virtual function
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;   // init spec param
  EEIteratorErrCode Reset(kwdbContext_p ctx) override;  // Reset
  EEIteratorErrCode Close(kwdbContext_p ctx) override;  // close and free ctx

  virtual KStatus ResolveAggFuncs(kwdbContext_p ctx);  // make agg func
  virtual void ResolveGroupByCols(kwdbContext_p ctx);  // resolve agg cols

  void InitFirstLastTimeStamp(DatumRowPtr ptr);

  friend class AggregatorResolve;

 public:
  TSAggregatorSpec* spec_;

 protected:
  virtual void CalculateAggOffsets();  // calculate buffer offset

  // accumulaten for agg
  KStatus accumulateRowIntoBucket(kwdbContext_p ctx, DatumRowPtr bucket, k_uint32 agg_null_offset,
                                  IChunk* chunk, k_uint32 line);

  // The Hash Agg OP uses an internal bucket to persist the temporary output of
  // agg function execution.
  // For the last/lastts/last_row/last_rowts and first/firstts/first_row/first_rowts functions,
  // it needs an additional int64 to save the selected value's timestamp.
  // The Ordered Agg OP can hold the selected value's timestamp in the function's local.
  [[nodiscard]] k_uint32 fixLength(k_uint32 len) const {
    return append_additional_timestamp_ ? len + sizeof(KTimestamp) : len;
  }

  TsAggregateParser param_;
  // AggregationFunc is an interface to an aggregate function object (including
  // the AddOrUpdate method) that inherits a class such as MaxAggregate
  std::vector<unique_ptr<AggregateFunc>> funcs_;
  // the list of The inputs column's type and storage length
  std::vector<roachpb::DataType> col_types_;
  std::vector<k_uint32> col_lens_;

  // group col
  std::vector<k_uint32> group_cols_;

  // agg spec
  std::vector<TSAggregatorSpec_Aggregation> aggregations_;

  // Having filter
  Field* having_filter_{nullptr};  // filter

  // agg row width
  k_uint32 agg_row_size_{0};  // row width
  k_uint32 agg_null_offset_{0};

  std::vector<k_uint32> func_offsets_;

  TSAggregatorSpec_Type group_type_;
  k_bool is_done_{false};

  k_uint32 offset_{0};
  k_uint32 limit_{0};

  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};   // for limit examine
  k_uint32 total_read_row_{0};  // total read

  // for hash agg, we need an additional timestamp column to save the temporary Timestamp column for
  // last/last_row/first/first_row functions.
  bool append_additional_timestamp_{true};
};

/**
 * @brief
 *        HashAggregateOperator
 * @author
 */
class HashAggregateOperator : public BaseAggregator {
 public:
  using BaseAggregator::BaseAggregator;

  ~HashAggregateOperator();

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  BaseOperator* Clone() override;

  enum OperatorType Type() override { return OperatorType::OPERATOR_HASH_GROUP_BY; }

  friend class FieldAggNum;

 protected:
  // accumulate one batch
  KStatus accumulateBatch(kwdbContext_p ctx, IChunk* chunk);

  // accumulate one row
  KStatus accumulateRow(kwdbContext_p ctx, DataChunkPtr& chunk, k_uint32 line);

  // accumulate rows
  virtual KStatus accumulateRows(kwdbContext_p ctx);

  virtual KStatus getAggResults(kwdbContext_p ctx, DataChunkPtr& chunk);

  LinearProbingHashTable* ht_{nullptr};
  LinearProbingHashTable::iterator iter_;
};

/**
 * @brief
 *        OrderedAggregateOperator
 * @author
 */
class OrderedAggregateOperator : public BaseAggregator {
 public:
  OrderedAggregateOperator(TsFetcherCollection* collection, TSAggregatorSpec* spec,
                           PostProcessSpec* post, TABLE* table, int32_t processor_id);

  OrderedAggregateOperator(const OrderedAggregateOperator&, int32_t processor_id);

  ~OrderedAggregateOperator() override;

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  BaseOperator* Clone() override;

  enum OperatorType Type() override { return OperatorType::OPERATOR_SORT_GROUP_BY; }

  friend class FieldAggNum;

 protected:
  KStatus getAggResult(kwdbContext_p ctx, DataChunkPtr& chunk);

  KStatus ProcessData(kwdbContext_p ctx, DataChunkPtr& chunk);

  // construct agg info
  inline void constructAggResults() {
    // initialize the agg output buffer.
    agg_data_chunk_ = std::make_unique<DataChunk>(agg_output_col_info_, agg_output_col_num_);
    if (agg_data_chunk_->Initialize() != true) {
      agg_data_chunk_ = nullptr;
      return;
    }
    agg_data_chunk_->SetAllNull();
  }

  inline void handleEmptyResults(kwdbContext_p ctx) {
    // Queries like `SELECT MAX(n) FROM t` expect a row of NULLs if nothing was aggregated.
    if (!has_agg_result && group_type_ == TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR) {
      temporary_data_chunk_ =
          std::make_unique<DataChunk>(agg_output_col_info_, agg_output_col_num_, 1);
      if (temporary_data_chunk_->Initialize() != true) {
        temporary_data_chunk_ = nullptr;
        return;
      }
      temporary_data_chunk_->AddCount();
      temporary_data_chunk_->NextLine();
      temporary_data_chunk_->SetAllNull();

      // return 0, if agg type is COUNT_ROW or COUNT
      for (int i = 0; i < aggregations_.size(); i++) {
        if (aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS ||
            aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT ||
            aggregations_[i].func() == TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT) {
          // set not null ,default 0
          temporary_data_chunk_->SetNotNull(0, i);
        }
      }
      getAggResult(ctx, current_data_chunk_);
      temporary_data_chunk_.reset();
      has_agg_result = true;
    }
  }

  DataChunkPtr agg_data_chunk_;
  ColumnInfo* agg_output_col_info_{nullptr};  // construct agg output col
  k_int32 agg_output_col_num_{0};

  // used to save if the current row is a new group based on the input groupby information.
  GroupByMetadata group_by_metadata_;
  CombinedGroupKey last_group_key_;
  k_bool has_agg_result{false};
};

}  // namespace kwdbts
