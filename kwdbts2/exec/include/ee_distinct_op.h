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

#include "ee_base_op.h"
#include "ee_distinct_parser.h"
#include "ee_global.h"
#include "ee_data_chunk.h"
#include "ee_pb_plan.pb.h"
#include "ee_combined_group_key.h"
#include "ee_hash_table.h"

namespace kwdbts {

/**
 * @brief DistinctOperator
 * @author
 */
class DistinctOperator : public BaseOperator {
 public:
  /**
   * @brief Construct a new Aggregate Operator object
   *
   * @param input
   * @param spec
   * @param post
   * @param table
   */
  DistinctOperator(TsFetcherCollection* collection, DistinctSpec* spec,
                        PostProcessSpec* post, TABLE* table, int32_t processor_id);

  DistinctOperator(const DistinctOperator&, int32_t processor_id);

  virtual ~DistinctOperator();

  /*
    Inherited from Barcelato's virtual function
  */
  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  BaseOperator* Clone() override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_DISTINCT;}

 protected:
  // resolve distinct cols
  KStatus ResolveDistinctCols(kwdbContext_p ctx);

  DistinctSpec* spec_;
  TsDistinctParser param_;

  // distinct column
  std::vector<k_uint32> distinct_cols_;
  LinearProbingHashTable* seen_{nullptr};

  k_uint32 offset_{0};
  k_uint32 limit_{0};

  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
};

}  // namespace kwdbts
