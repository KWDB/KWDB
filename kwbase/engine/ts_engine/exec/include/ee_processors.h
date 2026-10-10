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

#include <list>
#include <memory>
#include <vector>
#include <unordered_map>

#include "cm_assert.h"
#include "ee_base_op.h"
#include "ee_table.h"
#include "kwdb_type.h"
#include "ee_global.h"

namespace kwdbts {

class TSFlowSpec;
class PipelineGroup;
class PipelineTask;

/**
 * @brief Physical plan processor
 *
 */
class Processors {
 private:
  void FindTopProcessorId();
  KStatus BuildOperator(kwdbContext_p ctx);
  KStatus CreateTable(kwdbContext_p ctx, TABLE **table, const TSProcessorCoreUnion& core);
  KStatus ConnectOperator(kwdbContext_p ctx);
  KStatus ReCreateOperator(kwdbContext_p ctx);
  KStatus TransformOperator(kwdbContext_p ctx);
  KStatus BuildTopOperator(kwdbContext_p ctx);
  KStatus BuildPipeline(kwdbContext_p ctx);
  KStatus ScheduleTasks(kwdbContext_p ctx);
  inline EEIteratorErrCode EncodeDataChunk(kwdbContext_p ctx, DataChunk *chunk,
                                    EE_StringInfo msgBuffer, k_bool is_pg);

 public:
  Processors()
      : fspec_(nullptr),
        root_iterator_(nullptr),
        b_init_(KFALSE),
        b_close_(KFALSE) {}

  ~Processors() { Reset(); }

  Processors(const Processors &) = delete;
  Processors &operator=(const Processors &) = delete;

  void Reset();

  /**
   * @brief Init processors
   * @param ctx
   * @param fspec
   * @return KStatus
   */
  KStatus Init(kwdbContext_p ctx, const TSFlowSpec *fspec);

  KStatus InitIterator(kwdbContext_p ctx, TsNextRetState nextState);
  KStatus CloseIterator(kwdbContext_p ctx);
  /**
   * @brief Execute processors and encode
   * @param ctx
   * @param[out] buffer 
   * @param[out] count
   * @return KStatus
   */
  KStatus RunWithEncoding(kwdbContext_p ctx, char **buffer, k_uint32 *length,
                          k_uint32 *count, k_bool *is_last_record);
  KStatus RunWithVectorize(kwdbContext_p ctx, char **value, void *buffer, k_uint32 *length,
                          k_uint32 *count, k_bool *is_last_record);


  BaseOperator *GetRootIterator() { return root_iterator_; }

 public:
  k_bool b_is_cancel_{false};

 private:
  // tsflow spec of physical plan
  const TSFlowSpec *fspec_;
  // root operator
  BaseOperator *root_iterator_;
  // metadata table
  std::vector<TABLE *> tables_{nullptr};
  // mark init
  k_bool b_init_{false};
  // mark close
  k_bool b_close_{false};
  std::vector<BaseOperator *> operators_;
  std::vector<BaseOperator *> new_operators_;
  std::unordered_map<k_int32, BaseOperator *> stream_input_id_;
  // limit, for pgwire encoding
  k_int64 command_limit_{0};
  std::atomic<k_int64> count_for_limit_{0};
  TsFetcherCollection collection_;
  k_uint32 top_process_id_{0};
  PipelineGroup *root_pipeline_{nullptr};
  std::vector<PipelineGroup *> pipelines_;
  std::vector<std::weak_ptr<PipelineTask> > weak_tasks_;
};
}  // namespace kwdbts
