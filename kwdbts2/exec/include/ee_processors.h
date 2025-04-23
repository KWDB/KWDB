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
#include <vector>

#include "cm_assert.h"
#include "ee_base_op.h"
#include "ee_table.h"
#include "kwdb_type.h"
#include "ee_global.h"

namespace kwdbts {

class TSFlowSpec;

/**
 * @brief Physical plan processor
 *
 */
class Processors {
 private:
  /**
   * @brief Recursive creating operators' tree.
   * @param ctx
   * @param processor_id
   * @param iterator 
   * @param table 
   * @return KStatus
   */
  KStatus InitProcessorsOptimization(kwdbContext_p ctx, k_uint32 processor_id,
                                     BaseOperator **iterator, TABLE **table);
  inline EEIteratorErrCode EncodeDataChunk(kwdbContext_p ctx, DataChunk *chunk,
                                    EE_StringInfo msgBuffer, k_bool is_pg);
  inline void FindTopProcessorId(k_uint32 processor_id);

 public:
  Processors()
      : fspec_(nullptr),
        root_iterator_(nullptr),
        table_(nullptr),
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

 private:
  // tsflow spec of physical plan
  const TSFlowSpec *fspec_;
  // root operator
  BaseOperator *root_iterator_;
  // metadata table
  TABLE *table_{nullptr};
  // mark init
  k_bool b_init_{false};
  // mark close
  k_bool b_close_{false};
  std::list<BaseOperator *> iter_list_;
  // limit, for pgwire encoding
  k_int64 command_limit_{0};
  std::atomic<k_int64> count_for_limit_{0};
  TsFetcherCollection collection_;
  k_uint32 top_process_id_{0};
};
}  // namespace kwdbts
