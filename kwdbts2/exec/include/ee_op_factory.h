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

#include <vector>
#include "cm_assert.h"
#include "ee_base_op.h"
#include "kwdb_type.h"

namespace kwdbts {

/**
 * @brief Operator factory
 *
 */
class OpFactory {
 private:
  static KStatus NewTableScan(kwdbContext_p ctx, TsFetcherCollection* collection, const PostProcessSpec &post,
                             const TSProcessorCoreUnion &core,
                             BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewTagScan(kwdbContext_p ctx, TsFetcherCollection* collection, const PostProcessSpec &post,
                             const TSProcessorCoreUnion &core,
                             BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewAgg(kwdbContext_p ctx, TsFetcherCollection* collection, const PostProcessSpec &post,
                          const TSProcessorCoreUnion &core,
                          BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewNoop(kwdbContext_p ctx, TsFetcherCollection* collection, const PostProcessSpec &post,
                           const TSProcessorCoreUnion &core,
                           BaseOperator **iterator, TABLE **table, int32_t processor_id, bool only_operator);
  static KStatus NewSynchronizer(kwdbContext_p ctx, TsFetcherCollection* collection, const TSProcessorSpec& procSpec,
                                    const PostProcessSpec &post, const TSProcessorCoreUnion &core,
                                        BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewSynchronizer(kwdbContext_p ctx, TsFetcherCollection* collection, BaseOperator **iterator,
                                   TABLE **table, int32_t processor_id);
  static KStatus NewTsSampler(kwdbContext_p ctx, TsFetcherCollection* collection, const TSProcessorCoreUnion &core,
                            BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewSort(kwdbContext_p ctx, TsFetcherCollection* collection, const PostProcessSpec &post,
                           const TSProcessorCoreUnion &core,
                           BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewStatisticScan(kwdbContext_p ctx, TsFetcherCollection* collection,
                                      const PostProcessSpec &post,
                                      const TSProcessorCoreUnion &core,
                                      BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewDistinct(kwdbContext_p ctx, TsFetcherCollection* collection,
                                      const PostProcessSpec &post,
                                      const TSProcessorCoreUnion &core,
                                      BaseOperator **iterator, TABLE **table, int32_t processor_id);
  static KStatus NewWindowScan(kwdbContext_p ctx,
                               TsFetcherCollection *collection,
                               const PostProcessSpec &post,
                               const TSProcessorCoreUnion &core,
                               BaseOperator **iterator, TABLE **table,
                               int32_t processor_id);

 public:
  static KStatus NewOp(kwdbContext_p ctx, TsFetcherCollection* collection, const TSProcessorSpec& procSpec,
                        const PostProcessSpec &post, const TSProcessorCoreUnion &core, BaseOperator **iterator,
                        TABLE **table, int32_t processor, bool only_operator);

  static KStatus NewResultCollectorOp(kwdbContext_p ctx, BaseOperator **iterator);

  static KStatus NewInboundOperator(kwdbContext_p ctx, TsFetcherCollection* collection, TSInputSyncSpec* spec,
                                    BaseOperator **iterator, TABLE **table, bool is_remote, bool is_ordered);

  static KStatus NewOutboundOperator(kwdbContext_p ctx, TsFetcherCollection* collection,
                                    TSOutputRouterSpec* spec, BaseOperator **iterator, TABLE **table, bool is_remote);

  static KStatus ReCreateOperoatr(kwdbContext_p ctx, BaseOperator *child, std::vector<BaseOperator *> *operators);
};
};  // namespace kwdbts
