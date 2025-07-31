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

#include "ee_op_factory.h"

#include <regex>

#include "ee_agg_scan_op.h"
#include "ee_aggregate_op.h"
#include "ee_distinct_op.h"
#include "ee_noop_op.h"
#include "ee_pb_plan.pb.h"
#include "ee_post_agg_scan_op.h"
#include "ee_scan_op.h"
#include "ee_sort_op.h"
#include "ee_statistic_scan_op.h"
#include "ee_synchronizer_op.h"
#include "ee_sort_scan_op.h"
#include "ee_table.h"
#include "ee_tag_scan_op.h"
#include "ee_window_op.h"
#include "ee_hash_tag_scan_op.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "ts_sampler_op.h"
#include "ee_result_collector_op.h"
#include "ee_local_inbound_op.h"
#include "ee_local_merge_inbound_op.h"
#include "ee_remote_inbound_op.h"
#include "ee_remote_merge_sort_inbound_op.h"
#include "ee_router_outbound_op.h"
#include "ee_local_outbound_op.h"

namespace kwdbts {

KStatus OpFactory::NewTagScan(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                              const TSProcessorCoreUnion& core,
                              BaseOperator** iterator, TABLE** table,
                              int32_t processor_id) {
  EnterFunc();
  // New tag reader operator
  const TSTagReaderSpec& readerSpec = core.tagreader();
  // generate HashTagScan op for hashTagScan, primaryHashTagScan and hashRelScan access mode
  // for multiple model processing
  if (readerSpec.accessmode() == TSTableReadMode::hashTagScan
        || readerSpec.accessmode() == TSTableReadMode::primaryHashTagScan
        || readerSpec.accessmode() == TSTableReadMode::hashRelScan) {
    *iterator = NewIterator<HashTagScanOperator>(collection,
        const_cast<TSTagReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  } else {
    *iterator = NewIterator<TagScanOperator>(collection,
        const_cast<TSTagReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  }

  if (!(*iterator)) {
    delete *table;
    *table = nullptr;
    LOG_ERROR("create TagScanOperator/HashTagScanOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewTableScan(kwdbContext_p ctx, TsFetcherCollection* collection,
                                const TSPostProcessSpec& post,
                                const TSProcessorCoreUnion& core,
                                BaseOperator** iterator, TABLE** table, int32_t processor_id) {
  EnterFunc();
  // New table reader operator
  const TSReaderSpec& readerSpec = core.tablereader();
  if (readerSpec.has_aggregator()) {
    *iterator = NewIterator<AggTableScanOperator>(collection,
        const_cast<TSReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  } else if (readerSpec.has_sorter() || readerSpec.offsetopt()) {
    *iterator = NewIterator<SortScanOperator>(collection,
        const_cast<TSReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  } else {
    if (post.outputcols_size() == 0 && post.renders_size() == 0) {
      auto rewrite_post = const_cast<TSPostProcessSpec*>(&post);
      rewrite_post->add_outputcols(0);
      rewrite_post->add_outputtypes(kwdbts::KWDBTypeFamily::TimestampTZFamily);
    }
    *iterator =
        NewIterator<TableScanOperator>(collection, const_cast<TSReaderSpec*>(&readerSpec),
                                       const_cast<TSPostProcessSpec*>(&post),
                                       *table, processor_id);
  }
  if (!(*iterator)) {
    LOG_ERROR("create TableScanOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewAgg(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                          const TSProcessorCoreUnion& core,
                          BaseOperator** iterator, TABLE** table, int32_t processor_id) {
  EnterFunc();
  // New agg operator
  const TSAggregatorSpec& aggSpec = core.aggregator();
  if (aggSpec.agg_push_down()) {
    *iterator = NewIterator<PostAggScanOperator>(collection,
        const_cast<TSAggregatorSpec*>(&aggSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  } else {
    if (aggSpec.group_cols_size() == aggSpec.ordered_group_cols_size() ||
        aggSpec.group_window_id() >= 0) {
      *iterator = NewIterator<OrderedAggregateOperator>(collection, const_cast<TSAggregatorSpec*>(&aggSpec),
          const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
    } else {
      // k_uint32 group_size = aggSpec.group_cols_size();
      // k_uint32 k_group_ptag_size = 0;
      // TableScanOperator* input = dynamic_cast<TableScanOperator*>(childIterator);
      // if (input) {
      //   for (k_int32 i = 0; i < group_size; ++i) {
      //     k_uint32 groupcol = aggSpec.group_cols(i);
      //     k_int32 col = -1;
      //     if (input->post_->renders_size() > 0) {
      //       std::string render = input->post_->renders(groupcol);
      //       render = render.substr(1);
      //       try {
      //         col = std::stoi(render) - 1;
      //         if (input->post_->outputcols_size() > 0) {
      //           col = input->post_->outputcols(col);
      //         }
      //       } catch (...) {
      //         break;
      //       }
      //     } else if (input->post_->outputcols_size() > 0) {
      //       col = input->post_->outputcols(groupcol);
      //     } else {
      //       col = groupcol;
      //     }

      //     if (col == -1 || col >= (*table)->field_num_) {
      //       break;
      //     }
      //     Field* field = (*table)->fields_[col];
      //     if (field->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      //       ++k_group_ptag_size;
      //     }
      //   }
      // }
      // use OrderedAggregate, when grouping only by ptag.
      // for example:
      // select ptag, sum(v) from t group by ptag;
      // if (group_size == k_group_ptag_size && (*table)->PTagCount() == k_group_ptag_size) {
      //   *iterator =
      //       NewIterator<OrderedAggregateOperator>(collection, const_cast<TSAggregatorSpec*>(&aggSpec),
      //     const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
      // } else {
        *iterator =
            NewIterator<HashAggregateOperator>(collection,
          const_cast<TSAggregatorSpec*>(&aggSpec),
          const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
      // }
    }
  }
  if (!(*iterator)) {
    LOG_ERROR("create AggregateOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewNoop(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                           const TSProcessorCoreUnion& core,
                           BaseOperator** iterator, TABLE** table, int32_t processor_id, bool only_operator) {
  EnterFunc();
  const TSNoopSpec& noopSpec = core.noop();
  if (!only_operator) {
    *iterator = NewIterator<NoopOperator>(collection, const_cast<TSNoopSpec*>(&noopSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  } else {
    *iterator = NewIterator<PassThroughNoopOperaotr>(collection, const_cast<TSNoopSpec*>(&noopSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
  }
  // New noop operator
  if (!(*iterator)) {
    LOG_ERROR("create NoopOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(SUCCESS);
}

/**
 * @brief :  InitTsSamplerIterator initializes a time series (TS) sampler
 * iterator. It creates and configures an iterator based on the provided time
 * series sampler specification.
 *
 *
 * @param :ctx Context of the database operation. It provides necessary
 * environment and state.
 * @param :core An operators set, including statistical information operator,
 * used to initialize the TsSampler iterator.
 * @param :iterator pointer TsSampler iterator pointer.
 * @param :table TABLE pointer
 * @param :childIterator Input iterator
 * @return : Returns a status code of type KStatus. If the iterator is
 * initialized successfully, returns SUCCESS. If an error is encountered during
 * initialization, FAIL is returned.
 *
 * @note :
 */
KStatus OpFactory::NewTsSampler(kwdbContext_p ctx, TsFetcherCollection* collection,
                                const TSProcessorCoreUnion& core,
                                BaseOperator** iterator, TABLE** table,
                                int32_t processor_id) {
  EnterFunc();
  const TSSamplerSpec& tsInfo = core.sampler();
  *iterator =
      NewIterator<TsSamplerOperator>(collection, *table, processor_id);
  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  if (KStatus::FAIL ==
      dynamic_cast<TsSamplerOperator*>(*iterator)->setup(&tsInfo)) {
    delete *iterator;
    *iterator = nullptr;
    LOG_ERROR("Setup TsSampler error when creating TsSamplerOperator.");
    Return(KStatus::FAIL);
  }

  Return(SUCCESS);
}

KStatus OpFactory::NewSort(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                           const TSProcessorCoreUnion& core,
                           BaseOperator** iterator, TABLE** table, int32_t processor_id) {
  EnterFunc();
  // New sort operator
  const TSSorterSpec& spec = core.sorter();

  *iterator = NewIterator<SortOperator>(collection, const_cast<TSSorterSpec*>(&spec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    LOG_ERROR("create SortIterator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewSynchronizer(kwdbContext_p ctx, TsFetcherCollection* collection,
                                   const TSProcessorSpec& procSpec,
                                   const TSPostProcessSpec& post,
                                   const TSProcessorCoreUnion& core,
                                   BaseOperator** iterator, TABLE** table,
                                   int32_t processor_id) {
  EnterFunc();
  // New synchronizer operator
  const TSSynchronizerSpec& mergeSpec = core.synchronizer();
  const TSOutputRouterSpec &output_spec = procSpec.output(0);
  k_uint32 stream_size = output_spec.streams_size();
  bool is_routor = false;
  for (k_uint32 i = 0; i < stream_size; i++) {
    StreamEndpointType output_stream_type = output_spec.streams(i).type();
    if (StreamEndpointType::REMOTE == output_stream_type) {
      is_routor = true;
      break;
    }
  }

  if (is_routor) {
    *iterator = NewIterator<RouterOutboundOperator>(collection, const_cast<TSOutputRouterSpec*>(&output_spec), *table);
  } else {
    *iterator = NewIterator<LocalOutboundOperator>(collection, const_cast<TSOutputRouterSpec*>(&output_spec), *table);
  }

  if (!(*iterator)) {
    LOG_ERROR("create SynchronizerOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  // 设置并行度
  OutboundOperator *outbound = dynamic_cast<OutboundOperator*>(*iterator);
  if (outbound) {
    outbound->SetDegree(mergeSpec.degree());
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewDistinct(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                               const TSProcessorCoreUnion& core,
                               BaseOperator** iterator, TABLE** table,
                               int32_t processor_id) {
  EnterFunc();
  // New distinct operator
  const DistinctSpec& spec = core.distinct();
  *iterator = NewIterator<DistinctOperator>(collection, const_cast<DistinctSpec*>(&spec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    LOG_ERROR("create DistinctOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewStatisticScan(
    kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
    const TSProcessorCoreUnion& core, BaseOperator** iterator,
    TABLE** table, int32_t processor_id) {
  EnterFunc();
  // Create StatisticReader Operator
  const TSStatisticReaderSpec& statisticReaderSpec = core.statisticreader();
  LOG_DEBUG("NewTableScan creating TableStatisticScanOperator");
  *iterator = NewIterator<TableStatisticScanOperator>(collection,
      const_cast<TSStatisticReaderSpec*>(&statisticReaderSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(SUCCESS);
}

KStatus OpFactory::NewWindowScan(kwdbContext_p ctx,
                                 TsFetcherCollection* collection,
                                 const TSPostProcessSpec& post,
                                 const TSProcessorCoreUnion& core,
                                 BaseOperator** iterator, TABLE** table,
                                 int32_t processor_id) {
  EnterFunc();
  // Create WindowerSpec Operator
  const WindowerSpec& windowerSpec = core.window();
  *iterator = NewIterator<WindowOperator>(
      collection, const_cast<WindowerSpec*>(&windowerSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(SUCCESS);
}

KStatus OpFactory::NewOp(kwdbContext_p ctx, TsFetcherCollection* collection, const TSProcessorSpec& procSpec,
                          const TSPostProcessSpec& post, const TSProcessorCoreUnion& core, BaseOperator** iterator,
                          TABLE** table, int32_t processor_id, bool only_operator) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  // New operator by type
  if (core.has_tagreader()) {
    ret = NewTagScan(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_tablereader()) {
    ret = NewTableScan(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_statisticreader()) {
    ret = NewStatisticScan(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_aggregator()) {
    ret = NewAgg(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_sampler()) {
    // collect statistic
    ret = NewTsSampler(ctx, collection, core, iterator, table, processor_id);
  } else if (core.has_noop()) {
    ret = NewNoop(ctx, collection, post, core, iterator, table, processor_id, only_operator);
  } else if (core.has_synchronizer()) {
    ret = NewSynchronizer(ctx, collection, procSpec, post, core, iterator, table, processor_id);
  } else if (core.has_sorter()) {
    ret = NewSort(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_distinct()) {
    ret = NewDistinct(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_window()) {
    ret = NewWindowScan(ctx, collection, post, core, iterator, table, processor_id);
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid operator type");
    ret = KStatus::FAIL;
  }
  if (*iterator) {
    (*iterator)->BuildRpcSpec(procSpec);
  }
  if (procSpec.final_ts_processor()) {
    (*iterator)->SetFinalOperator(true);
  }
  Return(ret);
}

KStatus OpFactory::NewResultCollectorOp(kwdbContext_p ctx,  BaseOperator **iterator) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  *iterator = NewIterator<ResultCollectorOperator>();

  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(KStatus::SUCCESS);
}

KStatus OpFactory::NewInboundOperator(kwdbContext_p ctx, TsFetcherCollection* collection, TSInputSyncSpec* spec,
                          BaseOperator **iterator, TABLE **table, bool is_remote, bool is_ordered) {
  EnterFunc();
  if (is_remote) {
    if (is_ordered) {
      *iterator = NewIterator<RemoteMergeSortInboundOperator>(collection, spec, *table);
    } else {
      *iterator = NewIterator<RemoteInboundOperator>(collection, spec, *table);
    }
  } else {
    if (is_ordered) {
      *iterator = NewIterator<LocalMergeInboundOperator>(collection, spec, *table);
    } else {
      *iterator = NewIterator<LocalInboundOperator>(collection, spec, *table);
    }
  }

  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(KStatus::SUCCESS);
}

KStatus OpFactory::NewOutboundOperator(kwdbContext_p ctx, TsFetcherCollection* collection,
                        TSOutputRouterSpec* spec, BaseOperator **iterator, TABLE **table, bool is_remote) {
  EnterFunc();
  if (is_remote) {
    *iterator = NewIterator<RouterOutboundOperator>(collection, spec, *table);
  } else {
    *iterator = NewIterator<LocalOutboundOperator>(collection, spec, *table);
  }

  if (!(*iterator)) {
    LOG_ERROR("create OutboundOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(KStatus::SUCCESS);
}

KStatus OpFactory::ReCreateOperoatr(kwdbContext_p ctx, BaseOperator *child, std::vector<BaseOperator *> *operators) {
  std::vector<BaseOperator *> &parents = child->GetParent();
  HashAggregateOperator *parent = dynamic_cast<HashAggregateOperator *>(parents[0]);
  TSAggregatorSpec *aggSpec = parent->spec_;
  k_uint32 group_size = aggSpec->group_cols_size();
  k_uint32 k_group_ptag_size = 0;
  TableScanOperator* input = dynamic_cast<TableScanOperator*>(child);
  if (input) {
    for (k_int32 i = 0; i < group_size; ++i) {
      k_uint32 groupcol = aggSpec->group_cols(i);
      k_int32 col = -1;
      if (input->post_->renders_size() > 0) {
        std::string render = input->post_->renders(groupcol);
        render = render.substr(1);
        try {
          col = std::stoi(render) - 1;
          if (input->post_->outputcols_size() > 0) {
            col = input->post_->outputcols(col);
          }
        } catch (...) {
          break;
        }
      } else if (input->post_->outputcols_size() > 0) {
        col = input->post_->outputcols(groupcol);
      } else {
        col = groupcol;
      }

      if (col == -1 || col >= (input->table())->field_num_) {
        break;
      }
      Field* field = (input->table())->fields_[col];
      if (field->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
        ++k_group_ptag_size;
      }
    }
  }

  // use OrderedAggregate, when grouping only by ptag.
  // for example:
  // select ptag, sum(v) from t group by ptag;
  if (group_size == k_group_ptag_size && (input->table())->PTagCount() == k_group_ptag_size) {
    BaseOperator* op = NewIterator<OrderedAggregateOperator>(parent->collection_, aggSpec, parent->post_,
                                                          parent->table(), parent->GetProcessorId());
    parent->RemoveDependency(child);
    op->AddDependency(child);
    BaseOperator *parent_parent = parent->GetParent()[0];
    parent_parent->RemoveDependency(parent);
    parent_parent->AddDependency(op);
    for (k_uint32 i = 0; i < operators->size(); ++i) {
      if ((*operators)[i] == parent) {
        (*operators)[i] = op;
        break;
      }
    }

    RpcSpecResolve &new_prc = op->GetRpcSpecInfo();
    new_prc = parent->GetRpcSpecInfo();
    op->SetFinalOperator(parent->IsFinalOperator());
    op->SetOutputEncoding(parent->GetOutputEncoding());
    op->SetUseQueryShortCircuit(parent->IsUseQueryShortCircuit());

    SafeDeletePointer(parent);
  }

  return KStatus::SUCCESS;
}

}  // namespace kwdbts
