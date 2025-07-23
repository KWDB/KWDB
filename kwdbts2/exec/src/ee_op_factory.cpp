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

namespace kwdbts {

KStatus OpFactory::NewTagScan(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                              const TSProcessorCoreUnion& core,
                              BaseOperator** iterator, TABLE** table,
                              int32_t processor_id) {
  EnterFunc();
  // New tag reader operator
  const TSTagReaderSpec& readerSpec = core.tagreader();
  k_uint32 sId = 0;
  k_uint64 objId = readerSpec.tableid();
  *table = KNEW TABLE(sId, objId);
  if (*table == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  if (KStatus::FAIL == (*table)->Init(ctx, &readerSpec)) {
    delete *table;
    *table = nullptr;
    LOG_ERROR("Init table error when creating TableScanIterator.");
    Return(KStatus::FAIL);
  }
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
                                BaseOperator** iterator, TABLE** table,
                                BaseOperator* childIterator,
                                int32_t processor_id) {
  EnterFunc();
  // New table reader operator
  const TSReaderSpec& readerSpec = core.tablereader();
  if (readerSpec.has_aggregator()) {
    *iterator = NewIterator<AggTableScanOperator>(collection,
        const_cast<TSReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, childIterator,
        processor_id);
  } else if (readerSpec.has_sorter() || readerSpec.offsetopt()) {
    *iterator = NewIterator<SortScanOperator>(collection,
        const_cast<TSReaderSpec*>(&readerSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, childIterator, processor_id);
  } else {
    if (post.outputcols_size() == 0 && post.renders_size() == 0) {
      auto rewrite_post = const_cast<TSPostProcessSpec*>(&post);
      rewrite_post->add_outputcols(0);
      rewrite_post->add_outputtypes(kwdbts::KWDBTypeFamily::TimestampTZFamily);
    }
    *iterator =
        NewIterator<TableScanOperator>(collection, const_cast<TSReaderSpec*>(&readerSpec),
                                       const_cast<TSPostProcessSpec*>(&post),
                                       *table, childIterator, processor_id);
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
                          BaseOperator** iterator, TABLE** table,
                          BaseOperator* childIterator, int32_t processor_id) {
  EnterFunc();
  // New agg operator
  const TSAggregatorSpec& aggSpec = core.aggregator();
  if (aggSpec.agg_push_down()) {
    *iterator = NewIterator<PostAggScanOperator>(collection,
        childIterator, const_cast<TSAggregatorSpec*>(&aggSpec),
        const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
    AggTableScanOperator* input =
        dynamic_cast<AggTableScanOperator*>(childIterator);
    if (input) {
      input->SetHasPostAgg(true);
    }
  } else {
    if (aggSpec.group_cols_size() == aggSpec.ordered_group_cols_size() ||
        aggSpec.group_window_id() >= 0) {
      *iterator = NewIterator<OrderedAggregateOperator>(collection,
          childIterator, const_cast<TSAggregatorSpec*>(&aggSpec),
          const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
    } else {
      k_uint32 group_size = aggSpec.group_cols_size();
      k_uint32 k_group_ptag_size = 0;
      TableScanOperator* input = dynamic_cast<TableScanOperator*>(childIterator);
      if (input) {
        for (k_int32 i = 0; i < group_size; ++i) {
          k_uint32 groupcol = aggSpec.group_cols(i);
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

          if (col == -1 || col >= (*table)->field_num_) {
            break;
          }
          Field* field = (*table)->fields_[col];
          if (field->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
            ++k_group_ptag_size;
          }
        }
      }
      // use OrderedAggregate, when grouping only by ptag.
      // for example:
      // select ptag, sum(v) from t group by ptag;
      if (group_size == k_group_ptag_size && (*table)->PTagCount() == k_group_ptag_size) {
        *iterator =
            NewIterator<OrderedAggregateOperator>(collection, childIterator, const_cast<TSAggregatorSpec*>(&aggSpec),
                                                  const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
      } else {
        *iterator =
            NewIterator<HashAggregateOperator>(collection, childIterator, const_cast<TSAggregatorSpec*>(&aggSpec),
                                               const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
      }
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
                           BaseOperator** iterator, TABLE** table,
                           BaseOperator* childIterator, int32_t processor_id) {
  EnterFunc();
  const TSNoopSpec& noopSpec = core.noop();
  // New noop operator
  *iterator = NewIterator<NoopOperator>(collection,
      childIterator, const_cast<TSNoopSpec*>(&noopSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);
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
                                BaseOperator* childIterator,
                                int32_t processor_id) {
  EnterFunc();
  const TSSamplerSpec& tsInfo = core.sampler();
  *iterator =
      NewIterator<TsSamplerOperator>(collection, *table, childIterator, processor_id);
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
                           BaseOperator** iterator, TABLE** table,
                           BaseOperator* childIterator, int32_t processor_id) {
  EnterFunc();
  // New sort operator
  const TSSorterSpec& spec = core.sorter();

  *iterator = NewIterator<SortOperator>(collection,
      childIterator, const_cast<TSSorterSpec*>(&spec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    LOG_ERROR("create SortIterator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewSynchronizer(kwdbContext_p ctx, TsFetcherCollection* collection,
                                   const TSPostProcessSpec& post,
                                   const TSProcessorCoreUnion& core,
                                   BaseOperator** iterator, TABLE** table,
                                   BaseOperator* childIterator,
                                   int32_t processor_id) {
  EnterFunc();
  // New synchronizer operator
  const TSSynchronizerSpec& mergeSpec = core.synchronizer();
  *iterator = NewIterator<SynchronizerOperator>(collection,
      childIterator, const_cast<TSSynchronizerSpec*>(&mergeSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    LOG_ERROR("create SynchronizerOperator failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }
  Return(SUCCESS);
}

KStatus OpFactory::NewDistinct(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                               const TSProcessorCoreUnion& core,
                               BaseOperator** iterator, TABLE** table,
                               BaseOperator* childIterator,
                               int32_t processor_id) {
  EnterFunc();
  // New distinct operator
  const DistinctSpec& spec = core.distinct();
  *iterator = NewIterator<DistinctOperator>(collection,
      childIterator, const_cast<DistinctSpec*>(&spec),
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
    TABLE** table, BaseOperator* childIterator, int32_t processor_id) {
  EnterFunc();
  // Create StatisticReader Operator
  const TSStatisticReaderSpec& statisticReaderSpec = core.statisticreader();
  LOG_DEBUG("NewTableScan creating TableStatisticScanOperator");
  *iterator = NewIterator<TableStatisticScanOperator>(collection,
      const_cast<TSStatisticReaderSpec*>(&statisticReaderSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, childIterator, processor_id);

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
                                 BaseOperator* childIterator,
                                 int32_t processor_id) {
  EnterFunc();
  // Create WindowerSpec Operator
  const WindowerSpec& windowerSpec = core.window();
  *iterator = NewIterator<WindowOperator>(
      collection, childIterator, const_cast<WindowerSpec*>(&windowerSpec),
      const_cast<TSPostProcessSpec*>(&post), *table, processor_id);

  if (!(*iterator)) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    Return(KStatus::FAIL);
  }

  Return(SUCCESS);
}

KStatus OpFactory::NewOp(kwdbContext_p ctx, TsFetcherCollection* collection, const TSPostProcessSpec& post,
                         const TSProcessorCoreUnion& core,
                         BaseOperator** iterator, TABLE** table,
                         BaseOperator* childIterator, int32_t processor_id) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;
  // New operator by type
  if (core.has_tagreader()) {
    if (childIterator != nullptr) {
      LOG_ERROR("physical plan error");
      Return(KStatus::FAIL);
    }
    ret = NewTagScan(ctx, collection, post, core, iterator, table, processor_id);
  } else if (core.has_tablereader()) {
    ret = NewTableScan(ctx, collection, post, core, iterator, table, childIterator,
                       processor_id);
  } else if (core.has_statisticreader()) {
    ret = NewStatisticScan(ctx, collection, post, core, iterator, table, childIterator,
                           processor_id);
  } else if (core.has_aggregator()) {
    ret = NewAgg(ctx, collection, post, core, iterator, table, childIterator, processor_id);
  } else if (core.has_sampler()) {
    // collect statistic
    ret = NewTsSampler(ctx, collection, core, iterator, table, childIterator, processor_id);
  } else if (core.has_noop()) {
    ret =
        NewNoop(ctx, collection, post, core, iterator, table, childIterator, processor_id);
  } else if (core.has_synchronizer()) {
    ret = NewSynchronizer(ctx, collection, post, core, iterator, table, childIterator,
                          processor_id);
  } else if (core.has_sorter()) {
    ret =
        NewSort(ctx, collection, post, core, iterator, table, childIterator, processor_id);
  } else if (core.has_distinct()) {
    ret = NewDistinct(ctx, collection, post, core, iterator, table, childIterator,
                      processor_id);
  } else if (core.has_window()) {
    ret = NewWindowScan(ctx, collection, post, core, iterator, table,
                        childIterator, processor_id);
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid operator type");
    ret = KStatus::FAIL;
  }
  Return(ret);
}
}  // namespace kwdbts
