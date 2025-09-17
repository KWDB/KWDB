// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#ifndef KWDBTS2_EXEC_TESTS_EE_ITERATOR_CREATE_TEST_H_
#define KWDBTS2_EXEC_TESTS_EE_ITERATOR_CREATE_TEST_H_

#include <memory>

#include "ee_iterator_data_test.h"
#include "ee_noop_op.h"
#include "ee_scan_op.h"
#include "ee_sort_op.h"
#include "ee_aggregate_op.h"
#include "ee_distinct_op.h"
#include "ee_statistic_scan_op.h"
#include "ee_tag_scan_op.h"
#include "ee_router_outbound_op.h"
#include "ee_remote_inbound_op.h"
#include "ee_remote_merge_sort_inbound_op.h"

namespace kwdbts {
class CreateIterator {
 public:
  CreateIterator() {}
  ~CreateIterator() {}
  PostProcessSpec *post_{nullptr};
  BaseOperator *iter_{nullptr};
  TABLE *table_{nullptr};
  virtual void TearDown() {
    SafeDeletePointer(post_);
    SafeDeletePointer(iter_);
  }
};

class CreateTagReader : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    CreateTagReaderSpec(&spec_, table_id);
    post_ = KNEW PostProcessSpec();
    table_ = KNEW TABLE(1, table_id);
    table_->Init(ctx, spec_);
    iter_ = NewIterator<TagScanOperator>(nullptr, spec_, post_, table_, 0);
  }
  void TearDown() {
    SafeDeletePointer(spec_);
    SafeDeletePointer(table_);
    CreateIterator::TearDown();
  }
  TSTagReaderSpec *spec_{nullptr};
};
class CreateTableReader : public CreateIterator {
 public:
  CreateTableReader() : CreateIterator() {}
  ~CreateTableReader() {}

  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    tag_reader_.SetUp(ctx, table_id);
    CreateReaderSpec(&spec_, table_id);
    CreateReaderPostProcessSpec(&post_);
    table_ = tag_reader_.table_;
    iter_ =
        NewIterator<TableScanOperator>(nullptr, spec_, post_, table_, 0);
  }

  void TearDown() {
    tag_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }
  CreateTagReader tag_reader_;
  TSReaderSpec *spec_{nullptr};
};

class CreateDistinct : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateDistinctSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ = NewIterator<DistinctOperator>(nullptr, spec_,
                                               post_, table_, 0);
  }

  void TearDown() {
    table_reader_.TearDown();
    SafeDeletePointer(iter_);
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
  }

  CreateTableReader table_reader_;
  DistinctSpec *spec_{nullptr};
};


class CreateAggregate : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateAggSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ = NewIterator<HashAggregateOperator>(nullptr, spec_,
                                               post_, table_, 0);
  }

  void TearDown() {
    table_reader_.TearDown();
    SafeDeletePointer(iter_);
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
  }

  CreateTableReader table_reader_;
  TSAggregatorSpec *spec_{nullptr};
};

class CreateSort : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateSortSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ =
        NewIterator<SortOperator>(nullptr, spec_, post_, table_, 0);
  }

  void TearDown(kwdbContext_p ctx) {
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }

  CreateTableReader table_reader_;
  TSSorterSpec *spec_{nullptr};
};

class CreateSynchronizerIterator : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateMergeSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    // iter_ = NewIterator<SynchronizerOperator>(nullptr, spec_, post_,
    //                                           table_, 0);
  }

  void TearDown() {
    CreateIterator::TearDown();
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
  }

  CreateTableReader table_reader_;
  TSSynchronizerSpec *spec_{nullptr};
};
class CreateSortAggregate : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    sync_iter_.SetUp(ctx, table_id);
    CreateAggSpecs(&spec_, &post_);
    table_ = sync_iter_.table_;
    iter_ = NewIterator<OrderedAggregateOperator>(nullptr, spec_, post_,
                                               table_, 0);
  }

  void TearDown() {
    CreateIterator::TearDown();
    sync_iter_.TearDown();
    SafeDeletePointer(spec_);
    SafeDeletePointer(post_);
  }

  CreateSynchronizerIterator sync_iter_;
  TSAggregatorSpec *spec_{nullptr};
};

class CreateNoop : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateNoopSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ =
        NewIterator<NoopOperator>(nullptr, spec_, post_, table_, 0);
  }

  void TearDown(kwdbContext_p ctx) {
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }

  CreateTableReader table_reader_;
  NoopCoreSpec *spec_{nullptr};
};

class CreateSinkOp : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    tag_reader_.SetUp(ctx, table_id);
    table_reader_.SetUp(ctx, table_id);
    table_reader_.iter_->AddDependency(tag_reader_.iter_);
    CreateSinkSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    TsFetcherCollection *col = nullptr;
    iter_ =
        NewIterator<RouterOutboundOperator>(col, spec_, nullptr);
    iter_->AddDependency(table_reader_.iter_);
  }

  void TearDown() {
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }
  CreateTagReader tag_reader_;
  CreateTableReader table_reader_;
  TSOutputRouterSpec *spec_{nullptr};
};

class CreateSourceOp : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateSourceSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ = NewIterator<RemoteInboundOperator>(nullptr, spec_, nullptr);
  }

  void TearDown() {
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }

  CreateTableReader table_reader_;
  TSInputSyncSpec *spec_{nullptr};
};

class CreateSourceMergeOp : public CreateIterator {
 public:
  void SetUp(kwdbContext_p ctx, k_uint32 table_id) {
    table_reader_.SetUp(ctx, table_id);
    CreateSourceMergeSpecs(&spec_, &post_);
    table_ = table_reader_.table_;
    iter_ = NewIterator<RemoteMergeSortInboundOperator>(nullptr, spec_, nullptr);
  }

  void TearDown() {
    table_reader_.TearDown();
    SafeDeletePointer(spec_);
    CreateIterator::TearDown();
  }

  CreateTableReader table_reader_;
  TSInputSyncSpec *spec_{nullptr};
};

}  // namespace kwdbts
#endif  // KWDBTS2_EXEC_TESTS_EE_ITERATOR_CREATE_TEST_H_
