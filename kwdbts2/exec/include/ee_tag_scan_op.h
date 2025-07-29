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
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ee_base_op.h"
#include "ee_tag_scan_parser.h"
#include "ee_tag_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"

namespace kwdbts {

/**
 * @brief   tag scan base operator for multiple model processing
 *
 * @author  YuYue
 */
class TagScanBaseOperator : public BaseOperator {
 public:
  TagScanBaseOperator(TsFetcherCollection *collection,
                      TABLE* table, TSPostProcessSpec* post, int32_t processor_id)
      : BaseOperator(collection, table, post, processor_id) {}
  virtual KStatus GetEntities(kwdbContext_p ctx,
                      std::vector<EntityResultIndex>* entities,
                      TagRowBatchPtr* row_batch_ptr) = 0;
  bool IsHasTagFilter() { return filter_ != nullptr; }

  KStatus CreateInputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) override {
    return KStatus::SUCCESS;
  }

 protected:
  Field* filter_{nullptr};
};


/**
 * @brief   scan oper
 *
 * @author  liguoliang
 */

class StorageHandler;

class TagScanOperator : public TagScanBaseOperator {
 public:
  TagScanOperator(TsFetcherCollection *collection, TSTagReaderSpec* spec,
                              TSPostProcessSpec* post, TABLE* table, int32_t processor_id);

  ~TagScanOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_TAG_SCAN;}

  RowBatch* GetRowBatch(kwdbContext_p ctx) override;

  KStatus GetEntities(kwdbContext_p ctx,
                      std::vector<EntityResultIndex>* entities,
                      TagRowBatchPtr* row_batch_ptr) override;

 protected:
  k_bool ResolveOffset();

 protected:
  TSTagReaderSpec* spec_{nullptr};
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  k_uint32 examined_rows_{0};   // valid row count
  k_uint32 total_read_row_{0};  // total count
  char* data_{nullptr};
  k_uint32 count_{0};
  TsTagScanParser param_;
  TagRowBatchPtr tag_rowbatch_{nullptr};
  StorageHandler* handler_{nullptr};

 private:
  mutable std::mutex tag_lock_;
  bool is_init_{false};
  bool started_{false};
  EEIteratorErrCode init_code_;
  EEIteratorErrCode start_code_;
  bool tag_index_once_{true};
  bool is_first_entity_{true};
};

}  // namespace kwdbts
