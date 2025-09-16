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
#include "ee_statistic_scan_parser.h"
#include "ee_scan_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
namespace kwdbts {
class StorageHandler;
class TABLE;
class PostProcessSpec;
class TSStatisticReaderSpec;

class TableStatisticScanOperator : public BaseOperator {
 public:
  TableStatisticScanOperator(TsFetcherCollection* collection, TSStatisticReaderSpec *spec,
                             PostProcessSpec *post, TABLE *table, int32_t processor_id);
  TableStatisticScanOperator(const TableStatisticScanOperator&, int32_t processor_id);
  virtual ~TableStatisticScanOperator();
  EEIteratorErrCode Start(kwdbContext_p ctx) override;
  EEIteratorErrCode Init(kwdbContext_p ctx) override;
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;
  EEIteratorErrCode Close(kwdbContext_p ctx) override;
  EEIteratorErrCode Reset(kwdbContext_p ctx) override;
  BaseOperator* Clone() override;
  enum OperatorType Type() override {return OperatorType::OPERATOR_STATISTIC_SCAN;}
  k_int64 ProcessPTagSpanFilter(RowBatch *row_batch);

 protected:
  EEIteratorErrCode InitHandler(kwdbContext_p ctx);
  EEIteratorErrCode InitScanRowBatch(kwdbContext_p ctx,
                                     ScanRowBatch **row_batch);
  void ProcessScalar();

 protected:
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  std::vector<KwTsSpan> ts_kwspans_;
  TsStatisticScanParser param_;
  ScanRowBatch *row_batch_{nullptr};
  StorageHandler *handler_{nullptr};
  k_int32 tag_count_read_index_{-1};
  k_bool is_has_data_for_scalar_{false};
  k_bool is_scalar_{false};
  k_bool is_lastrow_optimize_{false};
};
}  // namespace kwdbts
