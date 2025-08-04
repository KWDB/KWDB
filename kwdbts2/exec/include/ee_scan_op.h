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
#include "ee_scan_helper.h"
#include "ee_scan_row_batch.h"
#include "ee_scan_parser.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "ee_global.h"

namespace kwdbts {

class ScanRowBatch;

class ReaderSpec;

class TABLE;

class KWDBPostProcessSpec;

class StorageHandler;

/**
 * @brief   scan operator
 *
 * @author  liguoliang
 */
class TableScanOperator : public BaseOperator {
 public:
  friend class ScanHelper;
  friend class WindowHelper;
  TableScanOperator(TsFetcherCollection* collection, TSReaderSpec* spec, TSPostProcessSpec* post, TABLE* table,
                                        int32_t processor_id);

  TableScanOperator(const TableScanOperator&, int32_t processor_id);

  ~TableScanOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_TABLE_SCAN;}

  BaseOperator* Clone() override;

  RowBatch* GetRowBatch(kwdbContext_p ctx) override;

  k_uint32 GetTotalReadRow() {return total_read_row_;}

 protected:
  EEIteratorErrCode InitHandler(kwdbContext_p ctx);
  EEIteratorErrCode InitScanRowBatch(kwdbContext_p ctx, ScanRowBatch **row_batch);
  k_bool ResolveOffset();
  EEIteratorErrCode InitHelper(kwdbContext_p ctx);

 public:
  TSReaderSpec* spec_{nullptr};

 protected:
  k_uint32 schema_id_{0};
  k_uint64 object_id_{0};
  std::vector<KwTsSpan> ts_kwspans_;
  k_uint32 limit_{0};
  k_uint32 offset_{0};
  TsTableScanParser param_;
  Field* filter_{nullptr};
  StorageHandler *handler_{nullptr};
  ScanRowBatch* row_batch_{nullptr};

  ScanHelper *helper_{nullptr};

 protected:
  k_uint32 cur_offset_{0};
  k_uint32 examined_rows_{0};
  k_uint32 total_read_row_{0};
  // if copy the data source using column mode.
  bool batch_copy_{false};
  bool ignore_outputtypes_{false};
};

}  // namespace kwdbts
