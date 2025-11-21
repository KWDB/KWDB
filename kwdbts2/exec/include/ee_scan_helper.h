// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.
#pragma once

#include "ee_global.h"
#include "ee_scan_row_batch.h"
#include "kwdb_type.h"

namespace kwdbts {
class TableScanOperator;
class ScanHelper {
 public:
  explicit ScanHelper(TableScanOperator *op) { op_ = op; }
  virtual ~ScanHelper() {}
  virtual EEIteratorErrCode NextChunk(kwdbContext_p ctx, DataChunkPtr &chunk);
  virtual EEIteratorErrCode Init(kwdbContext_p ctx) {
    return EEIteratorErrCode::EE_OK;
  }
  virtual EEIteratorErrCode BeginMaterialize(kwdbContext_p ctx, ScanRowBatch *rowbatch) {
    return EEIteratorErrCode::EE_OK;
  }
  virtual EEIteratorErrCode Materialize(kwdbContext_p ctx, ScanRowBatch *rowbatch, DataChunkPtr &chunk);

  TsScanStats& GetTsScanStats() {return ts_scan_stats;}

 protected:
  TableScanOperator *op_{nullptr};
  // Scan stats for TsFetcher
  TsScanStats ts_scan_stats;
};
};  // namespace kwdbts
