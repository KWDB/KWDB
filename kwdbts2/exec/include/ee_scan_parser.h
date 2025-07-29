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
#include "ee_base_parser.h"

namespace kwdbts {

class TSReaderSpec;

class TsTableScanParser : public TsScanParser {
 public:
  TsTableScanParser(TSReaderSpec* spec, TSPostProcessSpec* post, TABLE* table);

  EEIteratorErrCode ParserScanCols(kwdbContext_p ctx);

  Field *GetInputField(kwdbContext_p ctx, k_uint32 index) {
    return input_cols_[index];
  }

  EEIteratorErrCode ResolveBlockFilter();

 private:
  TSReaderSpec *spec_{nullptr};
};

}  // namespace kwdbts
