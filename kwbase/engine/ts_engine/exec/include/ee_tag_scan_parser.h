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

#include "ee_base_parser.h"


namespace kwdbts {

class TSTagReaderSpec;

class TsTagScanParser : public TsScanParser {
 public:
  TsTagScanParser(TSTagReaderSpec* spec , PostProcessSpec *post, TABLE *table);

  KStatus ParserTagSpec(kwdbContext_p ctx);

  EEIteratorErrCode ParserScanTags(kwdbContext_p ctx);

  EEIteratorErrCode ParserScanTagsRelCols(kwdbContext_p ctx);

  k_uint64 GetObjectId() { return object_id_; }

 private:
  TSTagReaderSpec* spec_{nullptr};
  k_uint64 object_id_{0};
};

}  // namespace kwdbts
