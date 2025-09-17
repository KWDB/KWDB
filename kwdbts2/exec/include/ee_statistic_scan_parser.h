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

#include "ee_base_parser.h"

#include <memory>

namespace kwdbts {

class TsStatisticScanParser : public TsScanParser {
 public:
  TsStatisticScanParser(TSStatisticReaderSpec *spec, PostProcessSpec *post,
                        TABLE *table)
      : TsScanParser(post, table), spec_(spec) {}

  ~TsStatisticScanParser() {}

  EEIteratorErrCode ParserRender(kwdbContext_p ctx, Field ***render,
                                 k_uint32 num) override;

  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;

  EEIteratorErrCode ParserReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;

  EEIteratorErrCode ResolveScanCols(kwdbContext_p ctx);

  k_int32 ResolveChecktTagCount();

 protected:
  EEIteratorErrCode NewAggBaseField(kwdbContext_p ctx, Field **field,
                                    Field *org_field, k_int32 agg_type,
                                    k_uint32 num);

 public:
  TSStatisticReaderSpec *spec_{nullptr};
  k_int16 insert_ts_index_{0};
  k_int16 statistic_last_tag_index_{0};
  k_int16 is_insert_ts_index_{0};
  k_int16 insert_last_tag_ts_num_{0};
  k_int32 tag_count_index_{-1};
  roachpb::DataType ts_type_{roachpb::DataType::TIMESTAMPTZ};

 protected:
  k_bool is_have_tag_first_{false};
};

}  // namespace kwdbts
