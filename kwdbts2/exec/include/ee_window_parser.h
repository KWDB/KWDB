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

#include <map>
#include <memory>
#include <vector>

#include "ee_base_parser.h"

namespace kwdbts {

class TsWindowParser : public TsOperatorParser {
 public:
  TsWindowParser(WindowerSpec *spec, TSPostProcessSpec *post, TABLE *table);

  ~TsWindowParser();

  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;

  EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                 k_uint32 num) override;

  EEIteratorErrCode ParserFilter(kwdbContext_p ctx, Field **field) override;

  EEIteratorErrCode ParserReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;

  void ParserWinFuncFields(std::vector<Field *> &output_fields);

  void ParserSetOutputFields(std::vector<Field *> &output_fields);

  void ParserFilterFields(std::vector<Field *> &fields);

 protected:
  EEIteratorErrCode ParserWindowFuncCol(kwdbContext_p ctx, Field **renders,
                                        k_uint32 num);

  EEIteratorErrCode MallocArray(kwdbContext_p ctx);

 protected:
  k_bool is_has_filter_{0};

 protected:
  WindowerSpec *spec_{nullptr};
  k_uint32 func_size_{0};
  Field **window_field_{nullptr};
  std::map<k_uint32, k_uint32> map_winfunc_index;
  std::vector<Field *> output_fields_;
  std::map<k_uint32, Field *> map_filter_index;
};

}  // namespace kwdbts
