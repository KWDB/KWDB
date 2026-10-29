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
#include <memory>
#include <string>
#include <vector>

#include "cm_kwdb_context.h"
#include "ee_binary_expr.h"
#include "ee_global.h"


namespace kwdbts {


class PostProcessSpec;
class TABLE;
class Field;
class BaseOperator;

class TsBaseParser {
 public:
  TsBaseParser(PostProcessSpec *post, TABLE *table);

  virtual ~TsBaseParser();

  virtual EEIteratorErrCode ParserFilter(kwdbContext_p ctx, Field **field);

  virtual EEIteratorErrCode ParserReference(kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
                                                                                                   Field **field) = 0;

  virtual EEIteratorErrCode ParserRender(kwdbContext_p ctx, Field ***render, k_uint32 num) = 0;

  virtual void RenderSize(kwdbContext_p ctx, k_uint32 *num) = 0;

  virtual EEIteratorErrCode ParserOutputFields(kwdbContext_p ctx, Field **renders, k_uint32 num,
                          std::vector<Field*> &output_fields, bool ignore_outputtype);
  virtual k_int32 ParserInputRenderSize() {
    return 0;
  }

 protected:
  EEIteratorErrCode BuildBinaryTree(kwdbContext_p ctx, const KString &str, ExprPtr *expr) const;

  Field *ParserBinaryTree(kwdbContext_p ctx, ExprPtr expr);

  Field *ParserOperator(kwdbContext_p ctx, AstEleType operator_type, Field *left, Field *right,
                                std::list<Field *> args, k_bool is_negative, KString func_name);

  Field *ParserCast(kwdbContext_p ctx, Field *left, const KString &output_type);

  EEIteratorErrCode ParserConst(kwdbContext_p ctx, std::shared_ptr<Element> const_ptr, Field **field);

  Field *ParserFuncOperator(kwdbContext_p ctx, const KString &func_name, std::list<Field *> args, k_bool is_negative);

  Field *ParserInOperator(kwdbContext_p ctx, Field *left, Field *right, k_bool is_negative);

  EEIteratorErrCode ParserInValueString(kwdbContext_p ctx, std::string substr, Field **malloc_field, k_bool *has_null);

  Field *ParserInField(kwdbContext_p ctx, const std::string &str);

  EEIteratorErrCode BuildOutputFields(kwdbContext_p ctx, Field **renders, k_uint32 num,
          const std::vector<k_uint32> &outputcol_indexs, const std::vector<k_uint32> &outputcol_types,
          std::vector<Field*> &output_fields, bool ignore_outputtype);

 protected:
  PostProcessSpec *post_{nullptr};
  TABLE *table_{nullptr};
  k_uint32 renders_size_{0};
  std::list<Field *> new_fields_;
};


class TsScanParser : public TsBaseParser {
 public:
  TsScanParser(PostProcessSpec *post, TABLE *table);

  virtual ~TsScanParser();

  EEIteratorErrCode ParserFilter(kwdbContext_p ctx, Field **field) override;

  EEIteratorErrCode ParserInputField(kwdbContext_p ctx);

  EEIteratorErrCode ParserReference(kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
                                                                                       Field **field) override;

  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;

  EEIteratorErrCode ParserRender(kwdbContext_p ctx, Field ***render, k_uint32 num) override;

 protected:
  k_uint32 inputcols_count_{0};
  bool is_filter_{false};
  Field **input_cols_{nullptr};
};


class TsOperatorParser : public TsBaseParser {
 public:
  TsOperatorParser(PostProcessSpec *post, TABLE *table);

  virtual ~TsOperatorParser() {}

  void AddInput(BaseOperator *input) { input_ = input; }

  EEIteratorErrCode ParserRender(kwdbContext_p ctx, Field ***render, k_uint32 num) override;

  virtual EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                         k_uint32 num) = 0;
  k_int32 ParserInputRenderSize() override;

 protected:
  BaseOperator *input_{nullptr};
  k_uint32 outputcol_count_{0};
};

}  // namespace kwdbts
