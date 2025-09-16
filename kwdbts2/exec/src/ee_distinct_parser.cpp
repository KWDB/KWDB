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

#include "ee_distinct_parser.h"

#include "ee_base_op.h"

namespace kwdbts {

TsDistinctParser::TsDistinctParser(DistinctSpec *spec, PostProcessSpec *post,
                                   TABLE *table)
    : TsOperatorParser(post, table), spec_(spec) {}

TsDistinctParser::~TsDistinctParser() {}

void TsDistinctParser::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcol_count_ > 0) {
    *num = outputcol_count_;
  } else {
    *num = input_->OutputFields().size();
  }
}

EEIteratorErrCode TsDistinctParser::HandleRender(kwdbContext_p ctx,
                                                 Field **render, k_uint32 num) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // outputcols_size_ > 0,  assign it to the render
  for (k_int32 i = 0; i < outputcol_count_; ++i) {
    k_uint32 tab = post_->output_columns(i);
    Field *field = input_->OutputFields()[tab];
    if (nullptr == field) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
    if (renders_size_ == 0) {
      (render)[i] = field;
    }
  }

  // renders_size_ > 0
  if (renders_size_ > 0) {
    for (k_int32 i = 0; i < renders_size_; ++i) {
      Expression render_expr = post_->render_exprs(i);
      // binary tree
      ExprPtr expr;
      code = BuildBinaryTree(ctx, render_expr.expr(), &expr);
      if (EEIteratorErrCode::EE_OK != code) {
        break;
      }
      // resolve tree
      Field *field = ParserBinaryTree(ctx, expr);
      if (nullptr == field) {
        code = EEIteratorErrCode::EE_ERROR;
        break;
      } else {
        (render)[i] = field;
      }
    }
  }

  if (0 == renders_size_ && 0 == outputcol_count_) {
    for (k_int32 i = 0; i < num; ++i) {
      render[i] = input_->OutputFields()[i];
    }
  }

  Return(code);
}

EEIteratorErrCode TsDistinctParser::ParserReference(
    kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
    Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    if (nullptr == *field) {
      *field = input_->OutputFields()[i - 1];
    } else {
      (*field)->next_ = input_->OutputFields()[i - 1];
    }
  }

  Return(code);
}

}  // namespace kwdbts
