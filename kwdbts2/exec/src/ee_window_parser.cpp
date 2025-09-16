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


#include "ee_window_parser.h"
#include "ee_base_op.h"

namespace kwdbts {

TsWindowParser::~TsWindowParser() {
  if (window_field_) {
    for (k_uint32 i = 0; i < func_size_; i++) {
      SafeDeletePointer(window_field_[i]);
    }
    SafeFreePointer(window_field_);
  }
}

TsWindowParser::TsWindowParser(WindowerSpec *spec, PostProcessSpec *post, TABLE *table)
  : TsOperatorParser(post, table), spec_(spec) {
  func_size_ = spec_->windowfns_size();
}

void TsWindowParser::RenderSize(kwdbContext_p ctx, k_uint32 *num) {
  if (renders_size_ > 0) {
    *num = renders_size_;
  } else if (outputcol_count_ > 0) {
    *num = outputcol_count_;
  } else {
    // if renders_size_ and outputcols_size_ is 0, it has limit
    *num = func_size_ + input_->OutputFields().size();
  }
}

EEIteratorErrCode TsWindowParser::HandleRender(kwdbContext_p ctx, Field **render, k_uint32 num) {
  EnterFunc();
  // resolve agg func
  EEIteratorErrCode code = ParserWindowFuncCol(ctx, render, false);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }

  for (k_uint32 i = 0; i < renders_size_; ++i) {
    Expression render_expr = post_->render_exprs(i);
    // produce Binary tree
    ExprPtr expr;
    code = BuildBinaryTree(ctx, render_expr.expr(), &expr);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve Binary tree
    Field *field = ParserBinaryTree(ctx, expr);
    if (nullptr == field) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    } else {
      render[i] = field;
    }
  }

  if ((0 == renders_size_ && 0 == outputcol_count_) || (0 == renders_size_ && outputcol_count_ > 0)) {
    size_t count = input_->GetRenderSize();
    for (size_t i = 0; i < count + func_size_; i++) {
      if (i < count) {
        render[i] = input_->GetRender()[i];
      } else {
        render[i] = window_field_[i - count];
      }
    }
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TsWindowParser::ParserFilter(kwdbContext_p ctx, Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  is_has_filter_ = 1;
  code = TsOperatorParser::ParserFilter(ctx, field);
  is_has_filter_ = 0;

  Return(code);
}

EEIteratorErrCode TsWindowParser::ParserReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  for (auto i : virtualField->args_) {
    k_bool has_win_func = false;
    auto it = map_winfunc_index.find(i - 1);
    if (it != map_winfunc_index.end()) {
      has_win_func = true;
    }

    if (is_has_filter_) {
      if (nullptr == *field) {
        if (has_win_func) {
          *field = output_fields_[it->second];
        } else {
          auto iter = map_filter_index.find(i);
          if (iter != map_filter_index.end()) {
            *field = iter->second;
          } else {
            *field = input_->GetRender(i - 1)->field_to_copy();
            (*field)->table_ = table_;
            map_filter_index[i] = *field;
          }
        }
      } else {
        if (has_win_func) {
          k_uint32 outFieldIndex =
              i - 1 - input_->OutputFields().size();  // relative
          (*field)->next_ = output_fields_[it->second];
        } else {
          auto iter = map_filter_index.find(i);
          if (iter != map_filter_index.end()) {
            (*field)->next_ = iter->second;
          } else {
            (*field)->next_ = input_->GetRender(i - 1)->field_to_copy();
            map_filter_index[i] = (*field)->next_;
            (*field)->next_->table_ = table_;
          }
        }
      }
      continue;
    }

    if (nullptr == *field) {
      if (has_win_func) {
        *field = window_field_[it->second];
      } else {
        *field = input_->GetRender(i - 1);
      }
    } else {
      if (has_win_func) {
        (*field)->next_ = window_field_[it->second];
      } else {
        (*field)->next_ = input_->GetRender(i - 1);
      }
    }
  }
  Return(code);
}

void TsWindowParser::ParserWinFuncFields(std::vector<Field *> &output_fields) {
  for (k_int32 i = 0; i < func_size_; i++) {
    output_fields.push_back(window_field_[i]->field_to_copy());
  }
}

void TsWindowParser::ParserSetOutputFields(std::vector<Field *> &output_fields) {
  output_fields_ = output_fields;
}

void TsWindowParser::ParserFilterFields(std::vector<Field *> &fields) {
  for (auto &it : map_filter_index) {
    fields.push_back(it.second);
  }
}

EEIteratorErrCode TsWindowParser::ParserWindowFuncCol(kwdbContext_p ctx, Field **renders, k_uint32 num) {
  // alloc memory
  EnterFunc();
  EEIteratorErrCode code = MallocArray(ctx);
  if (code != EEIteratorErrCode::EE_OK) {
    Return(code);
  }

  for (k_uint32 i = 0; i < func_size_; i++) {
    Field *func_field = nullptr;
    const auto &fn = spec_->windowfns(i);
    // for (k_uint32 j = 0; j < fn.argsidxs_size(); j++) {
    k_uint32 col = fn.argsidxs(0);
    k_uint32 func_type = fn.func().windowfunc();
    switch (func_type) {
      case DIFF:
        func_field = new FieldFuncDiff(input_->GetRender()[col]);
        func_field->set_return_type(
            input_->GetRender()[col]->get_return_type());
        func_field->table_ = table_;
        break;
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE,
                                      "Undecided datatype");
        LOG_ERROR("unknow window function num : %d\n", i);
        Return(EEIteratorErrCode::EE_ERROR);
        break;
    }
    if (nullptr != func_field) {
      window_field_[i] = func_field;
    }
    map_winfunc_index[fn.outputcolidx()] = i;
  }

  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode TsWindowParser::MallocArray(kwdbContext_p ctx) {
  EnterFunc();
  window_field_ = static_cast<Field **>(malloc(func_size_ * sizeof(Field *)));
  if (nullptr == window_field_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("window_field_funcs_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(window_field_, 0, func_size_ * sizeof(Field *));
  Return(EEIteratorErrCode::EE_OK);
}

}  // namespace kwdbts
