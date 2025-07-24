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
#include "ee_base_op.h"
#include "ee_binary_expr.h"
#include "ee_global.h"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"

/**
 * @brief  Operator parameter
 *
 * @author liguoliang
 */

namespace kwdbts {

class TABLE;
class Field;
class TableFieldFunc;
class RowIterator;

class PostResolve {
 public:
  PostResolve(TSPostProcessSpec *post, TABLE *table);
  PostResolve(BaseOperator *input, TSPostProcessSpec *post, TABLE *table);
  virtual ~PostResolve();

  PostResolve(const PostResolve &) = delete;
  PostResolve &operator=(const PostResolve &) = delete;
  PostResolve(PostResolve &&) = delete;
  PostResolve &operator=(PostResolve &&) = delete;

  virtual EEIteratorErrCode ResolveFilter(kwdbContext_p ctx, Field **field, bool is_tagFilter);
  virtual void RenderSize(kwdbContext_p ctx, k_uint32 *num) = 0;
  virtual EEIteratorErrCode ResolveRender(kwdbContext_p ctx, Field ***render,
                                          k_uint32 num) = 0;
  virtual EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) = 0;

  EEIteratorErrCode ResolveOutputType(kwdbContext_p ctx, Field **render,
                                      k_uint32 num) const;
  EEIteratorErrCode ResolveOutputFields(kwdbContext_p ctx, Field **render,
                                  k_uint32 num, std::vector<Field*> &output_fields);
  EEIteratorErrCode ResolveScanTags(kwdbContext_p ctx);
  EEIteratorErrCode ResolveScanCols(kwdbContext_p ctx);
  // resolve relational cols for multiple model processing
  EEIteratorErrCode ResolveScanRelCols(kwdbContext_p ctx);

 protected:
  EEIteratorErrCode BuildBinaryTree(kwdbContext_p ctx, const KString &str,
                                    ExprPtr *expr) const;
  Field *ResolveBinaryTree(kwdbContext_p ctx, ExprPtr expr);
  Field *ResolveOperator(kwdbContext_p ctx, AstEleType operator_type,
                         Field *left, Field *right, std::list<Field *> args,
                         k_bool is_negative, KString func_name);
  EEIteratorErrCode ResolveConst(kwdbContext_p ctx,
                                 std::shared_ptr<Element> const_ptr,
                                 Field **field);
  Field *ResolveInOperator(kwdbContext_p ctx, Field *left, Field *right,
                           k_bool is_negative);

  EEIteratorErrCode ResolveInValueString(kwdbContext_p ctx, std::string substr,
                                         Field **malloc_field,
                                         k_bool *have_null);

  Field *ResolveInField(kwdbContext_p ctx, const std::string &str);

  Field *ResolveFuncOperator(kwdbContext_p ctx, KString &func_name,
                             std::list<Field *> args, k_bool is_negative);

  Field *ResolveCast(kwdbContext_p ctx, Field *left, KString &output_type);

 protected:
  TSPostProcessSpec *post_{nullptr};
  TABLE *table_{nullptr};
  k_uint32 renders_size_{0};
  k_uint32 outputcols_size_{0};
  Field **outputcols_{nullptr};
  std::list<Field *> new_fields_;
};

class BasePostResolve : public PostResolve {
 public:
  BasePostResolve(BaseOperator *input, TSPostProcessSpec *post, TABLE *table);
  virtual ~BasePostResolve();

  EEIteratorErrCode ResolveRender(kwdbContext_p ctx, Field ***render,
                                  k_uint32 num) override;

  virtual EEIteratorErrCode HandleRender(kwdbContext_p ctx, Field **render,
                                         k_uint32 num) = 0;

  EEIteratorErrCode ResolveOutputCol(kwdbContext_p ctx);

 protected:
  BaseOperator *input_{nullptr};
};

class ReaderPostResolve : public PostResolve {
  typedef PostResolve super;

 public:
  using PostResolve::PostResolve;

  EEIteratorErrCode ResolveFilter(kwdbContext_p ctx, Field **field, bool is_tagFilter) override;
  EEIteratorErrCode ResolveRender(kwdbContext_p ctx, Field ***render,
                                  k_uint32 num) override;
  void RenderSize(kwdbContext_p ctx, k_uint32 *num) override;
  EEIteratorErrCode ResolveReference(
      kwdbContext_p ctx, const std::shared_ptr<VirtualField> &virtualField,
      Field **field) override;

  Field *GetOutputField(kwdbContext_p ctx, k_uint32 index) {
    return outputcols_[index];
  }

  EEIteratorErrCode ResolveBlockFilter(TSReaderSpec *spec);

 private:
  k_bool is_filter_{0};
  Field **renders_{nullptr};
};


}  // namespace kwdbts

