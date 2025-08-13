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

#include <random>

#include "ee_outbound_op.h"
#include "ee_inbound_op.h"
#include "ee_exec_pool.h"

namespace kwdbts {

OutboundOperator::OutboundOperator(TsFetcherCollection* collection, TSOutputRouterSpec* spec, TABLE *table)
    : BaseOperator(collection, table, nullptr, -1), spec_(spec) {}

OutboundOperator::OutboundOperator(const OutboundOperator& other, int32_t processor_id) : BaseOperator(other),
  spec_(other.spec_),
  part_type_(other.part_type_),
  query_id_(other.query_id_),
  stream_size_(other.stream_size_),
  group_cols_(other.group_cols_),
  is_redefine_degree_(other.is_redefine_degree_),
  degree_(other.degree_) {
    is_clone_ = true;
}

OutboundOperator::~OutboundOperator() {
  if (is_clone_) {
    for (auto it : childrens_) {
      SafeDeletePointer(it);
    }
    childrens_.clear();
  }
}

void OutboundOperator::SetDegree(k_int32 degree) {
  degree_ = degree;
  if (0 == degree_) {
    degree_ = 1;
  }
}

k_int32 OutboundOperator::GetDegree() {
  if (is_redefine_degree_) {
    return degree_;
  }
  k_uint32 dop = degree_;
  // TagScan does not support parallelism, forcing parallelism to be set to 1
  if (table_->only_tag_) {
    dop = 1;
  }
  if (childrens_[0]->Type() == OperatorType::OPERATOR_SAMPLER) {
    dop = 1;
  }

  if (table_->GetAccessMode() < TSTableReadMode::tableTableMeta) {
    if (dop > table_->ptag_size_) {
      dop = table_->ptag_size_;
    }
  }

  if (dop > 1 && ExecPool::GetInstance().IsActive()) {
    dop = 2;
  }
  if (dop < 1) dop = 1;
  degree_ = dop;
  is_redefine_degree_ = true;
  return degree_;
}

EEIteratorErrCode OutboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  size_t sz = childrens_.size();
  for (auto i = 0; i < sz; ++i) {
    code = childrens_[i]->Init(ctx);
    if (EEIteratorErrCode::EE_OK!= code) {
      LOG_ERROR("OutboundOperator Init child failed.");
      Return(code);
    }
  }

  part_type_ = spec_->type();
  stream_size_ = spec_->streams_size();
  k_int32 hash_col_size = spec_->hash_columns_size();
  for (k_int32 i = 0; i < hash_col_size; ++i) {
    k_uint32 col = spec_->hash_columns(i);
    group_cols_.push_back(col);
  }
  Return(code);
}

EEIteratorErrCode OutboundOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  size_t sz = childrens_.size();
  for (auto i = 0; i < sz; ++i) {
    code = childrens_[i]->Start(ctx);
    if (EEIteratorErrCode::EE_OK!= code) {
      LOG_ERROR("OutboundOperator Start child failed.");
      Return(code);
    }
  }
  Return(code);
}

EEIteratorErrCode OutboundOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  size_t sz = childrens_.size();
  for (auto i = 0; i < sz; ++i) {
    code = childrens_[i]->Reset(ctx);
    if (EEIteratorErrCode::EE_OK!= code) {
      LOG_ERROR("OutboundOperator Reset child failed.");
      Return(code);
    }
  }

  Return(code);
}

EEIteratorErrCode OutboundOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  size_t sz = childrens_.size();
  for (auto i = 0; i < sz; ++i) {
    code = childrens_[i]->Close(ctx);
    if (EEIteratorErrCode::EE_OK!= code) {
      LOG_ERROR("OutboundOperator Reset child failed.");
      Return(code);
    }
  }

  Return(code);
}



KStatus OutboundOperator::CreateOutputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) {
  KStatus ret = KStatus::SUCCESS;
  BaseOperator *input_bound = parent_operators_[0]->GetInboundOperator();
  input_bound->AddDependency(this);
  parent_operators_[0]->RemoveDependency(this);

  return ret;
}

KStatus OutboundOperator::CreateTopOutputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &operators) {
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
