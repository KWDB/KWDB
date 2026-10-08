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

#include "ee_inbound_op.h"
#include "ee_pipeline_task.h"
#include "ee_exec_pool.h"
#include "lg_api.h"
#include "ee_internal_type.h"

namespace kwdbts {

InboundOperator::InboundOperator(TsFetcherCollection* collection, TSInputSyncSpec* spec, TABLE *table)
    : BaseOperator(collection, table, nullptr, -1), spec_(spec) { }

InboundOperator::InboundOperator(const InboundOperator& other, int32_t processor_id)
    : BaseOperator(other), spec_(other.spec_) {}

EEIteratorErrCode InboundOperator::Init(kwdbContext_p ctx) {
  EnterFunc()
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  if (spec_->has_ordering()) {
    const TSOrdering& order = spec_->ordering();
    k_uint32 order_size_ = order.columns_size();
    for (k_uint32 i = 0; i < order_size_; i++) {
      if (kwdbts::TSOrdering_Column_Direction::
              TSOrdering_Column_Direction_ASC == order.columns(i).direction()) {
        asc_order_.push_back(true);
      } else {
        asc_order_.push_back(false);
      }
      null_first_.push_back(true);
      order_column_ids_.push_back(order.columns(i).col_idx());
      order_info_.push_back(ColumnOrderInfo{order.columns(i).col_idx(),
                                            order.columns(i).direction()});
    }
  }
  // inbound stream
  stream_size_ = spec_->streams_size();
  for (k_int32 i = 0; i < stream_size_; i++) {
    TSStreamEndpointSpec spec = spec_->streams(i);
    if (spec.type() == StreamEndpointType::LOCAL) {
      stream_id_ = spec.stream_id();
    } else if (spec.type() == StreamEndpointType::REMOTE) {
      target_id_ = spec.target_node_id();
      dest_processor_id_ = spec.dest_processor();
    }
  }

  code = ParserOutputFields(ctx);

  Return(code);
}

EEIteratorErrCode InboundOperator::Start(kwdbContext_p ctx) {
  EnterFunc()
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  Return(code);
}

EEIteratorErrCode InboundOperator::Reset(kwdbContext_p ctx) {
  EnterFunc()
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_uint32 sz = childrens_.size();
  for (k_uint32 i = 0; i < sz; i++) {
    code = childrens_[i]->Reset(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("childrens[%d] Reset failed.", i);
      break;
    }
  }
  Return(code);
}

EEIteratorErrCode InboundOperator::Close(kwdbContext_p ctx) {
  EnterFunc()
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  k_uint32 sz = childrens_.size();
  for (k_uint32 i = 0; i < sz; i++) {
    code = childrens_[i]->Close(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("childrens[%d] Close failed.", i);
      break;
    }
  }
  Return(code);
}

EEIteratorErrCode InboundOperator::ParserOutputFields(kwdbContext_p ctx) {
  if (!childrens_.empty()) {
    return EEIteratorErrCode::EE_OK;
  }

  k_int32 size_column_sizes = spec_->column_types_size();
  for (k_int32 i = 0; i < size_column_sizes; i++) {
    const std::string &str = spec_->column_types(i);
    Field *field = nullptr;
    KStatus status = GetInternalField(str.c_str(), str.length(), &field, i);
    if (status != KStatus::SUCCESS) {
      LOG_ERROR("GetInternalField faild");
      return EEIteratorErrCode::EE_ERROR;
    }
    output_fields_.push_back(field);
  }

  parser_output_fields_ = true;

  return EEIteratorErrCode::EE_OK;
}

}  // namespace kwdbts
