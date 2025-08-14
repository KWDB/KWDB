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

#include <vector>

#include "ee_base_op.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {
class OutboundOperator : public BaseOperator {
 public:
  OutboundOperator(TsFetcherCollection* collection, TSOutputRouterSpec* spec, TABLE *table);

  OutboundOperator(const OutboundOperator&, int32_t processor_id);

  virtual ~OutboundOperator();

  void SetDegree(k_int32 degree);
  k_int32 GetDegree();

  void SetCollected(bool is_collected) { is_collected_ =  is_collected; }

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  // KStatus CreateInputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) override;

  KStatus CreateOutputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &new_operators) override;

  KStatus CreateTopOutputChannel(kwdbContext_p ctx, std::vector<BaseOperator *> &operators) override;

  KStatus PullChunk(const DataChunkPtr& chunk) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_OPERATION, "Shouldn't call PullChunk from local exchange sink.");
    return KStatus::FAIL;
  }

  bool isSink() override { return true; }

  virtual k_bool HasOutput() { return true; }
  virtual k_bool NeedInput() { return true; }
  virtual k_bool IsFinished() { return true; }
  virtual KStatus SetFinishing() { return KStatus::SUCCESS; }

  Field **GetRender() { return childrens_[0]->GetRender(); }

  Field *GetRender(int i) { return childrens_[0]->GetRender(i); }

  k_uint32 GetRenderSize() { return childrens_[0]->GetRenderSize(); }

  std::vector<Field*>& OutputFields() { return childrens_[0]->OutputFields(); }

 protected:
  TSOutputRouterSpec* spec_;
  TSOutputRouterSpec_Type part_type_;
  k_int64 query_id_{1};  // instance id
  k_int32 stream_size_{0};
  std::vector<k_uint32> group_cols_;
  bool is_redefine_degree_{false};
  k_int32 degree_{1};
  k_uint32 total_rows_{0};
  DataChunkPtr chunk_tmp_;
  bool is_collected_{false};
};

}   // namespace kwdbts
