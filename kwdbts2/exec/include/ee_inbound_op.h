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
#include <set>
#include <memory>
#include <map>
#include <vector>
#include <queue>
#include <limits>
#include <string>
#include <ctime>

#include "ee_base_op.h"
#include "kwdb_type.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

class InboundOperator : public BaseOperator {
 public:
  InboundOperator(TsFetcherCollection* collection, TSInputSyncSpec* spec, TABLE *table);
  InboundOperator(const InboundOperator&, int32_t processor_id);

  virtual ~InboundOperator() = default;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;
  KStatus PushChunk(
      DataChunkPtr& chunk, k_int32 stream_id,
      EEIteratorErrCode code = EEIteratorErrCode::EE_OK) override {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_OPERATION,
                                  "Insufficient memory");
    return KStatus::FAIL;
  }

  // void PushTaskToQueue();

  bool isSource() override { return true; }
  virtual k_bool IsFinished() { return true; }
  virtual KStatus SetFinished() { return KStatus::SUCCESS; }
  virtual KStatus SetFinishing() { return KStatus::SUCCESS; }

  Field **GetRender() { return childrens_[0]->GetRender(); }

  Field *GetRender(int i) { return childrens_[0]->GetRender(i); }

  k_uint32 GetRenderSize() { return childrens_[0]->GetRenderSize(); }

  std::vector<Field*>& OutputFields() { return childrens_[0]->OutputFields(); }

 protected:
  EEIteratorErrCode ParserOutputFields(kwdbContext_p ctx);

 public:
  TSInputSyncSpec_Type type_;
  TSInputSyncSpec* spec_;
  k_int64 query_id_{1};  // instance id
  k_int32 stream_size_{0};
  k_int32 order_size_{0};
  k_int32 target_id_{0};
  k_int32 dest_processor_id_{0};
  k_int32 stream_id_{0};
  k_uint32 total_rows_{0};
  std::vector<k_bool> asc_order_;
  std::vector<k_bool> null_first_;
  std::vector<k_uint32> order_column_ids_;
  std::vector<ColumnOrderInfo> order_info_;   // for local merge use
  bool parser_output_fields_{false};
};

}   // namespace kwdbts
