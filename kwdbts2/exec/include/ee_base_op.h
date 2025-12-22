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

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cm_kwdb_context.h"
#include "ee_data_chunk.h"
#include "ee_global.h"
#include "ee_operator_type.h"
#include "ee_row_batch.h"
#include "ee_rpc_parser.h"
#include "er_api.h"
#include "explain_analyse.h"

namespace kwdbts {

struct GroupByColumnInfo {
  k_uint32 col_index;
  char* data_ptr;
  k_uint32 len;
};

class TABLE;
class PipelineGroup;
class Field;
class Processors;

/**
 * @brief   The base class of the operator module
 *
 * @author  liguoliang
 */
class BaseOperator {
 public:
  BaseOperator() {}

  BaseOperator(TsFetcherCollection* collection, TABLE* table,
               PostProcessSpec* post, int32_t processor_id)
      : table_(table),
        processor_id_(processor_id),
        post_(post),
        collection_(collection) {
    if (nullptr != collection) {
      collection->emplace(processor_id, &fetcher_);
    }
  }

  explicit BaseOperator(const BaseOperator& other)
      : table_(other.table_),
        processor_id_(other.processor_id_),
        output_encoding_(other.output_encoding_),
        post_(other.post_),
        collection_(other.collection_),
        rpcSpecInfo_(other.rpcSpecInfo_),
        pipeline_(other.pipeline_),
        is_final_operator_(other.is_final_operator_),
        use_query_short_circuit_(other.use_query_short_circuit_) {
    if (nullptr != collection_) {
      collection_->emplace(processor_id_, &fetcher_);
    }
  }

  virtual ~BaseOperator() {
    if (num_ > 0 && renders_) {
      free(renders_);
    }

    for (auto field : output_fields_) {
      SafeDeletePointer(field);
    }
    SafeDeleteArray(output_col_info_);
    num_ = 0;
    renders_ = nullptr;
  }

  BaseOperator& operator=(const BaseOperator&) = delete;

  BaseOperator(BaseOperator&&) = delete;

  BaseOperator& operator=(BaseOperator&&) = delete;

  k_int32 GetProcessorId() { return processor_id_; }

  virtual enum OperatorType Type() = 0;

  virtual EEIteratorErrCode Init(kwdbContext_p ctx) = 0;

  virtual EEIteratorErrCode Start(kwdbContext_p ctx) = 0;

  virtual EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) = 0;

  virtual EEIteratorErrCode Reset(kwdbContext_p ctx) = 0;

  virtual EEIteratorErrCode Close(kwdbContext_p ctx) = 0;

  // get next batch data
  virtual EEIteratorErrCode Next(kwdbContext_p ctx) {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  bool isClone() { return is_clone_; }

  void SetUseQueryShortCircuit(bool use) { use_query_short_circuit_ = use; }
  bool IsUseQueryShortCircuit() { return use_query_short_circuit_; }

  void SetOutputTypeOid(const vector<k_uint32>& output_type_oid) { output_type_oid_ = std::move(output_type_oid); }
  vector<k_uint32> GetOutputTypeOid() { return output_type_oid_; }

  virtual bool isSink() { return false; }

  virtual bool isSource() { return false; }

  virtual k_bool HasOutput() { return true; }

  virtual k_bool NeedInput() { return true; }

  virtual void PushFinish(EEIteratorErrCode code, k_int32 stream_id,
                          const EEPgErrorInfo& pgInfo) {}

  virtual void PrintFinishLog() {}

  virtual KStatus BuildPipeline(PipelineGroup* pipeline, Processors* processor);

  virtual KStatus CreateInputChannel(kwdbContext_p ctx,
                                     std::vector<BaseOperator*>& new_operators);

  virtual KStatus CreateOutputChannel(
      kwdbContext_p ctx, std::vector<BaseOperator*>& new_operators);

  virtual KStatus CreateTopOutputChannel(kwdbContext_p ctx,
                                         std::vector<BaseOperator*>& operators);

  void BelongsToPipeline(PipelineGroup* pipeline) { pipeline_ = pipeline; }

  virtual void AddDependency(BaseOperator* children) {
    childrens_.push_back(children);
    children->parent_operators_.push_back(this);
  }

  virtual void AddParent(BaseOperator* parent) {
    parent_operators_.push_back(parent);
  }

  virtual void RemoveDependency(BaseOperator* children) {
    childrens_.erase(
        std::remove(childrens_.begin(), childrens_.end(), children),
        childrens_.end());
    children->parent_operators_.erase(
        std::remove(children->parent_operators_.begin(),
                    children->parent_operators_.end(), this),
        children->parent_operators_.end());
  }

  BaseOperator* GetInboundOperator() { return childrens_.back(); }

  void BuildRpcSpec(const TSProcessorSpec& tsProcessorSpec) {
    rpcSpecInfo_.BuildRpcSpec(tsProcessorSpec);
  }

  RpcSpecResolve& GetRpcSpecInfo() { return rpcSpecInfo_; }

  std::set<k_int32>& GetOutputStreamIds() { return rpcSpecInfo_.output_ids_; }

  std::set<k_int32>& GetInputStreamIds() { return rpcSpecInfo_.input_ids_; }

  bool HasChildren() { return !childrens_.empty(); }

  const char* GetTypeName();
  std::vector<BaseOperator*>& GetChildren() { return childrens_; }

  std::vector<BaseOperator*>& GetParent() { return parent_operators_; }

  virtual KStatus PushChunk(DataChunkPtr& chunk, k_int32 stream_id,
                            EEIteratorErrCode code = EEIteratorErrCode::EE_OK) {
    return KStatus::FAIL;
  }
  virtual KStatus PullChunk(kwdbContext_p ctx, DataChunkPtr& chunk) { return KStatus::FAIL; }

  virtual RowBatch* GetRowBatch(kwdbContext_p ctx) { return nullptr; }

  virtual Field** GetRender() { return renders_; }

  virtual Field* GetRender(int i) {
    if (i < num_)
      return renders_[i];
    else
      return nullptr;
  }

  // render size
  virtual k_uint32 GetRenderSize() { return num_; }

  // table object
  virtual TABLE* table() { return table_; }

  // output fields
  virtual std::vector<Field*>& OutputFields() { return output_fields_; }

  virtual BaseOperator* Clone() { return nullptr; }

  virtual k_uint32 GetTotalReadRow() { return 0; }

  static const k_uint64 DEFAULT_MAX_MEM_BUFFER_SIZE = 268435456;  // 256M

  void SetOutputEncoding(bool encode) { output_encoding_ = encode; }

  bool GetOutputEncoding() { return output_encoding_; }

  void SetFinalOperator(bool is_final) { is_final_operator_ = is_final; }
  bool IsFinalOperator() { return is_final_operator_; }

 protected:
  inline void constructDataChunk(k_uint32 capacity = 0) {
    current_data_chunk_ = std::make_unique<DataChunk>(
        output_col_info_, output_col_num_, capacity);
    if (current_data_chunk_->Initialize() != true) {
      current_data_chunk_ = nullptr;
      return;
    }
  }
  inline void constructFilterDataChunk(ColumnInfo* column_info, k_int32 num,
                                       k_uint32 capacity = 0) {
    current_filter_data_chunk_ =
        std::make_unique<DataChunk>(column_info, num, capacity);
    if (current_filter_data_chunk_->Initialize() != true) {
      current_filter_data_chunk_ = nullptr;
      return;
    }
  }

  inline EEIteratorErrCode InitOutputColInfo(
      std::vector<Field*>& output_fields) {
    output_col_num_ = output_fields.size();
    output_col_info_ = KNEW ColumnInfo[output_col_num_];
    if (output_col_info_ == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return EEIteratorErrCode::EE_ERROR;
    }
    for (k_int32 i = 0; i < output_fields.size(); i++) {
      output_col_info_[i] = ColumnInfo(output_fields[i]->get_storage_length(),
                                       output_fields[i]->get_storage_type(),
                                       output_fields[i]->get_return_type());
      output_col_info_[i].allow_null = output_fields[i]->is_allow_null();
      output_col_info_[i].sql_type = output_fields[i]->get_sql_type();
    }
    return EEIteratorErrCode::EE_OK;
  }

  TABLE* table_{nullptr};                         // table object
  k_int32 processor_id_{0};                      // operator ID
  std::vector<BaseOperator*> childrens_;         // input iterator
  std::vector<BaseOperator*> parent_operators_;  // parent operator
  Field** renders_{nullptr};  // the operator projection column of this layer
  k_uint32 num_{0};           // count of projected column
  // output columns of the current layer，input columns of Parent
  // operator(FieldNum)
  std::vector<Field*> output_fields_;
  ColumnInfo* output_col_info_{nullptr};
  k_int32 output_col_num_{0};

  DataChunkPtr current_data_chunk_;
  DataChunkPtr current_filter_data_chunk_;
  std::queue<DataChunkPtr> output_queue_;
  k_bool is_done_{false};

  bool is_clone_{false};
  bool output_encoding_{false};
  bool is_final_operator_{false};

  RpcSpecResolve rpcSpecInfo_;

  PipelineGroup* pipeline_{nullptr};

  bool use_query_short_circuit_{false};

  vector<k_uint32> output_type_oid_;

 public:
  PostProcessSpec* post_{nullptr};
  DataChunkPtr temporary_data_chunk_;
  OperatorFetcher fetcher_;
  TsFetcherCollection* collection_{nullptr};
};

template <class RealNode, class... Args>
BaseOperator* NewIterator(Args&&... args) {
  return new RealNode(std::forward<Args>(args)...);
}

class GroupByMetadata {
 public:
  GroupByMetadata() : capacity_(DEFAULT_CAPACITY) {}

  ~GroupByMetadata() { SafeDeleteArray(bitmap_); }

  k_bool reset(k_uint32 capacity) {
    k_bool code = true;
    if (capacity_ < capacity) {
      delete[] bitmap_;
      bitmap_ = nullptr;
      capacity_ = capacity;
      code = initialize();
    } else {
      k_uint32 len = (capacity_ + 7) / 8;
      std::memset(bitmap_, 0, len);
    }
    return code;
  }

  void setNewGroup(k_uint32 line) {
    bitmap_[line >> 3] |= (1 << 7) >> (line & 7);
  }

  bool isNewGroup(k_uint32 line) {
    return (bitmap_[line >> 3] & ((1 << 7) >> (line & 7))) != 0;
  }

  k_bool initialize() {
    k_bool code = true;
    k_uint32 len = (capacity_ + 7) / 8;
    bitmap_ = new char[len];
    if (bitmap_ != nullptr) {
      std::memset(bitmap_, 0, len);
    } else {
      code = false;
    }
    return code;
  }

 private:
  static const k_uint32 DEFAULT_CAPACITY = 1000;
  k_uint32 capacity_;
  char* bitmap_{nullptr};
};

}  // namespace kwdbts
