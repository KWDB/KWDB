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

#include "ee_distinct_op.h"
#include "cm_func.h"
#include "lg_api.h"
#include "ee_common.h"
#include "ee_kwthd_context.h"

namespace kwdbts {

DistinctOperator::DistinctOperator(TsFetcherCollection* collection, DistinctSpec* spec,
                                   PostProcessSpec* post, TABLE* table, int32_t processor_id)
    : BaseOperator(collection, table, post, processor_id),
      spec_{spec},
      param_(spec, post, table),
      offset_(post->offset()),
      limit_(post->limit()) {
}

DistinctOperator::DistinctOperator(const DistinctOperator& other, int32_t processor_id)
    : BaseOperator(other),
      spec_(other.spec_),
      param_(other.spec_, other.post_, other.table_),
      offset_(other.offset_),
      limit_(other.limit_) {
  is_clone_ = true;
}

DistinctOperator::~DistinctOperator() {
  //  delete input
  if (is_clone_) {
    delete childrens_[0];
  }
  SafeDeletePointer(seen_);
}

EEIteratorErrCode DistinctOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  do {
    param_.AddInput(childrens_[0]);
    // init subquery iterator
    code = childrens_[0]->Init(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }
    // resolve renders num
    param_.RenderSize(ctx, &num_);
    // resolve render
    code = param_.ParserRender(ctx, &renders_, num_);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveRender() error\n");
      break;
    }

    // dispose Output Fields
    code = param_.ParserOutputFields(ctx, renders_, num_, output_fields_, false);
    if (EEIteratorErrCode::EE_OK != code) {
      LOG_ERROR("ResolveOutputFields() failed\n");
      break;
    }

    // dispose Distinct col
    KStatus ret = ResolveDistinctCols(ctx);
    if (ret != KStatus::SUCCESS) {
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }

    // custom hash set
    std::vector<roachpb::DataType> distinct_types;
    std::vector<k_uint32> distinct_lens;
    std::vector<bool> group_allow_null;
    std::vector<Field *> &input_fields = childrens_[0]->OutputFields();
    for (const auto& col : distinct_cols_) {
      distinct_types.push_back(input_fields[col]->get_storage_type());
      distinct_lens.push_back(input_fields[col]->get_storage_length());
      group_allow_null.push_back(input_fields[col]->is_allow_null());
    }

    seen_ = KNEW LinearProbingHashTable(distinct_types, distinct_lens, 0, group_allow_null);
    if (seen_ == nullptr || seen_->Resize() < 0) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    code = InitOutputColInfo(output_fields_);
  } while (0);
  Return(code);
}

EEIteratorErrCode DistinctOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  code = childrens_[0]->Start(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    Return(code);
  }

  // set current offset
  cur_offset_ = offset_;

  Return(code);
}

EEIteratorErrCode DistinctOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  std::chrono::_V2::system_clock::time_point start;
  int64_t read_row_num = 0;
  KWThdContext *thd = current_thd;

  while (true) {
    // read a batch of data
    DataChunkPtr data_chunk = nullptr;
    code = childrens_[0]->Next(ctx, data_chunk);
    if (code != EEIteratorErrCode::EE_OK) {
      break;
    }

    start = std::chrono::high_resolution_clock::now();
    read_row_num += data_chunk->Count();
    // result set
    if (nullptr == chunk) {
      chunk = std::make_unique<DataChunk>(output_col_info_, output_col_num_, data_chunk->Count());
      if (chunk->Initialize() != true) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        chunk = nullptr;
        Return(EEIteratorErrCode::EE_ERROR);
      }
    }

    thd->SetDataChunk(data_chunk.get());

    k_uint32 i = 0;
    data_chunk->ResetLine();
    while (i < data_chunk->Count()) {
      k_int32 row = data_chunk->NextLine();
      if (row < 0) {
        break;
      }

      k_uint64 loc;
      if (seen_->FindOrCreateGroups(data_chunk.get(), row, distinct_cols_, &loc) < 0) {
        Return(EEIteratorErrCode::EE_ERROR);
      }

      // not find
      if (!seen_->IsUsed(loc)) {
        // limit
        if (limit_ && examined_rows_ >= limit_) {
          code = EEIteratorErrCode::EE_END_OF_RECORD;
          break;
        }

        // offset
        if (cur_offset_ > 0) {
          --cur_offset_;
          continue;
        }

        // insert data
        FieldsToChunk(GetRender(), GetRenderSize(), i, chunk);
        chunk->AddCount();


        // rows++
        ++examined_rows_;
        ++i;

        // update distinct info
        seen_->SetUsed(loc);
        seen_->CopyGroups(data_chunk.get(), row, distinct_cols_, loc);
      }
    }

    if (0 == chunk->Count()) {
      chunk = nullptr;
      continue;
    } else {
      break;
    }
  }
  auto end = std::chrono::high_resolution_clock::now();

  if (chunk != nullptr && chunk->Count() > 0) {
    OPERATOR_DIRECT_ENCODING(ctx, output_encoding_, use_query_short_circuit_, output_type_oid_, thd, chunk);
    fetcher_.Update(read_row_num, (end - start).count(), chunk->Count() * chunk->RowSize(), 0, 0, chunk->Count());
    Return(EEIteratorErrCode::EE_OK)
  }

  if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
    fetcher_.Update(0, (end - start).count(), 0, seen_->Capacity() * seen_->tupleSize(), 0, 0);
  }

  Return(code);
}

EEIteratorErrCode DistinctOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = childrens_[0]->Close(ctx);
  Reset(ctx);

  Return(code);
}

EEIteratorErrCode DistinctOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();
  childrens_[0]->Reset(ctx);

  Return(EEIteratorErrCode::EE_OK);
}

BaseOperator* DistinctOperator::Clone() {
  BaseOperator* input = childrens_[0]->Clone();
  if (input == nullptr) {
    return nullptr;
  }
  BaseOperator* iter = NewIterator<DistinctOperator>(*this, this->processor_id_);
  if (nullptr != iter) {
    iter->AddDependency(input);
  } else {
    delete input;
  }
  return iter;
}

KStatus DistinctOperator::ResolveDistinctCols(kwdbContext_p ctx) {
  EnterFunc();

  k_int32 count = spec_->distinct_columns_size();
  for (k_int32 i = 0; i < count; ++i) {
    distinct_cols_.push_back(spec_->distinct_columns(i));
  }

  Return(KStatus::SUCCESS);
}

}  // namespace kwdbts
