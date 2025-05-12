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

#include "ee_processors.h"

#include <stdlib.h>

#include <string>

#include "cm_fault_injection.h"
#include "ee_cancel_checker.h"
#include "ee_kwthd_context.h"
#include "ee_op_factory.h"
#include "ee_pb_plan.pb.h"
#include "ee_pg_result.h"
#include "lg_api.h"

// support returning multi-rows datas
#define TSSCAN_RS_MULTILINE_SEND 1
#define TSSCAN_RS_SIZE_LIMIT 32768
namespace kwdbts {
/**
 *  Recursive creating operators' tree.
 *  The initialized structure is a multitree structure, 
 *  each node is an operator.
 *        5
 *      /  \
 *     4    2
 *    /    /  \
 *   3    0    1
 */
KStatus Processors::InitProcessorsOptimization(kwdbContext_p ctx,
                                               k_uint32 processor_id,
                                               BaseOperator** iterator,
                                               TABLE** table) {
  EnterFunc();
  const TSProcessorSpec& procSpec = fspec_->processors(processor_id);
  // child as input for parent iterator
  BaseOperator* child = nullptr;
  // Recursive new child operator
  if (procSpec.input_size() > 0) {
    TSInputSyncSpec input = procSpec.input(0);
    if (input.streams_size() > 0) {
      TSStreamEndpointSpec stream = input.streams(0);
      k_uint32 child_id = processor_id - 1;
      if (KStatus::SUCCESS != InitProcessorsOptimization(ctx, child_id, &child, table)) {
        Return(KStatus::FAIL);
      }
    }
  }
  // New root operator
  const TSPostProcessSpec& post = procSpec.post();
  const TSProcessorCoreUnion& core = procSpec.core();
  KStatus ret = OpFactory::NewOp(ctx, &collection_, post, core, iterator, table, child, procSpec.processor_id());
  if (ret != SUCCESS) {
    Return(ret);
  }
  if (processor_id == top_process_id_) {
    (*iterator)->SetOutputEncoding(true);
  }
  command_limit_ = post.commandlimit();
  iter_list_.push_back(*iterator);
  Return(ret);
}

void Processors::FindTopProcessorId(k_uint32 processor_id) {
  const TSProcessorCoreUnion& core = fspec_->processors(processor_id).core();
  top_process_id_ = processor_id;
  if (core.has_synchronizer()) {
    top_process_id_ = processor_id - 1;
  }
}

// Init processors
KStatus Processors::Init(kwdbContext_p ctx, const TSFlowSpec* fspec) {
  EnterFunc();
  AssertNotNull(fspec);
  fspec_ = fspec;
  if (fspec_->processors_size() < 1) {
    LOG_ERROR("The flowspec has no processors.");
    Return(KStatus::FAIL);
  }

  k_int32 processor_id = fspec_->processors_size() - 1;
  FindTopProcessorId(processor_id);
  // New operator
  KStatus ret = InitProcessorsOptimization(ctx, processor_id, &root_iterator_, &table_);
  if (KStatus::SUCCESS != ret) {
    LOG_ERROR("Init processors error.");
    Return(KStatus::FAIL);
  }
  if (CheckCancel(ctx) != SUCCESS) {
    Return(FAIL);
  }

  EEIteratorErrCode code = root_iterator_->Init(ctx);
  INJECT_DATA_FAULT(FAULT_EE_DML_SETUP_PREINIT_MSG_FAIL, code,
                    EEIteratorErrCode::EE_ERROR, nullptr);
  if (code != EEIteratorErrCode::EE_OK) {
    LOG_ERROR("Preinit iterator error when initing processors.");
    Return(KStatus::FAIL);
  }
  if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
    root_iterator_->Close(ctx);
    Return(KStatus::FAIL);
  }
  b_init_ = KTRUE;
  Return(KStatus::SUCCESS);
}

void Processors::Reset() {
  b_init_ = KFALSE;
  for (auto it : iter_list_) {
    SafeDeletePointer(it)
  }
  iter_list_.clear();
  root_iterator_ = nullptr;
  if (table_) {
    delete table_;
    table_ = nullptr;
  }
}

KStatus Processors::InitIterator(kwdbContext_p ctx, TsNextRetState nextState) {
  EnterFunc();
  if (!b_init_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Can't init operators again");
    Return(KStatus::FAIL);
  }
  AssertNotNull(root_iterator_);
  EEPgErrorInfo::ResetPgErrorInfo();
  KWThdContext * thd = current_thd;
  thd->SetPgEncode(nextState);
  thd->SetCommandLimit(&command_limit_);
  thd->SetCountForLimit(&count_for_limit_);
  // Init operators
  EEIteratorErrCode code = root_iterator_->Start(ctx);
  if (EEIteratorErrCode::EE_OK != code) {
    root_iterator_->Close(ctx);
    Return(KStatus::FAIL);
  }
  if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
    root_iterator_->Close(ctx);
    Return(KStatus::FAIL);
  }
  Return(KStatus::SUCCESS);
}

KStatus Processors::CloseIterator(kwdbContext_p ctx) {
  EnterFunc();
  if (!b_init_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Can't close operators");
    Return(KStatus::FAIL);
  }
  if (b_close_) {
    Return(KStatus::SUCCESS);
  }
  AssertNotNull(root_iterator_);
  // Close operators
  KStatus ret = root_iterator_->Close(ctx);
  b_close_ = KTRUE;
  Return(ret);
}

// Encode datachunk
EEIteratorErrCode Processors::EncodeDataChunk(kwdbContext_p ctx,
                                              DataChunk* chunk,
                                              EE_StringInfo msgBuffer,
                                              k_bool is_pg) {
  EEIteratorErrCode ret = EEIteratorErrCode::EE_OK;
  KStatus st = FAIL;
  if (is_pg) {
    for (k_uint32 row = 0; row < chunk->Count(); ++row) {
      st = chunk->PgResultData(ctx, row, msgBuffer);
      if (st != SUCCESS) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
        ret = EEIteratorErrCode::EE_ERROR;
        break;
      }
      count_for_limit_ = count_for_limit_ + 1;
      if (command_limit_ != 0 && count_for_limit_ > command_limit_) {
        ret = EEIteratorErrCode::EE_END_OF_RECORD;
        break;
      }
    }
  } else {
    for (k_uint32 row = 0; row < chunk->Count(); ++row) {
      for (k_uint32 col = 0; col < chunk->ColumnNum(); ++col) {
        st = chunk->EncodingValue(ctx, row, col, msgBuffer);
        if (st != SUCCESS) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          ret = EEIteratorErrCode::EE_ERROR;
          break;
        }
      }
      if (st != SUCCESS) {
        break;
      }
    }
  }
  return ret;
}

// Run processors
KStatus Processors::RunWithEncoding(kwdbContext_p ctx, char** buffer,
                                    k_uint32* length, k_uint32* count,
                                    k_bool* is_last_record) {
  EnterFunc();
  AssertNotNull(root_iterator_);
  *count = 0;
  EEIteratorErrCode ret;
  do {
    DataChunkPtr chunk = nullptr;
    EEPgErrorInfo::ResetPgErrorInfo();
    ret = root_iterator_->Next(ctx, chunk);

    if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    if (ret != EEIteratorErrCode::EE_OK) {
      break;
    }

    if (chunk == nullptr || chunk->Count() == 0) {
      continue;
    }

    chunk->GetEncodingBuffer(buffer, length, count);
    break;
  } while (true);

  collection_.GetAnalyse(ctx);
  if (ret == EEIteratorErrCode::EE_OK) {
    Return(KStatus::SUCCESS);
  } else if (ret == EEIteratorErrCode::EE_END_OF_RECORD) {
    *is_last_record = KTRUE;
    KStatus st = CloseIterator(ctx);
    if (st != KStatus::SUCCESS) {
      LOG_ERROR("Failed to close operator.");
      ret = EEIteratorErrCode::EE_ERROR;
      Return(KStatus::FAIL);
    } else {
      Return(KStatus::SUCCESS);
    }
  } else {
    Return(KStatus::FAIL);
  }
}

KStatus Processors::RunWithVectorize(kwdbContext_p ctx, char **value, void *buffer, k_uint32 *length,
                                                              k_uint32 *count, k_bool *is_last_record) {
  EnterFunc();
  AssertNotNull(root_iterator_);

  DataInfo *info = nullptr;
  *count = 0;
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    DataChunkPtr chunk = nullptr;
    EEPgErrorInfo::ResetPgErrorInfo();
    ret = root_iterator_->Next(ctx, chunk);

    if (EEPgErrorInfo::IsError() || CheckCancel(ctx) != SUCCESS) {
      ret = EEIteratorErrCode::EE_ERROR;
      break;
    }

    if (ret != EEIteratorErrCode::EE_OK) {
      break;
    }

    if (chunk == nullptr || chunk->Count() == 0) {
      continue;
    }

    *count = *count + chunk->Count();
    info = static_cast<DataInfo*>(buffer);
    if (nullptr == info) {
      ret = EEIteratorErrCode::EE_ERROR;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      break;
    }
    ret = chunk->VectorizeData(ctx, info);
    //  chunk.release();
    break;
  } while (true);
  collection_.GetAnalyse(ctx);
  if (ret != EEIteratorErrCode::EE_OK) {
    *is_last_record = KTRUE;
    KStatus st = CloseIterator(ctx);
    if (st != KStatus::SUCCESS) {
      LOG_ERROR("Failed to close operator.");
      ret = EEIteratorErrCode::EE_ERROR;
    }
    if (ret == EEIteratorErrCode::EE_ERROR) {
      Return(KStatus::FAIL);
    }
  }

  if (info) {
    *value = reinterpret_cast<char *>(info);
    *length = sizeof(info);
  }

  Return(KStatus::SUCCESS);
}

}  // namespace kwdbts
