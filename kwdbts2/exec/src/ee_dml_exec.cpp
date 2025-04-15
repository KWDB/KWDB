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

#include "ee_dml_exec.h"

#include "cm_fault_injection.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_processors.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "perf_stat.h"
#include "ee_cancel_checker.h"
namespace kwdbts {
DmlExec::~DmlExec() {
  auto ctx = ContextManager::GetThreadContext();
  if (rel_batch_queue_) {
    delete rel_batch_queue_;
    rel_batch_queue_ = nullptr;
  }
  ClearTsScans(ctx);
  if (thd_) {
    SafeDeletePointer(thd_);
  }
}

KStatus DmlExec::Init() {
  thd_ = new KWThdContext();
  current_thd = thd_;
  rel_batch_queue_ = new RelBatchQueue();
  if (rel_batch_queue_ == nullptr) {
    return FAIL;
  }
  return SUCCESS;
}

KStatus DmlExec::CreateTsScan(kwdbContext_p ctx, TsScan **tsScan) {
  EnterFunc();
  KStatus ret = KStatus::FAIL;
  do {
    *tsScan = KNEW TsScan();
    if (!*tsScan) {
      break;
    }
    (*tsScan)->fspecs = KNEW TSFlowSpec();
    if (!(*tsScan)->fspecs) {
      break;
    }
    (*tsScan)->processors = KNEW Processors();
    if (!(*tsScan)->processors) {
      break;
    }
    ret = KStatus::SUCCESS;
  } while (0);
  if (ret != KStatus::SUCCESS) {
    DestroyTsScan(*tsScan);
  }
  Return(ret);
}

void DmlExec::DestroyTsScan(TsScan *tsScan) {
  StStatistics::Get().Show();
  StStatistics::Get().ShowQuery();
  if (tsScan == nullptr) {
    return;
  }
  if (tsScan->processors) {
    delete tsScan->processors;
    tsScan->processors = nullptr;
  }
  if (tsScan->fspecs) {
    delete tsScan->fspecs;
    tsScan->fspecs = nullptr;
  }
  delete tsScan;
  tsScan = nullptr;
}

void DmlExec::ClearTsScans(kwdbContext_p ctx) {
  TsScan *head = tsscan_head_;
  while (head != nullptr) {
    // clear other tsscans when error occurs
    if (head->is_init_pr && ctx) {
      head->processors->CloseIterator(ctx);
    }
    TsScan *tmp = head;
    head = head->next;
    DestroyTsScan(tmp);
  }
  tsscan_head_ = nullptr;
}

KStatus DmlExec::CreateRelBatchQueue(kwdbContext_p ctx, std::vector<Field*> &output_fields) {
  EnterFunc();
  KStatus res = rel_batch_queue_->Init(output_fields);
  Return(res);
}

RelBatchQueue* DmlExec::GetRelBatchQueue() {
  return rel_batch_queue_;
}

KStatus DmlExec::ExecQuery(kwdbContext_p ctx, QueryInfo *req, RespInfo *resp) {
  EnterFunc();
  AssertNotNull(req);
  KStatus ret = KStatus::FAIL;
  ctx->relation_ctx = req->relation_ctx;
  ctx->timezone = req->time_zone;
  EnMqType type = req->tp;
  k_char *message = static_cast<k_char *>(req->value);
  k_uint32 len = req->len;
  k_int32 id = req->id;
  k_int32 uniqueID = req->unique_id;
  try {
    if (!req->handle && type != EnMqType::MQ_TYPE_DML_INIT &&
        type != EnMqType::MQ_TYPE_DML_SETUP) {
      Return(ret);
    }
    DmlExec *handle = nullptr;
    if (!req->handle) {
      handle = new DmlExec();
      handle->Init();
      req->handle = static_cast<char *>(static_cast<void *>(handle));
    } else {
      handle = static_cast<DmlExec *>(static_cast<void *>(req->handle));
    }
    ctx->dml_exec_handle = handle;
    switch (type) {
      case EnMqType::MQ_TYPE_DML_SETUP:
        ret = handle->Setup(ctx, message, len, id, uniqueID, resp);
        if (ret != KStatus::SUCCESS) {
          handle->ClearTsScans(ctx);
        }
        break;
      case EnMqType::MQ_TYPE_DML_CLOSE:
        handle->ClearTsScans(ctx);
        delete handle;
        resp->ret = 1;
        resp->tp = req->tp;
        break;
      case EnMqType::MQ_TYPE_DML_INIT:
        resp->ret = 1;
        resp->tp = req->tp;
        resp->code = 0;
        resp->handle = handle;
        break;
      case EnMqType::MQ_TYPE_DML_NEXT:
        ret = handle->Next(ctx, id, false, resp);
        break;
      case EnMqType::MQ_TYPE_DML_PG_RESULT:
        ret = handle->Next(ctx, id, true, resp);
        break;
      case EnMqType::MQ_TYPE_DML_PUSH:
        // push relational data from ME to AE for multiple model processing
        ret = handle->PushRelData(ctx, req, resp);
        break;
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "Undecided datatype");
        break;
    }
  } catch (...) {
    LOG_ERROR("Throw exception when ExecQuery.");
#ifdef K_DEBUG
    ctx->frame_level = my_frame_level;
#endif
  }
  Return(ret);
}

KStatus DmlExec::Setup(kwdbContext_p ctx, k_char *message, k_uint32 len,
                       k_int32 id, k_int32 uniqueID, RespInfo *resp) {
  KWDB_DURATION(StStatistics::Get().dml_setup);
  EnterFunc();
  KStatus ret = KStatus::FAIL;
  resp->tp = EnMqType::MQ_TYPE_DML_SETUP;
  resp->ret = 0;
  resp->value = 0;
  resp->len = 0;
  resp->unique_id = uniqueID;
  resp->handle = static_cast<char *>(static_cast<void *>(this));
  TsScan *tsScan = nullptr;
  do {
    if (tsscan_head_ && tsscan_head_->id == id) {
      ClearTsScans(ctx);
    }
    CreateTsScan(ctx, &tsScan);
    if (!tsScan) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      break;
    }
    tsScan->id = id;
    tsScan->unique_id = uniqueID;
    bool proto_parse = false;
    try {
      proto_parse = tsScan->fspecs->ParseFromArray(message, len);
    } catch (...) {
      LOG_ERROR("Throw exception where parsing physical plan.");
      proto_parse = false;
    }
    INJECT_DATA_FAULT(FAULT_EE_DML_SETUP_PARSE_PROTO_MSG_FAIL, proto_parse, 0,
                      nullptr);
    if (!proto_parse) {
      LOG_ERROR("Parse physical plan err when query setup.");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid physical plan");
      break;
    }
    ret = tsScan->processors->Init(ctx, tsScan->fspecs);
    if (KStatus::SUCCESS != ret) {
      break;
    }
    if (tsscan_head_ == nullptr) {
      tsscan_head_ = tsScan;
      tsscan_end_ = tsscan_head_;
    } else {
      tsscan_end_->next = tsScan;
      tsscan_end_ = tsScan;
    }
    resp->ret = 1;
  } while (0);
  if (resp->ret == 0) {
    DestroyTsScan(tsScan);
    DisposeError(ctx, resp);
  }
  Return(ret);
}

void DmlExec::DisposeError(kwdbContext_p ctx, QueryInfo *return_info) {
  kwdbts::KwdbError *error = nullptr;
  rel_batch_queue_->NotifyError();
  if (EEPgErrorInfo::IsError()) {
    return_info->code = EEPgErrorInfo::GetPgErrorInfo().code;
    return_info->len = strlen(EEPgErrorInfo::GetPgErrorInfo().msg);
    if (return_info->len > 0) {
      return_info->value = malloc(return_info->len);
      if (return_info->value != nullptr) {
        memcpy(return_info->value, EEPgErrorInfo::GetPgErrorInfo().msg, return_info->len);
      }
    }
    EEPgErrorInfo::ResetPgErrorInfo();
  } else {
    return_info->code = ERRCODE_DATA_EXCEPTION;
  }
}

KStatus DmlExec::InnerNext(kwdbContext_p ctx, TsScan *tsScan, bool isPG,
                           RespInfo *resp) {
  EnterFunc();
  KStatus ret = KStatus::FAIL;
  resp->tp = EnMqType::MQ_TYPE_DML_NEXT;
  resp->ret = 0;
  resp->value = 0;
  resp->len = 0;
  resp->code = -1;
  resp->row_num = 0;

  do {
    if (!tsScan) {
      LOG_ERROR("TsScan is null, need to check physical plan.");
      break;
    }
    resp->id = tsScan->id;
    resp->unique_id = tsScan->unique_id;
    // If the last record on the tsscan is complete, can't continue reading.
    if (TsScanRetState::TS_RT_LASTRECORD == tsScan->ret_state) {
      resp->ret = 1;
      break;
    }

    // init operators.
    if (!tsScan->is_init_pr) {
      tsScan->is_init_pr = KTRUE;
      ret = tsScan->processors->InitIterator(ctx, isPG);
      if (KStatus::SUCCESS != ret) {
        break;
      }
    }
    if (CheckCancel(ctx) != SUCCESS) {
      break;
    }
    k_char *result{nullptr};
    k_uint32 count = 0;
    k_uint32 length = 0;
    k_bool is_last_record = KFALSE;
    ret = tsScan->processors->RunWithEncoding(ctx, &result, &length, &count,
                                                &is_last_record);
    if (ret != KStatus::SUCCESS) {
      break;
    }
    resp->ret = 1;
    if (is_last_record) {
      tsScan->ret_state = TsScanRetState::TS_RT_LASTRECORD;
    }
    if (result) {
      resp->value = result;
      resp->len = length;
      resp->code = 1;
      resp->row_num = count;
    }
  } while (0);
  if (tsScan && resp->code == -1) {
    tsScan->ret_state = TsScanRetState::TS_RT_EOF;
  }
  if (tsScan && resp->ret == 0) {
    tsScan->ret_state = TsScanRetState::TS_RT_ERROR;
    DisposeError(ctx, resp);
  }
  resp->handle = static_cast<char *>(static_cast<void *>(this));
  Return(ret);
}

KStatus DmlExec::Next(kwdbContext_p ctx, k_int32 id, bool isPG, RespInfo *resp) {
  KWDB_DURATION(StStatistics::Get().dml_next);
  EnterFunc();
  current_thd = thd_;
  TsScan *head = tsscan_head_;
  TsScan *prev = tsscan_head_;
  if (!isPG) {
    // search tsscan by id
    for (; head != nullptr; head = head->next) {
      if (id == head->id) {
        break;
      }
      prev = head;
    }
  }

  KStatus ret = SUCCESS;

  InnerNext(ctx, head, isPG, resp);

  if (TsScanRetState::TS_RT_ERROR == head->ret_state) {
    ClearTsScans(ctx);
    ret = KStatus::FAIL;
    Return(ret);
  }

  if (TsScanRetState::TS_RT_EOF == head->ret_state) {
    TsScan *tmp = head;
    head = head->next;
    prev->next = head;
    if (tmp == tsscan_head_) {
      tsscan_head_ = head;
    }
    DestroyTsScan(tmp);
  }
  Return(ret);
}

KStatus DmlExec::PushRelData(kwdbContext_p ctx, QueryInfo *req, RespInfo *resp) {
  EnterFunc();
  resp->ret = 0;
  if (!rel_batch_queue_) {
    // RelBatchQueue wasn't created, so relational data should not be push down
    // and something must be wrong.
    Return(KStatus::FAIL);
  }
  // Put rel batch data into RelBatchQueue.
  if (rel_batch_queue_->Add(ctx, reinterpret_cast<char*>(req->relBatchData), req->relRowCount) == KStatus::SUCCESS) {
    resp->code = 1;
    resp->ret = 1;
  } else {
    resp->code = -1;
  }
  Return(KStatus::SUCCESS)
}

}  // namespace kwdbts
