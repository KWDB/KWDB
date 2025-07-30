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

// #include "ee_parallel_group.h"

// #include "ee_exec_pool.h"
// #include "ee_kwthd_context.h"
// #include "ee_synchronizer_op.h"
// #include "ee_cancel_checker.h"

// #define PAUSE_WAIT_INTERVAL 5     //  5 ms

// namespace kwdbts {

// ParallelGroup::~ParallelGroup() {
//   SafeDeletePointer(thd_);
// }

// KStatus ParallelGroup::Init(kwdbContext_p ctx) {
//   EnterFunc();
//   ts_engine_ = ctx->ts_engine;
//   fetcher_ = ctx->fetcher;
//   is_parallel_pg_ = false;
//   relation_ctx_ = ctx->relation_ctx;
//   timezone_ = ctx->timezone;
//   KWThdContext *main_thd = current_thd;
//   thd_ = KNEW KWThdContext();
//   if (!thd_) {
//     EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
//     LOG_ERROR("New KWThd failed.");
//     Return(FAIL);
//   }
//   thd_->Copy(main_thd);
//   ps_ = PS_TASK_INIT;
//   Return(SUCCESS);
// }

// void ParallelGroup::Run(kwdbContext_p ctx) {
//   EEPgErrorInfo::ResetPgErrorInfo();
//   ctx->ts_engine = ts_engine_;
//   ctx->relation_ctx = relation_ctx_;
//   ctx->fetcher = fetcher_;
//   ctx->timezone = timezone_;
//   EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
//   current_thd = thd_;
//   // thd_->SetParallelGroup(this);
//   auto &instance = ExecPool::GetInstance();
//   auto &g_error_info = EEPgErrorInfo::GetPgErrorInfo();
//   try {
//     if (is_stop_ || CheckCancel(ctx) != SUCCESS) {
//       Close(ctx, code);
//       return;
//     }
//     if (ps_ == PS_TASK_INIT) {
//       code = iterator_->Start(ctx);
//       if (code != EE_OK || g_error_info.code > 0 ||
//           CheckCancel(ctx) != SUCCESS) {
//         Close(ctx, code);
//         return;
//       }
//     }
//     if (ps_ == PS_TASK_PAUSE && chunk_) {
//       bool wait = instance.GetWaitThreadNum() > 0 ? true : false;
//       bool reduce_dop = false;
//       KStatus ret = sparent_->PushData(chunk_, reduce_dop, wait);
//       if (!wait && reduce_dop && index_ > 1) {
//         thd_->auto_quit_ = true;
//       }
//       if (ret != KStatus::SUCCESS) {
//         repeat_++;
//         Pause();
//         return;
//       }
//       chunk_.reset();
//     }
//     ps_ = PS_TASK_RUN;
//     repeat_ = 1;
//     while (true) {
//       if (is_stop_ || CheckCancel(ctx) != SUCCESS) {
//         Close(ctx, code);
//         break;
//       }

//       DataChunkPtr ptr = nullptr;
//       code = iterator_->Next(ctx, ptr);
//       if (EEIteratorErrCode::EE_OK != code || g_error_info.code > 0 ||
//           is_stop_) {
//         Close(ctx, code);
//         break;
//       }
//       ptr->ResetLine();
//       bool wait = instance.GetWaitThreadNum() > 0 ? true : false;
//       bool reduce_dop = false;
//       KStatus ret = sparent_->PushData(ptr, reduce_dop, wait);
//       if (!wait && reduce_dop && index_ > 1) {
//         thd_->auto_quit_ = true;
//       }
//       if (ret != KStatus::SUCCESS) {
//         chunk_ = std::move(ptr);
//         Pause();
//         break;
//       }
//     }
//   } catch (const std::bad_alloc &e) {
//     LOG_ERROR("throw bad_alloc exception: %s.", e.what());
//     EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
//     Close(ctx, EEIteratorErrCode::EE_ERROR);
//   } catch (const std::runtime_error &e) {
//     LOG_ERROR("throw runtime_error exception: %s.", e.what());
//     EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
//     Close(ctx, EEIteratorErrCode::EE_ERROR);
//   } catch (const std::exception &e) {
//     LOG_ERROR("throw other exception: %s.", e.what());
//     EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, e.what());
//     Close(ctx, EEIteratorErrCode::EE_ERROR);
//   } catch (...) {
//     LOG_ERROR("throw unknown exception.");
//     EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "unknown exception.");
//     Close(ctx, EEIteratorErrCode::EE_ERROR);
//   }
// }

// void ParallelGroup::Close(kwdbContext_p ctx, const EEIteratorErrCode &code) {
//   iterator_->Reset(ctx);
//   sparent_->FinishParallelGroup(code, EEPgErrorInfo::GetPgErrorInfo());
//   if (thd_) {
//     thd_->Reset();
//   }
//   ps_ = PS_TASK_CLOSE;
// }

// void ParallelGroup::Pause() {
//   ps_ = PS_TASK_PAUSE;
//   k_time_point time_point = TimerEvent::GetMonotonicMs() + PAUSE_WAIT_INTERVAL;
//   SetTimePoint(time_point);
//   SetType(TimerEventType::TE_TIME_POINT);
//   // ExecPool::GetInstance().PushTimeEvent(GetPtr());
//   ExecPool::GetInstance().PushTask(GetPtr());
// }

// KStatus ParallelGroup::TimeRun() {
//   return ExecPool::GetInstance().PushTask(GetPtr());
// }

// }  // namespace kwdbts
