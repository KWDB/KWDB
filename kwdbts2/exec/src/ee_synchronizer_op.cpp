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

// #include "ee_synchronizer_op.h"

// #include <cxxabi.h>
// #include <vector>

// #include "ee_base_op.h"
// #include "ee_exec_pool.h"
// #include "ee_parallel_group.h"
// #include "tag_iterator.h"

// namespace kwdbts {

// #define MAX_QUEUE_SIZE 128

// KStatus SynchronizerOperator::PushData(DataChunkPtr &chunk, bool &reduce_dop, bool wait) {
//   std::unique_lock unique_lock(lock_);
//   // storage to task queue
//   try {
//     if (!wait) {
//       if (data_queue_.size() >= max_queue_size_) {
//         return KStatus::FAIL;
//       } else if (data_queue_.size() > max_queue_size_ / 16) {
//         reduce_dop = true;
//       }
//     }
//     not_fill_cv_.wait(unique_lock, [this]() -> bool {
//       return ((data_queue_.size() < max_queue_size_) || is_tp_stop_);
//     });
//     data_queue_.push_back(std::move(chunk));
//   } catch (std::exception &e) {
//     LOG_ERROR("PushResult() error: %s\n", e.what());
//     return KStatus::FAIL;
//   }
//   // notify idle thread
//   wait_cond_.notify_one();
//   return KStatus::SUCCESS;
// }

// void SynchronizerOperator::PopData(kwdbContext_p ctx, DataChunkPtr &chunk) {
//   std::unique_lock l(lock_);
//   while (true) {
//     if (is_tp_stop_) {
//       break;
//     }
//     // data_queue_ is empty，wait
//     if (data_queue_.empty()) {
//       if (group_done_num_ == group_num_) {
//         break;
//       }
//       wait_cond_.wait_for(l, std::chrono::seconds(2));
//       continue;
//     }
//     // get task
//     chunk = std::move(data_queue_.front());
//     data_queue_.pop_front();
//     not_fill_cv_.notify_one();
//     break;
//   }
// }

// void SynchronizerOperator::FinishParallelGroup(EEIteratorErrCode code, const EEPgErrorInfo &pg_info) {
//   std::unique_lock l(lock_);
//   group_done_num_++;

//   if (code != EEIteratorErrCode::EE_OK &&
//       code != EEIteratorErrCode::EE_END_OF_RECORD &&
//       code != EEIteratorErrCode::EE_TIMESLICE_OUT) {
//     group_code_ = code;
//   }
//   if (pg_info.code > 0) {
//     pg_info_ = pg_info;
//   }
//   // notify idle thread
//   wait_cond_.notify_one();
// }

// EEIteratorErrCode SynchronizerOperator::InitParallelGroup(kwdbContext_p ctx) {
//   EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
//   // Creating the number of parallelgroups based on parallelism
//   max_queue_size_ = MAX_QUEUE_SIZE;  // degree_ * 2 + 2;
//   parallel_groups_.resize(degree_);
//   for (k_uint32 i = 0; i < degree_; ++i) {
//     ParallelGroupPtr parallelGroup = std::make_shared<ParallelGroup>();
//     if (i == 0) {
//       parallelGroup->SetIterator(childrens_[0]);
//     } else {
//       BaseOperator *clone_iter = childrens_[0]->Clone();
//       code = clone_iter->Init(ctx);
//       if (EEIteratorErrCode::EE_OK != code) {
//         return code;
//       }
//       parallelGroup->SetIterator(clone_iter);
//       clone_iter_list_.push_back(clone_iter);
//     }

//     parallelGroup->SetTable(table_);
//     if (parallelGroup->Init(ctx) != KStatus::SUCCESS) {
//       return EEIteratorErrCode::EE_ERROR;
//     }
//     parallelGroup->SetParent(this);
//     parallelGroup->SetParallel(EE_ENABLE_PARALLEL);
//     parallelGroup->SetDegree(degree_);
//     parallelGroup->SetIndex(i);
//     parallel_groups_[i] = parallelGroup;
//   }
//   group_num_ = degree_;
//   for (k_uint32 i = 0; i < degree_; ++i) {
//     ExecPool::GetInstance().PushTask(parallel_groups_[i]);
//   }

//   return EEIteratorErrCode::EE_OK;
// }

// EEIteratorErrCode SynchronizerOperator::Init(kwdbContext_p ctx) {
//   EnterFunc();
//   EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
//   code = childrens_[0]->Init(ctx);
//   if (EEIteratorErrCode::EE_OK != code) {
//     Return(code);
//   }
//   // maintain consistency between output and input columns
//   std::vector<Field *> &input_fields = childrens_[0]->OutputFields();
//   for (auto &input_field : input_fields) {
//     Field *field = input_field->field_to_copy();
//     output_fields_.push_back(field);
//   }

//   Return(code);
// }

// void SynchronizerOperator::CalculateDegree() {
//   k_uint32 dop = degree_;
//   // TagScan does not support parallelism, forcing parallelism to be set to 1
//   if (table_->only_tag_) {
//     dop = 1;
//   }
//   char *class_name =
//       abi::__cxa_demangle(typeid(*childrens_[0]).name(), NULL, NULL, NULL);
//   if (strcmp(class_name, "kwdbts::TsSamplerOperator") == 0) {
//     dop = 1;
//   }
//   free(class_name);
//   if (table_->GetAccessMode() < TSTableReadMode::tableTableMeta) {
//     if (dop > table_->ptag_size_) {
//       dop = table_->ptag_size_;
//     }
//   }
//   // dop = ExecPool::GetInstance().GetWaitThreadNum(dop);
//   if (dop > 1 && ExecPool::GetInstance().IsActive()) {
//     dop = 2;
//   }
//   if (dop < 1) dop = 1;
//   degree_ = dop;
//   is_parallel_ = true;
// }

// EEIteratorErrCode SynchronizerOperator::Start(kwdbContext_p ctx) {
//   EnterFunc();
//   EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
//   CalculateDegree();
//   if (is_parallel_) {
//     code = InitParallelGroup(ctx);
//   } else {
//     code = childrens_[0]->Start(ctx);
//   }
//   Return(code);
// }
// KStatus SynchronizerOperator::Close(kwdbContext_p ctx) {
//   EnterFunc();
//   is_tp_stop_ = true;
//   if (group_done_num_ < group_num_) {
//     for (auto &parallel_group : parallel_groups_) {
//       if (parallel_group != nullptr) {
//         parallel_group->Stop();
//       }
//     }
//     std::unique_lock l(lock_);
//     data_queue_.clear();
//     not_fill_cv_.notify_all();
//     while (true) {
//       // wait
//       if (group_done_num_ == group_num_) {
//         break;
//       }
//       wait_cond_.wait_for(l, std::chrono::seconds(2));
//       continue;
//     }
//   }
//   KStatus ret = childrens_[0]->Close(ctx);
//   Return(ret);
// }

// EEIteratorErrCode SynchronizerOperator::Next(kwdbContext_p ctx, DataChunkPtr &chunk) {
//   EnterFunc();
//   EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
//   if (is_parallel_) {
//     PopData(ctx, chunk);
//     if (pg_info_.code > 0) {
//       EEPgErrorInfo::SetPgErrorInfo(pg_info_.code, pg_info_.msg);
//       Return(EEIteratorErrCode::EE_ERROR);
//     }
//     if (!chunk) {
//       code = EEIteratorErrCode::EE_END_OF_RECORD;
//       if (group_code_ != EEIteratorErrCode::EE_OK) {
//         code = group_code_;
//       }
//     }
//   } else {
//     code = childrens_[0]->Next(ctx, chunk);
//   }
//   Return(code);
// }

// }  // namespace kwdbts
