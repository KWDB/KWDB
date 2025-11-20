// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
/*
 * @author jiadx
 * @date 2022/10/26
 * @version 1.0
 */

#include "worker.h"

#include <cstdint>

#include "util.h"

namespace kwdbts {
using namespace kwdbts;


KBStatus dump_zstatus(const char* func, kwdbts::kwdbContext_p ctx, kwdbts::KStatus s) {
  if (s == kwdbts::KStatus::SUCCESS) {
    return KBStatus::OK();
  }
  char buf[1024];
  (&ctx->err_stack)->DumpToJson(buf, 1024);
  return KBStatus::InternalError(std::string(func) + buf);
}

void WorkerRoutine::Run() {
  // initialize
  KTimestamp t_start = GetTimeNow();
  KTimestamp data_ts = params_.time_start;
  if (data_ts == 0) {
    data_ts = t_start / 1e6;
  }
  for (int i = 0 ; i < workers_.size() ; i++) {
    KBStatus s = workers_[i]->Init();
    if (s.isNotOK()) {
      log_ERROR("%s Init fail : %s\n", workers_[i]->GetName().c_str(), s.message().c_str());
      run_error = 1;
      running_ = false;
      return;
    }
    s = workers_[i]->InitData(data_ts);
    if (s.isNotOK()) {
      log_ERROR("%s InitData fail : %s\n", workers_[i]->GetName().c_str(), s.message().c_str());
      run_error = 1;
      running_ = false;
      return;
    }
  }
  log_INFO("**** WORKER[%s] Init success. ****", thread_name.c_str());

  t_start = GetTimeNow();
  KTimestamp t_show = GetTimeNow();
  int64_t show_count = 0;
  int64_t run_count = 0;
  KTimestamp ts_now;
  while (!stop_cmd_ && running_) {
    ts_now = GetTimeNow();
    if (params_.RUN_COUNT > 0 && run_count >= params_.RUN_COUNT) {
      running_ = false;
      break;
    }

    int run_num = 0;
    for (int i = 0; i < workers_.size(); i++) {
      // Each thread executes the assigned worker sequentially
      Worker* worker = workers_[i].get();
      if (!worker->CanRun()) {
        continue;
      }
      run_num++;
      KBStatus s = worker->do_work(data_ts);
      data_ts += params_.BATCH_NUM * params_.time_inc;

      show_count += params_.BATCH_NUM;
      if (s.isOK()) {
        run_success += params_.BATCH_NUM;
      } else {
        ErrCodeStatistics(s.code());
        run_error += params_.BATCH_NUM;
        if (s.isNotOK() && params_.ERROR_EXIT) {
          log_ERROR("Worker[%s] run error : %s\n", worker->GetName().c_str(), s.message().c_str());
          running_ = false;
          break;
        }
        continue;
      }

      if (worker->GetType() == Worker::BenchType::WRITE) {
        if (params_.wait_ms > 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(params_.wait_ms));
        }
        data_ts += params_.BATCH_NUM * params_.time_inc;
      } else if (worker->GetType() == Worker::BenchType::RETENTIONS) {
        if (params_.meta_param.RETENTIONS_TIME > 0) {
          std::this_thread::sleep_for(std::chrono::seconds(params_.meta_param.RETENTIONS_TIME));
        }
        data_ts = ts_now  / 1e6;
      } else if (worker->GetType() == Worker::BenchType::COMPRESS) {
        if (params_.meta_param.COMPRESS_TIME > 0) {
          std::this_thread::sleep_for(std::chrono::seconds(params_.meta_param.COMPRESS_TIME));
        }
        data_ts = ts_now  / 1e6;
      } else {
        if (params_.read_wait > 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(params_.read_wait));
        }
        data_ts += params_.BATCH_NUM * params_.time_inc;
      }
    }
    double time = (ts_now - t_show) / 1e9;
    if (time > params_.SHOW_INTERVAL) {
      fprintf(stdout, "*[%s]:%s rows=%.1fw, IOPS=%.1fw/s %s **\n",
              thread_name.c_str(), get_time_string(ts_now).c_str(), show_count / 10000.0,
              show_count / time / 10000,
              GetExtraInfo().c_str());

      t_show = GetTimeNow();
      show_count = 0;
    }
    if ((ts_now - t_start) / 1e9 >= params_.RUN_TIME) {
      break;
    }
    run_count++;

    if (run_num == 0) {
      running_ = false;
      break;
    }
  }

  double t_elapsed = (GetTimeNow() - t_start) / 1e9;
  int64_t total = run_error + run_success;
  double iops = 0;
  if (total > 1) {
    iops = total / t_elapsed / 1e4;
  }

  for (auto kv : err_code_times_) {
    log_ERROR("-----WORKER[%s] errcode: %d, number of occurrences: %d", thread_name.c_str(), kv.first, kv.second);
  }
  log_ERROR("-----WORKER[%s] end: Count=%ld(error:%ld), time=%.1fs ,IOPS=%.2f W/s , ** %s **---- \n",
            thread_name.c_str(), total, run_error, t_elapsed, iops,
            GetExtraInfo().c_str());
  running_ = false;
}

std::map<std::string, std::shared_ptr<WorkerFactory>> WorkerFactory::factories;

}
