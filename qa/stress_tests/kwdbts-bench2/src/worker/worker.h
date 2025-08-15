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

#pragma once
#include <string>
#include <functional>
#include <thread>
#include <cm_func.h>
#include <kwdb_type.h>
#include <map>
#include <unordered_map>
#include <sstream>
#include "util.h"
#include "statistics.h"
#include "../bench_params.h"
#include "data_type.h"
#include "mmap/mmap_tag_column_table.h"
#include "ts_payload.h"

namespace kwdbts {

KBStatus dump_zstatus(const char* func, kwdbts::kwdbContext_p ctx, kwdbts::KStatus s);

//  Gets the current nanosecond time
inline kwdbts::KTimestamp GetTimeNow() {
  struct timespec timestamp;
  clock_gettime(CLOCK_REALTIME, &timestamp);
  return timestamp.tv_sec * 1e9 + timestamp.tv_nsec;
}

inline std::string get_time_string(int64_t ts) {
  time_t timep = ts / 1e9;
  struct tm* t_tm;
  t_tm = localtime(&timep);  // Gets the local time of the tm structure
  char ts_str[30];
  // strftime(ts_str, 30, "%Y_%m_%d %H:%S", t_tm);
  strftime(ts_str, 30, "%T", t_tm);
  return std::string(ts_str);
}

class Worker {
 public:
  // worker task type
  enum BenchType {
    WRITE = 0,
    READ = 1,
    CHAOS = 2,
    COMPRESS = 3,
    RETENTIONS = 4,
    SNAPSHOT = 5,
  };

  Worker(const std::string& pname, const BenchParams& params,
         const std::vector<uint32_t>& table_ids) :
      name_(pname), params_(params), table_ids_(table_ids), can_run_(true) {
    if (table_ids_.size() == 0) {
      log_ERROR("Worker[%s]  no table to RUN\n", name_.c_str());
      return;
    }
    table_schemas_.resize(table_ids_.size());
  }

  virtual KBStatus SetEntity(int begin, int end) {
    _entity_begin = begin;
    _entity_end = end;
    return KBStatus::OK();
  }
  // Metadata initialization is complete
  virtual KBStatus Init() = 0;

  // Data initialization
  virtual KBStatus InitData(KTimestamp& new_ts) {
    return KBStatus::OK();
  }

  // Clear data
  virtual KBStatus Destroy() = 0;

  virtual std::string show_extra() {
    return "";
  }

  std::string GetName() {
    return name_;
  }

  virtual BenchType GetType() {
    return Worker::BenchType::WRITE;
  }

  bool CanRun() {
    return can_run_;
  }

  // device id for each cycle
  virtual uint32_t getCurrentTable() {
    // Select device sequentially to read the last data
    uint32_t r_device = table_ids_[table_i];
    // std::cout << "  !!!!!!!!"<<name_<< "-Device_" << device->getDeviceName() <<" ***ALLOCATE" <<std::endl;
    table_i++;
    if (table_i >= table_ids_.size()) {
      table_i = 0;
    }
    return r_device;
  }

  // The specific work of each loop, implemented by subclasses
  virtual KBStatus do_work(kwdbts::KTimestamp ts_now) = 0;

  inline void NextTS(KTimestamp& wr_ts) {
    wr_ts += params_.time_inc;
  }

  // A unit of conversion from nanosecond to millisecond
  const uint32_t TS_MS = 1e6;

 protected:
  struct TableSchemaCache {
    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> data_schema;
    std::shared_ptr<TSRowPayloadBuilder> build = nullptr;
  };
  std::string name_;
  std::vector<uint32_t> table_ids_;
  std::vector<TableSchemaCache> table_schemas_;
  // table that is currently executing work
  int table_i = 0;
  BenchParams params_;
  bool can_run_;
  int _entity_begin = 0;
  int _entity_end = 0;
  int _entity_i = 0;
};

class WorkerRoutine {
 public:
  WorkerRoutine(const char* name, const BenchParams& params) :
      thread_name(name), params_(params), stop_cmd_(false), running_(true) {
  }

  ~WorkerRoutine() {}

  void AddWorker(std::shared_ptr<Worker> worker) {
    workers_.push_back(std::move(worker));
  }

  virtual void Run();

  void Stop() {
    stop_cmd_ = true;
  }

  bool isRunning() {
    return running_;
  }

  void Destroy() {
    for (int i = 0; i < workers_.size(); i++) {
      workers_[i]->Destroy();
    }
  }

  std::string GetExtraInfo() {
    std::ostringstream stream;
    for (int i = 0; i < workers_.size(); i++) {
      stream << "-" << workers_[i]->GetName();
      stream << "[" << workers_[i]->show_extra() << "]";
    }
    return stream.str();
  }

  static void Start(void* p) {
    WorkerRoutine *worker = static_cast<WorkerRoutine *>(p);
    worker->Run();
    return;
  }

  void ErrCodeStatistics(StatusCode err) {
    if (err_code_times_.find(err) != err_code_times_.end()) {
      err_code_times_[err] += 1;
    } else {
      err_code_times_[err] = 1;
    }
  }

  std::string thread_name;
  int64_t run_success = 0;
  int64_t run_error = 0;

 private:
  BenchParams params_;
  std::vector<std::shared_ptr<Worker>> workers_;
  bool running_;
  bool stop_cmd_;
  // Record the error type and the number of times the error occurred
  std::unordered_map<StatusCode, int> err_code_times_;
};

class WorkerFactory{
 public:
  virtual Worker::BenchType GetBenchType() = 0;
  virtual const char* GetBenchName() = 0;

  virtual std::shared_ptr<Worker> NewWorker(const std::string& pname, const BenchParams& params,
                                            std::vector<uint32_t>& dev_ids) = 0;

 public:
  static std::map<std::string, std::shared_ptr<WorkerFactory>> factories;
  static void RegisterWorker(std::string name, std::shared_ptr<WorkerFactory> factory) {
    factories.insert(std::pair<std::string, std::shared_ptr<WorkerFactory>>(name, factory));
  }
};

// WorkerFactory template class
template<typename W, Worker::BenchType benchType = Worker::BenchType::WRITE>
class GenericWorkerFactory : public WorkerFactory {
 public:
  explicit GenericWorkerFactory(std::string& name) : name_(name) {}

  Worker::BenchType GetBenchType() override {
    return benchType;
  }

  virtual const char* GetBenchName() override {
    return name_.c_str();
  }

  virtual std::shared_ptr<Worker> NewWorker(
      const std::string& pname, const BenchParams& params, std::vector<uint32_t>& dev_ids) override {
    return std::make_shared<W>(pname, params, dev_ids);
  };

  static void RegisterInstance(std::string name) {
    auto factory = std::make_shared<GenericWorkerFactory<W,benchType>>(name);
    kwdbts::WorkerFactory::RegisterWorker(name, factory);
  }

 private:
  std::string name_;
};

}