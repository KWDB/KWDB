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

#ifndef COMMON_SRC_H_CM_FAULT_INJECTION_H_
#define COMMON_SRC_H_CM_FAULT_INJECTION_H_

#include <assert.h>
#include <unistd.h>

#include <atomic>
#include <string>
#include <thread>
#include <map>
#include <functional>
#include <iostream>

#include "kwdb_type.h"

namespace kwdbts {

#ifndef K_DO_NOT_SHIP

enum InjectorType {
  FAULT_DATA,
  FAULT_DELAY,
  FAULT_ABORT
};

#define ENUMS_FAULT_TYPE \
    DECL_FAULT(FAULT_CM_CONFIG_FILE_FAIL) \
    DECL_FAULT(FAULT_CONTEXT_INIT_FAIL) \
    DECL_FAULT(FAULT_DS_HASH_MAP_MALLOC_FAIL) \
    DECL_FAULT(FAULT_EE_EXECUTOR_APPLY_THREAD_MSG_FAIL) \
    DECL_FAULT(FAULT_EE_DML_SETUP_PREINIT_MSG_FAIL) \
    DECL_FAULT(FAULT_EE_DML_SETUP_PARSE_PROTO_MSG_FAIL) \
    DECL_FAULT(FAULT_EE_DML_NEXT_ENCODING_VALUE_MSG_FAIL) \
    DECL_FAULT(FAULT_FOR_FAULT_INJECTION_UT) \
    DECL_FAULT(FAULT_KMM_SEND_DELAY) \
    DECL_FAULT(FAULT_KMM_RECV_DELAY) \
    DECL_FAULT(FAULT_LOG_SYSCONFIG_CHECK_FAIL) \
    DECL_FAULT(FAULT_LOG_FILE_EXIT_FAIL) \
    DECL_FAULT(FAULT_LOG_FILE_EXIT_FAIL_INIT) \
    DECL_FAULT(FAULT_LOG_PITEM_NOT_NULL_FAIL) \
    DECL_FAULT(FAULT_LOG_MALLOC_FAIL) \
    DECL_FAULT(FAULT_MEM_DELAY) \
    DECL_FAULT(FAULT_MEM_LARGE_SIZE_INJECTION) \
    DECL_FAULT(FAULT_MEM_SMALL_SIZE_INJECTION) \
    DECL_FAULT(FAULT_MEM_SMALL_ALIGN_INJECTION) \
    DECL_FAULT(FAULT_MEM_LARGE_ALIGN_INJECTION) \
    DECL_FAULT(FAULT_MEM_SYSTEM_MEM_TYPE_INJECTION) \
    DECL_FAULT(FAULT_MEM_FREE_MEM_TYPE_INJECTION) \
    DECL_FAULT(FAULT_MEM_TS_DATA_MEM_TYPE_INJECTION) \
    DECL_FAULT(FAULT_MEM_COMMON_MEM_TYPE_INJECTION) \
    DECL_FAULT(FAULT_MEM_VALID_ADDRESS_INJECTION) \
    DECL_FAULT(FAULT_MEM_NULLPTR_INJECTION) \
    DECL_FAULT(FAULT_MEM_NEW_SPAN_FAIL) \
    DECL_FAULT(FAULT_MQ_SEND_CLOSE) \
    DECL_FAULT(FAULT_TH_FAULT_INJECTION_NUM) \
    DECL_FAULT(FAULT_TH_FAULT_INJECTION_DELAY) \
    DECL_FAULT(FAULT_TH_DELAY_EXEC) \
    DECL_FAULT(FAULT_MAX)

#define DECL_FAULT(a) a,
enum FaultType { ENUMS_FAULT_TYPE };
#undef DECL_FAULT

class FaultInjector;

void SetFaultInjector(k_int32 id, FaultInjector* fault);
FaultInjector* GetFaultInjector(k_int32 id);

typedef std::function<void(const KString &data, void *var)> Interpreter;

class FaultInjector {
 public:
  explicit FaultInjector(k_int32 id): id_(id), actived_(false), trigger_time_(0), call_count_(0) {
    SetFaultInjector(id_, this);
  }
  ~FaultInjector() = default;
  const KString& GetInput() { return input_; }
  void SetInput(const KString &v) { input_ = v;}

 public:
  k_int32 id_;
  bool actived_;
  bool auto_reset_;
  k_int32 his_trigger_count_;
  k_int32 trigger_time_;
  std::atomic<k_int32> call_count_;
  KString input_;
};

template < typename T >
class FaultInjectorRunTime {
 public:
  FaultInjectorRunTime(InjectorType type, k_int32 id, T def_val, Interpreter i):
    type_(type), id_(id), defval_(def_val), interpreter_(i) {}

  void Inject(T *val) {
    FaultInjector* conf = GetFaultInjector(id_);
    if (!conf) {
      return;
    }
    if (!conf->actived_) {
      return;
    }
    if (++conf->call_count_ >= conf->trigger_time_) {
      conf->his_trigger_count_++;
      if (conf->auto_reset_) {
        conf->actived_ = false;
        conf->auto_reset_ = false;
        conf->call_count_ = 0;
        conf->trigger_time_ = 0;
      }
      if (type_ == InjectorType::FAULT_DATA) {
        if (conf->GetInput().empty()) {
          *val = defval_;
        } else {
          interpreter_(conf->GetInput(), static_cast<void *>(val));
        }
      } else if (type_ == InjectorType::FAULT_DELAY) {
        if (conf->GetInput().empty()) {
          usleep(*static_cast<int *>(static_cast<void *>(&defval_)));
        } else {
          usleep(std::atol(conf->GetInput().c_str()));
        }
      } else if (type_ == InjectorType::FAULT_ABORT) {
        abort();
      }
    }
  }

 private:
  InjectorType type_;
  k_int32 id_;
  T defval_;
  Interpreter interpreter_;
};

class CmpxFaultInjectorRunTime {
 public:
  CmpxFaultInjectorRunTime(InjectorType type, k_int32 id, Interpreter i):
    type_(type), id_(id), interpreter_(i) {}

  void Inject(void *val) {
    FaultInjector* conf = GetFaultInjector(id_);
    if (!conf) {
      return;
    }
    if (!conf->actived_) {
      return;
    }
    if (conf->call_count_++ >= conf->trigger_time_) {
      interpreter_(conf->GetInput(), val);
    }
  }

 private:
  InjectorType type_;
  k_int32 id_;
  Interpreter interpreter_;
};

// for data type have a default construct function
#define INJECT_DATA_FAULT(Id, refval, def_val, interpreter) \
  do { \
    static FaultInjectorRunTime<decltype(refval)> fault_##Id(InjectorType::FAULT_DATA, Id, def_val, interpreter); \
    fault_##Id.Inject(&(refval));  \
  } while (0)

// for data type doesn't have a default construct function, just pass the (void *)ptr
#define INJECT_DATA_FAULT_CMPX(Id, ptr, interpreter) \
  do { \
    static kwdbts::CmpxFaultInjectorRunTime fault_##Id(InjectorType::FAULT_DATA, Id, interpreter); \
    fault_##Id.Inject(ptr);  \
  } while (0)

#define INJECT_DELAY_FAULT(Id, def_delay_us) \
  do { \
    int __fault_placeholder; \
    static kwdbts::FaultInjectorRunTime<int> fault_##Id(InjectorType::FAULT_DELAY, Id, def_delay_us, nullptr); \
    fault_##Id.Inject(&__fault_placeholder);  \
  } while (0)

#define INJECT_ABORT_FAULT(Id) \
  do { \
    int __fault_placeholder; \
    static kwdbts::FaultInjectorRunTime<int> fault_##Id(InjectorType::FAULT_ABORT, Id, 0, nullptr); \
    fault_##Id.Inject(&__fault_placeholder);  \
  } while (0)

template<typename T>
void InputInt(const std::string &data, void *var) { *static_cast<T*>(var) = std::stoi(data);}

void HandleInjectFaultCmd(void* args);

k_int32 ProcInjectFault(const KString &cmd, std::ostringstream &oss);

k_int32 InitFaultInjection();

void InjectFaultServer(void* args);

#else

#define INJECT_DATA_FAULT(Id, refval, def_val, interpreter)
#define INJECT_DELAY_FAULT(Id, def_delay_us)
#define INJECT_ABORT_FAULT(Id)

#endif

}  // namespace kwdbts

#endif  // COMMON_SRC_H_CM_FAULT_INJECTION_H_
