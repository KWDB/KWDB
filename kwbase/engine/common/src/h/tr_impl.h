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

#ifndef COMMON_SRC_H_TR_IMPL_H_
#define COMMON_SRC_H_TR_IMPL_H_
#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>

#include "cm_kwdb_context.h"
#include "cm_module.h"
#include "kwdb_type.h"
#include "lg_writer.h"
// #include "me_cache_manager.h"

namespace kwdbts {
#define TRACE_CONFIG_NAME "ts.trace.on_off_list"

/*!
 * @brief Tracer trace base class
 */
class Tracer {
 public:
  Tracer(Tracer &) = delete;
  Tracer &operator=(const Tracer &) = delete;
  /*!
   * @brief
   * get tracer instance 
   * @return Tracer instance
   */
  inline static Tracer &GetInstance() {
    static Tracer instance;
    return instance;
  }

  /*!
   * @brief
   * init the module
   * @param[in] ctx
   * @return KTRUE for success，othewise KFALSE
   */
  KStatus Init(kwdbContext_p ctx);
  KStatus init();
  // Destroy cleans up, and release connection
  KStatus Destroy(kwdbContext_p ctx);
  /**
   * @brief
   * @param status
   * @param file
   * @param line
   * @param fmt
   * @param ...
   */
  void Trace(kwdbContext_p ctx, const char *file, k_uint16 line, k_int32 level, const char *fmt, ...)
      __attribute__((format(printf, 6, 7)));
  void Trace(kwdbContext_p ctx, KwdbModule mod, const char *file, k_uint16 line, const char *fmt, va_list args);
  void SetTraceConfigStr(const KString &trace_config);
  void setMeStat() {
    if (tracer_writer_ != nullptr) {
      tracer_writer_->inited_ = false;
    }
  }
  k_bool isEnableFor(KwdbModule mod, k_int32 level) { return level <= level_of_module_[mod]; }
#ifndef WITH_TESTS

 private:
#endif
  Tracer() = default;
  ~Tracer() = default;
  std::mutex trace_mtx_;
  enum TracerStatus : k_int32 {
    INIT = 0,
    RUN,
    STOP,
  };
  std::atomic<TracerStatus> TRACER_STATUS_ = INIT;
  k_bool trace_switch_ = false;
  KString device_ = "";
  k_int32 level_of_module_[MAX_MODULE + 1];
  std::shared_ptr<LogWriter> tracer_writer_ = nullptr;
  k_int32 setModuleLevel(KwdbModule mod, k_int32 level);
  void clearModuleLevel();
  k_bool checkModuleLevel();
};

#define TRACER Tracer::GetInstance()
extern void splitString(const char *str, char delim, const std::function<void(char *)> &invoker);
extern void parseConfigStr(const char *str, const std::function<void(const char *key, k_int32 value)> &invoker);
}  // namespace kwdbts

#endif  // COMMON_SRC_H_TR_IMPL_H_
