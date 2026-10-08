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

#ifndef COMMON_SRC_H_CM_TRACE_PLUGIN_H_
#define COMMON_SRC_H_CM_TRACE_PLUGIN_H_
#include "lg_severity.h"
namespace kwdbts {
class TracePlugin {
 public:
  virtual ~TracePlugin() {}
  virtual void* StartSpan(kwdbContext_p ctx, const char* module,  const char* file, const char* function) = 0;
  virtual void EndSpan(kwdbContext_p ctx, void* span, const char* module, const char* file, const char* function) = 0;
  virtual void LogSpan(kwdbContext_p ctx, LogSeverity severity,
                       const char* file, k_uint32 line, const char* msg) = 0;
};

#ifdef K_OPENTRACING
void* StartSpan(kwdbContext_p ctx, const char* module, const char* file, const char* function);
void EndSpan(kwdbContext_p ctx, void* span, const char* module, const char* file, const char* function);
void LogSpan(kwdbContext_p ctx, LogSeverity severity, const char* file, k_uint32 line, const char* fmt, ...);
#define START_SPAN(ctx) StartSpan(ctx, __MODULE_NAME__, __FILE_NAME__, __FUNCTION__);
#define END_SPAN(ctx, span) EndSpan(ctx, span, __MODULE_NAME__, __FILE_NAME__, __FUNCTION__)
#define LOG_INFO_SPAN(...) LogSpan(ctx, kwdbts::INFO, __FILE_NAME__, __LINE__, __VA_ARGS__)
#define LOG_WARN_SPAN(...) LogSpan(ctx, kwdbts::WARN, __FILE_NAME__, __LINE__, __VA_ARGS__)
#define LOG_ERROR_SPAN(...) LogSpan(ctx, kwdbts::ERROR, __FILE_NAME__, __LINE__, __VA_ARGS__)
#else
#define START_SPAN(ctx) (nullptr)
#define END_SPAN(ctx, span)
#define LOG_INFO_SPAN(...)
#define LOG_WARN_SPAN(...)
#define LOG_ERROR_SPAN(...)
#endif

}  // namespace kwdbts
#endif  // COMMON_SRC_H_CM_TRACE_PLUGIN_H_
