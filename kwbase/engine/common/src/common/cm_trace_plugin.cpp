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
#include <cm_kwdb_context.h>
#include <dlfcn.h>
#include <cm_config.h>
#include <lg_api.h>
#include "cm_trace_plugin.h"

namespace kwdbts {
TracePlugin* g_trace_instance;
void* plugin_lib = nullptr;
typedef kwdbts::TracePlugin* (create_t)(const char*);
typedef int(destroy_t)(kwdbts::TracePlugin*);
// The maximum default log written to span is 256 bytes
const k_int16 MAX_SPAN_LOG = 256;

void DestroyTracePlugin(kwdbContext_p ctx) {
#ifndef K_OPENTRACING
  return;
#endif
  if (plugin_lib != nullptr) {
    if (g_trace_instance != nullptr) {
      // destroy the trace instance
      destroy_t* destroy = reinterpret_cast<destroy_t*>(dlsym(plugin_lib, "destroyTracePlugin"));
      destroy(g_trace_instance);
      g_trace_instance = nullptr;
    }

    // close the plugin so file
    dlclose(plugin_lib);
    plugin_lib = nullptr;
  }
  return;
}

// The trace plugin fails to be initialized, and only error information is recorded,
// which does not affect the normal execution of the main process
void InitTracePlugin(kwdbContext_p ctx, const char* service) {
#ifndef K_OPENTRACING
  return;
#endif
  if (g_trace_instance != nullptr) {
    LOG_ERROR("The trace plugin already init.");
    return;
  }
  auto so_name = GetSysConfig("TRACE_PLUGIN", "lib_so");
  if (so_name == nullptr) {
    return;
  }
  plugin_lib = dlopen(so_name, RTLD_LAZY | RTLD_GLOBAL);
  if (plugin_lib == nullptr) {
    LOG_ERROR("load trace plugin lib error : %s", dlerror());
    return;
  }
  create_t* create = reinterpret_cast<create_t*>(dlsym(plugin_lib, "createTracePlugin"));
  destroy_t* destroy = reinterpret_cast<destroy_t*>(dlsym(plugin_lib, "destroyTracePlugin"));
  if (!create || !destroy) {
    LOG_ERROR("Cannot find symbols of trace plugin.");
    DestroyTracePlugin(ctx);
    return;
  }
  std::string service_name;
  if (strcmp(service, "ME") == 0) {
    KString host = GetKwdbSysConfig("server_ip");
    KString port = DEFAULT_ME_PORT;
    service_name = "ME_" + host + ":" + port;
  } else {
    // default TsAE
    KString host = GetKwdbSysConfig("server_ip");
    KString port = GetTsAeSysConfig("ts_port");
    service_name = "TsAE_" + host + ":" + port;
  }
  g_trace_instance = create(service_name.c_str());
  if (g_trace_instance == nullptr) {
    LOG_ERROR("Cannot create trace plugin instance.");
    DestroyTracePlugin(ctx);
    return;
  }
  return;
}

void* StartSpan(kwdbContext_p ctx, const char* module, const char* file, const char* function) {
  if (g_trace_instance != nullptr) {
    return g_trace_instance->StartSpan(ctx, module, file, function);
  }
  return nullptr;
}

void EndSpan(kwdbContext_p ctx, void* span, const char* module, const char* file, const char* function) {
  if (g_trace_instance != nullptr) {
    g_trace_instance->EndSpan(ctx, span, module, file, function);
  }
}

void LogSpan(kwdbContext_p ctx, LogSeverity severity, const char* file, k_uint32 line, const char* fmt, ...) {
  if (g_trace_instance != nullptr) {
    char msg[MAX_SPAN_LOG];
    {
      va_list args;
      va_start(args, fmt);
      vsnprintf(msg, MAX_SPAN_LOG, fmt, args);
      va_end(args);
    }
    g_trace_instance->LogSpan(ctx, severity, file, line, msg);
  }
}

}  // namespace kwdbts
