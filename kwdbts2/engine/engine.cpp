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

#include "engine.h"

#include <dirent.h>
#include <iostream>
#include <utility>
#include <shared_mutex>

#include "ee_dml_exec.h"
#include "sys_utils.h"
#include "ts_table.h"
#include "th_kwdb_dynamic_thread_pool.h"
#include "ee_exec_pool.h"
#include "st_tier.h"

#ifndef KWBASE_OSS
#include "ts_config_autonomy.h"
#endif

extern std::map<std::string, std::string> g_cluster_settings;
extern std::shared_mutex g_settings_mutex;
extern bool g_go_start_service;

KStatus TSEngine::Execute(kwdbContext_p ctx, QueryInfo* req, RespInfo* resp) {
  ctx->ts_engine = this;
  KStatus ret = DmlExec::ExecQuery(ctx, req, resp);
  return ret;
}

namespace kwdbts {
int32_t EngineOptions::iot_interval  = 864000;
string EngineOptions::home_;  // NOLINT
size_t EngineOptions::ps_ = sysconf(_SC_PAGESIZE);

#define DEFAULT_NS_ALIGN_SIZE       2  // 16GB name service

int EngineOptions::ns_align_size_ = DEFAULT_NS_ALIGN_SIZE;
int EngineOptions::table_type_ = ROW_TABLE;
int EngineOptions::double_precision_ = 12;
int EngineOptions::float_precision_ = 6;
int64_t EngineOptions::max_anon_memory_size_ = 1*1024*1024*1024;  // 1G
int EngineOptions::dt32_base_year_ = 2000;
bool EngineOptions::zero_if_null_ = false;
#if defined(_WINDOWS_)
const char BigObjectConfig::slash_ = '\\';
#else
const char EngineOptions::slash_ = '/';
#endif
bool EngineOptions::is_single_node_ = false;
int EngineOptions::table_cache_capacity_ = 1000;
std::atomic<int64_t> kw_used_anon_memory_size;


void EngineOptions::init() {
  char * env_var = getenv(ENV_KW_HOME);
  if (env_var) {
    home_ = string(env_var);
  } else {
    home_ =  getenv(ENV_CLUSTER_CONFIG_HOME);
  }

  env_var = getenv(ENV_KW_IOT_INTERVAL);
  if (env_var) {
    setInteger(iot_interval, string(env_var), 30);
  }
}
}  //  namespace kwdbts
