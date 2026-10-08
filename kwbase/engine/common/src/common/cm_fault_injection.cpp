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

#include "cm_fault_injection.h"

#include <bits/stdc++.h>
#include <arpa/inet.h>

#include<algorithm>

#include "kwdb_type.h"
#include "lg_api.h"
#include "th_kwdb_dynamic_thread_pool.h"

namespace kwdbts {

#ifndef K_DO_NOT_SHIP

#define DECL_FAULT(a) #a,
const char* kFaultTypeStrs[] = { ENUMS_FAULT_TYPE };
#undef DECL_FAULT

#define DECL_FAULT(a) FaultInjector(a),
FaultInjector kFaultInjectors_[] = { ENUMS_FAULT_TYPE };
#undef DECL_FAULT

const char* kFaultConfig = "fault_config.txt";
k_char kFaultConfigPath[FULL_FILE_NAME_MAX_LEN];
size_t kMaxFaultIdLen = 40;
// one more space to store FaultInjector(FAULT_MAX)
FaultInjector* fault_injectors_[FAULT_MAX + 1];

std::map<KString, FaultType> look_up_tbl;

FaultInjector* GetFaultInjector(k_int32 id) {
  return fault_injectors_[id];
}

void SetFaultInjector(k_int32 id, FaultInjector* fault) {
  fault_injectors_[id] = fault;
}

// cmd_tokens format: active|trigger fault when [value]
k_int32 ActiveFault(const std::vector<KString> &cmd_tokens, std::ostringstream &oss) {
  if (cmd_tokens.size() != 3 && cmd_tokens.size() != 4) {
    oss << "Invalid active parameter number " << cmd_tokens.size() << std::endl;
    return -1;
  }
  auto fault = look_up_tbl.find(cmd_tokens[1]);
  if (fault == look_up_tbl.end()) {
    oss << "Invalid fault:" << cmd_tokens[1] << std::endl;
    return -1;
  }
  k_int32 times = std::stoi(cmd_tokens[2]);
  // dont need reset call_count_ to 0
  fault_injectors_[fault->second]->trigger_time_ = times;
  fault_injectors_[fault->second]->SetInput(cmd_tokens.size() == 4 ? cmd_tokens[3] : "");
  fault_injectors_[fault->second]->actived_ = true;
  fault_injectors_[fault->second]->auto_reset_ = cmd_tokens[0] == "trigger" ? true : false;
  return 0;
}

// cmd_tokens format: deactive fault
k_int32 DeactiveFault(const std::vector<KString> &cmd_tokens, std::ostringstream &oss) {
  if (cmd_tokens.size() != 2) {
    oss << "Invalid deactive parameter number" << cmd_tokens.size() << std::endl;
    return -1;
  }
  auto fault = look_up_tbl.find(cmd_tokens[1]);
  if (fault == look_up_tbl.end()) {
    oss << "Invalid fault:" << cmd_tokens[1] << std::endl;
    return -1;
  }
  fault_injectors_[fault->second]->actived_ = false;
  fault_injectors_[fault->second]->auto_reset_ = false;
  fault_injectors_[fault->second]->SetInput("");
  fault_injectors_[fault->second]->trigger_time_ = 0;
  fault_injectors_[fault->second]->call_count_ = 0;
  return 0;
}

// cmd_tokens format: ls
k_int32 ListFaultInjector(const std::vector<KString> &cmd_tokens, std::ostringstream &oss) {
  if (cmd_tokens.size() != 1) {
    oss << "Invalid deactive parameter number" << cmd_tokens.size() << std::endl;
    return -1;
  }
  oss << std::left << std::setw(8) << "No." <<  std::setw(kMaxFaultIdLen + 1) << "Id" << std::setw(8) << "actived"
      << std::setw(8) << "history" << std::setw(8) << "when"
      << std::setw(8) << "cur" << std::setw(8) << "val" << std::endl;
  for (FaultType i = FAULT_CM_CONFIG_FILE_FAIL; i < FAULT_MAX; i = FaultType(i + 1)) {
    look_up_tbl.insert(std::make_pair(kFaultTypeStrs[i], i));
    oss << std::left << std::setw(8) << i <<  std::setw(kMaxFaultIdLen + 1) << kFaultTypeStrs[i]
            << std::setw(8) << fault_injectors_[i]->actived_
            << std::setw(8) << fault_injectors_[i]->his_trigger_count_
            << std::setw(8) << fault_injectors_[i]->trigger_time_
            << std::setw(8) << fault_injectors_[i]->call_count_
            << std::setw(8) << fault_injectors_[i]->input_  << std::endl;
  }
  return 0;
}

k_int32 ProcInjectFault(const KString &cmd, std::ostringstream &oss) {
  std::istringstream ss(cmd);
  std::vector<KString> tokens;
  KString token;
  while (ss >> token) {
    tokens.push_back(token);
  }
  if (tokens.size() < 1) {
    oss << "Invalid cmd: " << cmd << std::endl;
    return -1;
  }

  if (tokens[0] == "active") {
    return ActiveFault(tokens, oss);
  } else if (tokens[0] == "trigger") {
    return ActiveFault(tokens, oss);
  } else if (tokens[0] == "deactive") {
    return DeactiveFault(tokens, oss);
  } else if (tokens[0] == "ls") {
    return ListFaultInjector(tokens, oss);
  } else {
    oss << "Invalid cmd:" << cmd << std::endl;
    return -1;
  }
  return 0;
}

void HandleInjectFaultCmd(void* args) {
  k_int64* conn_args = static_cast<k_int64*>(args);
  char cmd[100] = {0};
  recv(conn_args[0], cmd, 100, MSG_NOSIGNAL);
  std::ostringstream oss;
  k_int32 ret = ProcInjectFault(cmd, oss);
  if (ret < 0) {
    oss << "Fail\n";
  } else {
    oss << "Success\n";
  }
  KString status = oss.str();
  send(conn_args[0], status.c_str(), status.length() + 1, MSG_NOSIGNAL);
  close(conn_args[0]);
}

k_int32 InitFaultInjection() {
  for (FaultType i = FAULT_CM_CONFIG_FILE_FAIL; i < FAULT_MAX; i = FaultType(i + 1)) {
    look_up_tbl.insert(std::make_pair(kFaultTypeStrs[i], i));
    kMaxFaultIdLen = std::max(strlen(kFaultTypeStrs[i]), kMaxFaultIdLen);
  }

  const char* kwdb_data_root;
  if ( (kwdb_data_root = std::getenv("KWDB_DATA_ROOT")) &&
    ((std::strlen(kwdb_data_root) + 1 + std::strlen(kFaultConfig)) < FULL_FILE_NAME_MAX_LEN) ) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(kFaultConfigPath, FULL_FILE_NAME_MAX_LEN, "%s/%s", kwdb_data_root,
             kFaultConfig);
#pragma GCC diagnostic pop
  } else {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
    snprintf(kFaultConfigPath, FULL_FILE_NAME_MAX_LEN, "./%s",
             kFaultConfig);
#pragma GCC diagnostic pop
  }
  // active fault from file
  std::fstream fault_cfg(kFaultConfigPath);
  KString line;
  std::ostringstream oss;
  if (fault_cfg.is_open()) {
    while (std::getline(fault_cfg, line)) {
      ProcInjectFault(line, oss);
    }
    fault_cfg.close();
  }
  return 0;
}

void InjectFaultServer(void* args) {
  k_int64 listen_port = static_cast<k_int64*>(args)[0];
  int listen_sock = -1;
  try {
    listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
      LOG_ERROR("InjectFaultServer socket failed, err: %s \n", strerror(errno));
      return;
    }
  } catch (...) {
    LOG_ERROR("InjectFaultServer socket failed \n");
    return;
  }

  sockaddr_in serv_addr;
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(listen_port);

  try {
    k_int32 mw_optval = 1;
    setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char*>(&mw_optval), sizeof(mw_optval));
    if (bind(listen_sock, reinterpret_cast<sockaddr*>(&serv_addr), sizeof(serv_addr)) < 0) {
      LOG_ERROR("InjectFaultServer bind failed, err: %s \n", strerror(errno));
      return;
    }
  } catch (...) {
    LOG_ERROR("InjectFaultServer bind failed \n");
    return;
  }

  try {
    int MAX_CLIENT = 5;
    if (listen(listen_sock, 5) < 0) {
      LOG_ERROR("InjectFaultServer listen failed, err: %s \n", strerror(errno));
      return;
    }
  } catch (...) {
    LOG_ERROR("InjectFaultServer listen failed \n");
    return;
  }
  LOG_ERROR("The port of InjectFaultServer is %lu \n", listen_port);

  KWDBDynamicThreadPool& thread_pool = KWDBDynamicThreadPool::GetThreadPool();
  KWDBOperatorInfo kwdb_operator_info;
  kwdb_operator_info.SetOperatorName("HandleInjectFaultCmd");
  kwdb_operator_info.SetOperatorOwner("InjectFaultServer");

  while (true) {
    k_int32 socket_id = 0;
    try {
      sockaddr_in clnt_addr;
      socklen_t clnt_addr_size = sizeof(clnt_addr);
      socket_id = accept(listen_sock, reinterpret_cast<sockaddr*>(&clnt_addr), &clnt_addr_size);
    } catch (...) {
      LOG_ERROR("InjectFaultServer accept failed \n");
      return;
    }

    if (thread_pool.IsStop()) {
      if (socket_id != -1) {
        close(socket_id);
      }
      break;
    }

    if (socket_id < 0) {
      continue;
    }

    if (thread_pool.IsAvailable()) {
      time_t now;
      kwdb_operator_info.SetOperatorStartTime((k_uint64) time(&now));
      k_int32 args[1] = {socket_id};
      thread_pool.ApplyThread(HandleInjectFaultCmd, &args, &kwdb_operator_info);
    }
  }
}

#endif

}  // namespace kwdbts
