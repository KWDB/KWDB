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

#include "cm_config.h"

#include <arpa/inet.h>
#include <iostream>
#include <algorithm>

#include "cm_func.h"
#include "cm_new.h"
#include "lg_api.h"
#include "cm_fault_injection.h"

namespace kwdbts {

Config* kTmpSysConfig = nullptr;  // before share memory setup, use kTmpSysConfig
ShmSysConfig* kShmSysConfig = nullptr;
KString kKwdbProcName;
KString kwdbSection;
KString kSysSection;
KString kMeSection;
KString kTsAeSection;
k_bool kIsMeProcess = KFALSE;

#define MIN_AVAIL_PORT (1024)
#define MAX_AVAIL_PORT (65535)

#define MAX_CONNECTION_LIMIT (10000)

const char* ipCheckList[] = {
  "SEC_SYSTEM,server_ip",
  // "server_ip",  // ips' will support later.
  "SEC_ME,kap_log_trace_ip",
  "SEC_TS_AE,ts_st_ip",  // need normalizing
  "SEC_TS_AE,ts_st_scheduler_ip",  // need normalizing
};

const char* portCheckList[] = {
  "SEC_SYSTEM,server_port",
  // "server_port",  // ports' will support later.
  "SEC_ME,mem_debugger_server_port",
  "SEC_ME,kap_log_trace_port",
  "SEC_TS_AE,ts_st_query_port",
  "SEC_TS_AE,ts_st_inject_port",
  "SEC_TS_AE,ts_st_auth_port",
};

const char* otherConfigsCheckList[] = {
  "SEC_ME,log_level",
  "SEC_ME,log_file_max_size",
};

// Log directory key name in config file.
const char* LOG_DIR_KEY = "log_dir";

#ifdef WITH_TESTS
MockConfig* mockobj = nullptr;
#endif

k_bool Config::ValidCfgToken(const KString& str) {
  if (str.length() < 1 || std::isspace(str.front()) ||
      std::isspace(str.back())) {
    return KFALSE;
  }
  return KTRUE;
}

k_bool Config::readConfigFile(const char* path, std::ostringstream &oss) {
  if (path == nullptr) {
    oss << "Invalid path!\n";
    return KFALSE;
  }
  std::fstream cfgFile(path);
  k_bool accept_status = KFALSE;
  KString line;
  k_int32 cur_line_num = 0;
  if (!cfgFile.is_open()) {
    oss << "Can't open the config file at:" << path << std::endl;
    return KFALSE;
  }
  KString segment;
  KString key;
  KString value;
  size_t delim_pos;
  while (std::getline(cfgFile, line)) {
    cur_line_num++;
    if (std::all_of(line.begin(), line.end(), isspace)) continue;  // empty
    // a=b [a], so at least 3 characters
    if (line.length() < 3 && line.front() != '#') break;  // error
    if (line.front() == '#') continue;                    // comment
    if (line.front() == '[' || line.back() == ']') {
      segment = line.substr(1, line.length() - 2);
      if (!ValidCfgToken(segment)) break;  // error
      continue;                            // new segment
    }
    if ((delim_pos = line.find('=')) == KString::npos) break;
    key = line.substr(0, delim_pos);
    value = line.substr(delim_pos + 1);
    if (!ValidCfgToken(segment) || !ValidCfgToken(key) ||
        !ValidCfgToken(value))
      break;
    KString newKey = segment + "@" + key;
    if (settings_.find(newKey) != settings_.end()) break;  // exist same key
    settings_.insert(std::pair<KString, KString>(newKey, value));
  }
  if (cfgFile.eof()) {
    accept_status = KTRUE;
  }
  cfgFile.close();
  if (!accept_status) {
    oss << "Cfgfile syntax error at " << cur_line_num << ":" << line
             << std::endl;
  }
  return accept_status;
}

const k_char* Config::find(const k_char* seg_at_key) const {
  const auto& iter = settings_.find(seg_at_key);
  if (iter == settings_.end()) {
//    std::cout << "can not find [" << seg_at_key << "]." << std::endl;
    return nullptr;
  }
  return iter->second.c_str();
}

const k_char* Config::find(const k_char* seg, const k_char* key) const {
  const KString seg_at_key = KString(seg) + "@" + KString(key);
  return find(seg_at_key.c_str());
}

k_bool Config::AddOrUpdate(const KString& seg, const KString& key, const KString& val) {
  return settings_.insert_or_assign(KString(seg + "@" + key).c_str(), val.c_str()).second;
}

// 1. some UTs don't need use share memory
// 2. some UTs need to replace configs such as port
KStatus InitSysConfig(kwdbContext_p ctx, const char* path, const char* pre_check_list[],
                      k_uint32 size) {
  EnterFunc();
  k_bool ret;
  Config* config = KNEW Config();
    if (config == nullptr) {
    Return(KStatus::FAIL);
  }
  std::ostringstream oss;
  ret = config->readConfigFile(path, oss);
  INJECT_DATA_FAULT(FAULT_CM_CONFIG_FILE_FAIL, ret, KFALSE, nullptr);
  if (ret == KFALSE) {
    delete config;
    Return(KStatus::FAIL);
  }

  for (k_int32 i = 0; i < size; i++) {
    if (config->find(pre_check_list[i]) == nullptr) {
//      std::cout << "Miss " << pre_check_list[i] << " config!";
      delete config;
      Return(KStatus::FAIL);
    }
  }

#ifdef WITH_TESTS
  if (mockobj) {
    mockobj->DoMockConfig(config);
  }
#endif
  // init config succeed
  kTmpSysConfig = config;
  Return(KStatus::SUCCESS);
}

/* If LOG_DIR_KEY is not defined in sys config, then set
 * log directory to ${KWDB_DATA_ROOT}/log if KWDB_DATA_ROOT
 * is defined, otherwise set it to ./kwdb_log.
 */
KStatus AddDefaultSysConfig(kwdbContext_p ctx) {
  if (kTmpSysConfig->find(kMeSection.c_str(), LOG_DIR_KEY) == nullptr) {
    if (const char* kwdb_data_root = std::getenv("KWDB_DATA_ROOT")) {
      char log_dir[FULL_FILE_NAME_MAX_LEN];
      snprintf(log_dir, FULL_FILE_NAME_MAX_LEN, "%s/log", kwdb_data_root);
      kTmpSysConfig->AddOrUpdate(kMeSection, LOG_DIR_KEY, log_dir);
    } else {
      kTmpSysConfig->AddOrUpdate(kMeSection, LOG_DIR_KEY, "./kwdb_log");
    }
  }
  return KStatus::SUCCESS;
}

KStatus ShmSysConfig::LoadConfig(kwdbContext_p ctx, const Config& config) {
  for (auto const& [key, val] : config.settings_) {
    try {
      if ((settings_.insert(std::pair<std::string, std::string>(
              key.c_str(), val.c_str())).second == KFALSE)) {
        return KStatus::FAIL;
      }
    } catch(...) {
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

const k_char* ShmSysConfig::find(const k_char* seg_at_key) const {
  const auto& iter = settings_.find(seg_at_key);
  if (iter == settings_.end()) {
//    std::cout << "can not find [" << seg_at_key << "]." << std::endl;
    return nullptr;
  }
  return iter->second.c_str();
}

const k_char* ShmSysConfig::find(const k_char* seg, const k_char* key) const {
  const KString seg_at_key = KString(seg) + "@" + KString(key);
  return find(seg_at_key.c_str());
}

//KStatus AddSysConfigToHeader(kwdbContext_p ctx) {
//  EnterFunc();
//  Assert(kTmpSysConfig != nullptr);
//  void* ptr = k_malloc(sizeof(ShmSysConfig));
//  if (ptr == nullptr) {
//    Return(KStatus::FAIL);
//  }
//  kShmSysConfig = new(ptr) ShmSysConfig();
//  if (kShmSysConfig == nullptr) {
//    k_free(ptr);
//    Return(KStatus::FAIL);
//  }
//  KStatus ret = kShmSysConfig->LoadConfig(ctx, *kTmpSysConfig);
//  if (ret == KStatus::FAIL) {
//    kShmSysConfig->~ShmSysConfig();
//    k_free(kShmSysConfig);
//    kShmSysConfig = nullptr;
//    Return(KStatus::FAIL);
//  }
//  KWDBHEADER.sys_config_ = ADDR_OFFSET(kShmSysConfig);
//  Return(KStatus::SUCCESS);
//}

//KStatus RecoverySysConfig(kwdbContext_p ctx) {
//  size_t off = KWDBHEADER.sys_config_;
//  if (off == 0) {
//    return KStatus::FAIL;
//  }
//  kShmSysConfig = static_cast<ShmSysConfig*>(OFFSET_ADDR(off));
//  return KStatus::SUCCESS;
//}

//KStatus DestroySysConfig(kwdbContext_p ctx) {
//  if (kTmpSysConfig) {
//    delete kTmpSysConfig;
//    kTmpSysConfig = nullptr;
//  }
//  if (kShmSysConfig) {
//    kShmSysConfig->~ShmSysConfig();
//    k_free(kShmSysConfig);
//    kShmSysConfig = nullptr;
//    KWDBHEADER.sys_config_ = 0;
//  }
//  return KStatus::SUCCESS;
//}

const k_char* GetSysConfig(const k_char* seg, const k_char* key) {
  // use likely kShmSysConfig not null
  if (kShmSysConfig) {
    return kShmSysConfig->find(seg, key);
  }
  if (kTmpSysConfig) {
    return kTmpSysConfig->find(seg, key);
  }
  return nullptr;
}

void SetMeSection(const KString& section) {
  kMeSection = section;
}

void SetTsAeSection(const KString& section) {
  kTsAeSection = section;
}

void SetKwdbSection(const KString& section) {
  kwdbSection = section;
}

void SetSysSection(const KString& section) {
  kSysSection = section;
}

void SetKwdbProcName(const KString& kwdb_name, const KString& proc_name) {
  kKwdbProcName = kwdb_name + "_" + proc_name;
  if (proc_name == "ME") {
    kIsMeProcess = KTRUE;
  } else {
    kIsMeProcess = KFALSE;
  }
}

k_bool IsMeProcess() {
  return kIsMeProcess;
}

const KString& GetKwdbProcName() {
  return kKwdbProcName;
}

k_bool UpdateKwdbSysConfig(const KString& key, const KString& val) {
  if (kShmSysConfig) {
    return KFALSE;
  }
  kTmpSysConfig->AddOrUpdate(kwdbSection, key, val);
  return KTRUE;
}

k_bool UpdateMeSysConfig(const KString& key, const KString& val) {
  if (kShmSysConfig) {
    return KFALSE;
  }
  kTmpSysConfig->AddOrUpdate(kMeSection, key, val);
  return KTRUE;
}

k_bool UpdateTsAeSysConfig(const KString& key, const KString& val) {
  if (kShmSysConfig) {
    return KFALSE;
  }
  kTmpSysConfig->AddOrUpdate(kTsAeSection, key, val);
  return KTRUE;
}

const k_char* GetMeSysConfig(const k_char* key) {
  return GetSysConfig(kMeSection.c_str(), key);
}

const k_char* GetKwdbConfig(const k_char* key) {
  return GetSysConfig(kwdbSection.c_str(), key);
}

const k_char* GetKwdbSysConfig(const k_char* key) {
  return GetSysConfig(kSysSection.c_str(), key);
}

const k_char* GetTsAeSysConfig(const k_char* key) {
  return GetSysConfig(kTsAeSection.c_str(), key);
}

k_bool IsValidIpV4Addr(const KString& ip) {
  // correct ip format.
  struct sockaddr_in sa;
  k_int32 ret = inet_pton(AF_INET, ip.c_str(), &(sa.sin_addr));
  if (ret <= 0) {
    return KFALSE;
  }
  return KTRUE;
}

// Well-known ports are numbered from 0 to 1023
// we can use all other port[1024, 65535](even private port[49152, 65535])
k_bool IsValidKwdbServerPort(const KString& port) {
  k_int32 port_num;
  try {
    if (std::all_of(port.begin(), port.end(), ::isdigit) != KTRUE) {
      return KFALSE;
    }
    port_num = std::stoi(port);
  } catch (const std::exception &e) {
    return KFALSE;
  }
  if (port_num < MIN_AVAIL_PORT || port_num > MAX_AVAIL_PORT) {
    return KFALSE;
  }
  return KTRUE;
}

const KString findCheckSec(const KString& val, KString* key) {
  // separate and get section.
  // "," is used to seperate section type
  size_t pos = val.find(',');
  if (pos == KString::npos) {
    Assert(pos != KString::npos);
    return NULL;
  }
  KString section;
  section = val.substr(0, pos);  // the part before ","
  *key = val.substr(pos+1);      // the part after ","
  return section;
}

KStatus checkConfigIp(std::ostringstream &oss) {
  KString key;
  const k_char* tmp = nullptr;
  KString host;

  for (k_int32 k = 0; k < (sizeof(ipCheckList) / sizeof(char*)); k++) {
    KString section = findCheckSec(ipCheckList[k], &key);
    if (section.c_str() == NULL) {
      oss << "unknown check type for ips.\n";
      return FAIL;
    }
    // find ip val.
    if (section == "SEC_SYSTEM") {
      tmp = GetKwdbSysConfig(key.c_str());
    } else if (section == "SEC_ME") {
      tmp = GetMeSysConfig(key.c_str());
    } else if (section == "SEC_TS_AE") {
      tmp = GetTsAeSysConfig(key.c_str());
    }
    if (tmp != nullptr) {
      host = tmp;
    } else {
      // do not have the specific key, just skip.
      continue;
    }

    // check ip val.
    if (IsValidIpV4Addr(host) == KFALSE) {
      oss << "invalid address " << host << std::endl;
      return FAIL;
    }
  }
  return SUCCESS;
}

KStatus checkConfigPort(std::ostringstream &oss) {
  KString key;
  const k_char* tmp = nullptr;
  KString port;

  for (k_int32 k = 0; k < (sizeof(portCheckList) / sizeof(char*)); k++) {
    KString section = findCheckSec(portCheckList[k], &key);
    if (section.c_str() == NULL) {
      oss << "unknown check type for ports.\n";
      return FAIL;
    }
    // find port val.
    if (section == "SEC_SYSTEM") {
      tmp = GetKwdbSysConfig(key.c_str());
    } else if (section == "SEC_ME") {
      tmp = GetMeSysConfig(key.c_str());
    } else if (section == "SEC_TS_AE") {
      tmp = GetTsAeSysConfig(key.c_str());
    }
    if (tmp != nullptr) {
      port = tmp;
    } else {
      // do not have the specific key, just skip.
      continue;
    }

    // check port val.
    if (IsValidKwdbServerPort(port) == KFALSE) {
      oss << "invalid port " << port << std::endl;
      return FAIL;
    }
  }
  return SUCCESS;
}

KStatus checkOtherConfigs(std::ostringstream &oss) {
  KString key;
  const k_char* tmp = nullptr;
  KString conf;

  for (k_int32 k = 0; k < (sizeof(otherConfigsCheckList) / sizeof(char*)); k++) {
    KString section = findCheckSec(otherConfigsCheckList[k], &key);
    if (section.c_str() == NULL) {
      Assert("unknown check type for configs.");
      return FAIL;
    }
    if (section == "SEC_SYSTEM") {
      tmp = GetKwdbSysConfig(key.c_str());
    } else if (section == "SEC_PORTAL") {
      tmp = GetKwdbConfig(key.c_str());
    } else if (section == "SEC_ME") {
      tmp = GetMeSysConfig(key.c_str());
    } else if (section == "SEC_TS_AE") {
      tmp = GetTsAeSysConfig(key.c_str());
    }
    if (tmp != nullptr) {
      conf = tmp;
    } else {
      oss << "invalid/missing configuration " <<  key.c_str() << std::endl;
      return FAIL;
    }
  }

  k_int32 kMaxConnLimit = std::stoi(GetKwdbConfig("max_connection"));
  if (kMaxConnLimit <= 0 || kMaxConnLimit > MAX_CONNECTION_LIMIT) {
    oss << "Invalid configuration max_connection, limited to " << MAX_CONNECTION_LIMIT << std::endl;
    return FAIL;
  }

  return SUCCESS;
}

KStatus PreCheckConfig(std::ostringstream &oss) {
  // check config's legality.
  KStatus ret = checkConfigIp(oss);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  ret = checkConfigPort(oss);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  ret = checkOtherConfigs(oss);
  if (ret != KStatus::SUCCESS) {
    return ret;
  }
  // you can add some other checker at here.
  return SUCCESS;
}

}  // namespace kwdbts
