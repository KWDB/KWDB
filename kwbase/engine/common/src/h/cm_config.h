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

#ifndef COMMON_SRC_H_CM_CONFIG_H_
#define COMMON_SRC_H_CM_CONFIG_H_

#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>

#include "cm_func.h"
#include "cm_kwdb_context.h"
#include "kwdb_type.h"

namespace kwdbts {

#define DEFAULT_IP_MAX_NUM   64
#define DEFAULT_PORT_MAX_NUM 50
#define DEFAULT_KWDBT_MAX_NUM 255
#define DEFAULT_NAME_MAX_NUM 255

class ShmSysConfig;

#ifdef WITH_TESTS
class Config;
class MockConfig {
 public:
  MockConfig() {}
  virtual ~MockConfig() {}
  virtual k_bool DoMockConfig(Config* config) {
    return KTRUE;
  }
};
#endif

class Config final {
  friend class ShmSysConfig;
 public:
  // Read the contents of the configuration file.
  k_bool readConfigFile(const char *cfgfilepath, std::ostringstream &oss);
  const k_char* find(const k_char* seg, const k_char* key) const;
  const k_char* find(const k_char* seg_at_key) const;
  /**
   * @brief used for change configuration from command line opts.
   * @return true if the add took place and false if the update took place.
   */
  k_bool AddOrUpdate(const KString& seg, const KString& key, const KString& val);

 private:
  static k_bool ValidCfgToken(const KString &token);
  // the map used to store key and value of configuration file
  std::map<KString, KString> settings_;
};

// Can't use template for Conifg and ShmSysConfig
class ShmSysConfig final {
 public:
  KStatus LoadConfig(kwdbContext_p ctx, const Config& config);
  const k_char* find(const k_char* seg, const k_char* key) const;
  const k_char* find(const k_char* seg_at_key) const;
 private:
  // the map used to store key and value of configuration file
  std::map<std::string, std::string> settings_;
};

/**
 * @brief Init sys configuration from config file, will precheck according to the pre_check_list
 * @param[in] path config file path
 * @param[in] pre_check_list pre check list
 * @param[in] size pre check count
 * @return KStatus
 */
KStatus InitSysConfig(kwdbContext_p ctx, const char* path, const char* pre_check_list[] = nullptr,
                      k_uint32 size = 0);

/**
 * @brief Add default configurations if not defined in sys config.
 * @return KStatus
 */
KStatus AddDefaultSysConfig(kwdbContext_p ctx);

/**
 * @brief ME add sysconfig to share memory
 * @return KStatus
 */
//KStatus AddSysConfigToHeader(kwdbContext_p ctx);

/**
 * @brief ME use this functin to destroy sysconfig
 * @return KStatus
 */
//KStatus DestroySysConfig(kwdbContext_p ctx);

/**
 * @brief AE or other Component use this function to recovery sysconfig
 * @return KStatus
 */
//KStatus RecoverySysConfig(kwdbContext_p ctx);

/**
 * @brief query cfg value, thread safe
 * @param[in] seg segment of cfg file
 * @param[out] key key in segment
 * @return failed: nullptr, succeed:value in const char*
 */
const k_char* GetSysConfig(const k_char* seg, const k_char* key);

typedef struct {
  k_int32 type_;
  KString key_;
  k_char internalIp_[DEFAULT_IP_MAX_NUM];
  k_uint32 internalPort_;
  k_uint32 agentInternalPort_[DEFAULT_PORT_MAX_NUM];
  k_uint32 storageInternalPort_;
  k_char nodeExternalIp_[DEFAULT_IP_MAX_NUM];
  k_uint32 nodeExternalPort_;
  k_uint32 agentExternalPort_[DEFAULT_PORT_MAX_NUM];
  k_uint32 storageExternalPort_;
  k_char kwdbtList_[DEFAULT_KWDBT_MAX_NUM];
  k_char portalName_[DEFAULT_NAME_MAX_NUM];
}deviceDescriptor;

enum CHECK_SECTION {
  SEC_SYSTEM = 1,
  SEC_ME,
  SEC_TS_AE,
  SEC_MAX
};

void getDeviceDescriptor(deviceDescriptor* device);
void SetMeSection(const KString& section);
void SetTsAeSection(const KString& section);
void SetKwdbSection(const KString& section);
void SetSysSection(const KString& section);

void SetKwdbProcName(const KString& kwdb_name, const KString& proc_name);
const KString& GetKwdbProcName();
k_bool IsMeProcess();

/**
 * @brief used for change Portal/Instance configuration, only can update before AddSysConfigToHeader(shmmem)
 * @param[in] key key in Portal/Instance section
 * @param[in] val updated or insert val of key
 * @return bool
 */
k_bool UpdateKwdbSysConfig(const KString& key, const KString& val);

/**
 * @brief used for change DeviceName:ME configuration, only can update before AddSysConfigToHeader(shmmem)
 * @param[in] key key in DeviceName:ME section
 * @param[in] val updated or insert val of key
 * @return bool
 */
k_bool UpdateMeSysConfig(const KString& key, const KString& val);

/**
 * @brief used for change DeviceName:TS_AE configuration, only can update before AddSysConfigToHeader(shmmem)
 * @param[in] key key in DeviceName:TS_AE section
 * @param[in] val updated or insert val of key
 * @return bool
 */
k_bool UpdateTsAeSysConfig(const KString& key, const KString& val);

/**
 * @brief query cfg value, thread safe
 * @param[out] key key in DeviceName:ME section
 * @return failed: nullptr, succeed:value in const char*
 */
const k_char* GetMeSysConfig(const k_char* key);

/**
 * @brief query cfg value, thread safe
 * @param[out] key key in DeviceName:System section
 * @return failed: nullptr, succeed:value in const char*
 */
const k_char* GetKwdbSysConfig(const k_char* key);

/**
 * @brief query cfg value, thread safe
 * @param[out] key key in DeviceName:Portal/Instance section
 * @return failed: nullptr, succeed:value in const char*
*/
const k_char* GetKwdbConfig(const k_char* key);

/**
 * @brief query cfg value, thread safe
 * @param[out] key key in DeviceName:TS_AE section
 * @return failed: nullptr, succeed:value in const char*
 */
const k_char* GetTsAeSysConfig(const k_char* key);

k_bool IsValidKwdbServerPort(const KString& port);

k_bool IsValidIpV4Addr(const KString& ip);

KStatus PreCheckConfig(std::ostringstream &ss);

}  // namespace kwdbts

#endif  // COMMON_SRC_H_CM_CONFIG_H_
