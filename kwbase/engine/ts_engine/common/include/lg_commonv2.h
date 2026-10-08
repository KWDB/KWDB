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

#include <libkwdbts2.h>

#include <map>
#include <string>

#include "tr_impl.h"
#pragma once

namespace kwdbts {
struct TsEngineLogOption {
  string path;
  int64_t file_max_size;
  int64_t dir_max_size;
  LogSeverity level;
  string trace_on_off;
};

// @since kwbase/pkg/util/log/log.proto:18 enum Severity
static std::map<LgSeverity, LogSeverity> kLgSeverityMap = {
    {LgSeverity::UNKNOWN_K, LogSeverity::DEBUG},
    {LgSeverity::INFO_K, LogSeverity::INFO},
    {LgSeverity::WARN_K, LogSeverity::WARN},
    {LgSeverity::ERROR_K, LogSeverity::ERROR},
    {LgSeverity::FATAL_K, LogSeverity::FATAL},
    {LgSeverity::NONE_K, LogSeverity::DEBUG},
    {LgSeverity::DEFAULT_K, LogSeverity::INFO},
};
}  // namespace kwdbts
