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
#include "tr_impl.h"
#include "lg_impl.h"
#include <sys/time.h>

#include <mutex>

#include "cm_config.h"
#include "lg_writer.h"
#include "lg_writer_console.h"
namespace kwdbts {

KStatus Tracer::Init(kwdbContext_p ctx) { return KStatus::SUCCESS; }
/*!
 *
 * @param ctx
 * @return
 */
KStatus Tracer::init() {
  if (TRACER_STATUS_ != INIT) {
    return (FAIL);
  }
  device_ = GetKwdbProcName();
  TRACER_STATUS_ = RUN;
  return KStatus::SUCCESS;
}
KStatus Tracer::Destroy(kwdbContext_p ctx) {
  EnterFunc();
  if (tracer_writer_ != nullptr) {
    tracer_writer_->Destroy();
    tracer_writer_.reset();
  }
  TRACER_STATUS_ = STOP;
  Return(KStatus::SUCCESS);
}

/*!
 * @brief trace generate statement and store them in cache
 * @param status
 * @param file
 * @param line
 * @param fmt
 * @param ...
 */

void Tracer::Trace(kwdbContext_p ctx, const char *file, k_uint16 line, k_int32 level, const char *fmt, ...) {
  KwdbModule mod = KwdbModule::UN;
  if (FAIL == GetModuleByFileName(file, &mod)) {
    // if use TRACE_. but file not start with module. will always log 2 file
    // Tracer::nestCount_ = 0;
    // return;
  } else if (!isEnableFor(mod, level)) {
    // only file start with module && get settings from cluster can be control
    // read the configuration of the metadata module
    // and compare it with the levle passed in.
    // if the incoming levle is less than the configured levle, trace can be written
    return;
  }
  va_list args;
  va_start(args, fmt);
  Trace(ctx, mod, GetFileName(file), line, fmt, args);
  va_end(args);
}

void Tracer::Trace(kwdbContext_p ctx, KwdbModule mod, const char *file, k_uint16 line, const char *fmt, va_list args) {
  va_list tmp_args;
  va_copy(tmp_args, args);
  k_int32 len = vsnprintf(nullptr, 0, fmt, tmp_args) + 1;
  va_end(tmp_args);

  auto *pItem = static_cast<LogItem *>(malloc(sizeof(LogItem) + len));
  if (nullptr == pItem) {
    return;
  }
  memset(pItem, 0, sizeof(LogItem) + len);

  pItem->module = mod;
  snprintf(pItem->file_name, sizeof(pItem->file_name), "%s", file);
  pItem->line = line;
  if (nullptr != ctx) {
    pItem->thread_id = ctx->thread_id;
    pItem->connID = ctx->connection_id;
  }

  struct timeval time_secs;
  gettimeofday(&time_secs, nullptr);
  pItem->time_stamp = time_secs.tv_sec * 1000000 + time_secs.tv_usec;

  pItem->buf_size = (k_uint16)len;
  pItem->itemType = ItemType::TYPE_TRACE;
  snprintf(pItem->device, sizeof(pItem->device), "%s", device_.c_str());
  vsnprintf(pItem->buf, pItem->buf_size, fmt, args);
  LOGGER.LogItemToFile(pItem);
  // if (nullptr != tracer_writer_) {
  //   KStatus status = tracer_writer_->Write( pItem);
  //   if (status == kwdbts::FAIL) {
  //     free(pItem);
  //     return;
  //   }
  // }
  free(pItem);
}
void splitString(const char *str, char delim, const std::function<void(char *)> &invoker) {
  Assert(invoker != nullptr);
  char tmp[128] = {};
  snprintf(tmp, sizeof(tmp), "%s", str);
  char *p, *token;
  for (p = token = tmp; *p != '\0'; p++) {
    if (*p == delim) {
      *p = '\0';
      if (p != token) {
        invoker(token);
      }
      token = p + 1;
      continue;
    }
  }
  if (p != token) {
    invoker(token);
  }
}
void parseConfigStr(const char *str, const std::function<void(const char *key, k_int32 value)> &invoker) {
  splitString(str, ',', [&](char *token) {
    char name[8] = {};
    k_int32 value = 0;
    splitString(token, ' ', [&](char *key) {
      if (name[0] == '\0') {
        snprintf(name, sizeof(name), "%s", key);
      } else {
        value = std::strtol(key, nullptr, 10);
      }
    });
    invoker(name, value);
  });
}
void Tracer::SetTraceConfigStr(const KString &trace_config) {
  clearModuleLevel();
  parseConfigStr(trace_config.c_str(), [this](const char *key, k_int32 value) {
    KwdbModule mod;
    if (GetModuleByName(key, &mod) == FAIL) {
      return;
    }
    this->setModuleLevel(mod, value);
  });
  if (checkModuleLevel()) {
    trace_switch_ = true;
  } else {
    trace_switch_ = false;
  }
}
void Tracer::clearModuleLevel() {
  for (KwdbModule i = SU; i < MAX_MODULE; i = KwdbModule(i + 1)) {
    level_of_module_[i] = 0;
  }
}

k_bool Tracer::checkModuleLevel() {
  for (KwdbModule i = SU; i < MAX_MODULE; i = KwdbModule(i + 1)) {
    if (level_of_module_[i] != 0) {
      return true;
    }
  }
  return false;
}
k_int32 Tracer::setModuleLevel(KwdbModule mod, k_int32 level) {
  k_int32 last_level = level_of_module_[mod];
  level_of_module_[mod] = level;
  return last_level;
}
}  // namespace kwdbts
