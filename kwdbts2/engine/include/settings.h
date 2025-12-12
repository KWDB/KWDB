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

#pragma once

#include <string>
#include <vector>
#include "libkwdbts2.h"
#include "ts_common.h"
#include "ts_io.h"

namespace kwdbts {

#define ENV_KW_HOME                 "KW_HOME"
#define ENV_CLUSTER_CONFIG_HOME     "KW_CLUSTER_HOME"
#define ENV_KW_IOT_INTERVAL         "KW_IOT_INTERVAL"

enum WALMode : uint8_t {
  OFF = 0,
  SYNC = 1,
  FLUSH = 2,
  BYRL = 3    // by raft log
};

inline const string& s_defaultDateFormat()
{ static string s = "%Y-%m-%d"; return s; }
inline const string& s_defaultDateTimeFormat()
{ static string s = "%Y-%m-%d %H:%M:%S"; return s; }
inline const string& s_defaultTimeFormat()
{ static string s = "%H:%M:%S"; return s; }

enum TsIOMode {
  FIO = 0,
  FIO_AND_MMAP = 1,
  MMAP = 2,
};

struct EngineOptions {
  EngineOptions() {}

  static void init();

  std::string db_path;
  // WAL work level: 0:off, 1:sync, 2:flush, default is flush.
  uint8_t wal_level = WALMode::FLUSH;
  // whether use raft log as WAL
  bool use_raft_log_as_wal = false;
  uint16_t wal_buffer_size = 4;
  uint16_t thread_pool_size = 10;
  uint16_t task_queue_size = 1024;
  uint32_t buffer_pool_size = 4096;  // NEWPOOL_MAX_SIZE (4096)
  std::string brpc_addr = "127.0.0.1:27257";
  std::string cluster_id = "00000000-0000-0000-0000-000000000000";
  // 1MiB / 4KiB(BLOCK_SIZE) = 256
  [[nodiscard]] uint32_t GetBlockNumPerFile() const {
    return UINT32_MAX;
  }
  bool wal_archive = false;
  char* wal_archive_command = nullptr;
  TsEngineLogOption lg_opts = {"", 0, 0, DEBUG, ""};
  std::vector<RangeGroup> ranges;  // record engine hash value range

  static int32_t iot_interval;
  static const char slash_;
  static size_t ps_;

  static int ns_align_size_;
  static int table_type_;
  static int precision_;
  static int dt32_base_year_;           // DateTime32 base year.
  static bool zero_if_null_;
  static int  double_precision_;
  static int  float_precision_;
  static int64_t max_anon_memory_size_;
  static string home_;  // NOLINT
  static bool is_single_node_;
  static int table_cache_capacity_;
  static const string & dateFormat() { return s_defaultDateFormat(); }
  static const string & dateTimeFormat() { return s_defaultDateTimeFormat(); }
  static size_t pageSize() { return ps_; }
  static char directorySeperator() { return slash_; }
  static bool zeroIfNull() { return zero_if_null_; }
  static int precision() { return double_precision_; }
  static int float_precision() { return float_precision_; }
  static int64_t max_anon_memory_size() {return max_anon_memory_size_;}
  static const string & home() { return home_; }
  static bool isSingleNode() {return is_single_node_;}

  // V2
  static int vgroup_max_num;
  static DedupRule g_dedup_rule;
  static size_t mem_segment_max_size;
  static int32_t mem_segment_max_height;
  static uint32_t max_last_segment_num;
  static uint32_t max_compact_num;
  static size_t max_rows_per_block;
  static size_t min_rows_per_block;
  static int64_t default_partition_interval;
  static int64_t block_cache_max_size;
  static TsIOMode g_io_mode;
  static uint8_t compress_stage;

  TsIOEnv* io_env = &TsIOEnv::GetInstance();
};
extern std::atomic<int64_t> kw_used_anon_memory_size;

}  // namespace kwdbts
