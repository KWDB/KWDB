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

#pragma once

#include <string>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "ts_object_error.h"
#include "data_type.h"
#include "kwdb_consts.h"

using namespace std;

namespace kwdbts {

enum CompressionType {
  GZIP = 0,
  LZ4 = 1,
  LZMA = 2,
  LZO = 3,
  XZ = 4,
  ZSTD = 5,
};

enum CompressionLevel {
  LOW = 0,
  MIDDLE = 1,
  HIGH = 2,
};

struct Compression {
  CompressionType compression_type;  // Compression algorithms
  CompressionLevel compression_level;  // Degree of compression
  bool has_compression_level_option;  // Whether the compression_level option is included
  bool has_lz4_hc_option;  // Whether the xhc option is included
};

// mksquashfs options
struct MkSquashfsOption {
  std::unordered_map<CompressionType, Compression> compressions;
  bool has_mem_option;  // Whether the -mem option is included
  bool has_processors_option;  // Whether the -processors option is included
  int processors_scheduled = 1;  // The number of processors used for scheduled compression, default 1
  int processors_immediate = 3;  // The number of processors used for immediate compression, default 3
};

// mount supports compression algorithms
struct MountOption {
  std::unordered_set<CompressionType> mount_compression_types;
};

inline string CompressionTypeToLowerCase(const CompressionType& type) {
  switch (type) {
    case GZIP:
      return "gzip";
    case LZ4:
      return "lz4";
    case LZMA:
      return "lzma";
    case LZO:
      return "lzo";
    case XZ:
      return "xz";
    case ZSTD:
      return "zstd";
  }
  return "";
}

inline string CompressionTypeToUpperCase(const CompressionType& type) {
  switch (type) {
    case GZIP:
      return "GZIP";
    case LZ4:
      return "LZ4";
    case LZMA:
      return "LZMA";
    case LZO:
      return "LZO";
    case XZ:
      return "XZ";
    case ZSTD:
      return "ZSTD";
  }
  return "";
}

extern std::string sudo_cmd;

extern int g_max_mount_cnt_;

extern atomic<int> g_cur_mount_cnt_;

extern int64_t g_compress_interval;

extern Compression g_compression;

extern MkSquashfsOption g_mk_squashfs_option;

extern bool g_compress_info_initialized;

extern MountOption g_mount_option;

bool compress(const string& db_path, const string& tbl_sub_path, const string& dir_name, int nthreads, ErrorInfo& err_info);

bool compressToPath(const string& db_path, const string& tbl_sub_path, const string& dir_name,
                    const string& desc_path, int nthreads, ErrorInfo& err_info);

void initSudo();

bool mount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info);

bool umount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info);

bool isMounted(const string& dir_path);

int executeShell(const std::string& cmd, std::string &result);

void InitCompressInfo();

} // namespace kwdbts
