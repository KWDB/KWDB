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

#include <shared_mutex>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include "lt_latch.h"
#include "utils/compress_utils.h"
#include "sys_utils.h"

namespace kwdbts {

std::string sudo_cmd = " ";

int g_max_mount_cnt_ = 1000;

atomic<int> g_cur_mount_cnt_ = 0;

int64_t g_compress_interval = 3600;

Compression g_compression = {kwdbts::CompressionType::GZIP, kwdbts::CompressionLevel::MIDDLE, true, false};

MkSquashfsOption g_mk_squashfs_option;

bool g_compress_info_initialized = false;

KLatch g_compress_info_mutex(LATCH_ID_COMPRESS_INFO_INIT_MUTEX);

MountOption g_mount_option;

inline void initSudo() {
  if (sudo_cmd == " ") {
    if (getuid() == 0) {
      sudo_cmd = "";
    } else {
      sudo_cmd = "sudo ";
    }
  }
}

void getErrorInfo(string cmd, string dir_path, string log_file_name) {
  if (!cmd.empty()) {
    cmd += " >> ./" + log_file_name + " 2>&1;";
  }
  cmd += " echo $? >> ./" + log_file_name + " 2>&1;"
         + " df -h >> ./" + log_file_name + " 2>&1;"
         + " ls -l " + dir_path + " >> ./" + log_file_name + " 2>&1";
  System(cmd);
}

inline string compressCmd(const string& dir_path, const string& file_path, int nthreads) {
  string cmd = "nice -n 5 mksquashfs " + dir_path + " " + file_path;
  if (g_mk_squashfs_option.has_mem_option) {
    cmd += " -mem 16M";
  }
  switch (g_compression.compression_type) {
    case kwdbts::CompressionType::GZIP:
      cmd += " -comp gzip";
      if (g_compression.has_compression_level_option) {
        switch (g_compression.compression_level) {
          case kwdbts::CompressionLevel::LOW:
            cmd += " -Xcompression-level 1";
            break;
          case kwdbts::CompressionLevel::MIDDLE:
            cmd += " -Xcompression-level 6";
            break;
          case kwdbts::CompressionLevel::HIGH:
            cmd += " -Xcompression-level 9";
            break;
        }
      }
      break;
    case kwdbts::CompressionType::LZ4:
      cmd += " -comp lz4";
      if (g_compression.has_lz4_hc_option) {
        switch (g_compression.compression_level) {
          case kwdbts::CompressionLevel::HIGH:
            cmd += " -Xhc";
            break;
          default:
            break;
        }
      }
      break;
    case kwdbts::CompressionType::LZMA:
      cmd += " -comp lzma";
      break;
    case kwdbts::CompressionType::LZO:
      cmd += " -comp lzo";
      if (g_compression.has_compression_level_option) {
        switch (g_compression.compression_level) {
          case kwdbts::CompressionLevel::LOW:
            cmd += " -Xcompression-level 1";
            break;
          case kwdbts::CompressionLevel::MIDDLE:
            cmd += " -Xcompression-level 7";
            break;
          case kwdbts::CompressionLevel::HIGH:
            cmd += " -Xcompression-level 9";
            break;
        }
      }
      break;
    case kwdbts::CompressionType::XZ:
      cmd += " -comp xz";
      break;
    case kwdbts::CompressionType::ZSTD:
      cmd += " -comp zstd";
      if (g_compression.has_compression_level_option) {
        switch (g_compression.compression_level) {
          case kwdbts::CompressionLevel::LOW:
            cmd += " -Xcompression-level 1";
            break;
          case kwdbts::CompressionLevel::MIDDLE:
            cmd += " -Xcompression-level 15";
            break;
          case kwdbts::CompressionLevel::HIGH:
            cmd += " -Xcompression-level 22";
            break;
        }
      }
      break;
  }
  if (g_mk_squashfs_option.has_processors_option) {
    cmd += " -processors " + std::to_string(nthreads);
  }
  cmd += " > /dev/null 2>&1";
  return cmd;
}

bool compress(const string& db_path, const string& tbl_sub_path, const string& dir_name, int nthreads, ErrorInfo& err_info) {
  return compressToPath(db_path, tbl_sub_path, dir_name, db_path + tbl_sub_path, nthreads, err_info);
}

bool compressToPath(const string& db_path, const string& tbl_sub_path, const string& dir_name,
                    const string& desc_path, int nthreads, ErrorInfo& err_info) {
  string dir_path = desc_path + dir_name;
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    // If the directory is a mounted directory, return success directly
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    }
    // The generated sqfs file will not take effect for now, by adding the suffix _tmp
    string file_path_tmp = desc_path + dir_name + ".sqfs_tmp";
    string file_path_desc = desc_path + dir_name + ".sqfs";
    // Delete previously failed sqfs files
    if (IsExists(file_path_tmp)) {
      Remove(file_path_tmp);
    }
    string cmd = compressCmd(dir_path, file_path_tmp, nthreads);
    if (System(cmd)) {
      LOG_DEBUG("Compress succeeded, shell: %s", cmd.c_str());
      cmd = "mv " + file_path_tmp + " " + file_path_desc;
      if (!System(cmd)) {
        err_info.errcode = KWEOTHER;
        err_info.errmsg = "mv .sqfs_tmp .sqfs failed";
        Remove(file_path_tmp);
        return false;
      }
    } else {
      LOG_DEBUG("Compress failed, shell: %s", cmd.c_str());
      err_info.errcode = KWEOTHER;
      err_info.errmsg = "mksquashfs failed";
      return false;
    }
  } else {
    err_info.errcode = KWENOOBJ;
    err_info.errmsg = db_path + " is not directory";
    return false;
  }
  return true;
}

bool mount(const string& sqfs_file_path, const string& dir_path, ErrorInfo& err_info) {
  initSudo();
  struct stat buffer;
  if (stat(sqfs_file_path.c_str(), &buffer) != 0) {
    err_info.errcode = KWENOOBJ;
    err_info.errmsg = sqfs_file_path + " does not exist";
    return false;
  }
  // check mount directory exists.
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    } else {
      if (!RemoveDirContents(dir_path, err_info)) {
        return false;
      }
    }
  }
  // create mount directory
  if (!MakeDirectory(dir_path, err_info)) {
    return false;
  }
  // mount sqfs file
  int retry = 10;
  std::string cmd = sudo_cmd + "mount -o noatime,nodiratime -t squashfs " + sqfs_file_path + " " + dir_path;
  while(retry > 0 && !System(cmd)) {
    if (isMounted(dir_path)) {
      break;
    }
    sleep(1);
    --retry;
    RemoveDirContents(dir_path, err_info);
  }
  if (!isMounted(dir_path)) {
    getErrorInfo(cmd, dir_path, "_log_mount_err");
    err_info.errcode = KWEOTHER;
    err_info.errmsg = sqfs_file_path + " mount failed";
    if (!Remove(dir_path)) {
      err_info.errcode = KWEEXIST;
      err_info.errmsg = sqfs_file_path + " mount failed, " + dir_path + " rm failed";
    }
    LOG_ERROR("mount failed. If it's a docker environment, check if /dev:/dev is mapped")
    return false;
  }

  g_cur_mount_cnt_++;
  return true;
}

bool umount(const string& sqfs_file_path, const string& dir_name, ErrorInfo& err_info) {
  initSudo();
  string dir_path = sqfs_file_path + dir_name;
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      int retry = 5;
      string cmd  = sudo_cmd + "umount " + dir_path;
      while(retry > 0 && !System(cmd)) {
        sleep(1);
        --retry;
      }
      if (isMounted(dir_path)) {
        getErrorInfo(cmd, dir_path, "_log_umount_err");
        err_info.errcode = KWEOTHER;
        err_info.errmsg = dir_path + " umount failed";
        return false;
      } else {
        g_cur_mount_cnt_--;
      }
    }
  }
  return true;
}

bool isMounted(const string& dir_path) {
  struct stat st;
  if ((stat(dir_path.c_str(), &st) == 0) && (S_ISDIR(st.st_mode))) {
    struct statfs sfs;
    int ret = statfs(dir_path.c_str(), &sfs);
    if (!ret && sfs.f_type == SQUASHFS_MAGIC) {
      return true;
    }
  }
  return false;
}

int executeShell(const std::string& cmd, std::string &result) {
  result = {};
  FILE *ptr = nullptr;
  int ret = -1;
  if ((ptr = popen(cmd.c_str(), "r")) != nullptr) {
    char buf_temp[1024] = {0};
    while(fgets(buf_temp, sizeof(buf_temp), ptr) != nullptr) {
      result += buf_temp;
    }
    pclose(ptr);
    ptr = nullptr;
    ret = 0;
  } else {
    ret = -1;
  }
  return ret;
}

void InitCompressInfo() {
  if (g_compress_info_initialized) {
    return;
  }
  MUTEX_LOCK(&g_compress_info_mutex);
  if (g_compress_info_initialized) {
    MUTEX_UNLOCK(&g_compress_info_mutex);
    return;
  }
  // Check whether the mksquashfs supports the `-mem` command
  string result;
  string cmd = "strings `which mksquashfs` | grep 'mem <size>' | wc -l";
  if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
    g_mk_squashfs_option.has_mem_option = true;
  }
  // Check whether the mksquashfs supports the `-processors` command
  cmd = "strings `which mksquashfs` | grep 'processors <number>' | wc -l";
  if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
    g_mk_squashfs_option.has_processors_option = true;
  }
  // Check mksquashfs support for compression algorithms
  for (CompressionType type = CompressionType::GZIP; type <= CompressionType::ZSTD; type = (CompressionType)(type + 1)) {
    cmd = "strings `which mksquashfs` | grep " + CompressionTypeToLowerCase(type) + " | wc -l";
    if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
      Compression compression{type, CompressionLevel::MIDDLE, false, false};
      if (type == CompressionType::GZIP || type == CompressionType::ZSTD || type == CompressionType::LZO) {
        cmd = "strings `which mksquashfs` | grep " + CompressionTypeToLowerCase(type) +
              " | grep Xcompression-level | wc -l";
        if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
          compression.has_compression_level_option = true;
        }
      } else if (type == CompressionType::LZ4) {
        cmd = "strings `which mksquashfs` | grep Xhc | wc -l";
        if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
          compression.has_lz4_hc_option = true;
        }
      }
      g_mk_squashfs_option.compressions[type] = compression;
    }
  }
  // Check mount's support for compression algorithms
  for (CompressionType type = CompressionType::GZIP; type <= CompressionType::ZSTD; type = (CompressionType)(type + 1)) {
    if (type == CompressionType::GZIP) {
      cmd = "grep SQUASHFS_ZLIB=y /boot/config-$(uname -r) | wc -l";
    } else {
      cmd = "grep SQUASHFS_" + CompressionTypeToUpperCase(type) + "=y /boot/config-$(uname -r) | wc -l";
    }
    if (executeShell(cmd, result) != -1 && atoi(result.c_str()) > 0) {
      g_mount_option.mount_compression_types.insert(type);
    }
  }

  g_compression = g_mk_squashfs_option.compressions[GZIP];

  g_compress_info_initialized = true;
  MUTEX_UNLOCK(&g_compress_info_mutex);
}

} // namespace kwdbts
