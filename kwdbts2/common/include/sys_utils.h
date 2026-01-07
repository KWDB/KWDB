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

#if defined(__has_include)
  #if __has_include(<filesystem>)
    #include <filesystem>
    namespace fs = std::filesystem;
  #elif __has_include(<experimental/filesystem>)
    #include <experimental/filesystem>
    namespace fs = std::experimental::filesystem;
  #else
    #error "No <filesystem> or <experimental/filesystem> available"
  #endif
#else
  #if defined(__GNUC__) && (__GNUC__ < 8)
    #include <experimental/filesystem>
    namespace fs = std::experimental::filesystem;
  #else
    #include <filesystem>
    namespace fs = std::filesystem;
  #endif
#endif

#include <string>
#include <vector>
#include "lg_api.h"
#include "ts_object_error.h"

extern int64_t g_free_space_alert_threshold;

/**
 * @brief Check whether the file or directory exists
 * @param path The path of file or directory
 * @return false/true
 */
bool IsExists(const fs::path& path);

/**
 * @brief Remove file or directory
 * @param path The path of file or directory
 * @return false/true
 */
bool Remove(const string& path, ErrorInfo& error_info = getDummyErrorInfo());

/**
 * @brief Remove directory contents
 * @param dir_path The path of directory
 * @param error_info
 * @return
 */
bool RemoveDirContents(const string& dir_path, ErrorInfo& error_info = getDummyErrorInfo());

/**
 * @brief Recursively create a directory (mkdir -p xxx)
 * @param dir_path The path of directory
 * @return true/false
 */
bool MakeDirectory(const fs::path& dir_path, ErrorInfo& error_info = getDummyErrorInfo());

/**
 * @brief Get the modify time of file
 * @param filePath The path of file
 * @return modify time
 */
std::time_t ModifyTime(const std::string& filePath);

/**
 * @brief call system()
 * @param cmd shell command
 * @return true/false
 */
bool System(const string& cmd,  bool print_log = true, ErrorInfo& error_info = getDummyErrorInfo());


/**
 * Change link directory for link_path
 * @param link_path source directory path
 * @param new_path dest directory path
 * @return true/false
 */
bool ChangeDirLink(string link_path, string new_path, ErrorInfo& error_info);

/**
 * Resolve a symbolic link to its real (or absolute) path
 * @param link_path
 * @param error_info
 * @return
 */
std::string ParseLinkDirToReal(string link_path, ErrorInfo& error_info);

bool DirExists(const std::string& path);

int64_t GetDiskFreeSpace(const std::string& path);

bool IsDiskSpaceEnough(const std::string& path);

bool isSoftLink(const std::string& path);

/**
 * @brief gcc 7 does not support std::filesystem::path().lexically_normal(), so we need to implement it ourselves
 * @param path
 * @return
 */
std::string lexically_normal(const std::string& path);

/**
 * @brief Copy file
 * @param src_path The path of source file
 * @param dst_path The path of destination file
 * @return false/true
 */
bool CopyFile(const string& src_path, const string& dst_path);
