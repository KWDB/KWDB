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

#include "sys_utils.h"

#include <unistd.h>
#include <sys/stat.h>
#if defined(__GNUC__) && (__GNUC__ < 8)
  #include <experimental/filesystem>
  namespace fs = std::experimental::filesystem;
#else
  #include <filesystem>
  namespace fs = std::filesystem;
#endif

int64_t g_free_space_alert_threshold = 0;

bool IsExists(const string& path) {
  return fs::exists(path);
}

bool Remove(const string& path, ErrorInfo& error_info) {
  try {
    if (!fs::remove_all(path.c_str()) && errno != 0) {
      error_info.errcode = errnumToErrorCode(errno);
      error_info.errmsg = strerror(errno);
      LOG_ERROR("%s remove failed: errno[%d], strerror[%s]", path.c_str(), errno, error_info.errmsg.c_str());
      return false;
    }
  } catch (const std::exception& e) {
    LOG_ERROR("%s remove failed: errno message[%s]", path.c_str(), e.what());
    return false;
  }
//  LOG_INFO("Remove path [%s] succeeded", path.c_str());
  return true;
}

bool RemoveDirContents(const string& dir_path, ErrorInfo& error_info) {
  if (!fs::exists(dir_path) || !fs::is_directory(dir_path)) {
    LOG_WARN("RemoveDirContents[%s] failed: dir not exists", dir_path.c_str());
    return false;
  }
  try {
    for (auto& path : fs::directory_iterator(dir_path)) {
      if (!fs::remove_all(path) && errno != 0) {
        error_info.errcode = errnumToErrorCode(errno);
        error_info.errmsg = strerror(errno);
        LOG_ERROR("%s remove failed: errno[%d], strerror[%s]", path.path().c_str(), errno, error_info.errmsg.c_str());
        return false;
      }
    }
  } catch (const std::exception& e) {
    LOG_ERROR("RemoveDirContents[%s] failed: errno message[%s]", dir_path.c_str(), e.what());
    return false;
  }
  LOG_INFO("RemoveDirContents[%s] succeeded", dir_path.c_str());
  return true;
}

bool MakeDirectory(const string& dir_path, ErrorInfo& error_info) {
  if (fs::exists(dir_path) && fs::is_directory(dir_path)) {
    return true;
  }
  struct stat st;
  size_t e_pos = 1;
  char *path = const_cast<char *>(dir_path.data());
  while (e_pos < dir_path.size()) {
    e_pos = dir_path.find_first_of('/', e_pos);
    if (e_pos != string::npos)
      path[e_pos] = 0;
    if (stat(path, &st) != 0) {
      if (mkdir(path, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) < 0) {
        error_info.errcode = errnumToErrorCode(errno);
        error_info.errmsg = strerror(errno);
        LOG_ERROR("mkdir [%s] failed: errno[%d], strerror[%s]", path, errno, error_info.errmsg.c_str());
        return false;
      }
    } else {
      if (!S_ISDIR(st.st_mode)) {
        error_info.errcode = KWEOTHER;
        error_info.errmsg = std::string(path) + " is not directory";
        LOG_ERROR("mkdir [%s] failed: %s", path, error_info.errmsg.c_str());
        return false;
      }
    }
    if (e_pos != string::npos)
      path[e_pos] = '/';
    else
      break;
    e_pos++;
  }
  LOG_INFO("Make directory [%s] succeeded", dir_path.c_str());
  return true;
}

std::time_t ModifyTime(const std::string& filePath) {
  struct stat fileInfo;
  if (stat(filePath.c_str(), &fileInfo) != 0) {
    return 0;
  }
  return fileInfo.st_mtime;
}

bool System(const string& cmd, bool print_log, ErrorInfo& error_info) {
  int status = system(cmd.c_str());
  if (WIFEXITED(status)) {
    auto exit_code = WEXITSTATUS(status);
    if (exit_code == 0) {
      if (print_log) {
        LOG_INFO("system() success, cmd: [%s]", cmd.c_str());
      }
      return true;
    }
    if (exit_code == 1) {
      LOG_WARN("system() success, cmd[%s], status[%d], exit_code[%d], errno[%d], strerror[%s]",
               cmd.c_str(), status, exit_code, errno, strerror(errno));
      return true;
    }
    LOG_ERROR("system() failed: cmd[%s], status[%d], exit_code[%d], errno[%d], strerror[%s]",
              cmd.c_str(), status, exit_code, errno, strerror(errno));
    return false;
  }

  if (status == -1) {
    cerr << "OS system fork error." << std::endl;
    return false;
  }
  char msg[1024];
  snprintf(msg, sizeof(msg), "exec-shell [%s] faild. errno[%d], shell exit code[%d,%d(%d),%d], cmd exit code[%d].",
                cmd.c_str(), errno, WIFEXITED(status), WIFSIGNALED(status), WTERMSIG(status),
                WIFSTOPPED(status), WEXITSTATUS(status));
  cerr << msg << std::endl;
  error_info.errcode = errnumToErrorCode(errno);
  error_info.errmsg = strerror(errno);
  LOG_ERROR("system() failed: cmd[%s], errno[%d], strerror[%s]", cmd.c_str(), errno, error_info.errmsg.c_str());
  return false;
}

bool CopyDirectory(std::vector<string>& src_path, const string& dst_path, ErrorInfo& error_info) {
  for (size_t i = 0; i < src_path.size(); i++) {
    std::string mv_cmd = "cp ";
    if (src_path[i].back() == '/') {
      src_path[i].pop_back();
      mv_cmd += " -r ";
    }
    mv_cmd = mv_cmd + src_path[i] + " " + dst_path;
    if (!System(mv_cmd, true, error_info)) {
      return false;
    }
  }
  return true;
}

bool ChangeDirLink(string link_path, string new_path, ErrorInfo& error_info) {
  if (link_path.back() == '/') {
    link_path = link_path.substr(0, link_path.length() - 1);
  }
  if (new_path.back() == '/') {
    new_path = new_path.substr(0, new_path.length() - 1);
  }
  std::string link_rm_cmd = "mv " + link_path + " " + link_path + "_tmp";
  if (System(link_rm_cmd, true, error_info)) {
    std::string link_cmd = "ln -s " + new_path + " " + link_path;
    if (System(link_cmd, true, error_info)) {
      std::string link_rm_cmd = "rm " + link_path + "_tmp";
      System(link_rm_cmd, true, error_info);
      return true;
    } else {
      System("mv " + link_path + "_tmp " + link_path);
    }
  }
  return false;
}

std::string ParseLinkDirToReal(string link_path, ErrorInfo& error_info) {
  if (link_path.back() == '/') {
    link_path = link_path.substr(0, link_path.length() - 1);
  }
  struct stat st;
  std::string real_path = "";
  if (lstat(link_path.c_str(), &st) != 0) {
    error_info.errcode = KWENOOBJ;
    error_info.errmsg = std::string(link_path) + " cannot stat.";
    LOG_ERROR("cannot stat directory path [%s].", link_path.c_str());
    return real_path;
  }
  if (!S_ISLNK(st.st_mode)) {
    return link_path;
  }
  const int PATH_MAX_LENGTH = 4096;
  char rpath[PATH_MAX_LENGTH];
  memset(rpath, 0, PATH_MAX_LENGTH);
  int rslt = readlink(link_path.c_str(), rpath, PATH_MAX_LENGTH);
  if (rslt < 0 || rslt >= PATH_MAX_LENGTH) {
    LOG_ERROR("read link failed.[%s] errno[%d]", link_path.c_str(), errno);
    error_info.errcode = KWEINVALPATH;
    error_info.errmsg = std::string(link_path) + " read link failed.";
    return real_path;
  }
  return std::string(rpath);
}

bool DirExists(const std::string& path) {
  return fs::exists(path) && fs::is_directory(path);
}

int64_t GetDiskFreeSpace(const std::string &path) {
  std::error_code ec;
  auto space_info = fs::space(path, ec);
  if (ec) {
    // error happens, just quits here
    return -1;
  }
  // the available size is space_info.available
  return static_cast<int64_t>(space_info.available);
}

bool IsDiskSpaceEnough(const std::string& path) {
  if (g_free_space_alert_threshold == 0) {
    return true;
  }
  auto free_space = GetDiskFreeSpace(path);
  return free_space > g_free_space_alert_threshold;
}
