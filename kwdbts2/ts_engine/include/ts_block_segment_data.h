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

#include <map>
#include <memory>
#include <string>
#include <cstdio>
#include "ts_common.h"

namespace kwdbts {

class TsBlockFile {
 private:
  string file_path_;
  FILE* file_{nullptr};
  std::unique_ptr<KRWLatch> file_mtx_;
  uint64_t file_len_{0};

 public:
  explicit TsBlockFile(const string& file_path);

  ~TsBlockFile();

  KStatus Open();
  KStatus Append(const TSSlice& block, uint64_t* offset);
  KStatus ReadBlock(uint64_t offset, char* buff);
};

}  // namespace kwdbts
