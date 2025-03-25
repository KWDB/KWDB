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
#include "ts_io.h"

namespace kwdbts {
static constexpr uint64_t TS_BLOCK_SEGMENT_BLOCK_FILE_MAGIC = 0xcb2ffe9321847274;

class TsBlockSegmentBlockFile {
 private:
  string file_path_;
  std::unique_ptr<TsFile> file_{nullptr};
  std::unique_ptr<KRWLatch> file_mtx_;

  struct TsBlockFileHeader {
    uint64_t magic;
    int32_t encoding;
    int32_t status;
  };

  TsBlockFileHeader header_;

 public:
  explicit TsBlockSegmentBlockFile(const string& file_path);

  TsBlockSegmentBlockFile();

  KStatus Open();
  KStatus AppendBlock(const TSSlice& block, uint64_t* offset);
  KStatus ReadBlock(uint64_t offset, char* buff, size_t len);
};

}  // namespace kwdbts
