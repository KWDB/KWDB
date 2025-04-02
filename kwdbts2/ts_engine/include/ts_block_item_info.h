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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"

namespace kwdbts {

class TsBlockItemInfo {
 public:
  virtual TSEntityID GetEntityId() = 0;
  virtual TSTableID GetTableId() = 0;
  virtual uint32_t GetTableVersion() = 0;
  virtual void GetTSRange(timestamp64* min_ts, timestamp64* max_ts) = 0;
  virtual size_t GetRowNum() = 0;
  virtual KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) = 0;
  // if just get timestamp , this function return fast. 
  virtual timestamp64 GetTS(int row_num) = 0;
};


}  // namespace kwdbts
