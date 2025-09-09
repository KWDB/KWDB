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

#include "ts_schema_utils.h"
#include <map>
#include "me_metadata.pb.h"

using roachpb::DataType;
int kwdbts::GetDataTypeSize(roachpb::DataType datatype) {
  // TODO(zhangzirui):
  std::map<roachpb::DataType, int> m{{DataType::TIMESTAMP, 8},
                                     {DataType::INT, 4},
                                     {DataType::BIGINT, 4},
                                     {DataType::DOUBLE, 8},
                                     {DataType::VARCHAR, 4}};
  auto it = m.find(datatype);
  assert(it != m.end());
  return it->second;
}
