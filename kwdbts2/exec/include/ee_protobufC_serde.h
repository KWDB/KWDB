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
#include <set>
#include <memory>
#include <map>
#include <vector>
#include <queue>
#include <limits>
#include <string>
#include <ctime>

#include "ee_base_op.h"
#include "kwdb_type.h"
#include "ee_pb_plan.pb.h"
#include "br_internal_service.pb.h"

namespace kwdbts {

inline uint32_t decode_fixed32(const uint8_t* buf);
inline uint64_t decode_fixed64(const uint8_t* buf);

class ProtobufChunkSerrialde {
 public:
  ProtobufChunkSerrialde() {}
  ~ProtobufChunkSerrialde() {}
  ChunkPB SerializeChunk(DataChunk* src, k_bool* is_first_chunk);
  bool Deserialize(DataChunkPtr& chunk, std::string_view buff, bool is_encoding, ColumnInfo* col_info, k_int32 num);
  void DeserializeColumn(DataChunkPtr& chunk, std::string_view buff, ColumnInfo* col_info, k_int32 num);
  ChunkPB SerializeColumn(DataChunk* src, k_bool* is_first_chunk);
  ChunkPB SerializeColumnData(DataChunk* src);

 private:
  ChunkPB Serialize(DataChunk* src);
};

}  // namespace kwdbts
