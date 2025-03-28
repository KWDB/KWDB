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

#include <endian.h>
#include <cassert>
#include <cstdint>
#include <cstring>
#include "libkwdbts2.h"
#include "rocksdb/slice.h"
#include "ts_coding.h"

namespace kwdbts {
struct TsInternalKey {
  TSTableID table_id;
  uint32_t version;
  TSEntityID entity_id;
  static constexpr uint32_t size = sizeof(TSTableID) + sizeof(uint32_t) + sizeof(TSEntityID);
  void Encode(rocksdb::Slice *s, char *buf) const {
    s->data_ = buf;
    s->size_ = size;
    buf = EncodeFixed64(buf, htobe64(table_id));
    buf = EncodeFixed32(buf, htobe32(version));
    buf = EncodeFixed64(buf, htobe64(entity_id));
    assert(buf - s->data_ == s->size_);
  }

  void Decode(const rocksdb::Slice &data) {
    assert(data.size() >= 20);
    rocksdb::Slice tmp = data;
    table_id = be64toh(DecodeFixed64(tmp.data()));
    tmp.remove_prefix(sizeof(table_id));
    version = be32toh(DecodeFixed32(tmp.data()));
    tmp.remove_prefix(sizeof(version));
    entity_id = be64toh(DecodeFixed64(tmp.data()));
    tmp.remove_prefix(sizeof(entity_id));
  }
};
};  // namespace kwdbts
