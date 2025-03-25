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

#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
#include "libkwdbts2.h"
#include "ts_slice.h"
namespace kwdbts {
class EntityKey {
 private:
  std::string rep_;

  struct KeyFormat {
    TSTableID table_id;
    TSEntityID entity_id;
    uint32_t version;
  };
  const KeyFormat *AsKeyFormat() const {
    assert(rep_.size() == 8 + 8 + 4);
    return reinterpret_cast<const KeyFormat *>(rep_.data());
  }

 public:
  EntityKey() {}
  EntityKey(TSTableID table_id, TSEntityID entity_id, uint32_t version) {
    rep_.append(reinterpret_cast<char *>(&table_id), sizeof(table_id));
    rep_.append(reinterpret_cast<char *>(&entity_id), sizeof(entity_id));
    rep_.append(reinterpret_cast<char *>(&version), sizeof(version));
  }

  void Clear() { rep_.clear(); }
  std::string Encode() { return rep_; }

  TSTableID GetTableID() const { return AsKeyFormat()->table_id; }
  TSTableID GetEntityID() const { return AsKeyFormat()->entity_id; }
  TSTableID GetVersion() const { return AsKeyFormat()->version; }
};

class EntityKeyComparator {
 public:
  int Compare(const EntityKey &lhs, const EntityKey &rhs) const {
    if (lhs.GetTableID() < rhs.GetTableID()) {
      return -1;
    } else if (lhs.GetTableID() > rhs.GetTableID()) {
      return 1;
    }

    if (lhs.GetEntityID() < rhs.GetEntityID()) {
      return -1;
    } else if (lhs.GetEntityID() > rhs.GetEntityID()) {
      return 1;
    }

    if (lhs.GetVersion() > lhs.GetVersion()) {
      return -1;
    } else if (lhs.GetVersion() < lhs.GetVersion()) {
      return 1;
    }
    return 0;
  }
};

struct StdEntityKeyComparator {
  bool operator()(const EntityKey &lhs, const EntityKey &rhs) const {
    EntityKeyComparator cmp;
    return cmp.Compare(lhs, rhs) < 0;
  }
};

class TimeStampKey {
 private:
  std::string rep_;

 public:
  explicit TimeStampKey(TSSlice s) { rep_.assign(s.data, s.len); }
  uint64_t GetTimeStamp() const {
    assert(rep_.size() >= 8);
    return *reinterpret_cast<const uint64_t *>(rep_.data());
  }

  const char *GetAddress() const { return rep_.data(); }
};

class TimeStampKeyComparator {
 public:
  int Compare(const TimeStampKey &lhs, const TimeStampKey &rhs) const {
    if (lhs.GetTimeStamp() < rhs.GetTimeStamp()) {
      return -1;
    } else if (lhs.GetTimeStamp() > rhs.GetTimeStamp()) {
      return 1;
    }
    return 0;
  }
};

struct StdTimeStampKeyComparator {
  bool operator()(const TimeStampKey &lhs, const TimeStampKey &rhs) const {
    TimeStampKeyComparator cmp;
    return cmp.Compare(lhs, rhs) < 0;
  }
};

}  // namespace kwdbts
