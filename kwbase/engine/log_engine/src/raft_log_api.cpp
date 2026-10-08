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

#include <libkwdbts2.h>
#include <sstream>

#include "engine.h"
#include "raft_store.h"
#include "sys_utils.h"

TSStatus TSRaftOpen(RaftStore** engine, TSSlice dir) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  std::string ts_store_path(dir.data, dir.len);
  if (fs::exists(ts_store_path + "/tsdb")) {
    if (!fs::exists(ts_store_path + "/tsdb/raftlog")) {
      return ToTsStatus("Init raftlog store Error! Raftlog store should not be used.");
    }
  }
  auto* raftStore = new RaftStore();
  s = raftStore->init(ts_store_path + "/tsdb/raftlog");
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("open raftlog store Error!");
  }
  *engine = raftStore;
  return kTsSuccess;
}

TSStatus TSWriteRaftLog(RaftStore *engine, int cnt, TSRaftlog *raftlog, bool sync) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->WriteRaftLog(ctx_p, cnt, raftlog, sync);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Modify Raftlog Error!");
  }
  return kTsSuccess;
}

TSStatus TSGetRaftLog(RaftStore* engine, uint64_t range_id, uint64_t start, uint64_t end, TSSlice* value) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Get(ctx_p, range_id, start, end, value);
  if (s != KStatus::SUCCESS) {
    std::ostringstream range, index_start, index_end;
    range << range_id;
    index_start << start;
    index_end << end;
    return ToTsStatus("Get Raftlog Error! Range:" + range.str()
                      + ",start index:" + index_start.str() + "end index:" + index_end.str());
  }
  return kTsSuccess;
}

TSStatus TSGetFirstIndex(RaftStore* engine, uint64_t range_id, uint64_t *index_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetFirstIndex(ctx_p, range_id, index_id);
  if (s != KStatus::SUCCESS) {
    std::ostringstream range;
    range << range_id;
    return ToTsStatus("Get First Index Error! Range:" + range.str());
  }
  return kTsSuccess;
}

TSStatus TSGetLastIndex(RaftStore* engine, uint64_t range_id, uint64_t* index_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetLastIndex(ctx_p, range_id, index_id);
  if (s != KStatus::SUCCESS) {
    std::ostringstream range;
    range << range_id;
    return ToTsStatus("Get Last Index Error! Range:" + range.str());
  }
  return kTsSuccess;
}

TSStatus TSGetFirstRaftLog(RaftStore *engine, uint64_t range_id, TSSlice *value) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->GetFirst(ctx_p, range_id, value);
  if (s != KStatus::SUCCESS) {
    std::ostringstream range;
    range << range_id;
    return ToTsStatus("Get First Raftlog Error! Range:" + range.str());
  }
  return kTsSuccess;
}

TSStatus TSSyncRaftLog(RaftStore* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Sync(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Sync Raftlog Error!");
  }
  return kTsSuccess;
}

TSStatus TSHasRange(RaftStore* engine, uint64_t range_id) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->HasRange(ctx_p, range_id);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("has no range");
  }
  return kTsSuccess;
}
