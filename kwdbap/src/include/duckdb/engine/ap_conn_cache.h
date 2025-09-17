// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

// #include <memory>
// #include <string>

// #include "cm_assert.h"
// #include "duckdb.h"
// #include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include <mutex>
// #include "duckdb/main/prepared_statement_data.hpp"
// #include "duckdb/engine/ap_processors.h"
// #include "ee_comm_def.h"
// #include "ee_pb_plan.pb.h"
// #include "kwdb_type.h"

// using namespace std;
// using duckdb::DatabaseWrapper;

// TODO, using ts namespace for now
namespace kwdbts {

  enum CacheState {
    BUSY = 0,
    IDLE = 1
  };

typedef struct DConEntry {
  duckdb::Connection *conn;
  std::string user;
  std::string dbName;
  k_uint64 sessionID;
  int status;  // 0: in use,  1: idle
}DConEntry;

/**
 * @brief Duckdb connection cache under APEngineImpl
 */
class DConnCache {
 public:
  DConnCache();

  duckdb::Connection* GetOrAddConn(k_uint64 sessionID, std::string dbName, std::string userName);
  bool ReturnDConn(duckdb::Connection* conn);
  void SetDBWrapper(struct duckdb::DatabaseWrapper* wrapper); 
  void Init();
  static const int MAX_CACHE_ENTRY_NUM = 2048;
  
 private:
  DConEntry * lookForValidEntry(k_uint64 sessionID, std::string dbName, std::string userName);
  DConEntry * lookForEntryByAddr(duckdb::Connection* conn);
  bool createEntry(DConEntry ** ent, k_uint64 sessionID, std::string dbName, std::string userName);
  bool addEntryToList(DConEntry * ent);
  bool doReturn(DConEntry * ent);
  
  struct duckdb::DatabaseWrapper* copyOfEngineDBWrapper;
  DConEntry * dConCache[MAX_CACHE_ENTRY_NUM];
  int current_sz; 
  std::map<k_uint64, int> session2EntMap;  // session ID map to index in the core array
  std::mutex cap_mux;

}; 

} // namespace