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

typedef struct DConList {
  DConList *prev;
  DConList *next;
  duckdb::Connection conn;
  std::string user;
  std::string dbName;
  k_uint64 sessionID;
  int status;  // 0: in use,  1: idle
}DConList;

typedef struct DConListHead {
  DConList *next;
  std::mutex list_mux;
}DConListHead;

/**
 * @brief Duckdb connection cache under APEngineImpl
 */
class DConnCache {
 public:
  explicit DConnCache();

  duckdb::Connection* GetOrAddConn(k_uint64 sessionID, std::string dbName, std::string userName);
  

 private:
  std::map<k_uint64, DConListHead *> cacheMap; 
  std::mutex map_mux;

}; 

} // namespace