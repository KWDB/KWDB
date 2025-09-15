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

// #include <libkwdbap.h>

// #include <regex>
// #include <limits>
// #include <thread>
// #include <string>

#include "duckdb.hpp"
#include "duckdb/engine/ap_conn_cache.h"
// #include "ee_comm_def.h"

namespace kwdbts {

DConnCache::DConnCache() {}

duckdb::Connection* GetOrAddConn(k_uint64 sessionID, std::string dbName, std::string userName)
{
  return NULL;
}

}
