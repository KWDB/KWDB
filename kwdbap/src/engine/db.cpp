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

#include <libkwdbap.h>

#include <regex>
#include <limits>
#include <thread>

#include "duckdb.hpp"
#include "duckdb_exec.h"

// TSStatus APOpen(APEngine** engine, TSSlice dir, APOptions options) {
//   *engine = new APEngine();
//   (*engine)->db = new duckdb::DuckDB({dir.data, dir.len});
//   return TSStatus{nullptr, 0};
// }

// TSStatus APClose(APEngine* engine) {
//   if(engine){
//     if(engine->db){
//       delete (duckdb::DuckDB *)(engine->db);
//     }
//     delete engine;
//   }
//   return TSStatus{nullptr, 0};
// }


TSStatus APOpen(APEngine** engine) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  APEngine* apEngine;
  s = APEngineImpl::OpenEngine(ctx, &apEngine);
  if (s == KStatus::FAIL) {
    return ToTsStatus("OpenEngine Internal Error!");
  }
  *engine = apEngine;
  return kTsSuccess;
}

TSStatus APExecQuery(APEngine* engine, APQueryInfo* req, APRespInfo* resp) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Execute(ctx_p, req, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Execute Error!");
  }

  LOG_ERROR("MQ_TYPE_DML_PG_RESULT = %d, code = %d", resp->ret, resp->code);
  return kTsSuccess;
}