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
#include <string>

#include "duckdb.hpp"
#include "duckdb/engine/duckdb_exec.h"
#include "ee_comm_def.h"

using namespace kwdbts;
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

TSStatus APOpen(APEngine** engine, APConnectionPtr *out, duckdb_database *out_db, APString* path) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }

  APEngine* apEngine;
  char tmp[256];
  memset(tmp, 0, 256);
  memcpy(tmp, path->value, path->len);
  s = APEngineImpl::OpenEngine(ctx, &apEngine, out, out_db, tmp);
  if (s == KStatus::FAIL) {
    return ToTsStatus("OpenEngine Internal Error!");
  }
  *engine = apEngine;
  return kTsSuccess;
}

TSStatus APClose(APEngine* engine) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  KStatus s = InitServerKWDBContext(ctx);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
//  LOG_INFO("TSClose")
//  engine->CreateCheckpoint(ctx);
  delete engine;
  return TSStatus{nullptr, 0};
}

TSStatus APExecQuery(APEngine* engine, APQueryInfo* req, APRespInfo* resp) {
  kwdbContext_t context;
  kwdbContext_p ctx_p = &context;
  KStatus s = InitServerKWDBContext(ctx_p, req->sessionID);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("InitServerKWDBContext Error!");
  }
  s = engine->Execute(ctx_p, req, resp);
  if (s != KStatus::SUCCESS) {
    return ToTsStatus("Execute Error!");
  }

  // LOG_ERROR("MQ_TYPE_DML_PG_RESULT = %d, code = %d", resp->ret, resp->code);
  return kTsSuccess;
}

TSStatus APExecSQL(APEngine* engine, APString* sql, APRespInfo* resp) {
  char *tmp = new char[sql->len+1];
  memset(tmp, 0, sql->len+1);
  memcpy(tmp, sql->value, sql->len);
  KStatus s = engine->Query(tmp, resp);
  if (s != KStatus::SUCCESS) {
    delete []tmp;
    return ToTsStatus("APExecQuery Error!");
  }
  delete []tmp;
  return kTsSuccess;
}

TSStatus APDatabaseOperate(APEngine* engine, APString* name, EnDBOperateType type) {
  if (engine == nullptr) {
    return  ToTsStatus("CreateDatabase Internal Error!");
  }
  char tmp[64];
  memset(tmp, 0, 64);
  memcpy(tmp, name->value, name->len);
  KStatus s = engine->DatabaseOperate(tmp, type);
  if (s == KStatus::FAIL) {
    return ToTsStatus("CreateDatabase Internal Error!");
  }
  return kTsSuccess;
}

TSStatus APDropDatabase(APEngine* engine, APString* current_db_name, APString* drop_db_name) {
  char tmp[64];
  memset(tmp, 0, 64);
  memcpy(tmp, current_db_name->value, current_db_name->len);
  
  char tmp1[64];
  memset(tmp1, 0, 64);
  memcpy(tmp1, drop_db_name->value, drop_db_name->len);
  KStatus s = engine->DropDatabase(tmp1, tmp1);
  if (s == KStatus::FAIL) {
    return ToTsStatus("CreateDatabase Internal Error!");
  }
  return kTsSuccess;
}

TSStatus APCreateAppender(APEngine* engine, APString *catalog, APString *schema, APString *table,
                          APAppender *out_appender) {
  char tmp[64];
  memset(tmp, 0, 64);
  memcpy(tmp, catalog->value, catalog->len);
  char tmp1[64];
  memset(tmp1, 0, 64);
  memcpy(tmp1, schema->value, schema->len);
  char tmp2[64];
  memset(tmp2, 0, 64);
  memcpy(tmp2, table->value, table->len);
  KStatus s = engine->CreateAppender(tmp, tmp1, tmp2, out_appender);
  if (s == KStatus::FAIL) {
    return ToTsStatus("CreateAppender Internal Error!");
  }
  return kTsSuccess;
}

uint64_t APAppenderColumnCount(APAppender out_appender) {
  return AppenderColumnCount(out_appender);
}

void TSFree(void* ptr) {
  free(ptr);
}