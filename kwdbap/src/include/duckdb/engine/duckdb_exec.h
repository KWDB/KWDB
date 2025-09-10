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

#include <memory>
#include <string>

#include "cm_assert.h"
#include "duckdb.h"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/engine/ap_processors.h"
#include "ee_comm_def.h"
#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"

using namespace duckdb;
using duckdb::DatabaseWrapper;

uint64_t AppenderColumnCount(APAppender out_appender);

struct APEngine {
  virtual ~APEngine() {}

  /**
   * @brief  calculate pushdown
   * @param[in] req
   * @param[out]  resp
   *
   * @return KStatus
   */
  virtual kwdbts::KStatus Execute(kwdbts::kwdbContext_p ctx, APQueryInfo *req,
                                  APRespInfo *resp) = 0;

  virtual kwdbts::KStatus Query(const char *stmt, APRespInfo *resp) = 0;

  virtual kwdbts::KStatus CreateAppender(const char *catalog,
                                         const char *schema, const char *table,
                                         APAppender *out_appender) = 0;

  virtual kwdbts::KStatus DatabaseOperate(const char *name,
                                          EnDBOperateType type) = 0;

  virtual kwdbts::KStatus DropDatabase(const char *current,
                                       const char *name) = 0;
};

namespace kwdbts {
/**
 * @brief APEngineImpl
 */
class APEngineImpl : public APEngine {
 public:
  explicit APEngineImpl(kwdbContext_p ctx, const char *db_path);

  static KStatus OpenEngine(kwdbContext_p ctx, APEngine **engine,
                            APConnectionPtr *out, duckdb_database *out_db,
                            const char *path);

  KStatus DatabaseOperate(const char *name, EnDBOperateType type) override;

  KStatus Execute(kwdbContext_p ctx, APQueryInfo *req,
                  APRespInfo *resp) override;

  KStatus Query(const char *stmt, APRespInfo *resp) override;

  KStatus CreateAppender(const char *catalog, const char *schema,
                         const char *table, APAppender *out_appender) override;

  KStatus DropDatabase(const char *current, const char *name) override;

 private:
  std::shared_ptr<duckdb::Connection> conn_;
  duckdb::shared_ptr<duckdb::DatabaseInstance> instance_;
  std::string db_path_;
  std::mutex context_lock_;
};

class KWThdContext;

// 执行结果结构
struct ExecutionResult {
  bool success;
  std::string error_message;
  std::vector<std::string> column_names;
  std::vector<duckdb::LogicalType> column_types;
  std::vector<duckdb::DataChunk> data_chunks;
  void *value;
  uint32_t len;
  idx_t row_count;

  ExecutionResult() : success(false), row_count(0) {}
};

class DuckdbExec {
 public:
  DuckdbExec(std::string db_path);
  ~DuckdbExec();

  void Init(void *instance, void *connect);

  // dml exec query func
  static KStatus ExecQuery(kwdbContext_p ctx, APQueryInfo *req,
                           APRespInfo *resp);

  static KStatus ExecSQL(void *handle, const char *sql);

  KStatus Setup(kwdbContext_p ctx, k_char *message, k_uint32 len, k_int32 id,
                k_int32 uniqueID, APRespInfo *resp);
  KStatus Next(kwdbContext_p ctx, k_int32 id, TsNextRetState nextState,
               APRespInfo *resp);

  void Clear(kwdbContext_p ctx);

  ExecutionResult ExecuteCustomPlan(kwdbContext_p ctx,
                                    const std::string &table_name);

  unique_ptr<PhysicalPlan> ConvertFlowToPhysicalPlan(int *start_idx);

  ExecutionResult PrepareExecutePlan(kwdbContext_p ctx);

  KStatus AttachDBs();

  KStatus DetachDB(vector<std::string> dbs);

 private:

  std::string db_path_;
  DatabaseInstance *instance_;
  Connection *connect_;
  std::string sql_;
  std::mutex context_lock_;

  // physical plan
  vector <std::string> *res_names_;
  unique_ptr<kwdbap::Processors> processors_;

  // result
  bool setup_ = false;
  ExecutionResult res_;
};

}  // namespace kwdbts
