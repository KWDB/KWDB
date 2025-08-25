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

#include "duckdb.h"
#include "cm_assert.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ee_pb_plan.pb.h"
#include "ee_global.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

using namespace duckdb;
using duckdb::DatabaseWrapper;

namespace kwdbts {

  struct EmptyStruct {};

  class KWThdContext;
	bool checkDuckdbParam(void *db, void *connect);

	// 执行结果结构
	struct ExecutionResult {
		bool success;
		std::string error_message;
		std::vector<std::string> column_names;
		std::vector<duckdb::LogicalType> column_types;
		std::vector<duckdb::DataChunk> data_chunks;
    void* value;
    uint32_t len;
		idx_t row_count;

		ExecutionResult() : success(false), row_count(0) {}
	};

  class DuckdbExec {
  public:
    DuckdbExec(void *db, void *connect, std::string db_path);
    ~DuckdbExec();

    void Init(){}

    // dml exec query func
    static KStatus ExecQuery(kwdbContext_p ctx, APQueryInfo *req, APRespInfo *resp);

    KStatus Setup(kwdbContext_p ctx, k_char *message, k_uint32 len, k_int32 id, k_int32 uniqueID, APRespInfo *resp);
    KStatus Next(kwdbContext_p ctx, k_int32 id, TsNextRetState nextState, APRespInfo *resp);

    void Clear(kwdbContext_p ctx);

    ExecutionResult ExecuteCustomPlan(kwdbContext_p ctx, const string &table_name);

    duckdb::shared_ptr<PreparedStatementData>
    ConvertFlowToPhysicalPlan();

    ExecutionResult PrepareExecutePlan(kwdbContext_p ctx);

    KStatus AttachDBs();

    KStatus AttachDB(std::string &db_name, std::map<std::string, EmptyStruct> &db_map);

    KStatus DetachDB(duckdb::vector<std::string> dbs);

  private:
    void ReInit(const string &db_name);

    FlowSpec *fspecs_;
    std::string db_path_;
    DatabaseWrapper * db_;
    duckdb::shared_ptr<DuckDB> init_db_;
    Connection *connect_;
    std::string sql_;
    mutex context_lock_;
    bool setup_=false;
    ExecutionResult res_;
  };
}
