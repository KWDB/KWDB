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

#include "duckdb_exec.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/extra_operator_info.hpp" // 包含 ExtraOperatorInfo 定义
#include "duckdb/execution/operator/scan/physical_table_scan.hpp" // 确保包含此类定义


#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/execution/executor.hpp"

#include "ee_global.h"
#include "lg_api.h"
#include "cm_func.h"

namespace kwdbts {
  bool checkDuckdbParam(void *db, void *connect) {
    auto database = static_cast<duckdb_database>(db);
    if (!database) {
      return false;
    }
    auto connection = static_cast<duckdb_connection>(connect);
    if (!connection) {
      return false;
    }

    return true;
  }

  unique_ptr<PhysicalTableScan> CreateTableScanOperator(
      ClientContext &context,
      TableCatalogEntry &table_entry,
      string table_name,
      const vector<ColumnIndex> &column_ids,
      unique_ptr<TableFilterSet> table_filters = nullptr
  ) {
    // 1. 获取表扫描函数
    auto ctx = QueryErrorContext();
    EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name, nullptr, ctx);
    unique_ptr<FunctionData> bind_data;
    TableFunction scan_function = table_entry.GetScanFunction(context, bind_data, table_lookup);

    // 4. 处理投影和输出类型
    vector<LogicalType> table_types;
    vector<string> table_names;
    vector<TableColumnType> table_categories;

    vector<LogicalType> return_types;
    vector<string> return_names;
    for (auto &col: table_entry.GetColumns().Logical()) {
      table_types.push_back(col.Type());
      table_names.push_back(col.Name());
      return_types.push_back(col.Type());
      return_names.push_back(col.Name());
    }

    virtual_column_map_t virtual_columns;
    if (scan_function.get_virtual_columns) {
      virtual_columns = scan_function.get_virtual_columns(
          context, bind_data.get());
    } else {
      virtual_columns = table_entry.GetVirtualColumns();
    }
    vector<idx_t> projection_ids;
    for (auto col_id: column_ids) {
      projection_ids.push_back(col_id.GetPrimaryIndex());
    }
    vector<LogicalType> output_types;
    for (auto col_id: column_ids) {
      output_types.push_back(return_types[static_cast<size_t>(col_id.GetPrimaryIndex())]);
    }

    // 5. 估计基数
    idx_t estimated_cardinality = 1000; // 临时值

    // 6. 其他参数
    ExtraOperatorInfo extra_info;
    vector<Value> parameters;

    // 7. 创建算子（严格匹配参数）
    return duckdb::make_uniq<PhysicalTableScan>(
        output_types, scan_function, std::move(bind_data),
        return_types, column_ids, projection_ids, return_names,
        std::move(table_filters), estimated_cardinality,
        std::move(extra_info), parameters, virtual_columns);
  }

// 获取所有表的主函数
  vector<TableCatalogEntry*> GetAllTables(ClientContext& context) {
    vector<TableCatalogEntry*> tables;

    // 1. 获取系统目录
    auto& catalog = Catalog::GetSystemCatalog(context);

    // 2. 遍历所有模式（Schema），通常默认模式为 "main"
    vector<string> schemas = {"main"}; // 可添加其他模式，如 "pg_catalog"
    for (auto& schema_name : schemas) {
      // 3. 获取模式下的所有目录项
      auto entries = catalog.GetSchemas(context);
    }

    return tables;
  }

  DuckdbExec::DuckdbExec(void *db, void *connect) {
    auto wrapper = reinterpret_cast<DatabaseWrapper *>(db);
		db_ = wrapper;
//    auto &scheduler = duckdb::TaskScheduler::GetScheduler(*wrapper->database->instance);
//    scheduler.ExecuteTasks(3);

    // conn->context get client context
//    Connection *conn = reinterpret_cast<Connection *>(connect);
//		connect_ = conn;

    DuckDB db1("/root/go/src/gitee.com/kwbasedb/kwbase/local/tpch");
    connect_ = new Connection(db1);
    ClientContext& context = *connect_->context.get();
    // 获取所有表并打印信息
//    connect_->BeginTransaction();
//    auto res = connect_->Query("show tables;");
//    printf("数据库中共有 %s：\n", res->ToString().c_str());
//    connect_->Commit();

    fspecs_ = new FlowSpec();
  }

  DuckdbExec::~DuckdbExec() {
    if (connect_ != nullptr) {
      delete connect_;
    }
    if (fspecs_ != nullptr) {
      delete fspecs_;
    }
  }

  KStatus DuckdbExec::ExecQuery(kwdbContext_p ctx, APQueryInfo *req, APRespInfo *resp) {
    EnterFunc();
    KWAssertNotNull(req);
    KStatus ret = KStatus::FAIL;
    ctx->relation_ctx = req->relation_ctx;
    ctx->timezone = req->time_zone;
    EnMqType type = req->tp;
    k_char *message = static_cast<k_char *>(req->value);
    k_uint32 len = req->len;
    k_int32 id = req->id;
    k_int32 uniqueID = req->unique_id;

    try {
      if (!req->handle && type != EnMqType::MQ_TYPE_DML_INIT &&
          type != EnMqType::MQ_TYPE_DML_SETUP) {
        Return(ret);
      }

      if (type == EnMqType::MQ_TYPE_DML_INIT && !checkDuckdbParam(req->db, req->connection)) {
        resp->ret = 0;
        Return(ret);
      }

      kwdbts::DuckdbExec *handle = nullptr;
      if (!req->handle) {
        handle = new kwdbts::DuckdbExec(req->db, req->connection);
        handle->Init();
        req->handle = static_cast<char *>(static_cast<void *>(handle));
      } else {
        handle = static_cast<kwdbts::DuckdbExec *>(static_cast<void *>(req->handle));
      }
      //    ctx->dml_exec_handle = handle;

      switch (type) {
        case EnMqType::MQ_TYPE_DML_SETUP:
        {
//          handle->thd_->SetSQL(std::string(req->sql.data, req->sql.len));
          ret = handle->Setup(ctx, message, len, id, uniqueID, resp);
          if (ret != KStatus::SUCCESS) {
//            handle->ClearTsScans(ctx);
          }
          break;
        }
        case EnMqType::MQ_TYPE_DML_CLOSE:
          //        handle->ClearTsScans(ctx);
          //        delete handle;
          resp->ret = 1;
          resp->tp = req->tp;
          break;
        case EnMqType::MQ_TYPE_DML_INIT:
          resp->ret = 1;
          resp->tp = req->tp;
          resp->code = 0;
          resp->handle = handle;
          break;
        case EnMqType::MQ_TYPE_DML_NEXT:
          //        ret = handle->Next(ctx, id, TsNextRetState::DML_NEXT, resp);
        {
#if 0
          auto res = handle->ExecuteCustomPlan();
          if (res.success) {
            resp->ret = 1;
            resp->code = 1;
            resp->tp = req->tp;
          } else {
            resp->ret = 1;
            resp->code = 2202;
            resp->len = res.error_message.size() + 1;
            if (resp->len > 0) {
              resp->value = malloc(resp->len);
              if (resp->value != nullptr) {
                memcpy(resp->value, res.error_message.c_str(), resp->len);
              }
            }
          }
#else
          resp->ret = 1;
#endif
          break;
        }

        case EnMqType::MQ_TYPE_DML_PG_RESULT:
          //        ret = handle->Next(ctx, id, TsNextRetState::DML_PG_RESULT, resp);
          resp->ret = 1;
          resp->code = 1;
          resp->tp = req->tp;
          break;
        case EnMqType::MQ_TYPE_DML_VECTORIZE_NEXT:
          //        ret = handle->Next(ctx, id, TsNextRetState::DML_VECTORIZE_NEXT, resp);
          break;
        case EnMqType::MQ_TYPE_DML_PUSH:
          // push relational data from ME to AE for multiple model processing
          //        ret = handle->PushRelData(ctx, req, resp);
          break;
        default:
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE,
                                        "Undecided datatype");
          break;
      }
    } catch (...) {
      return ret;
    }

    return ret;
  }

  KStatus DuckdbExec::Setup(kwdbContext_p ctx, k_char *message, k_uint32 len, k_int32 id, k_int32 uniqueID,
                            APRespInfo *resp) {
    KStatus ret = KStatus::FAIL;
    resp->tp = EnMqType::MQ_TYPE_DML_SETUP;
    resp->ret = 0;
    resp->value = 0;
    resp->len = 0;
    resp->unique_id = uniqueID;
    resp->handle = static_cast<char *>(static_cast<void *>(this));
    do {
//      if (tsscan_head_ && tsscan_head_->id == id) {
//        ClearTsScans(ctx);
//      }
//      CreateTsScan(ctx, &tsScan);
//      if (!tsScan) {
//        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
//        break;
//      }
//      tsScan->id = id;
//      tsScan->unique_id = uniqueID;
      bool proto_parse = false;
      try {
        proto_parse = fspecs_->ParseFromArray(message, len);
      } catch (...) {
        LOG_ERROR("Throw exception where parsing physical plan.");
        proto_parse = false;
      }
      if (!proto_parse) {
        LOG_ERROR("Parse physical plan err when query setup.");
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "Invalid physical plan");
        break;
      }
      auto gate = fspecs_->gateway();
      printf("gate is %d \n", gate);
      for (size_t i=0; i < fspecs_->processors_size(); i++) {
        auto proc = fspecs_->processors(i);
        if (proc.has_core()) {
          if (proc.core().has_aptablereader()){
            auto apReader = proc.core().aptablereader();
            if (apReader.has_db_name() && !apReader.db_name().empty()) {
              printf("db is %s, ", apReader.db_name().c_str());
            }
            printf("table is %s \n", apReader.table_name().c_str());
          }
        }
      }
//      ret = tsScan->processors->Init(ctx, tsScan->fspecs);
//      if (KStatus::SUCCESS != ret) {
//        break;
//      }
      // LOG_ERROR("execute sql: %s, query_id = %ld", thd_->sql_.c_str(), brpc_info_.query_id_);
//      if (tsscan_head_ == nullptr) {
//        tsscan_head_ = tsScan;
//        tsscan_end_ = tsscan_head_;
//      } else {
//        tsscan_end_->next = tsScan;
//        tsscan_end_ = tsScan;
//      }
      resp->ret = 1;
      ret = KStatus::SUCCESS;
    } while (0);
    if (resp->ret == 0) {
//      DestroyTsScan(tsScan);
//      DisposeError(ctx, resp);
    }
    return ret;
  }

	ExecutionResult DuckdbExec::ExecuteCustomPlan() {
		ExecutionResult result;
		if (!connect_ || !connect_->context) {
			result.error_message = "Database not open";
			return result;
		}

    connect_->BeginTransaction();

		try {
			// 转换自定义物理计划为DuckDB物理计划
			// 获取表信息
      auto& context = *connect_->context.get();
			auto& catalog = Catalog::GetSystemCatalog(context);
			std::string table_name = "region"; // 用于扫描
			auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, DEFAULT_SCHEMA,
																		table_name, OnEntryNotFound::RETURN_NULL);
			if (!entry) {
        connect_->Rollback();
        result.error_message = "can not find table " + table_name;
        return result;
			}

			auto *scanCatalog = dynamic_cast<TableCatalogEntry*>(entry.get());
			vector<ColumnIndex> column_ids;
			column_ids.push_back(ColumnIndex(0));
			auto scanOp = CreateTableScanOperator(context, *scanCatalog, table_name, column_ids);
			auto root_op = move(scanOp);

			// 初始化执行器
			Executor executor(context);
			executor.Initialize(*root_op);
      executor.ExecuteTask(true);
      executor.WaitForTask();
      auto res = executor.GetResult();
      result.success = true;
			// 获取结果
      connect_->Commit();
			return result;
		} catch (const Exception& e) {
			result.error_message = e.what();
      connect_->Rollback();
			return result;
		} catch (const std::exception& e) {
			result.error_message = e.what();
      connect_->Rollback();
			return result;
		}
	}

}