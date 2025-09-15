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

#include "duckdb/engine/duckdb_exec.h"
#include "libkwdbap.h"

#include "cm_func.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/engine/ap_parse_query.h"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "ee_encoding.h"
#include "ee_string_info.h"
#include "lg_api.h"
#include "libkwdbap.h"

using namespace duckdb;

uint64_t AppenderColumnCount(APAppender out_appender) {
  auto appender = static_cast<Appender*>(out_appender->value);
  if (nullptr == appender) {
    return 0;
  }

  return appender->GetActiveTypes().size();
}

namespace kwdbts {
KStatus AttachDB(ClientContext& context, std::string name, std::string path) {
  KStatus ret = KStatus::SUCCESS;
  try {
    DatabaseManager& manager = DatabaseManager::Get(context);
    auto existing_db = manager.GetDatabase(context, name);
    if (existing_db) {
      //      auto &cfg = existing_db->GetDatabase().GetConfig();
      //      if (cfg.options.database_path != path) {
      //
      //      }
      return ret;
    }
    auto attach_info = make_uniq<AttachInfo>();
    attach_info->name = name;
    attach_info->path = path + "/" + name;
    //  attach_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
    AttachOptions options(attach_info, duckdb::AccessMode::AUTOMATIC);
    auto attached_db = manager.AttachDatabase(context, *attach_info, options);
    const auto storage_options = attach_info->GetStorageOptions();
    attached_db->Initialize(context, storage_options);
    if (!options.default_table.name.empty()) {
      attached_db->GetCatalog().SetDefaultTable(options.default_table.schema,
                                                options.default_table.name);
    }
    attached_db->FinalizeLoad(context);
  } catch (const Exception& e) {
    ret = KStatus::FAIL;
  }
  return ret;
}

KStatus DetachDB(DatabaseManager& db_manager, ClientContext& context,
                 string db_name) {
  KStatus ret = KStatus::SUCCESS;
  db_manager.DetachDatabase(context, db_name,
                            duckdb::OnEntryNotFound::RETURN_NULL);
  return ret;
}

APEngineImpl::APEngineImpl(kwdbContext_p ctx, const char* db_path)
    : db_path_(db_path), c_cache() {}

KStatus APEngineImpl::OpenEngine(kwdbContext_p ctx, APEngine** engine,
                                 APConnectionPtr* out, duckdb_database* out_db,
                                 const char* path) {
  auto* engineImpl = new APEngineImpl(ctx, path);
  string defaultDB = string(path) + "/defaultdb";
  auto wrapper = new DatabaseWrapper();
  try {
    DBConfig default_config;
    DBConfig* db_config = &default_config;
    wrapper->database =
        duckdb::make_shared_ptr<DuckDB>(defaultDB.c_str(), db_config);
  } catch (std::exception& ex) {
    //    if (out_error) {
    ErrorData parsed_error(ex);
    printf("open ap engine error is %s \n", parsed_error.Message().c_str());
    //    }
    delete wrapper;
    return KStatus::FAIL;
  } catch (...) {
    // LCOV_EXCL_START
    printf("open ap engine error unknow\n");
    delete wrapper;
    return KStatus::FAIL;
  }  // LCOV_EXCL_STOP

  *out_db = reinterpret_cast<duckdb_database>(wrapper);
  auto conn = std::make_shared<Connection>(*wrapper->database);
  engineImpl->conn_ = conn;
  engineImpl->instance_ = wrapper->database->instance;
  *engine = engineImpl;
  *out = reinterpret_cast<APConnectionPtr>(conn.get());
  return KStatus::SUCCESS;
}

KStatus APEngineImpl::DatabaseOperate(const char* name, EnDBOperateType type) {
  KStatus ret = KStatus::FAIL;
  auto lock = make_uniq<ClientContextLock>(context_lock_);
  conn_->BeginTransaction();
  switch (type) {
    case DB_CREATE:
    case DB_ATTACH: {
      ret = conn_->context->AttachDB(name, db_path_) ? KStatus::SUCCESS
                                                     : KStatus::FAIL;
      break;
    }
    case DB_DETACH: {
      ret = DetachDB(instance_->GetDatabaseManager(), *conn_->context.get(),
                     name);
      break;
    }
    default: {
    }
  }

  conn_->Commit();
  return ret;
}

KStatus APEngineImpl::DropDatabase(const char* current, const char* name) {
  //  DuckDB defaultDB(path);
  auto lock = make_uniq<ClientContextLock>(context_lock_);
  conn_->BeginTransaction();
  DetachDB(instance_->GetDatabaseManager(), *conn_->context.get(), name);
  conn_->Commit();
  return KStatus::SUCCESS;
}

KStatus APEngineImpl::UpdateDBCache(std::string dbName) {
  
  // to be implemented
  return SUCCESS;
}

duckdb::Connection* APEngineImpl::GetAPConnFromCache(k_uint64 sessionID, 
    std::string dbName, std::string userName) {
  
  // TODO: we need to support multiple databases, which is better to 
  // be cached in APEngineImpl level.  (now it's a class member)

  this->UpdateDBCache(dbName);  // not implemented

  return c_cache.GetOrAddConn(sessionID, dbName, userName);
}

KStatus APEngineImpl::Execute(kwdbContext_p ctx, APQueryInfo* req,
                              APRespInfo* resp) {
  req->db = instance_.get();
  // req->connection = conn_.get();
  req->connection = this->GetAPConnFromCache(req->sessionID, "", "");
  auto lock = make_uniq<ClientContextLock>(context_lock_);
  conn_->BeginTransaction();
  KStatus ret = DuckdbExec::ExecQuery(ctx, req, resp);
  conn_->Commit();
  return ret;
}

KStatus APEngineImpl::Query(const char* stmt, APRespInfo* resp) {
  auto lock = make_uniq<ClientContextLock>(context_lock_);
  conn_->BeginTransaction();
  try {
    auto res = conn_->Query(stmt);
    if (res->HasError()) {
      auto errString = res->GetError();
      resp->ret = 0;
      resp->code = 2202;
      resp->len = errString.length() + 1;
      if (resp->len > 0) {
        resp->value = malloc(resp->len);
        if (resp->value != nullptr) {
          memcpy(resp->value, errString.c_str(), resp->len);
        }
      }
      conn_->Commit();
      return FAIL;
    }
    resp->row_num = res->RowCount();
    resp->ret = 1;
  } catch (const Exception& e) {
    resp->ret = 0;
    resp->code = 2202;
    resp->len = strlen(e.what()) + 1;
    if (resp->len > 0) {
      resp->value = malloc(resp->len);
      if (resp->value != nullptr) {
        memcpy(resp->value, e.what(), resp->len);
      }
    }
    conn_->Commit();
    return FAIL;
  } catch (const std::exception& e) {
    resp->ret = 0;
    resp->code = 2202;
    resp->len = strlen(e.what()) + 1;
    if (resp->len > 0) {
      resp->value = malloc(resp->len);
      if (resp->value != nullptr) {
        memcpy(resp->value, e.what(), resp->len);
      }
    }
    conn_->Commit();
    return FAIL;
  }

  conn_->Commit();
  return SUCCESS;
}

KStatus APEngineImpl::CreateAppender(const char* catalog, const char* schema,
                                     const char* table,
                                     APAppender* out_appender) {
  auto appender = new Appender(*conn_, catalog, schema, table);
  (*out_appender)->value = appender;
  return SUCCESS;
}

unique_ptr<PhysicalTableScan> CreateTableScanOperator(
    ClientContext& context, TableCatalogEntry& table_entry, string table_name,
    const vector<ColumnIndex>& column_ids, unique_ptr<TableFilterSet> table_filters = nullptr) {
  // 1. 获取表扫描函数
  auto ctx = QueryErrorContext();
  EntryLookupInfo table_lookup(CatalogType::TABLE_ENTRY, table_name, nullptr,
                               ctx);
  unique_ptr<FunctionData> bind_data;
  TableFunction scan_function =
      table_entry.GetScanFunction(context, bind_data, table_lookup);

  // 4. 处理投影和输出类型
  vector<LogicalType> table_types;
  vector<string> table_names;
  vector<TableColumnType> table_categories;

  vector<LogicalType> return_types;
  vector<string> return_names;
  for (auto& col : table_entry.GetColumns().Logical()) {
    table_types.push_back(col.Type());
    table_names.push_back(col.Name());
    return_types.push_back(col.Type());
    return_names.push_back(col.Name());
  }

  virtual_column_map_t virtual_columns;
  if (scan_function.get_virtual_columns) {
    virtual_columns =
        scan_function.get_virtual_columns(context, bind_data.get());
  } else {
    virtual_columns = table_entry.GetVirtualColumns();
  }
  vector<idx_t> projection_ids;
  for (const ColumnIndex& col_id : column_ids) {
    projection_ids.push_back(col_id.GetPrimaryIndex());
  }
  vector<LogicalType> output_types;
  for (auto col_id : column_ids) {
    output_types.push_back(
        return_types[static_cast<size_t>(col_id.GetPrimaryIndex())]);
  }

  // 5. 估计基数
  idx_t estimated_cardinality = 1000;  // 临时值

  // 6. 其他参数
  ExtraOperatorInfo extra_info;
  vector<Value> parameters;

  // 7. 创建算子（严格匹配参数）
  return duckdb::make_uniq<PhysicalTableScan>(
      output_types, scan_function, std::move(bind_data), return_types,
      column_ids, projection_ids, return_names, std::move(table_filters),
      estimated_cardinality, std::move(extra_info), parameters,
      virtual_columns);
}

DuckdbExec::DuckdbExec(std::string db_path) { db_path_ = std::move(db_path); }

DuckdbExec::~DuckdbExec() {}

void DuckdbExec::Init(void* instance, void* connect) {
  instance_ = static_cast<DatabaseInstance*>(instance);
  connect_ = static_cast<Connection*>(connect);
  processors_ =
      make_uniq<kwdbap::Processors>(*connect_->context.get(), db_path_);
}

KStatus DuckdbExec::ExecQuery(kwdbContext_p ctx, APQueryInfo* req,
                              APRespInfo* resp) {
  EnterFunc();
  KWAssertNotNull(req);
  KStatus ret = KStatus::FAIL;
  ctx->relation_ctx = req->relation_ctx;
  ctx->timezone = req->time_zone;
  ctx->sessionID = req->sessionID;
  EnMqType type = req->tp;
  k_char* message = static_cast<k_char*>(req->value);
  k_uint32 len = req->len;
  k_int32 id = req->id;
  k_int32 uniqueID = req->unique_id;

  try {
    if (!req->handle && type != EnMqType::MQ_TYPE_DML_SETUP) {
      Return(ret);
    }

    kwdbts::DuckdbExec* handle = nullptr;
    if (!req->handle) {
      handle = new kwdbts::DuckdbExec(
          std::string(req->db_path.data, req->db_path.len));
      handle->Init(req->db, req->connection);

      req->handle = static_cast<char*>(static_cast<void*>(handle));
    } else {
      handle =
          static_cast<kwdbts::DuckdbExec*>(static_cast<void*>(req->handle));
    }
    //    ctx->dml_exec_handle = handle;
    switch (type) {
      case EnMqType::MQ_TYPE_DML_SETUP: {
        //          handle->thd_->SetSQL(std::string(req->sql.data,
        //          req->sql.len));
        if (req->sql.data) {
          handle->sql_ = std::string(req->sql.data, req->sql.len);
        }
        ret = handle->Setup(ctx, message, len, id, uniqueID, resp);
        if (ret != KStatus::SUCCESS) {
          //            handle->ClearTsScans(ctx);
        }
        break;
      }
      case EnMqType::MQ_TYPE_DML_CLOSE:
        handle->Clear(ctx);
        delete handle;
        resp->ret = 1;
        resp->tp = req->tp;
        break;
      case EnMqType::MQ_TYPE_DML_NEXT: {
        ret = handle->Next(ctx, id, TsNextRetState::DML_NEXT, resp);
        break;
      }
      case EnMqType::MQ_TYPE_DML_PG_RESULT: {
        ret = handle->Next(ctx, id, TsNextRetState::DML_PG_RESULT, resp);
        break;
      }
      case EnMqType::MQ_TYPE_DML_VECTORIZE_NEXT: {
        ret = handle->Next(ctx, id, TsNextRetState::DML_VECTORIZE_NEXT, resp);
        break;
      }
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

KStatus DuckdbExec::Setup(kwdbContext_p ctx, k_char* message, k_uint32 len, k_int32 id, k_int32 uniqueID,
                          APRespInfo* resp) {
  if (setup_) {
    return KStatus::FAIL;
  }
  setup_ = true;
  KStatus ret = KStatus::FAIL;
  resp->tp = EnMqType::MQ_TYPE_DML_SETUP;
  resp->ret = 0;
  resp->value = 0;
  resp->len = 0;
  resp->unique_id = uniqueID;
  resp->handle = static_cast<char*>(static_cast<void*>(this));
  if (nullptr != connect_ && nullptr != connect_->context.get()) {
    try {
      if (KStatus::SUCCESS == processors_->Init(message, len)) {
        res_ = PrepareExecutePlan(ctx);
      }
      resp->ret = 1;
      ret = KStatus::SUCCESS;
    } catch (const Exception& e) {
      auto error = e.what();
      printf("DuckdbExec::Setup catch error %s \n", error);
    }
  }

  return ret;
}

KStatus DuckdbExec::Next(kwdbContext_p ctx, k_int32 id, TsNextRetState nextState, APRespInfo* resp) {
  KStatus ret = KStatus::FAIL;
#if 1
  if (setup_ && res_.success) {
    resp->ret = 1;
    if (res_.row_count != 0) {
      resp->code = 1;
      resp->value = res_.value;
      resp->len = res_.len;
      resp->row_num = res_.row_count;
      res_.row_count = 0;
    } else {
      resp->code = -1;
      setup_ = false;
    }
    ret = KStatus::SUCCESS;
  } else {
    resp->ret = 1;
    resp->code = 2202;
    resp->len = res_.error_message.size() + 1;
    if (resp->len > 0) {
      resp->value = malloc(resp->len);
      if (resp->value != nullptr) {
        memcpy(resp->value, res_.error_message.c_str(), resp->len);
      }
    }
  }
#else
  resp->ret = 1;
#endif
  return ret;
}

void DuckdbExec::Clear(kwdbContext_p ctx) {
  if (res_.value != nullptr) {
    free(res_.value);
  }
}

const char val_str_t[1] = {'t'};
const char val_str_f[1] = {'f'};

KStatus PGResultDataForOneRow(kwdbContext_p ctx, const ColumnDataRow row, vector<LogicalType>& types, k_int32 col_count,
                              const EE_StringInfo& info) {
  EnterFunc();
  k_uint32 temp_len = info->len;
  char* temp_addr = nullptr;

  if (ee_appendBinaryStringInfo(info, "D0000", 5) != SUCCESS) {
    Return(FAIL);
  }

  // write column quantity
  if (ee_sendint(info, col_count, 2) != SUCCESS) {
    Return(FAIL);
  }

  for (k_int32 col_idx = 0; col_idx < col_count; col_idx++) {
    auto val = row.GetValue(col_idx);
    if (val.IsNull()) {
      // write a negative value to indicate that the column is NULL
      if (ee_sendint(info, -1, 4) != SUCCESS) {
        Return(FAIL);
      }
      continue;
    }

    switch (types[col_idx].id()) {
      case LogicalTypeId::BOOLEAN: {
        // write the length of col value
        if (ee_sendint(info, 1, 4) != SUCCESS) {
          Return(FAIL);
        }
        auto res = BooleanValue::Get(val);
        if (res) {
          // write string
          if (ee_appendBinaryStringInfo(info, val_str_t, 1) != SUCCESS) {
            Return(FAIL);
          }
        } else {
          if (ee_appendBinaryStringInfo(info, val_str_f, 1) != SUCCESS) {
            Return(FAIL);
          }
        }
        break;
      }
      case LogicalTypeId::TINYINT:
      case LogicalTypeId::SMALLINT:
      case LogicalTypeId::INTEGER:
      case LogicalTypeId::BIGINT: {
        auto val_char = val.ToString();
        auto len = val_char.length();
        if (ee_sendint(info, len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char.c_str(), len) != SUCCESS) {
          Return(FAIL);
        }
        break;
      }
      default: {
        auto val_char = val.ToString();
        auto len = val_char.length();
        if (ee_sendint(info, len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char.c_str(), len) != SUCCESS) {
          Return(FAIL);
        }
        break;
      }
    }
  }

  temp_addr = &info->data[temp_len + 1];
  k_uint32 n32 = be32toh(info->len - temp_len - 1);
  memcpy(temp_addr, &n32, 4);
  Return(SUCCESS);
}

KStatus PGResultDataForOneChunk(kwdbContext_p ctx, const DataChunk& chunk, vector<LogicalType>& types,
                                k_int32 col_count, const EE_StringInfo& info) {
  EnterFunc();
  k_uint32 temp_len = info->len;
  char* temp_addr = nullptr;

  if (ee_appendBinaryStringInfo(info, "D0000", 5) != SUCCESS) {
    Return(FAIL);
  }

  // write column quantity
  if (ee_sendint(info, col_count, 2) != SUCCESS) {
    Return(FAIL);
  }

  for (idx_t row = 0; row < chunk.size(); row++) {
    for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
      auto val = chunk.GetValue(col, row);
      if (val.IsNull()) {
        // write a negative value to indicate that the column is NULL
        if (ee_sendint(info, -1, 4) != SUCCESS) {
          Return(FAIL);
        }
        continue;
      }

      switch (types[col].id()) {
        case LogicalTypeId::BOOLEAN: {
          // write the length of col value
          if (ee_sendint(info, 1, 4) != SUCCESS) {
            Return(FAIL);
          }
          auto res = BooleanValue::Get(val);
          if (res) {
            // write string
            if (ee_appendBinaryStringInfo(info, val_str_t, 1) != SUCCESS) {
              Return(FAIL);
            }
          } else {
            if (ee_appendBinaryStringInfo(info, val_str_f, 1) != SUCCESS) {
              Return(FAIL);
            }
          }
          break;
        }
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT: {
          auto val_char = val.ToString();
          auto len = val_char.length();
          if (ee_sendint(info, len, 4) != SUCCESS) {
            Return(FAIL);
          }
          // write string format
          if (ee_appendBinaryStringInfo(info, val_char.c_str(), len) !=
              SUCCESS) {
            Return(FAIL);
          }
          break;
        }
        default: {
          auto val_char = val.ToString();
          auto len = val_char.length();
          if (ee_sendint(info, len, 4) != SUCCESS) {
            Return(FAIL);
          }
          // write string format
          if (ee_appendBinaryStringInfo(info, val_char.c_str(), len) !=
              SUCCESS) {
            Return(FAIL);
          }
          break;
        }
      }
    }
  }

  temp_addr = &info->data[temp_len + 1];
  k_uint32 n32 = be32toh(info->len - temp_len - 1);
  memcpy(temp_addr, &n32, 4);
  Return(SUCCESS);
}

KStatus EncodingValue(kwdbContext_p ctx, Value& val, const EE_StringInfo& info, LogicalType& return_types) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;

  // dispose null
  if (val.IsNull()) {
    k_int32 len = ValueEncoding::EncodeComputeLenNull(0);
    ret = ee_enlargeStringInfo(info, len);
    if (ret != SUCCESS) {
      Return(ret);
    }

    CKSlice slice{info->data + info->len, len};
    ValueEncoding::EncodeNullValue(&slice, 0);
    info->len = info->len + len;
    Return(ret);
  }

  Return(ret);
}

KStatus Encoding(kwdbContext_p ctx, MaterializedQueryResult* res, char*& encoding_buf_, k_uint32& encoding_len_) {
  KStatus st = KStatus::SUCCESS;
  EE_StringInfo msgBuffer = ee_makeStringInfo();
  if (msgBuffer == nullptr) {
    return KStatus::FAIL;
  }

  auto& coll = res->Collection();
  // for (auto &chunk : coll.Chunks()) {
  //   st = PGResultDataForOneChunk(ctx, chunk, res->types, coll.ColumnCount(),
  //   msgBuffer); if (st == KStatus::FAIL) {
  //     break;
  //   }
  // }
  for (auto& row : coll.Rows()) {
    st = PGResultDataForOneRow(ctx, row, res->types, coll.ColumnCount(),
                               msgBuffer);
    if (st == KStatus::FAIL) {
      break;
    }
  }

  if (st == SUCCESS) {
    encoding_buf_ = msgBuffer->data;
    encoding_len_ = msgBuffer->len;
  } else {
    free(msgBuffer->data);
  }
  delete msgBuffer;
  return st;
}

ExecutionResult DuckdbExec::ExecuteCustomPlan(kwdbContext_p ctx, const string& table_name) {
  ExecutionResult result;
  if (!connect_ || !connect_->context) {
    result.error_message = "Database not open";
    return result;
  }

  connect_->BeginTransaction();

  try {
    // 1. 准备上下文和元数据
    auto& context = *connect_->context.get();
    std::string db_name = "tpch";
    auto& catalog = Catalog::GetCatalog(context, db_name);

    auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, "public",
                                  table_name, OnEntryNotFound::RETURN_NULL);
    if (!entry) {
      connect_->Rollback();
      result.error_message = "can not find table " + table_name;
      return result;
    }

    // 2. 创建扫描运算符
    auto* scanCatalog = dynamic_cast<TableCatalogEntry*>(entry.get());
    vector<ColumnIndex> column_ids;
    column_ids.push_back(ColumnIndex(0));  // 只扫描第0列
    column_ids.push_back(ColumnIndex(1));
    column_ids.push_back(ColumnIndex(2));
    auto scanOp =
        CreateTableScanOperator(context, *scanCatalog, table_name, column_ids);
    //      auto root_op = move(scanOp);

    // 3. 准备结果收集（适配DuckDB 1.3.2版本）
    // 3.1 获取返回类型（通过公共API获取列信息）
    vector<LogicalType> return_types;
    vector<string> names;
    for (auto& col_idx : column_ids) {
      // 修正：通过公共方法GetColumn获取列定义，使用GetPrimaryIndex()访问索引
      auto& column =
          scanCatalog->GetColumn(LogicalIndex(col_idx.GetPrimaryIndex()));
      names.push_back(column.Name());
      return_types.push_back(column.Type());
    }

    StatementType statement_type = StatementType::SELECT_STATEMENT;
    auto prepareResult = make_shared_ptr<PreparedStatementData>(statement_type);
    prepareResult->properties.allow_stream_result = false;
    prepareResult->properties.always_require_rebind = false;
    prepareResult->names = names;
    prepareResult->types = return_types;
    //      prepareResult->value_map = std::move(logical_planner.value_map);
    auto physical_plan = make_uniq<PhysicalPlan>(Allocator::Get(context));
    physical_plan->SetRoot(*scanOp.get());
    prepareResult->physical_plan = std::move(physical_plan);
    auto root_op = PhysicalResultCollector::GetResultCollector(
        context, *prepareResult.get());
    // 4. 初始化执行器并执行
    Executor executor(context);
    executor.Initialize(*root_op);

    auto lock = make_uniq<ClientContextLock>(context_lock_);

    PendingExecutionResult execution_result;
    while (!PendingQueryResult::IsResultReady(
        execution_result = executor.ExecuteTask(false))) {
      if (execution_result == PendingExecutionResult::BLOCKED) {
        executor.WaitForTask();
      }
    }

    // 5. 通过Executor获取结果（替代ResultCollector，适配1.3.2版本）
    auto query_result = executor.GetResult();
    if (!query_result || query_result->HasError()) {
      result.error_message =
          query_result ? query_result->GetError() : "Unknown execution error";
      connect_->Rollback();
      return result;
    }

    switch (query_result->type) {
      case QueryResultType::MATERIALIZED_RESULT: {
        auto res = dynamic_cast<MaterializedQueryResult*>(query_result.get());
        char* encoding_buf_;
        k_uint32 encoding_len_;
        auto ret = Encoding(ctx, res, encoding_buf_, encoding_len_);
        if (ret == KStatus::SUCCESS) {
          result.value = encoding_buf_;
          result.len = encoding_len_;
          result.row_count = res->RowCount();
        }
        break;
      }
      case QueryResultType::PENDING_RESULT:
        break;
      default:
        break;
    }

    connect_->Commit();
    result.success = true;
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

KStatus DuckdbExec::DetachDB(duckdb::vector<std::string> dbs) {
  KStatus ret = KStatus::SUCCESS;
  for (auto& db : dbs) {
    DatabaseManager& db_manager = instance_->GetDatabaseManager();
    db_manager.DetachDatabase(*connect_->context, db,
                              duckdb::OnEntryNotFound::RETURN_NULL);
  }
  return ret;
}

ExecutionResult DuckdbExec::PrepareExecutePlan(kwdbContext_p ctx) {
  ExecutionResult result;
  try {
    StatementType statement_type = StatementType::SELECT_STATEMENT;
    auto prepared = make_shared_ptr<PreparedStatementData>(statement_type);
    prepared->physical_plan = processors_->GetPlan();
    if (!prepared->physical_plan) {
      connect_->Rollback();
      result.error_message = "convert physical plan failed";
      return result;
    }
    prepared->physical_plan->Root().Verify();
    prepared->types = prepared->physical_plan->Root().types;
    prepared->names.resize(prepared->types.size());
    auto query_result = connect_->KWQuery(sql_, prepared);
    if (query_result->HasError()) {
      connect_->Rollback();
      result.error_message = query_result->GetError();
      return result;
    }
    result.success = true;

    char* encoding_buf_;
    k_uint32 encoding_len_;
    auto ret = Encoding(ctx, query_result.get(), encoding_buf_, encoding_len_);
    if (ret == KStatus::SUCCESS) {
      result.value = encoding_buf_;
      result.len = encoding_len_;
      result.row_count = query_result->RowCount();
    }
  } catch (const Exception& e) {
    result.error_message = e.what();
    connect_->Rollback();
    return result;
  }

  return result;
}

}  // namespace kwdbts
