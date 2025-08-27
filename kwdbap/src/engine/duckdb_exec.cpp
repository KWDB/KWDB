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


#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/helper/physical_batch_collector.hpp"

#include "kwdb_type.h"
#include "lg_api.h"
#include "ee_comm_def.h"
#include "cm_func.h"
#include "ee_string_info.h"
#include "ee_encoding.h"

namespace kwdbts {

  APEngineImpl::APEngineImpl(kwdbContext_p ctx) {
  }

  KStatus APEngineImpl::OpenEngine(kwdbContext_p ctx, APEngine** engine) {
    auto* engineImpl = new APEngineImpl(ctx);
    *engine = engineImpl;
    return KStatus::SUCCESS;
  }

  KStatus APEngineImpl::Execute(kwdbContext_p ctx, APQueryInfo* req, APRespInfo* resp) {
    KStatus ret = DuckdbExec::ExecQuery(ctx, req, resp);
    return ret;
  }

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
      virtual_columns = scan_function.get_virtual_columns(context, bind_data.get());
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

  DuckdbExec::DuckdbExec(void *db, void *connect, std::string db_path) {
    auto wrapper = reinterpret_cast<DatabaseWrapper *>(db);
    db_ = wrapper;
    db_path_ = std::move(db_path);
    DuckDB init_db(db_path_ + "/tpch");
    connect_ = new Connection(init_db);
    init_db_ = make_shared_ptr<DuckDB>(init_db);
    //connect_ = new Connection(*db_->database);
    fspecs_ = new FlowSpec();
  }

  DuckdbExec::~DuckdbExec() {
    if (connect_ != nullptr) {
      delete connect_;
      connect_ = nullptr;
    }
    if (fspecs_ != nullptr) {
      delete fspecs_;
      fspecs_ = nullptr;
    }
  }

  void DuckdbExec::ReInit(const string &db_name) {
    if (db_name.empty()) {
      return;
    }

    if (connect_ != nullptr) {
      delete connect_;
      connect_ = nullptr;
    }
    DuckDB db1(db_path_ + "/" + db_name);
    connect_ = new Connection(db1);
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
        handle = new kwdbts::DuckdbExec(req->db, req->connection, std::string(req->db_path.data, req->db_path.len));
        handle->Init();

        req->handle = static_cast<char *>(static_cast<void *>(handle));
      } else {
        handle = static_cast<kwdbts::DuckdbExec *>(static_cast<void *>(req->handle));
      }
      //    ctx->dml_exec_handle = handle;
      switch (type) {
        case EnMqType::MQ_TYPE_DML_SETUP: {
//          handle->thd_->SetSQL(std::string(req->sql.data, req->sql.len));
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
        case EnMqType::MQ_TYPE_DML_INIT:
          resp->ret = 1;
          resp->tp = req->tp;
          resp->code = 0;
          resp->handle = handle;
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

  KStatus DuckdbExec::Setup(kwdbContext_p ctx, k_char *message, k_uint32 len, k_int32 id, k_int32 uniqueID,
                            APRespInfo *resp) {
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
    resp->handle = static_cast<char *>(static_cast<void *>(this));
    do {
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
//      auto gate = fspecs_->gateway();
//      printf("gate is %d \n", gate);
//      for (size_t i=0; i < fspecs_->processors_size(); i++) {
//        auto proc = fspecs_->processors(i);
//        if (proc.has_core()) {
//          if (proc.core().has_aptablereader()){
//            auto apReader = proc.core().aptablereader();
//            if (apReader.has_db_name() && !apReader.db_name().empty()) {
//              printf("db is %s, ", apReader.db_name().c_str());
//              //ReInit(apReader.db_name());
//            }
//            printf("table is %s \n", apReader.table_name().c_str());
//
//            res_ = ExecuteCustomPlan(ctx, apReader.table_name());
//          }
//        }
//      }
      res_  = PrepareExecutePlan(ctx);

      resp->ret = 1;
      ret = KStatus::SUCCESS;
    } while (0);
    if (resp->ret == 0) {
    }
    return ret;
  }

  KStatus DuckdbExec::Next(kwdbContext_p ctx, k_int32 id, TsNextRetState nextState, APRespInfo *resp) {
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
  const char val_str_t[1]={'t'};
  const char val_str_f[1]={'f'};

  KStatus PGResultDataForOneRow(kwdbContext_p ctx, const ColumnDataRow row, vector<LogicalType> &types,
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

    for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
      auto val = row.GetValue(col_idx);
      if (val.IsNull()) {
        // write a negative value to indicate that the column is NULL
        if (ee_sendint(info, -1, 4) != SUCCESS) {
          Return(FAIL);
        }
        continue;
      }

      switch(types[col_idx].id()) {
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
        default:{
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

  KStatus PGResultDataForOneChunk(kwdbContext_p ctx, const DataChunk &chunk, vector<LogicalType> &types,
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

        switch(types[col].id()) {
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
          default:{
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
    }

    temp_addr = &info->data[temp_len + 1];
    k_uint32 n32 = be32toh(info->len - temp_len - 1);
    memcpy(temp_addr, &n32, 4);
    Return(SUCCESS);
  }

  KStatus EncodingValue(kwdbContext_p ctx, Value &val, const EE_StringInfo& info, LogicalType &return_types) {
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

//    switch (return_types.id()) {
//      case LogicalTypeId::BOOLEAN: {
//        auto res = BooleanValue::Get(val);
//        std::memcpy(&val, val, sizeof(k_bool));
//        k_int32 len = ValueEncoding::EncodeComputeLenBool(0, val);
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeBoolValue(&slice, 0, val);
//        info->len = info->len + len;
//        break;
//      }
//      case LogicalTypeId::STRING_LITERAL: {
//        k_uint16 val_len;
//        DatumPtr raw = GetData(row, col, val_len);
//        std::string val = std::string{static_cast<char*>(raw), val_len};
//        k_int32 len = ValueEncoding::EncodeComputeLenString(0, val.size());
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeBytesValue(&slice, 0, val);
//        info->len = info->len + len;
//        break;
//      }
//      case LogicalTypeId::TIMESTAMP:
//      case LogicalTypeId::TIMESTAMP_TZ:{
//        DatumPtr raw = GetData(row, col);
//        k_int64 val;
//        std::memcpy(&val, raw, sizeof(k_int64));
//        CKTime ck_time = getCKTime(val, col_info_[col].storage_type, ctx->timezone);
//        k_int32 len = ValueEncoding::EncodeComputeLenTime(0, ck_time);
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeTimeValue(&slice, 0, ck_time);
//        info->len = info->len + len;
//        break;
//      }
//      case LogicalTypeId::TINYINT:
//      case LogicalTypeId::SMALLINT:
//      case LogicalTypeId::INTEGER:
//      case LogicalTypeId::BIGINT:{
//        DatumPtr raw = GetData(row, col);
//        k_int64 val;
//        switch (col_info_[col].storage_type) {
//          case roachpb::DataType::BIGINT:
//          case roachpb::DataType::TIMESTAMP:
//          case roachpb::DataType::TIMESTAMPTZ:
//          case roachpb::DataType::TIMESTAMP_MICRO:
//          case roachpb::DataType::TIMESTAMP_NANO:
//          case roachpb::DataType::TIMESTAMPTZ_MICRO:
//          case roachpb::DataType::TIMESTAMPTZ_NANO:
//          case roachpb::DataType::DATE:
//            std::memcpy(&val, raw, sizeof(k_int64));
//            break;
//          case roachpb::DataType::SMALLINT:
//            k_int16 val16;
//            std::memcpy(&val16, raw, sizeof(k_int16));
//            val = val16;
//            break;
//          default:
//            k_int32 val32;
//            std::memcpy(&val32, raw, sizeof(k_int32));
//            val = val32;
//            break;
//        }
//        k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeIntValue(&slice, 0, val);
//        info->len = info->len + len;
//        break;
//      }
//      case LogicalTypeId::FLOAT:
//      case LogicalTypeId::DOUBLE:{
//        DatumPtr raw = GetData(row, col);
//        k_double64 val;
//        if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
//          k_float32 val32;
//          std::memcpy(&val32, raw, sizeof(k_float32));
//          val = val32;
//        } else {
//          std::memcpy(&val, raw, sizeof(k_double64));
//        }
//
//        k_int32 len = ValueEncoding::EncodeComputeLenFloat(0);
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeFloatValue(&slice, 0, val);
//        info->len = info->len + len;
//        break;
//      }
//      case LogicalTypeId::DECIMAL: {
//        switch (col_info_[col].storage_type) {
//          case roachpb::DataType::SMALLINT: {
//            DatumPtr ptr = GetData(row, col);
//            EncodeDecimal<k_int16>(ptr, info);
//            break;
//          }
//          case roachpb::DataType::INT: {
//            DatumPtr ptr = GetData(row, col);
//            EncodeDecimal<k_int32>(ptr, info);
//            break;
//          }
//          case roachpb::DataType::TIMESTAMP:
//          case roachpb::DataType::TIMESTAMPTZ:
//          case roachpb::DataType::TIMESTAMP_MICRO:
//          case roachpb::DataType::TIMESTAMP_NANO:
//          case roachpb::DataType::TIMESTAMPTZ_MICRO:
//          case roachpb::DataType::TIMESTAMPTZ_NANO:
//          case roachpb::DataType::DATE:
//          case roachpb::DataType::BIGINT: {
//            DatumPtr ptr = GetData(row, col);
//            EncodeDecimal<k_int64>(ptr, info);
//            break;
//          }
//          case roachpb::DataType::FLOAT: {
//            DatumPtr ptr = GetData(row, col);
//            EncodeDecimal<k_float32>(ptr, info);
//            break;
//          }
//          case roachpb::DataType::DOUBLE: {
//            DatumPtr ptr = GetData(row, col);
//            EncodeDecimal<k_double64>(ptr, info);
//            break;
//          }
//          case roachpb::DataType::DECIMAL: {
//            DatumPtr ptr = GetData(row, col);
//            k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
//            if (is_double) {
//              EncodeDecimal<k_double64>(ptr + sizeof(k_bool), info);
//            } else {
//              EncodeDecimal<k_int64>(ptr + sizeof(k_bool), info);
//            }
//            break;
//          }
//          default: {
//            LOG_ERROR("Unsupported Decimal type for encoding: %d ", col_info_[col].storage_type)
//            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
//            break;
//          }
//        }
//        break;
//      }
////      case KWDBTypeFamily::IntervalFamily: {
////        DatumPtr raw = GetData(row, col);
////        k_int64 val;
////        std::memcpy(&val, raw, sizeof(k_int64));
////
////        struct KWDuration duration;
////        switch (col_info_[col].storage_type) {
////          case roachpb::TIMESTAMP_MICRO:
////          case roachpb::TIMESTAMPTZ_MICRO:
////            duration.format(val, 1000);
////            break;
////          case roachpb::TIMESTAMP_NANO:
////          case roachpb::TIMESTAMPTZ_NANO:
////            duration.format(val, 1);
////            break;
////          default:
////            duration.format(val, 1000000);
////            break;
////        }
////        k_int32 len = ValueEncoding::EncodeComputeLenDuration(0, duration);
////        ret = ee_enlargeStringInfo(info, len);
////        if (ret != SUCCESS) {
////          break;
////        }
////
////        CKSlice slice{info->data + info->len, len};
////        ValueEncoding::EncodeDurationValue(&slice, 0, duration);
////        info->len = info->len + len;
////        break;
////      }
////      case KWDBTypeFamily::DateFamily: {
////        const int secondOfDay = 24 * 3600;
////        DatumPtr raw = GetData(row, col);
////        std::string date_str = std::string{static_cast<char*>(raw)};
////        struct tm stm {
////            0
////        };
////        int year, mon, day;
////        k_int64 msec;
////        std::memcpy(&msec, raw, sizeof(k_int64));
////        k_int64 seconds = msec / 1000;
////        time_t rawtime = (time_t) seconds;
////        tm timeinfo;
////        gmtime_r(&rawtime, &timeinfo);
////
////        stm.tm_year = timeinfo.tm_year;
////        stm.tm_mon = timeinfo.tm_mon;
////        stm.tm_mday = timeinfo.tm_mday;
////        time_t val = timelocal(&stm);
////        val += ctx->timezone * 60 * 60;
////        val /= secondOfDay;
////        k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
////        ret = ee_enlargeStringInfo(info, len);
////        if (ret != SUCCESS) {
////          break;
////        }
////
////        CKSlice slice{info->data + info->len, len};
////        ValueEncoding::EncodeIntValue(&slice, 0, val);
////        info->len = info->len + len;
////        break;
////      }
//      default: {
//        DatumPtr raw = GetData(row, col);
//        k_int64 val;
//        std::memcpy(&val, raw, sizeof(k_int64));
//        k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
//        ret = ee_enlargeStringInfo(info, len);
//        if (ret != SUCCESS) {
//          break;
//        }
//
//        CKSlice slice{info->data + info->len, len};
//        ValueEncoding::EncodeIntValue(&slice, 0, val);
//        info->len = info->len + len;
//        break;
//      }
//    }
    Return(ret);
  }

  KStatus Encoding(kwdbContext_p ctx, MaterializedQueryResult * res, char* &encoding_buf_, k_uint32 &encoding_len_) {
    KStatus st = KStatus::SUCCESS;
    EE_StringInfo msgBuffer = ee_makeStringInfo();
    if (msgBuffer == nullptr) {
      return KStatus::FAIL;
    }

    auto &coll = res->Collection();
    // for (auto &chunk : coll.Chunks()) {
    //   st = PGResultDataForOneChunk(ctx, chunk, res->types, coll.ColumnCount(), msgBuffer);
    //   if (st == KStatus::FAIL) {
    //     break;
    //   }
    // }
    for (auto &row : coll.Rows()) {
      st = PGResultDataForOneRow(ctx, row, res->types, coll.ColumnCount(), msgBuffer);
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

	ExecutionResult DuckdbExec::ExecuteCustomPlan(kwdbContext_p ctx, const string &table_name) {
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

      auto entry = catalog.GetEntry(context, CatalogType::TABLE_ENTRY, "main",
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
      auto scanOp = CreateTableScanOperator(context, *scanCatalog, table_name, column_ids);
//      auto root_op = move(scanOp);

      // 3. 准备结果收集（适配DuckDB 1.3.2版本）
      // 3.1 获取返回类型（通过公共API获取列信息）
      vector<LogicalType> return_types;
      vector<string> names;
      for (auto& col_idx : column_ids) {
        // 修正：通过公共方法GetColumn获取列定义，使用GetPrimaryIndex()访问索引
        auto& column = scanCatalog->GetColumn(LogicalIndex(col_idx.GetPrimaryIndex()));
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
      prepareResult->physical_plan = move(physical_plan);
      auto root_op = PhysicalResultCollector::GetResultCollector(context, *prepareResult.get());
      // 4. 初始化执行器并执行
      Executor executor(context);
      executor.Initialize(*root_op);

      auto lock = make_uniq<ClientContextLock>(context_lock_);

      PendingExecutionResult execution_result;
      while (!PendingQueryResult::IsResultReady(execution_result = executor.ExecuteTask(false))) {
        if (execution_result == PendingExecutionResult::BLOCKED) {
          executor.WaitForTask();
        }
      }

      // 5. 通过Executor获取结果（替代ResultCollector，适配1.3.2版本）
      auto query_result = executor.GetResult();
      if (!query_result || query_result->HasError()) {
        result.error_message = query_result ? query_result->GetError() : "Unknown execution error";
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

  KStatus DuckdbExec::AttachDBs() {
    KStatus ret = KStatus::SUCCESS;
    // get all database
    std::map<std::string, EmptyStruct> db_map;
    auto init_dbs = init_db_->instance->GetDatabaseManager().GetDatabases();
    for(auto& init_db : init_dbs) {
      auto name = init_db.get().GetName();
      db_map[name] = {};
    }

    connect_->BeginTransaction();
    try {
      for (size_t i=0; i < fspecs_->processors_size(); i++) {
        const ProcessorSpec &proc = fspecs_->processors(i);
        if (proc.has_core() && proc.core().has_aptablereader()) {
          auto apReader = proc.core().aptablereader();
          auto db_name = apReader.db_name();
          if (db_map.find(db_name) == db_map.end()) {
            // attach database
            auto attach_ret = AttachDB(db_name, db_map);
            if (attach_ret == KStatus::FAIL) {
              connect_->Rollback();
              return attach_ret;
            }
          }
        }
      }
      connect_->Commit();
    } catch (const Exception& e) {
      connect_->Rollback();
      return KStatus::FAIL;
    }
    return ret;
  }

  KStatus DuckdbExec::AttachDB(std::string &db_name, std::map<std::string, EmptyStruct> &db_map) {
    KStatus ret = KStatus::SUCCESS;
    try {
      DatabaseManager& db_manager = DatabaseManager::Get(*connect_->context);
      auto attach_info = make_uniq<AttachInfo>();
      attach_info->name = db_name;
      attach_info->path = db_path_ + "/" + db_name;
      //attach_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
      AttachOptions options(attach_info, duckdb::AccessMode::AUTOMATIC);
      auto attached_db = db_manager.AttachDatabase(*connect_->context, *attach_info, options);
      const auto storage_options = attach_info->GetStorageOptions();
      attached_db->Initialize(*connect_->context, storage_options);
      if (!options.default_table.name.empty()) {
        attached_db->GetCatalog().SetDefaultTable(options.default_table.schema, options.default_table.name);
      }
      attached_db->FinalizeLoad(*connect_->context);
      if (attached_db) {
        db_map[db_name] = {};
      } else {
        ret = KStatus::FAIL;
      }
    } catch (const Exception& e) {
      ret = KStatus::FAIL;
    }
    return ret;
  }

  KStatus DuckdbExec::DetachDB(duckdb::vector<std::string> dbs) {
    KStatus ret = KStatus::SUCCESS;
    for (auto &db : dbs)  {
      DatabaseManager& db_manager = DatabaseManager::Get(*db_->database->instance);
      connect_->BeginTransaction();
      db_manager.DetachDatabase(*connect_->context, db, duckdb::OnEntryNotFound::RETURN_NULL);
      connect_->Commit();
    }
    return ret;
  }

  duckdb::shared_ptr<PreparedStatementData>
  DuckdbExec::ConvertFlowToPhysicalPlan() {
    StatementType statement_type = StatementType::SELECT_STATEMENT;
    auto result = make_shared_ptr<PreparedStatementData>(statement_type);

    PhysicalPlanGenerator physical_planner(*connect_->context);
    physical_planner.InitPhysicalPlan();
    StatementProperties properties;
    duckdb::vector<string> res_names;

    auto physical_plan = make_uniq<PhysicalPlan>(Allocator::Get(*connect_->context));

    // printf("processor size: %d \n", fspecs_->processors_size());
    for (int i=0; i < fspecs_->processors_size(); i++) {
      const ProcessorSpec &proc = fspecs_->processors(i);
      if (proc.has_core() && proc.core().has_aptablereader()) {
        auto apReader = proc.core().aptablereader();
        const PostProcessSpec &post = proc.post();
        if (post.output_columns_size() > 0 && post.output_columns_size() != post.output_types_size()) {
          return result;
        }
        std::string db_name;
        if (apReader.has_db_name() && !apReader.db_name().empty()) {
          db_name = apReader.db_name();
        } else {
          db_name = init_db_->instance->GetDatabaseManager().GetDefaultDatabase(*connect_->context);
        }
        auto table_name = "";
        if (apReader.has_table_name() && !apReader.table_name().empty()) {
          table_name = apReader.table_name().c_str();
        } else {
          return result;
        }
        auto schema_name = "";
        if (apReader.has_schema_name() && !apReader.schema_name().empty()) {
          schema_name = apReader.schema_name().c_str();
        } else {
          schema_name = "main";
        }
        optional_ptr<Catalog> catalog;
        catalog = Catalog::GetCatalog(*connect_->context, db_name);
        auto &schema = catalog->GetSchema(*connect_->context, schema_name);
        auto entry = schema.GetEntry(catalog->GetCatalogTransaction(*connect_->context), CatalogType::TABLE_ENTRY, table_name);
        auto &table = entry->Cast<TableCatalogEntry>();
        properties.RegisterDBRead(table.ParentCatalog(), *connect_->context);

        duckdb::vector<LogicalType> types;
        duckdb::vector<string> names;
        duckdb::vector<ColumnIndex> column_ids;
        if (post.output_columns_size() <= 0 && post.output_types_size() == table.GetColumns().LogicalColumnCount()) {
          for (auto &col : table.GetColumns().Logical()) {
            types.push_back(col.Type());
            names.push_back(col.Name());
            res_names.push_back(col.Name());
            column_t column_index;
            column_index = col.Oid();
            column_ids.emplace_back(column_index);
          }
        } else {
          // stars
          for (auto& out_col : post.output_columns()) {
            auto index = LogicalIndex(out_col);
            auto &col = table.GetColumn(index);
            types.push_back(col.Type());
            names.push_back(col.Name());
            res_names.push_back(col.Name());
            column_t column_index;
            column_index = col.Oid();
            column_ids.emplace_back(column_index);
          }
        }

        duckdb::unique_ptr<FunctionData> bind_data;
        auto scan_function = table.GetScanFunction(*connect_->context, bind_data);

        virtual_column_map_t virtual_columns;
        if (scan_function.get_virtual_columns) {
          virtual_columns = scan_function.get_virtual_columns(*connect_->context, bind_data.get());
        } else {
          virtual_columns = table.GetVirtualColumns();
        }

        duckdb::unique_ptr<TableFilterSet> table_filters;
        // map<idx_t, unique_ptr<TableFilter>> filters;
        // table_filters->filters = std::move(filters);
        ExtraOperatorInfo extra_info;
        duckdb::vector<Value> parameters;
        duckdb::vector<idx_t> projection_ids;

        auto &table_scan = physical_planner.Make<PhysicalTableScan>(
                types, scan_function, std::move(bind_data), types, column_ids, projection_ids, names,
                std::move(table_filters), 0, std::move(extra_info), std::move(parameters), std::move(virtual_columns));

        physical_plan = physical_planner.GetPhysicalPlan();
        physical_plan->SetRoot(table_scan);
      }
    }

    physical_plan->Root().Verify();
    result->physical_plan = std::move(physical_plan);
    result->properties = properties;
    result->names = res_names;
    result->types = result->physical_plan->Root().types;

    return result;
  }

  ExecutionResult DuckdbExec::PrepareExecutePlan(kwdbContext_p ctx) {
    ExecutionResult result;
    if (!connect_ || !connect_->context) {
      result.error_message = "database not open";
      return result;
    }

    auto attach_ret = AttachDBs();
    if (attach_ret == KStatus::FAIL) {
      result.error_message = "attach database failed";
      return result;
    }
    connect_->BeginTransaction();
    try {
      auto prepared = ConvertFlowToPhysicalPlan();
      if (!prepared->physical_plan) {
        connect_->Rollback();
        result.error_message = "convert physical plan failed";
        return result;
      }
      auto qurry_result = connect_->KWQuery(sql_, prepared);
      if (qurry_result->HasError()) {
        connect_->Rollback();
        //DetachDB(attach_ret);
        result.error_message = qurry_result->GetError();
        return result;
      } else {
        result.success = true;
        connect_->Commit();
      }
      char* encoding_buf_;
      k_uint32 encoding_len_;
      auto ret = Encoding(ctx, qurry_result.get(), encoding_buf_, encoding_len_);
      if (ret == KStatus::SUCCESS) {
        result.value = encoding_buf_;
        result.len = encoding_len_;
        result.row_count = qurry_result->RowCount();
      }
    } catch (const Exception& e) {
      result.error_message = e.what();
      connect_->Rollback();
      return result;
    }

   // DetachDB(attach_ret);
    return result;
  }

}