// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "libkwdbts2.h"
#include "st_instance.h"
#include "st_meta.h"
#include <th_kwdb_dynamic_thread_pool.h>
#include "payload_builder.h"
#include "sys_utils.h"
#include "ts_table_schema_manager.h"
#include "ts_payload.h"

extern DedupRule g_dedup_rule_;

namespace kwdbts {

StInstance* StInstance::st_inst_{nullptr};

RangeGroup test_range{1, 0};

uint64_t StInstance::rangeGroup() {
  return test_range.range_group_id;
}

KStatus StInstance::GetSchemaInfo(kwdbContext_p ctx, uint32_t table_id,
 std::vector<TagInfo>* tag_schema, std::vector<AttributeInfo>* data_schema) {
  if (params_.engine_version == "2") {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema;
    KStatus s = ts_engine_->GetTableSchemaMgr(ctx, table_id, schema);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    auto version = schema->GetCurrentVersion();
    std::shared_ptr<MMapMetricsTable> schema_tbl;
    s = schema->GetMetricSchema(ctx, version, &schema_tbl);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    *data_schema = schema_tbl->getSchemaInfoExcludeDropped();
    s = schema->GetTagAttr(version, *tag_schema);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    return KStatus::SUCCESS;
  } else {
    std::shared_ptr<kwdbts::TsTable> tags_table;
    KStatus s = ts_engine_->GetTsTable(ctx, table_id, tags_table);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    s = tags_table->GetTagSchema(ctx, test_range, tag_schema);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    s = tags_table->GetDataSchemaExcludeDropped(ctx, data_schema);
    return s;
  }
  return KStatus::FAIL;
}

void StInstance::ParseInputParams() {
  int pos = 0;
  int len = params_.engine_params.size();
  while (pos < len) {
    int pos_sec = params_.engine_params.find(",", pos);
    if (pos_sec == params_.engine_params.npos) {
      pos_sec = len;
    }
    std::string kv_pair = params_.engine_params.substr(pos, pos_sec - pos);
    {
      int key_idx = kv_pair.find(":", 0);
      std::string key = kv_pair.substr(0, key_idx);
      std::string value = kv_pair.substr(key_idx + 1, kv_pair.length());
      SetInputParams(key, value);
    }
    pos = pos_sec + 1;
  }
}

void StInstance::SetInputParams(const std::string& key, const std::string& value) {
  if (key == "KW_WAL_LEVEL") {
    ts_opts_.wal_level = atoi(value.c_str());
  } else if (key == "KW_IOT_INTERVAL") {
    setenv("KW_IOT_INTERVAL", value.c_str(), 1);
  } else if (key == "KW_PARTITION_ROWS") {
    setenv("KW_PARTITION_ROWS", value.c_str(), 0);
  } else if (key == "ts.dedup.rule") {
    if ("override" == value) {
      dedup_rule_ = kwdbts::DedupRule::OVERRIDE;
    } else if ("merge" == value) {
      dedup_rule_ = kwdbts::DedupRule::MERGE;
    } else if ("keep" == value) {
      dedup_rule_ = kwdbts::DedupRule::KEEP;
    } else if ("reject" == value) {
      dedup_rule_ = kwdbts::DedupRule::REJECT;
    } else if ("discard" == value) {
      dedup_rule_ = kwdbts::DedupRule::DISCARD;
    }
  } else {
    std::cout << "error: cannot parse key: " << key << ", value: " << value << std::endl;
  }
}

KBStatus StInstance::Init(BenchParams params, std::vector<uint32_t> table_ids_) {
  params_ = params;
  ParseInputParams();

  if (g_contet_p != nullptr) {
    return KBStatus::OK();
  }
  std::lock_guard<std::mutex> lk(mutex_);
  if (ts_engine_ != nullptr) {
    return KBStatus::OK();
  }

  g_contet_p = &g_context;
  if (kwdbts::InitServerKWDBContext(g_contet_p) != kwdbts::KStatus::SUCCESS) {
    return KBStatus::InternalError("InitServerKWDBContext ");
  }

  kwdbts::KStatus ret;

  std::string db_path;
  char* env_data_dir = getenv("DATA_DIR");
  if (env_data_dir != nullptr) {
    db_path = std::string(env_data_dir);
  } else {
    db_path = std::string(DATA_DIR);
  }

  if (!params.exist_db) {
    // clean old data
    int cmd_ret = system(("rm -rf " + db_path + "/*").c_str());
    assert(cmd_ret == 0);
  }

  // initialize TSEngine
  ts_opts_.engine_version = params_.engine_version.c_str();
  ts_opts_.wal_file_size = 64;
  ts_opts_.wal_file_in_group = 3;
  ts_opts_.wal_buffer_size = 4;
  ts_opts_.thread_pool_size = 0;
  ts_opts_.lg_opts.LogFileVerbosityThreshold = LgSeverity::INFO_K;
  auto index = new AppliedRangeIndex[1]{AppliedRangeIndex{1, 1}};
  TSStatus t_status = TSOpen(&ts_engine_, TSSlice{db_path.data(), db_path.size()}, ts_opts_, index, 1);
  if (t_status.data != nullptr) {
    KBStatus ks = KBStatus::InternalError("Open TSEngine error : " + string(t_status.data));
    delete t_status.data;
    return ks;
  }
  log_INFO("Open TSEngine %s \n", (t_status.data == nullptr ? "succeed" : "failed"));

  for (int i = 0; i <= table_ids_.size(); i++) {
    KTableKey table_id;
    if (i != table_ids_.size()) {
      table_id = table_ids_[i];
      if (snapshot_desc_table_id <= table_id) {
        snapshot_desc_table_id *= 2;
      }
    } else {
      // create one table to receive snapshot data.
      table_id = snapshot_desc_table_id;
    }
    // create ts table
    roachpb::CreateTsTable meta;
    StMetaBuilder::constructRoachpbTable(&meta, table_id, params_);
    table_metas.push_back(meta);
    std::vector<RangeGroup> ranges{test_range};
    KStatus s = ts_engine_->CreateTsTable(g_contet_p, table_id, &meta, ranges);
    if (s != kwdbts::KStatus::SUCCESS) {
      log_ERROR("CreateKObjectTable fail");
    }
    std::shared_ptr<TsTable> ts_table;
    s = ts_engine_->GetTsTable(g_contet_p, table_id, ts_table);
    if (s != kwdbts::KStatus::SUCCESS) {
      char buf[1024];
      (&g_contet_p->err_stack)->DumpToJson(buf, 1024);
      (&g_contet_p->err_stack)->Reset();
      return KBStatus::NOT_FOUND("GetTsTable : "  +std::to_string(table_id) );
    }

    // create table SQL
    stringstream buf;
    buf << " CREATE TABLE t_" << table_id << "( k_timestamp timestamp NOT NULL ";
    for (size_t j = 1 ; j < meta.k_column_size() ; j++) {
      if (meta.k_column(j).col_type() != ::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA){
        continue;
      }
      switch (meta.k_column(j).storage_type()) {
        case ::roachpb::DataType::TIMESTAMP:
          buf << ", c_" << j << " timestamp NOT NULL";
          break;
        case ::roachpb::DataType::BIGINT:
          buf << ", c_" << j << " INT8 NOT NULL";
          break;
        case ::roachpb::DataType::DOUBLE:
          buf << ", c_" << j << " DOUBLE NOT NULL";
          break;
        case ::roachpb::DataType::CHAR:
          buf << ", c_" << j << " char(" << meta.k_column(j).storage_len() << ")  NOT NULL";
          break;
        case ::roachpb::DataType::VARCHAR:
          buf << ", c_" << j << " varchar(" << meta.k_column(j).storage_len() << ")  NOT NULL";
          break;
        default:
          buf << ", c_" << j << " " << meta.k_column(j).storage_type() << "(" << meta.k_column(j).storage_len() << ")  NOT NULL";
          // return KBStatus::NOT_IMPLEMENTED("column type : "  +std::to_string(meta.k_column(j).storage_type()) );
          break;
      }
    }
    buf << ") attributes (" ;
    bool is_first_col = true;
    for (size_t j = 1 ; j < meta.k_column_size() ; j++) {
      if (meta.k_column(j).col_type() == ::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA){
        continue;
      }
      if (!is_first_col) {
        buf << ", ";
      }
      is_first_col = false;
      switch (meta.k_column(j).storage_type()) {
        case ::roachpb::DataType::TIMESTAMP:
          buf << "a_" << j << " timestamp NOT NULL";
          break;
        case ::roachpb::DataType::INT:
          buf << "a_" << j << " INT NOT NULL";
          break;
        case ::roachpb::DataType::BIGINT:
          buf << "a_" << j << " INT8 NOT NULL";
          break;
        case ::roachpb::DataType::DOUBLE:
          buf << "a_" << j << " DOUBLE NOT NULL";
          break;
        case ::roachpb::DataType::CHAR:
          buf << "a_" << j << " char(" << meta.k_column(j).storage_len() << ")  NOT NULL";
          break;
        case ::roachpb::DataType::VARCHAR:
          buf << "a_" << j << " varchar(" << meta.k_column(j).storage_len() << ")  NOT NULL";
          break;
        default:
          return KBStatus::NOT_IMPLEMENTED("column type : "  +std::to_string(meta.k_column(j).storage_type()) );
      }
    }
    buf << ") primary tags(" ;
    is_first_col = true;
    for (size_t j = 1 ; j < meta.k_column_size() ; j++) {
      if (meta.k_column(j).col_type() != ::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG){
        continue;
      }
      if (!is_first_col) {
        buf << ", ";
      }
      is_first_col = false;
      buf << "a_" << j;
    }
    buf << ")";
    
    fprintf(stdout , "****CREATE SQL[%d]: %s \n" , i, buf.str().c_str());
  }

  return KBStatus::OK();
}

StInstance::~StInstance() {
  if (ts_engine_ != nullptr) {
    TSStatus t_status = TSClose(ts_engine_);
    ts_engine_ = nullptr;
  }
  kwdbts::DestroyKWDBContext(g_contet_p);
  KWDBDynamicThreadPool::GetThreadPool().Stop();
//  int ret = system(("rm -rf " + db_path + "/*").c_str());
//  assert(ret == 0);
  printf("StInstance::~StInstance() OVER.");
}

std::shared_ptr<StEngityGroupInstance> StEngityGroupInstance::st_inst_ = nullptr;
std::shared_mutex StEngityGroupInstance::mutex_ = std::shared_mutex();

KBStatus StEngityGroupInstance::Init(BenchParams params) {
  std::lock_guard<std::shared_mutex> lk(mutex_);
  if (inited_) {
    return KBStatus::OK();
  }
  auto ret_status = StInstance::Get()->Init(params, vector<uint32_t>());
  if (!ret_status.isOK()) {
    return ret_status;
  }
  params_ = params;

  ctx = &g_context;
  if (kwdbts::InitServerKWDBContext(ctx) != kwdbts::KStatus::SUCCESS) {
    return KBStatus::InternalError("InitServerKWDBContext ");
  }

  // set storage directory
  EngineOptions::init();

  string db_path = normalizePath("entitygp_bench");
  string ws = rmPathSeperator(db_path);
  assert(!ws.empty());
  string dir_path = makeDirectoryPath(EngineOptions::home() + ws);
  bool ret = MakeDirectory(dir_path);
  assert(ret == true);

  roachpb::CreateTsTable meta;
  constructRoachpbTable(&meta, table_id_, params);
  std::vector<RangeGroup> ranges;
  ranges.push_back({456, 0});
  table_ = CreateTable(ctx, &meta, dir_path, ranges);

  KStatus s = table_->GetEntityGroup(ctx, range_group_id_, &entity_group_);
  assert(s == KStatus::SUCCESS);
  inited_ = true;
  return KBStatus::OK();
}

TsTable* CreateTable(kwdbts::kwdbContext_p ctx, roachpb::CreateTsTable* meta, std::string db_path,
                     const std::vector<RangeGroup>& range_groups) {
  std::vector<TagInfo> tag_schema;
  std::vector<AttributeInfo> metric_schema;
  KStatus s;
  for (int i = 0 ; i < meta->k_column_size() ; i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    s = TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i==0);
    assert(s == KStatus::SUCCESS);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_schema.push_back(std::move(TagInfo{col.column_id(), col_var.type,
                                            static_cast<uint32_t>(col_var.length),
                                            0, static_cast<uint32_t>(col_var.length),
                                            static_cast<TagType>(col_var.col_flag)}));
    } else {
      metric_schema.push_back(std::move(col_var));
    }
  }
  TsTableImpl* table = new TsTableImpl(ctx, db_path, meta->ts_table().ts_table_id());
  s = table->Create(ctx, metric_schema, meta->ts_table().partition_interval());
  assert(s == KStatus::SUCCESS);
  std::shared_ptr<TsEntityGroup> table_range;
  for (size_t i = 0; i < range_groups.size(); i++) {
    s = table->CreateEntityGroup(ctx, range_groups[i], tag_schema, &table_range);
    assert(s == KStatus::SUCCESS);
  }
  return table;
}

void constructRoachpbTable(roachpb::CreateTsTable* meta, uint64_t table_id, const BenchParams& params,
                           uint64_t partition_interval) {
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name("table_" + std::to_string(table_id));
  table->set_partition_interval(partition_interval);
  meta->set_allocated_ts_table(table);

  // first column name: k_timestamp
  roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::TIMESTAMPTZ);
  column->set_storage_len(16);
  column->set_column_id(1);
  column->set_name("k_timestamp");

  for (int i = 0; i < params.data_types.size(); i++) {
    column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(params.data_types[i]));
    column->set_storage_len(17);
    column->set_column_id(i + 2);
    column->set_name("column" + std::to_string(i + 2));
  }

  // add primary tag column
  column = meta->mutable_k_column()->Add();
  column->set_storage_type(roachpb::DataType::INT);
  column->set_storage_len(4);
  column->set_column_id(1);
  column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
  column->set_name("tag" + std::to_string(1));

  // add other tag column
  for (int i = 0; i < params.tag_types.size(); i++) {
    roachpb::KWDBKTSColumn* t_column = meta->mutable_k_column()->Add();
    t_column->set_storage_type((roachpb::DataType)(params.tag_types[i]));
    t_column->set_storage_len(17);
    t_column->set_column_id(2 + i);
    t_column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    t_column->set_name("tag" + std::to_string(i + 2));
  }
}

bool checkColValue(const std::vector<AttributeInfo>& data_schema, const ResultSet& res, int ret_cnt, int time_inc) {
  char* ts_bach_mem = reinterpret_cast<char*>(res.data.at(0)[0]->mem);
  for (size_t i = 1; i < data_schema.size(); i++) {
    const Batch* batch = res.data.at(i)[0];
    char* tmp = reinterpret_cast<char*>(batch->mem);
    char* batch_addr = tmp;
    for (size_t r = 0; r < ret_cnt; r++) {
      int value = ((KTimestamp(ts_bach_mem + r * 16)) / time_inc) % 11 + i;
      bool check_col_ok = true;
      switch (data_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          if (KTimestamp(tmp) != value) {
            check_col_ok = false;
          }
          tmp += 8;
          break;
        case DATATYPE::INT16:
          if (KInt16(tmp) != value) {
            check_col_ok = false;
          }
          tmp += 2;
          break;
        case DATATYPE::INT64:
          if (KInt64(tmp) != value) {
            check_col_ok = false;
          }
          tmp += 8;
          break;
        case DATATYPE::BOOL:
          if (KUint8(tmp) != value) {
            check_col_ok = false;
          }
          tmp += 1;
          break;
        case DATATYPE::CHAR:
          if (atoi(tmp) != value) {
            check_col_ok = false;
          }
          tmp += data_schema[i].size;
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY:
        {
          string val(reinterpret_cast<char*>(batch->getVarColData(r)), batch->getVarColDataLen(r));
          if (stoi(val) != value) {
            check_col_ok = false;
          }
          tmp += 8;
        }
          break;
        default:
          break;
      }
      if (!check_col_ok) {
        return false;
      }
    }
  }
  return true;
}

TSSlice genValue4Col(DATATYPE type, int size, int store_value) {
  char* addr = nullptr;
  size_t length = 0;
  string str_value;
  switch (type) {
    case DATATYPE::TIMESTAMP64_LSN:
      std::cout << "cannot run here . exit." << std::endl;
      exit(-1);
      break;
    case DATATYPE::TIMESTAMP64:
      length = sizeof(uint64_t);
      addr = new char[length];
      KTimestamp(addr) = store_value;
      break;
    case DATATYPE::INT32:
      addr = reinterpret_cast<char*>(new int32_t(store_value));
      length = sizeof(int32_t);
      break;
    case DATATYPE::INT16:
      length = sizeof(int16_t);
      addr = new char[length];
      KInt16(addr) = store_value;
      break;
    case DATATYPE::INT64:
      length = sizeof(int64_t);
      addr = new char[length];
      KInt64(addr) = store_value;
      break;
    case DATATYPE::BOOL:
      length = sizeof(int8_t);
      addr = new char[length];
      *(int8_t*)(addr) = store_value;
      break;
    case DATATYPE::CHAR:
      length = size;
      addr = new char[length];
      memset(addr, 0, length);
      str_value = intToString(store_value);
      strncpy(addr, str_value.c_str(), str_value.length());
      break;
    case DATATYPE::VARSTRING:
    case DATATYPE::VARBINARY:
      length = size;
      addr = new char[length];
      memset(addr, 0, length);
      str_value = intToString(store_value);
      strncpy(addr, str_value.c_str(), str_value.length());
      break;
    default:
      break;
  }
  return TSSlice{addr, length};
}

void genRowBasedPayloadData(std::vector<TagInfo> tag_schema, std::vector<AttributeInfo> data_schema, TSTableID table_id, uint32_t version,
 int32_t primary_tag, KTimestamp start_ts, int count, int time_inc, TSSlice *payload) {
  TSRowPayloadBuilder pay_build(tag_schema, data_schema, count);
  TSSlice pri_val = genValue4Col((DATATYPE)tag_schema[0].m_data_type, tag_schema[0].m_size, primary_tag);
  pay_build.SetTagValue(0, pri_val.data, pri_val.len);
  for (size_t i = 1; i < tag_schema.size(); i++) {
    int store_value = i + 10;
    TSSlice val = genValue4Col((DATATYPE)tag_schema[i].m_data_type, tag_schema[i].m_size, store_value);
    if (val.data) {
      pay_build.SetTagValue(i, val.data, val.len);
      delete[] val.data;
    }
  }

  for (size_t j = 0; j < count; j++) {
    for (size_t i = 0; i < data_schema.size(); i++) {
      char* addr = nullptr;
      int length = 0;
      if (data_schema[i].type == DATATYPE::TIMESTAMP64_LSN) {
        length = sizeof(uint64_t) * 2;
        addr = new char[length];
        KTimestamp(addr) = (start_ts + j * time_inc);
        KTimestamp(addr + 8) = 1;
      } else {
        int store_value = ((start_ts + j * time_inc) / time_inc) % 11 + i;
        TSSlice ret = genValue4Col((DATATYPE)data_schema[i].type, data_schema[i].size, store_value);
        addr = ret.data;
        length = ret.len;
      }
      
      if (addr) {
        pay_build.SetColumnValue(j, i, addr, length);
        delete[] addr;
        addr = nullptr;
      }
    }
  }
  pay_build.Build(table_id, version, payload);
}

void genPayloadData(std::vector<TagInfo> tag_schema, std::vector<AttributeInfo> data_schema,
 int32_t primary_tag, KTimestamp start_ts, int count, int time_inc, TSSlice *payload) {
  PayloadBuilder pay_build(tag_schema, data_schema);
  TSSlice pri_val = genValue4Col((DATATYPE)tag_schema[0].m_data_type, tag_schema[0].m_size, primary_tag);
  pay_build.SetTagValue(0, pri_val.data, pri_val.len);
  for (size_t i = 1; i < tag_schema.size(); i++) {
    int store_value = i + 10;
    TSSlice val = genValue4Col((DATATYPE)tag_schema[i].m_data_type, tag_schema[i].m_size, store_value);
    if (val.data) {
      pay_build.SetTagValue(i, val.data, val.len);
      delete[] val.data;
    }
  }
  pay_build.SetDataRows(count);

  for (size_t j = 0; j < count; j++) {
    for (size_t i = 0; i < data_schema.size(); i++) {
      char* addr = nullptr;
      int length = 0;
      if (data_schema[i].type == DATATYPE::TIMESTAMP64_LSN) {
        length = sizeof(uint64_t) * 2;
        addr = new char[length];
        KTimestamp(addr) = (start_ts + j * time_inc);
        KTimestamp(addr + 8) = 1;
      } else {
        int store_value = ((start_ts + j * time_inc) / time_inc) % 11 + i;
        TSSlice ret = genValue4Col((DATATYPE)data_schema[i].type, data_schema[i].size, store_value);
        addr = ret.data;
        length = ret.len;
      }
      
      if (addr) {
        pay_build.SetColumnValue(j, i, addr, length);
        delete[] addr;
        addr = nullptr;
      }
    }
  }
  pay_build.Build(payload);
}

}  // namespace kwdbts
