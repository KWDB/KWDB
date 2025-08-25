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

#include <gtest/gtest.h>
#include <string>
#include "ee_kwthd_context.h"
#include "ts_engine.h"
#include "ee_exec_pool.h"
#include "ee_op_engine_utils.h"

#include "../../ts_engine/tests/test_util.h"

extern DedupRule g_dedup_rule;

extern "C" {
// Tests are run in plain C++, we need a symbol for isCanceledCtx, normally
// implemented on the Go side.
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}  // extern "C"

namespace kwdbts {
const int row_num_per_payload = 1;
const int insert_batch = 1;
const int test_table_id = 800;
class OperatorTestBase : public ::testing::Test {
 public:
  static const string kw_home;
  static const string data_root;

  explicit OperatorTestBase() : table_id_(test_table_id) {
    g_dedup_rule = kwdbts::DedupRule::KEEP;
    system(("rm -rf " + kw_home).c_str());
    system(("rm -rf " + data_root).c_str());

    InitServerKWDBContext(ctx_);
    engine_ = CreateTestTsEngine(ctx_, data_root);
  }

  ~OperatorTestBase() override {
  }

 public:
  void CreateTable(roachpb::CreateTsTable& meta) {
    std::shared_ptr<TsTable> ts_table;
    KStatus s = engine_->CreateTsTable(ctx_, table_id_, &meta, ts_table);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }

  void InsertRecords(roachpb::CreateTsTable& meta) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id_, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    const std::vector<AttributeInfo>* metric_schema{nullptr};
    s = schema_mgr->GetMetricMeta(1, &metric_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    int entity_num = 3;
    int entity_rows = 10;
    int start_ts = 1000;
    auto pay_load = GenRowPayload(*metric_schema, tag_schema, table_id_, 1, entity_num, entity_rows, start_ts);
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
    s = engine_->PutData(ctx_, table_id_, 0, &pay_load, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    EXPECT_EQ(s, KStatus::SUCCESS);
    free(pay_load.data);
  }

 protected:
  void SetUp() override {
    ExecPool::GetInstance().Init(ctx_);
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, table_id_);
    CreateTable(meta);
    InsertRecords(meta);
  }

  void TearDown() override {
    ExecPool::GetInstance().Stop();
    CloseTestTsEngine(ctx_);
  }

  kwdbContext_t test_context;
  kwdbContext_p ctx_ = &test_context;
  TSEngineV2Impl* engine_{nullptr};
  KTableId table_id_{0};
};

const string OperatorTestBase::kw_home = "./test_db";
const string OperatorTestBase::data_root = "tsdb";

struct zTableColumnMeta {
  roachpb::DataType type;        // col type
  k_uint32 storage_len;  // length
  k_uint32 actual_len;   // the len of bytes stored
  roachpb::VariableLengthType storage_type;
};

class TSBSSchema {
 public:
  static void constructColumnMetas(std::vector<zTableColumnMeta>* metas) {
    // construct all col type
    metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::BIGINT, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  static void constructTagMetas(std::vector<zTableColumnMeta>* metas) {
    // construct all tag type
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::CHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
//      metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  static void constructTableMetadata(roachpb::CreateTsTable& meta, const KString& prefix_table_name,
                                     KTableKey table_id, uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id);
    table->set_table_name(prefix_table_name + std::to_string(table_id));
    table->set_partition_interval(partition_interval);
    table->set_hash_num(g_testcase_hash_num);
    meta.set_allocated_ts_table(table);

    std::vector<zTableColumnMeta> col_meta;
    constructColumnMetas(&col_meta);

    for (int i = 0; i < col_meta.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (col_meta[i].type));
      column->set_storage_len(col_meta[i].storage_len);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // first timestmap name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag
    std::vector<zTableColumnMeta> tag_metas;
    constructTagMetas(&tag_metas);
    for (int i = 0; i < tag_metas.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta.mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (tag_metas[i].type));
      column->set_storage_len(tag_metas[i].storage_len);
      column->set_column_id(tag_metas.size() + 1 + i);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }

  }

  const static int HEADER_SIZE = Payload::header_size_;  // NOLINT

  // generate tag data pay load with provided row data for multiple model processing
  static void genPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                                KTimestamp start_ts, vector<string>& row_data,
                                k_uint32 metric_col_count, bool fix_primary_tag = true) {
    if (fix_primary_tag) {
      start_ts = 100;
    }
    char* primary_start_ptr = payload.GetPrimaryTagAddr();
    char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
    for (int i = 0; i < tag_schema.size(); i++) {
      // Generate the Primaritag part
      if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
        switch (tag_schema[i].type) {
          case DATATYPE::TIMESTAMP64:
            KTimestamp(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT8:
            *(static_cast<k_int8*>(static_cast<void*>(primary_start_ptr))) = atoi(row_data[i + metric_col_count].c_str());
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT32:
            KInt32(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::CHAR:
            strcpy(primary_start_ptr, row_data[i + metric_col_count].c_str());
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::VARSTRING:
            strncpy(primary_start_ptr, row_data[i + metric_col_count].c_str(), row_data[i + metric_col_count].length());
            primary_start_ptr += tag_schema[i].size;
            break;
          default:
            break;
        }
      }
      // Generate the tag part
      switch (tag_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT8:
          *(static_cast<k_int8*>(static_cast<void*>(tag_data_start_ptr))) = atoi(row_data[i + metric_col_count].c_str());
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::CHAR:
          strcpy(tag_data_start_ptr, row_data[i + metric_col_count].c_str());
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::VARSTRING: {
          *reinterpret_cast<int16_t*>(tag_data_start_ptr) = ((int16_t) row_data[i + metric_col_count].length());
          tag_data_start_ptr += sizeof(int16_t);
          strncpy(tag_data_start_ptr, row_data[i + metric_col_count].c_str(), row_data[i + metric_col_count].length());
          tag_data_start_ptr += row_data[i + metric_col_count].length();
        }
          break;
        default:
          break;
      }
    }
  }

  // generate data pay load with provided row data for multiple model processing
  static unique_ptr<char[]> genPayloadData(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length,
                                           KTimestamp start_ts,
                                           roachpb::CreateTsTable& meta,
                                           vector<string>& row_data,
                                           k_uint32 ms_interval = 10, int test_value = 0,
                                           bool fix_entityid = true) {
    vector<AttributeInfo> schema;
    vector<uint32_t> actual_cols;
    vector<AttributeInfo> tag_schema;
    k_int32 tag_value_len = 0;
    payload_length = 0;
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      if (i == 0) {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, true);
      } else {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, false);
      }
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_value_len += col_var.size;
        tag_schema.emplace_back(std::move(col_var));
      } else {
        payload_length += col_var.size;
        if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
          payload_length += (row_data[i].size() + 2);
        }
        actual_cols.push_back(schema.size());
        schema.push_back(std::move(col_var));
      }
    }

    k_uint32 header_len = HEADER_SIZE;
    k_int32 primary_tag_len = 30;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = payload_length * count + bitmap_len * schema.size();
    k_uint32 data_length =
        header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
    payload_length = data_length;
    auto value = make_unique<char[]>(data_length);
    memset(value.get(), 0, data_length);
    KInt32(value.get() + Payload::row_num_offset_) = count;
    KUint32(value.get() + Payload::ts_version_offset_) = 1;
    // set primary_len_len
    KInt16(value.get() + HEADER_SIZE) = primary_tag_len;
    // set tag_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len) = tag_value_len;
    // set data_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
    Payload p(schema, actual_cols, {value.get(), data_length});
    int16_t len = 0;
    genPayloadTagData(p, tag_schema, start_ts, row_data, schema.size(), fix_entityid);
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::INT16:
          for (int j = 0; j < count; j++) {
            KInt16(p.GetColumnAddr(j, i)) = atoi(row_data[i].c_str());
          }
          break;
        case DATATYPE::INT32:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = atoi(row_data[i].c_str());
          }
          break;
        case DATATYPE::INT64:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = atol(row_data[i].c_str());
          }
          break;
        case DATATYPE::CHAR:
          for (int j = 0; j < count; j++) {
            strncpy(p.GetColumnAddr(j, i), row_data[i].c_str(), schema[i].size);
          }
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY: {
          len = row_data[i].length();
          uint64_t var_type_offset = 0;
          for (int k = i; k < schema.size(); k++) {
            var_type_offset += (schema[k].size * count + bitmap_len);
          }
          for (int j = 0; j < count; j++) {
            KInt64(p.GetColumnAddr(j, i)) = var_type_offset + var_exist_len;
            // len + value
            memcpy(p.GetVarColumnAddr(j, i), &len, 2);
            strncpy(p.GetVarColumnAddr(j, i) + 2, row_data[i].c_str(), row_data[i].size());
            var_exist_len += (row_data[i].size() + 2);
          }
        }
          break;
        default:
          break;
      }
    }
     return value;
  }

  static void genPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                                KTimestamp start_ts, bool fix_primary_tag = true) {
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    if (fix_primary_tag) {
      start_ts = 100;
    }
    char* primary_start_ptr = payload.GetPrimaryTagAddr();
    char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
    for (int i = 0; i < tag_schema.size(); i++) {
      // Generate the Primaritag part
      if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
        switch (tag_schema[i].type) {
          case DATATYPE::TIMESTAMP64:
            KTimestamp(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT8:
            *(static_cast<k_int8*>(static_cast<void*>(primary_start_ptr))) = 10;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT32:
            KInt32(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::CHAR:
            memset(primary_start_ptr, '1', tag_schema[i].size);
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::VARSTRING: {
            strncpy(primary_start_ptr, test_str.c_str(), test_str.length());
            primary_start_ptr += tag_schema[i].size;
          }
            break;
          default:
            break;
        }
      }
      // Generate the tag part
      switch (tag_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT8:
          *(static_cast<k_int8*>(static_cast<void*>(tag_data_start_ptr))) = 10;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::CHAR:
          memset(tag_data_start_ptr, '1', tag_schema[i].size);
          memset(tag_data_start_ptr + tag_schema[i].size - 1, '\0', 1);
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::VARSTRING: {
          *reinterpret_cast<int16_t*>(tag_data_start_ptr) = ((int16_t) test_str.length());
          tag_data_start_ptr += sizeof(int16_t);
          strncpy(tag_data_start_ptr, test_str.c_str(), test_str.length());
          tag_data_start_ptr += test_str.length();
        }
          break;
        default:
          break;
      }
    }
  }

  static unique_ptr<char[]> genPayloadData(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length,
                                           KTimestamp start_ts,
                                           roachpb::CreateTsTable& meta,
                                           k_uint32 ms_interval = 10, int test_value = 0,
                                           bool fix_entityid = true) {
    vector<AttributeInfo> schema;
    vector<uint32_t> actual_cols;
    vector<AttributeInfo> tag_schema;
    k_int32 tag_value_len = 0;
    payload_length = 0;
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      if (i == 0) {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, true);
      } else {
        TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, false);
      }
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_value_len += col_var.size;
        tag_schema.emplace_back(std::move(col_var));
      } else {
        payload_length += col_var.size;
        if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
          payload_length += (test_str.size() + 2);
        }
        actual_cols.push_back(schema.size());
        schema.push_back(std::move(col_var));
      }
    }

    k_uint32 header_len = HEADER_SIZE;
    k_int32 primary_tag_len = 30;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = payload_length * count + bitmap_len * schema.size();
    k_uint32 data_length =
        header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
    payload_length = data_length;
    auto value = make_unique<char[]>(data_length);
    memset(value.get(), 0, data_length);
    KInt32(value.get() + Payload::row_num_offset_) = count;
    KUint32(value.get() + Payload::ts_version_offset_) = 1;
    // set primary_len_len
    KInt16(value.get() + HEADER_SIZE) = primary_tag_len;
    // set tag_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len) = tag_value_len;
    // set data_len_len
    KInt32(value.get() + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
    Payload p(schema, actual_cols, {value.get(), data_length});
    int16_t len = 0;
    genPayloadTagData(p, tag_schema, start_ts, fix_entityid);
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::INT16:
          for (int j = 0; j < count; j++) {
            KInt16(p.GetColumnAddr(j, i)) = 11;
          }
          break;
        case DATATYPE::INT32:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = 2222;
          }
          break;
        case DATATYPE::INT64:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = 33333;
          }
          break;
        case DATATYPE::CHAR:
          for (int j = 0; j < count; j++) {
            strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
          }
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY: {
          len = test_str.size();
          uint64_t var_type_offset = 0;
          for (int k = i; k < schema.size(); k++) {
            var_type_offset += (schema[k].size * count + bitmap_len);
          }
          for (int j = 0; j < count; j++) {
            KInt64(p.GetColumnAddr(j, i)) = var_type_offset + var_exist_len;
            // len + value
            memcpy(p.GetVarColumnAddr(j, i), &len, 2);
            strncpy(p.GetVarColumnAddr(j, i) + 2, test_str.c_str(), test_str.size());
            var_exist_len += (test_str.size() + 2);
          }
        }
          break;
        default:
          break;
      }
    }
    return value;
  }
};
}
