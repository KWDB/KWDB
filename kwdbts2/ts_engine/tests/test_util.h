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

#include <dirent.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>
#include <gtest/gtest.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <any>
#include <string>
#include <vector>
#include <utility>
#include <random>
#include "data_type.h"
#include "libkwdbts2.h"
#include "lt_latch.h"
#include "me_metadata.pb.h"
#include "ts_coding.h"
#include "ts_payload.h"
#include "utils/big_table_utils.h"
#include "sys_utils.h"
#include "column_utils.h"
#include "payload.h"

using namespace kwdbts;

#define Def_Column(col_var, pname, ptype, poffset, psize, plength, pencoding, pflag, pmax_len, pversion) \
                  struct AttributeInfo col_var;                                      \
                  {col_var.name = pname; col_var.type = ptype; col_var.offset = poffset; col_var.size = psize;} \
                  {col_var.length = plength; col_var.encoding = pencoding; col_var.flag = pflag;       \
                  col_var.max_len = pmax_len; col_var.version = pversion;}

#define DIR_SEP "/"

static const k_uint32 g_testcase_hash_num = 2000;

int IsDbNameValid(const string& db) {
  if (db.size() > MAX_DATABASE_NAME_LEN)  // can`t longer than 63
    return KWELENLIMIT;
  for (size_t i = 0 ; i < db.size() ; ++i) {
    char c = db[i];
    if (c == '?' || c == '*' || c == ':' || c == '|' || c == '"' || c == '<'
        || c == '>' || c == '.')
      return KWEINVALIDNAME;
  }
  return 0;
}

bool RemoveDirectory(const char* path) {
  DIR* dir = opendir(path);
  if (dir == nullptr) {
    return false;
  }

  dirent* entry;
  while ((entry = readdir(dir)) != nullptr) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
      continue;
    }

    std::string full_path = std::string(path) + "/" + entry->d_name;
    struct stat file_stat{};
    if (stat(full_path.c_str(), &file_stat) != 0) {
      closedir(dir);
      return false;
    }
    if (S_ISDIR(file_stat.st_mode)) {
      if (!RemoveDirectory(full_path.c_str())) {
        closedir(dir);
        return false;
      }
    } else {
      if (remove(full_path.c_str()) != 0) {
        closedir(dir);
        return false;
      }
    }
  }
  closedir(dir);
  if (rmdir(path) != 0) {
    return false;
  }

  return true;
}

uint64_t GetRandomNumber(uint64_t max) {
  static std::mt19937 eng(2);  // Declare static random number engine
  // Define the range
  std::uniform_int_distribution<> distr(0, max);
  // Generate and return the random number
  return distr(eng);
}

class BtUtil {
 public:

  static int CheckError(const char* msg, ErrorInfo err_info) {
    if (err_info.errcode != 0) {
      fprintf(stderr, "%s : %s\n", msg, err_info.errmsg.c_str());
    }
    return err_info.errcode;
  }
};

struct ZTableColumnMeta {
  roachpb::DataType type;
  k_uint32 storage_len;
  k_uint32 actual_len;
  roachpb::VariableLengthType storage_type;
};

const k_uint32 g_testcase_col_count = 20;

// all kind of column types.
std::vector<ZTableColumnMeta> g_all_col_types({
  {roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::SMALLINT, 2, 2, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::VARCHAR, 8, 19, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::BIGINT, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::FLOAT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::DOUBLE, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::BOOL, 1, 1, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::CHAR, 13, 13, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::BINARY, 14, 14, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::NCHAR, 17, 17, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::NVARCHAR, 8, 21, roachpb::VariableLengthType::ColStorageTypeTuple},  // 11
  {roachpb::DataType::VARBINARY, 8, 23, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMPTZ, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
});

void ConstructRoachpbTable(roachpb::CreateTsTable* meta, KTableKey table_id, uint32_t db_id = 1) {
  // create table :  TIMESTAMP | INT | DOUBLE
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name("tbl_" + std::to_string(table_id));
  table->set_partition_interval(86400);
  table->set_ts_version(1);
  table->set_database_id(db_id);
  table->set_hash_num(2000);
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  col_meta.push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  col_meta.push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
  col_meta.push_back({roachpb::DataType::DOUBLE, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first column name: k_timestamp
    } else {
      column->set_name("column_" + std::to_string(i + 1));
    }
    column->set_nullable(true);
  }

  // add tag infos
  std::vector<ZTableColumnMeta> tag_metas;
  tag_metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  tag_metas.push_back({roachpb::DataType::VARCHAR, 3000, 3000, roachpb::VariableLengthType::ColStorageTypeTuple});
  tag_metas.push_back({roachpb::DataType::VARCHAR, 200, 200, roachpb::VariableLengthType::ColStorageTypeTuple});
  tag_metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});

  for (int i = 0; i< tag_metas.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
    column->set_storage_len(tag_metas[i].storage_len);
    column->set_column_id(tag_metas.size() + 1 + i);
    if (i % 2 == 0) {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
    } else {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
    column->set_name("tag_" + std::to_string(i + 1));
  }
}

// payload
const static int g_header_size = Payload::header_size_;  // NOLINT
void GenPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                       KTimestamp start_ts, bool fix_primary_tag = true) {
  if (fix_primary_tag) {
    start_ts = 100;
  }
  char* primary_start_ptr = payload.GetPrimaryTagAddr();
  char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
  char* bitmap_ptr = payload.GetTagAddr();
  char* var_data_ptr = tag_data_start_ptr + (tag_schema.back().offset + tag_schema.back().size);
  std::string var_str = std::to_string(start_ts);
  for (int i = 0 ; i < tag_schema.size() ; i++) {
    // generating primary tag
    if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
       switch (tag_schema[i].type) {
         case DATATYPE::TIMESTAMP64:
             KTimestamp(primary_start_ptr) = start_ts;
             primary_start_ptr +=tag_schema[i].size;
           break;
         default:
           break;
       }
    }
    // generating other tags
    switch (tag_schema[i].type) {
      case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
        break;
      default:
        break;
    }
  }
  return;
}

TSSlice GenSomePayloadData(kwdbContext_p ctx, k_uint32 count, KTimestamp start_ts,
                         roachpb::CreateTsTable* meta, k_uint32 ms_interval = 10) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  vector<uint32_t> actual_cols;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  k_int32 tag_value_len = 0;
  uint32_t payload_length = 0;
  k_int32 tag_offset = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    ParseToAttributeInfo(col, col_var, i == 0);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      col_var.offset = tag_offset;
      tag_offset += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        tag_value_len += (32 + 2);
      }
      tag_schema.emplace_back(std::move(col_var));
    } else if (!col.dropped()) {
      payload_length += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        payload_length += (test_str.size() + 2);
      }
      actual_cols.push_back(schema.size());
      schema.push_back(std::move(col_var));
    }
  }

  k_uint32 header_len = g_header_size;
  k_int32 primary_tag_len = 8;
  k_int16 primary_len_len = 2;
  k_int32 tag_len_len = 4;
  k_int32 data_len_len = 4;
  k_int32 bitmap_len = (count + 7) / 8;
  tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
  k_int32 data_len = payload_length * count + bitmap_len * schema.size();
  k_uint32 data_length =
      header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
  payload_length = data_length;
  char* value = reinterpret_cast<char*>(malloc(data_length));
  memset(value, 0, data_length);
  TSSlice ret{value, payload_length};
  KInt32(value + Payload::row_num_offset_) = count;
  if (meta->ts_table().ts_version() == 0) {
    KUint32(value + Payload::ts_version_offset_) = 1;
  } else {
    KUint32(value + Payload::ts_version_offset_) = meta->ts_table().ts_version();
  }
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
  Payload p(schema, actual_cols, {value, data_length});
  int16_t len = 0;
  GenPayloadTagData(p, tag_schema, start_ts, true);
  uint64_t var_exist_len = 0;
  for (int i = 0; i < schema.size(); i++) {
    switch (schema[i].type) {
      case DATATYPE::TIMESTAMP64:
        for (int j = 0; j < count; j++) {
          KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
          start_ts += ms_interval;
        }
        break;
      case DATATYPE::INT64:
        for (int j = 0; j < count; j++) {
          KInt64(p.GetColumnAddr(j, i)) = 333333;
        }
        break;
      case DATATYPE::DOUBLE:
        for (int j = 0; j < count; j++) {
          KDouble64(p.GetColumnAddr(j, i)) = 55.555;
        }
        break;
      default:
        // std::cout<<"unsupport type: " << schema[i].type << std::endl;
        break;
    }
  }
  return ret;
}

void ConstructRoachpbTableWithTypes(roachpb::CreateTsTable* meta, KTableKey table_id,
                                    const std::vector<roachpb::DataType>& metric_col) {
  roachpb::KWDBTsTable* table = new roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name("tbl_" + std::to_string(table_id));
  table->set_database_id(1);
  table->set_partition_interval(86400);
  table->set_ts_version(1);
  table->set_hash_num(2000);
  meta->set_allocated_ts_table(table);

  for (int i = 0; i < metric_col.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((metric_col[i]));
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first column name: k_timestamp
    } else {
      column->set_name("column_" + std::to_string(i + 1));
    }
    column->set_nullable(true);
  }
  // add tag infos
  std::vector<ZTableColumnMeta> tag_metas;
  tag_metas.push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  for (int i = 0; i< tag_metas.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
    column->set_storage_len(tag_metas[i].storage_len);
    column->set_column_id(tag_metas.size() + 1 + i);
    if (i == 0) {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
    } else {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
    column->set_name("tag_" + std::to_string(i + 1));
  }
}

struct SliceGuard {
  TSSlice result;
  std::string guard;
};

TSSlice GenRowPayload(const std::vector<AttributeInfo>& metric, const std::vector<TagInfo>& tag, TSTableID table_id, uint32_t version, TSEntityID dev_id, int num, KTimestamp ts, KTimestamp interval = 1000) {
  TSRowPayloadBuilder builder(tag, metric, num);
  builder.SetTagValue(0, (char*)(&dev_id), sizeof(dev_id));
  string var_str = intToString(dev_id);
  for (size_t i = 1; i < tag.size(); i++) {
    if (tag[i].m_data_type == DATATYPE::VARSTRING) {
      builder.SetTagValue(i, var_str.data(), var_str.length());
    } else {
      builder.SetTagValue(i, (char*)(&dev_id), tag[i].m_size);
    }
  }

  timestamp64 cur_ts = ts;
  for (size_t i = 0; i < num; i++) {
    for (int j = 0; j < metric.size(); ++j) {
      switch (metric[j].type) {
        case DATATYPE::TIMESTAMP:
        case DATATYPE::TIMESTAMP64: {
          builder.SetColumnValue(i, j, (char*)(&cur_ts), sizeof(cur_ts));
          break;
        }
        case DATATYPE::INT32: {
          int data = GetRandomNumber(1024);
          builder.SetColumnValue(i, j, (char*)(&data), sizeof(data));
          break;
        }
        case DATATYPE::INT64: {
          int data = GetRandomNumber(10240);
          builder.SetColumnValue(i, j, (char*)(&data), sizeof(data));
          break;
        }
        case DATATYPE::FLOAT: {
          float data = GetRandomNumber(1024 * 1024);
          builder.SetColumnValue(i, j, (char*)(&data), sizeof(data));
          break;
        }
        case DATATYPE::DOUBLE: {
          double data = GetRandomNumber(1024 * 1024);
          builder.SetColumnValue(i, j, (char*)(&data), sizeof(data));
          break;
        }
        case ::roachpb::DataType::VARCHAR: {
          char buf[128];
          std::sprintf(buf, "varstring_%lu_%" PRIu64, i, GetRandomNumber(100000));
          builder.SetColumnValue(i, j, buf, string(buf).size());
          break;
        }
        default:
          assert(false);
      }
    }
    cur_ts += interval;
  }
  TSSlice payload{nullptr, 0};
  builder.Build(table_id, version, &payload);
  return payload;
}

namespace kwtest {
class PayloadBuilder {
 private:
  const std::vector<AttributeInfo>& metric_;
  const std::vector<TagInfo>& tag_;

 public:
  explicit PayloadBuilder(const std::vector<AttributeInfo>& metric, const std::vector<TagInfo>& tag)
      : metric_(metric), tag_(tag) {}
};
}  // namespace kwtest

void make_hashpoint(std::unordered_set<k_uint32> *hps) {
  for (uint32_t i=0; i< g_testcase_hash_num; i++) {
    hps->insert(i);
  }
}

KwTsSpan ConvertMsToPrecision(KwTsSpan& span, DATATYPE ts_type) {
  return {convertMSToPrecisionTS(span.begin, ts_type), convertMSToPrecisionTS(span.end, ts_type)};
}