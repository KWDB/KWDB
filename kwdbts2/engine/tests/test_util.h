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
#include <unistd.h>
#include <string.h>
#include <gtest/gtest.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <iostream>
#include <any>
#include <string>
#include <vector>
#include <utility>
#include <random>
#include "data_type.h"
#include "utils/big_table_utils.h"
#include "sys_utils.h"

using namespace kwdbts;

#define Def_Column(col_var, pname, ptype, poffset, psize, plength, pencoding, pflag, pmax_len, pversion) \
                  struct AttributeInfo col_var;                                      \
                  {col_var.name = pname; col_var.type = ptype; col_var.offset = poffset; col_var.size = psize;} \
                  {col_var.length = plength; col_var.encoding = pencoding; col_var.flag = pflag;       \
                  col_var.max_len = pmax_len; col_var.version = pversion;}

#define DIR_SEP "/"

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
  static std::random_device rd;   // Declare static random device
  static std::mt19937 eng(rd());  // Declare static random number engine
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

static const k_uint32 g_testcase_hash_num = 2000;

// all kind of column types.
std::vector<ZTableColumnMeta> g_all_col_types({
  {roachpb::DataType::TIMESTAMP_NANO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
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
  {roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMPTZ, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMP_MICRO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMP_NANO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMPTZ_MICRO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  {roachpb::DataType::TIMESTAMPTZ_NANO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple},
  // {roachpb::DataType::SDECHAR, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple},
  // {roachpb::DataType::SDEVARCHAR, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple},
});

void ConstructColumnMetas(std::vector<ZTableColumnMeta>* metas, int col_num = 4) {
  int col_type_idx = 0;
  for (size_t i = 0; i < col_num; i++) {
    if (col_type_idx >= g_all_col_types.size()) {
      col_type_idx = 0;
    }
    metas->push_back(g_all_col_types[col_type_idx]);
    col_type_idx++;
  }
}
void ConstructTagMetas(std::vector<ZTableColumnMeta>* metas) {
  metas->push_back({roachpb::DataType::TIMESTAMP_NANO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 8, 32, roachpb::VariableLengthType::ColStorageTypeTuple});
}
void ConstructVarColumnMetas(std::vector<ZTableColumnMeta>* metas) {
  metas->push_back({roachpb::DataType::TIMESTAMP_NANO, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::CHAR, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 8, 32, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 8, 32, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARBINARY, 8, 32, roachpb::VariableLengthType::ColStorageTypeTuple});
}

void ConstructOffsetMetas(std::vector<ZTableColumnMeta>* metas) {
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::SMALLINT, 2, 2, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::FLOAT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::DOUBLE, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
}

void ConstructRoachpbTable(roachpb::CreateTsTable* meta, const KString& prefix_table_name, KTableKey table_id,
                           uint64_t partition_interval = kwdbts::EngineOptions::iot_interval, int col_num = 4) {
  // create table :  TIMESTAMP | SMALLINT | INT | VARCHAR | TIMESTAMP(PTAG) | INT(TAG) | VARCHAR(TAG)
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  table->set_partition_interval(partition_interval);
  table->set_ts_version(1);
  table->set_hash_num(g_testcase_hash_num);
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructColumnMetas(&col_meta, col_num);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first column name: k_timestamp
    } else {
      column->set_name("column" + std::to_string(i + 1));
    }
    column->set_nullable(true);
  }
  // add tag infos
  std::vector<ZTableColumnMeta> tag_metas;
  ConstructTagMetas(&tag_metas);
  for (int i = 0; i< tag_metas.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
    column->set_storage_len(tag_metas[i].storage_len);
    column->set_column_id(col_meta.size() + 1 + i);
    if (i == 0) {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
    } else {
      column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
    column->set_name("tag" + std::to_string(i + 1));
  }
}

void ConstructOffsetTable(roachpb::CreateTsTable* meta, const KString& prefix_table_name, KTableKey table_id,
                          uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
  // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  table->set_partition_interval(partition_interval);
  table->set_ts_version(1);
  table->set_hash_num(g_testcase_hash_num);
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructOffsetMetas(&col_meta);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");  // first column name: k_timestamp
    } else {
      column->set_name("column" + std::to_string(i + 1));
    }
    column->set_nullable(true);
  }
  // add tag infos
  std::vector<ZTableColumnMeta> tag_metas;
  ConstructTagMetas(&tag_metas);
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
    column->set_name("tag" + std::to_string(i + 1));
  }
}

KwTsSpan ConvertMsToPrecision(KwTsSpan& span, DATATYPE ts_type) {
  return {convertMSToPrecisionTS(span.begin, ts_type), convertMSToPrecisionTS(span.end, ts_type)};
}

void ConstructVarRoachpbTable(roachpb::CreateTsTable* meta, const KString& prefix_table_name, KTableKey table_id,
                              uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
  // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  table->set_partition_interval(partition_interval);
  table->set_hash_num(g_testcase_hash_num);
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructVarColumnMetas(&col_meta);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");\
    } else {
      column->set_name("column" + std::to_string(i + 1));
    }
  }
  std::vector<ZTableColumnMeta> tag_metas;
  ConstructTagMetas(&tag_metas);
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
    column->set_name("tag" + std::to_string(i + 1));
  }

}

class TestBigTableInstance : public ::testing::Test {
 public:
  static const string kw_home_;
  static const string db_name_;
  static const uint64_t iot_interval_;

 protected:
  virtual void SetUp() {
    setenv("KW_IOT_INTERVAL", std::to_string(iot_interval_).c_str(), 1);
    g_compress_interval = -1;
    EngineOptions::init();
    ErrorInfo err_info;
    // clear all data in db.
    system(("rm -rf " + kw_home_ + "default*").c_str());
    system(("rm -rf " + kw_home_ + DIR_SEP + db_name_ + "*").c_str());
    err_info.clear();
  }

  virtual void TearDown() {
  }

};


// payload
const static int g_header_size = Payload::header_size_;  // NOLINT
void GenPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                       KTimestamp start_ts, bool fix_primary_tag = true, int test_value = 0) {
  if (fix_primary_tag) {
    start_ts = 100;
  }
  char* primary_start_ptr = payload.GetPrimaryTagAddr();
  char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
  char* bitmap_ptr = payload.GetTagAddr();
  char* var_data_ptr = tag_data_start_ptr + (tag_schema.back().offset + tag_schema.back().size);
  std::string var_str = std::to_string(start_ts);
  int backup_test_value = test_value;
  for (int i = 0 ; i < tag_schema.size() ; i++) {
    // generating primary tag
    if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
       test_value = 0;
       switch (tag_schema[i].type) {
         case DATATYPE::TIMESTAMP64:
         case DATATYPE::TIMESTAMP64_NANO:
         case DATATYPE::TIMESTAMP64_MICRO:
             KTimestamp(primary_start_ptr) = start_ts;
             primary_start_ptr +=tag_schema[i].size;
           break;
         case DATATYPE::INT8:
             *(static_cast<k_int8*>(static_cast<void*>(primary_start_ptr))) = 10;
             primary_start_ptr += tag_schema[i].size;
           break;
         case DATATYPE::INT32:
             KInt32(primary_start_ptr) = start_ts;
             primary_start_ptr += tag_schema[i].size;
           break;
         case DATATYPE::VARSTRING:
             if (var_str.size() < tag_schema[i].size) {
               memcpy(primary_start_ptr, var_str.c_str(), var_str.size());
             } else {
               memcpy(primary_start_ptr, var_str.c_str(), tag_schema[i].size);
             }
             primary_start_ptr += tag_schema[i].size;
           break;
         default:
           break;
       }
    }
    // generating other tags
    switch (tag_schema[i].type) {
      case DATATYPE::TIMESTAMP64:
      case DATATYPE::TIMESTAMP64_NANO:
      case DATATYPE::TIMESTAMP64_MICRO:
          KTimestamp(tag_data_start_ptr) = start_ts + test_value;
          tag_data_start_ptr +=tag_schema[i].size;
        break;
      case DATATYPE::INT8:
          *(static_cast<k_int8*>(static_cast<void*>(tag_data_start_ptr))) = 10;
          tag_data_start_ptr += tag_schema[i].size;
        break;
      case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts + test_value;
          tag_data_start_ptr += tag_schema[i].size;
        break;
      case DATATYPE::VARSTRING:
        var_str = std::to_string(start_ts + test_value);
        if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
          memcpy(tag_data_start_ptr, var_str.c_str(), var_str.length());
          tag_data_start_ptr += tag_schema[i].size;
          break;
        }
        KInt64(tag_data_start_ptr) = var_data_ptr - bitmap_ptr;
        tag_data_start_ptr += tag_schema[i].size;
        KInt16(var_data_ptr) = var_str.length();
        memcpy(var_data_ptr + 2, var_str.c_str(), var_str.length());
        var_data_ptr += 2 + var_str.length();
        break;
      default:
        break;
    }
    test_value = backup_test_value;
  }
  return;
}

void fillTSIntoPayload(Payload &p, int col_id, DATATYPE type, int count,  bool rand, KTimestamp &start_ts, k_uint32 ms_interval) {
  KTimestamp precision = 1;
  bool is_lsn =false;
  switch (type) {
    case DATATYPE::TIMESTAMP:
    case DATATYPE::TIMESTAMP64:
      is_lsn = false;
      precision = 1;
      break;
    case DATATYPE::TIMESTAMP64_MICRO:
      is_lsn = false;
      precision = 1000;
      break;
    case DATATYPE::TIMESTAMP64_NANO:
      is_lsn = false;
      precision = 1000000;
      break;
    default:
      break;
  }
  if (!is_lsn) {
    for (int j = 0; j < count; j++) {
      KTimestamp(p.GetColumnAddr(j, col_id)) = start_ts * precision;
      start_ts += ms_interval;
    }
  } else {
    for (int j = 0; j < count; j++) {
      if (rand) {
        uint64_t r = GetRandomNumber(ms_interval);
        KTimestamp(p.GetColumnAddr(j, col_id)) = (start_ts + r) *precision;
        KTimestamp(p.GetColumnAddr(j, col_id) + 8) = (start_ts + r);
      } else {
        KTimestamp(p.GetColumnAddr(j, col_id)) = start_ts * precision;
        KTimestamp(p.GetColumnAddr(j, col_id) + 8) = 1;
        start_ts += ms_interval;
      }
    }
  }
}


void fillTSData(DATATYPE type,  KTimestamp m_ts, char* dest_ts) {
  KTimestamp precision = 1;
  bool is_lsn =false;
  switch (type) {
    case DATATYPE::TIMESTAMP:
    case DATATYPE::TIMESTAMP64:
      is_lsn = false;
      precision = 1;
      break;
    case DATATYPE::TIMESTAMP64_MICRO:
      is_lsn = false;
      precision = 1000;
      break;
    case DATATYPE::TIMESTAMP64_NANO:
      is_lsn = false;
      precision = 1000000;
      break;
    default:
      break;
  }
  if (!is_lsn) {
    KTimestamp(dest_ts) = m_ts * precision;
  } else {
    KTimestamp(dest_ts) = m_ts * precision;
    KTimestamp(dest_ts + 8) = 1;
  }
}

char* GenSomePayloadData(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length, KTimestamp start_ts,
                         roachpb::CreateTsTable* meta, k_uint32 ms_interval = 10, int test_value = 0,
                         bool fix_entity_id = true, bool random_ts = false) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  vector<uint32_t> actual_cols;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  k_int32 tag_value_len = 0;
  payload_length = 0;
  k_int32 tag_offset = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
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
  char* value = new char[data_length];
  memset(value, 0, data_length);
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
  GenPayloadTagData(p, tag_schema, start_ts, fix_entity_id, test_value);
  uint64_t var_exist_len = 0;
  for (int i = 0; i < schema.size(); i++) {
    if (isTsType((DATATYPE)schema[i].type)) {
      fillTSIntoPayload(p, i, (DATATYPE)schema[i].type, count, random_ts, start_ts, ms_interval);
      continue;
    }
    switch (schema[i].type) {
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
          KInt64(p.GetColumnAddr(j, i)) = 333333;
        }
        break;
      case DATATYPE::FLOAT:
        for (int j = 0; j < count; j++) {
          KFloat32(p.GetColumnAddr(j, i)) = 44.44;
        }
        break;
      case DATATYPE::DOUBLE:
        for (int j = 0; j < count; j++) {
          KDouble64(p.GetColumnAddr(j, i)) = 55.555;
        }
        break;
      case DATATYPE::CHAR:
        for (int j = 0; j < count; j++) {
          strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
        }
        break;
      case DATATYPE::BINARY:
        for (int j = 0; j < count; j++) {
          memcpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
        }
        break;
      case DATATYPE::VARSTRING:
      case DATATYPE::VARBINARY:
      {
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
        // std::cout<<"unsupport type: " << schema[i].type << std::endl;
        break;
    }
  }
  char * primaryKey = p.GetPrimaryTag().data;
  int len1 = p.GetPrimaryTag().len;
  uint16_t hashpoint = GetConsistentHashId(primaryKey, len1, g_testcase_hash_num);
  p.SetHashPoint(hashpoint);
  std::cout<<"set hashpoint " << hashpoint << std::endl;
  return value;
}

TSSlice GenSomePayloadDataRowBased(kwdbContext_p ctx, k_uint32 count, KTimestamp start_ts,
                         roachpb::CreateTsTable* meta,
                         k_uint32 ms_interval = 10, int test_value = 0, bool fix_entityid = true) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  k_int32 tag_value_len = 0;
  size_t row_tuple_length = 0;
  k_int32 tag_offset = 0;
  size_t row_based_tuple_size = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      col_var.offset = tag_offset;
      tag_offset += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        tag_value_len += (32 + 2);
      }
      tag_schema.emplace_back(std::move(col_var));
    } else {
      row_tuple_length += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        row_tuple_length += (test_str.size() + 2);
        row_based_tuple_size += sizeof(intptr_t);
      }else {
        row_based_tuple_size += col_var.size;
      }
      schema.push_back(std::move(col_var));
    }
  }

  k_uint32 header_len = g_header_size;
  k_int32 primary_tag_len = 8;
  k_int16 primary_len_len = 2;
  k_int32 tag_len_len = 4;
  k_int32 data_len_len = 4;
  tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
  k_int32 bitmap_len = (schema.size() + 7) / 8;
  k_int32 data_len = bitmap_len * count + 4 * count +  row_tuple_length * count;
  k_uint32 data_length =
      header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
  char* value = new char[data_length];
  TSSlice ret;
  ret.data = value;
  ret.len = data_length;

  memset(value, 0, data_length);
  k_uint32 ts_version = 1;
  if (meta->ts_table().has_ts_version()) {
    ts_version = meta->ts_table().ts_version();
  }
  KInt32(value + Payload::ts_version_offset_) = ts_version;
  KInt32(value + Payload::row_num_offset_) = count;
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
  std::vector<uint32_t> actual_cols;
  Payload p(schema, actual_cols, {value, data_length});
  int16_t len = 0;
  GenPayloadTagData(p, tag_schema, start_ts, fix_entityid, test_value);

  char* cur_row_mem = value + (data_length - data_len);
  for (size_t i = 0; i < count; i++) {
    size_t cur_row_len = row_based_tuple_size + bitmap_len;
    size_t cur_col_offset_in_row_mem = bitmap_len;
    for (int col = 0; col < schema.size(); col++) {
      size_t col_store_len = isVarLenType(schema[col].type) ? sizeof(intptr_t) : schema[col].size;
      char* column_addr = cur_row_mem + 4 + cur_col_offset_in_row_mem;
      cur_col_offset_in_row_mem += col_store_len;
      if (isTsType((DATATYPE)schema[col].type)) {
        fillTSData((DATATYPE)schema[col].type, start_ts, column_addr);
        start_ts += ms_interval;
        continue;
      }
      switch (schema[col].type) {
        case DATATYPE::INT16:
            KInt16(column_addr) = 11;
          break;
        case DATATYPE::INT32:
            KInt32(column_addr) = 2222;
          break;
        case DATATYPE::CHAR:
            strncpy(column_addr, test_str.c_str(), schema[col].size);

          break;
        case DATATYPE::BINARY:
            memcpy(column_addr, test_str.c_str(), schema[col].size);
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY:
        {
            len = test_str.size();
            KInt64(column_addr) = cur_row_len;
            // len + value
            KUint16(cur_row_mem + 4 + cur_row_len) = len;
            strncpy(cur_row_mem + 4 + cur_row_len + 2, test_str.c_str(), test_str.size());
            cur_row_len += (test_str.size() + 2);
        }
          break;
        default:
          break;
      }
    }
    KUint32(cur_row_mem) = cur_row_len;
    cur_row_mem += cur_row_len + 4;
  }

  return ret;
}

char* GenSomePayloadDataWithBigValue(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length, KTimestamp start_ts,
                                     roachpb::CreateTsTable* meta, k_uint32 ms_interval = 10, int test_value = 0,
                                     bool fix_entity_id = true) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  vector<uint32_t> actual_cols;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  k_int32 tag_value_len = 0;
  payload_length = 0;
  k_int32 tag_offset = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      col_var.offset = tag_offset;
      tag_offset += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        tag_value_len += (32 + 2);
      }
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
  char* value = new char[data_length];
  memset(value, 0, data_length);
  KInt32(value + Payload::row_num_offset_) = count;
  KUint32(value + Payload::ts_version_offset_) = 1;
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
  Payload p(schema, actual_cols, {value, data_length});
  int16_t len = 0;
  GenPayloadTagData(p, tag_schema, start_ts, fix_entity_id, test_value);
  uint64_t var_exist_len = 0;
  for (int i = 0; i < schema.size(); i++) {
    if (isTsType((DATATYPE)schema[i].type)) {
      fillTSIntoPayload(p, i, (DATATYPE)schema[i].type, count, false, start_ts, ms_interval);
      continue;
    }
    switch (schema[i].type) {
      case DATATYPE::INT16:
        for (int j = 0; j < count; j++) {
          KInt16(p.GetColumnAddr(j, i)) = INT16_MAX / count + 1;
        }
        break;
      case DATATYPE::INT32:
        for (int j = 0; j < count; j++) {
          KInt32(p.GetColumnAddr(j, i)) = INT32_MAX / count + 1;
        }
        break;
      case DATATYPE::CHAR:
        for (int j = 0; j < count; j++) {
          strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
        }
        break;
      case DATATYPE::BINARY:
        for (int j = 0; j < count; j++) {
          memcpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
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
        break;
      }
      default:
        break;
    }
  }
  return value;
}

void checkTSData(KTimestamp a_ts, KTimestamp e_ts, DATATYPE type) {
  KTimestamp precision = 1;
  bool is_lsn = false;
  switch (type) {
    case DATATYPE::TIMESTAMP:
    case DATATYPE::TIMESTAMP64:
      is_lsn = false;
      precision = 1;
      break;
    case DATATYPE::TIMESTAMP64_MICRO:
      is_lsn = false;
      precision = 1000;
      break;
    case DATATYPE::TIMESTAMP64_NANO:
      is_lsn = false;
      precision = 1000000;
      break;
    default:
      break;
  }
  ASSERT_EQ(a_ts, e_ts * precision);
}

void CheckgenSomePayloadData(kwdbContext_p ctx, Payload* payload, KTimestamp start_ts,
                             const std::vector<AttributeInfo>& schema, k_uint32 ms_interval = 10, int test_value = 0) {
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  for (size_t j = 0; j < schema.size(); j++) {
    for (size_t i = 0; i < payload->GetRowCount(); i++) {
      char* value = payload->GetColumnAddr(i, j);
      if (isTsType((DATATYPE)schema[j].type)) {
        checkTSData(KTimestamp(value), start_ts, (DATATYPE)schema[j].type);
        start_ts += ms_interval;
        continue;
      }
      switch (schema[j].type) {
        case DATATYPE::INT16:
          ASSERT_EQ(KInt16(value), 11);
          break;
        case DATATYPE::INT32:
          ASSERT_EQ(KInt32(value), 2222);
          break;
        case DATATYPE::CHAR:
          ASSERT_TRUE(memcmp(value, test_str.c_str(), schema[j].size) == 0);
          break;
        case DATATYPE::BINARY:
          ASSERT_TRUE(memcmp(value, test_str.c_str(), schema[j].size) == 0);
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY:
        {
          ASSERT_EQ(payload->GetVarColumnLen(i, j), test_str.size());
          int cmp_len = test_str.size();
          if (cmp_len < schema[j].size) {
            cmp_len = schema[j].size;
          }
          ASSERT_TRUE(memcmp(payload->GetVarColumnAddr(i, j) + 2, test_str.c_str(), cmp_len) == 0);
          break;
        }
        default:
          break;
      }
    }
  }
}

void CheckBatchData(kwdbContext_p ctx, ResultSet &rs, KTimestamp start_ts,
                    roachpb::CreateTsTable* meta, k_uint32 ms_interval = 10, int test_value = 0) {
  vector<AttributeInfo> schema;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      // tag_schema.emplace_back(std::move(col_var));
    } else {
      schema.push_back(std::move(col_var));
    }
  }

  for (size_t j = 0; j < schema.size(); j++) {
    const Batch* batch = rs.data[j][0];
    int cmp_len = 0;
    for (size_t i = 0; i < batch->count; i++) {
      char* value = reinterpret_cast<char*>(batch->mem) + i * schema[j].size;
      if (isTsType((DATATYPE)schema[i].type)) {
        checkTSData(KTimestamp(value), start_ts, (DATATYPE)schema[j].type);
        start_ts += ms_interval;
        continue;
      }
      switch (schema[j].type) {
        case DATATYPE::INT16:
          EXPECT_EQ(KInt16(value), 11);
          break;
        case DATATYPE::INT32:
          EXPECT_EQ(KInt32(value), 2222);
          break;
        case DATATYPE::CHAR:
          cmp_len = test_str.size();
          if (cmp_len > schema[j].size) {
            cmp_len = schema[j].size;
          }
          EXPECT_TRUE(memcmp(value, test_str.c_str(), cmp_len) == 0);
          break;
        case DATATYPE::BINARY:
          cmp_len = test_str.size();
          if (cmp_len > schema[j].size) {
            cmp_len = schema[j].size;
          }
          EXPECT_TRUE(memcmp(value, test_str.c_str(), cmp_len) == 0);
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY:
          EXPECT_GE(batch->getVarColDataLen(i), test_str.size());
          EXPECT_TRUE(memcmp(reinterpret_cast<char*>(batch->getVarColData(i)), test_str.c_str(), test_str.length()) == 0);
          break;
        default:
          break;
      }
    }
  }
}

char* GenPayloadDataWithNull(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length, KTimestamp start_ts,
                             roachpb::CreateTsTable* meta,
                             k_uint32 ms_interval = 10, int test_value = 0, bool fix_entityid = true) {
  vector<AttributeInfo> schema;
  vector<AttributeInfo> tag_schema;
  vector<uint32_t> actual_cols;
  string test_str = "abcdefghijklmnopqrstuvwxyz";
  k_int32 tag_value_len = 0;
  payload_length = 0;
  k_int32 tag_offset = 0;
  for (int i = 0; i < meta->k_column_size(); i++) {
    const auto& col = meta->k_column(i);
    struct AttributeInfo col_var;
    TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i == 0);
    if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
      tag_value_len += col_var.size;
      col_var.offset = tag_offset;
      tag_offset += col_var.size;
      if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
        tag_value_len += (32 + 2);
      }
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
  char* value = new char[data_length];
  memset(value, 0, data_length);
  KInt32(value + Payload::row_num_offset_) = count;
  KUint32(value + Payload::ts_version_offset_) = 1;
  // set primary_len_len
  KInt16(value + g_header_size) = primary_tag_len;
  // set tag_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
  // set data_len_len
  KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
  Payload p(schema, actual_cols, {value, data_length});
  int16_t len = 0;
  GenPayloadTagData(p, tag_schema, start_ts, fix_entityid);
  uint64_t var_exist_len = 0;
  for (int i = 0; i < schema.size(); i++) {
    if (isTsType((DATATYPE)schema[i].type)) {
      fillTSIntoPayload(p, i, (DATATYPE)schema[i].type, count, false, start_ts, ms_interval);
      continue;
    }
    switch (schema[i].type) {
      case DATATYPE::INT16:
        for (int j = 0; j < count; j++) {
          *(static_cast<k_int8*>(static_cast<void*>(p.GetColumnAddr(j, i)))) = 11;
        }
        break;
      case DATATYPE::INT32:
        for (int j = 0; j < count; j++) {
          int row_id = j;
          auto bitmap = (unsigned char*)(p.GetNullBitMapAddr(i));
          unsigned char bit_pos = (1 << (j & 7));
          row_id = row_id >> 3;
          bitmap[row_id] |= bit_pos;
        }
        break;
      case DATATYPE::VARSTRING:
      case DATATYPE::VARBINARY:
      {
        len = test_str.size();
        uint64_t var_type_offset = 0;
        for (int k = i; k < schema.size(); k++) {
          var_type_offset += (schema[k].size * count + bitmap_len);
        }
        for (int j = 0; j < count; j++) {
          if (j % 2 == 0) {
            int row_id = j;
            auto bitmap = (unsigned char*)(p.GetNullBitMapAddr(i));
            unsigned char bit_pos = (1 << (j & 7));
            row_id = row_id >> 3;
            bitmap[row_id] |= bit_pos;
            var_exist_len += (test_str.size() + 2);
            continue;
          }
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

void make_hashpoint(std::vector<k_uint32> *hps) {
  for (uint32_t i=0; i<g_testcase_hash_num; i++) {
    hps->push_back(i);
  }
}
