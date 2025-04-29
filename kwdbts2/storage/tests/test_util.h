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
#include <gtest/gtest.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <iostream>
#include <any>
#include <cstring>
#include <utility>
#include <string>
#include <vector>
#include "utils/big_table_utils.h"
#include "ts_time_partition.h"
#include "payload.h"
#include "cm_func.h"
#include "sys_utils.h"

// change char* memory to k_double64
#define KDouble64(buf) (*(static_cast<double *>(static_cast<void *>(buf))))

#define Def_Column(col_var, col_id, pname, ptype, poffset, psize, plength, pencoding, \
       pflag, pmax_len, pversion, pattrtype)      \
    struct AttributeInfo col_var;          \
    {col_var.id = col_id; strcpy(col_var.name, pname); col_var.type = ptype; col_var.offset = poffset; }  \
    {col_var.size = psize; col_var.length = plength; }      \
    {col_var.encoding = pencoding; col_var.flag = pflag; }      \
    {col_var.max_len = pmax_len; col_var.version = pversion; }      \
    {col_var.col_flag = (ColumnFlag)pattrtype; }

#define Def_Tag(tag_var, id, data_type, data_length, tag_type)    \
    struct TagInfo tag_var;          \
    {tag_var.m_id = id; tag_var.m_data_type = data_type; tag_var.m_length = data_length; }  \
    {tag_var.m_size = data_length; tag_var.m_tag_type = tag_type; }

#define STORE_HOME (BigObjectConfig::home())
#define DIR_SEP "/"

int IsDbNameValid(const string& db) {
  if (db.size() > MAX_DATABASE_NAME_LEN)  // can`t longer than 63
    return KWELENLIMIT;
  for (char c : db) {
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

struct ZTableColumnMeta {
  roachpb::DataType type;
  uint32_t storage_len;
  uint32_t actual_len;
  roachpb::VariableLengthType storage_type;
};

void ConstructColumnMetas(std::vector<ZTableColumnMeta>* metas) {
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::SMALLINT, 2, 2, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
}
void ConstructTagMetas(std::vector<ZTableColumnMeta>* metas) {
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 8, 32, roachpb::VariableLengthType::ColStorageTypeTuple});
}

void ConstructRoachpbTable(roachpb::CreateTsTable* meta, const std::string& prefix_table_name, uint64_t table_id) {
  // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
  roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
  table->set_ts_table_id(table_id);
  table->set_table_name(prefix_table_name + std::to_string(table_id));
  meta->set_allocated_ts_table(table);

  std::vector<ZTableColumnMeta> col_meta;
  ConstructColumnMetas(&col_meta);

  for (int i = 0; i < col_meta.size(); i++) {
    roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
    column->set_storage_type((roachpb::DataType)(col_meta[i].type));
    column->set_storage_len(col_meta[i].storage_len);
    column->set_column_id(i + 1);
    if (i == 0) {
      column->set_name("k_timestamp");
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

class BtUtil {
 public:
  static char* GenRawData(BigTable* bt, std::vector<std::any>& batch) {
    DataHelper* rec_helper = bt->getRecordHelper();
    int rec_size = bt->recordSize();
    char* rec = new char[rec_size];
    int start_col = bt->firstDataColumn();
    int num_col = bt->numColumn();
    int col_idx = start_col;
    for (int i = 0; i < batch.size(); i++) {
      if (col_idx >= num_col) {
        break;
      }
      int d_type = bt->getSchemaInfo()[col_idx].type;
      switch (d_type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(rec_helper->columnAddr(col_idx, rec)) = std::any_cast<timestamp64>(batch[i]);
          break;
        case DATATYPE::TIMESTAMP64_LSN: {
          auto ts_lsn = std::any_cast<TimeStamp64LSN>(batch[i]);
          memcpy(rec_helper->columnAddr(col_idx, rec), &ts_lsn, sizeof(TimeStamp64LSN));
          break;
        }
        case DATATYPE::INT64:
          KInt64(rec_helper->columnAddr(col_idx, rec)) = std::any_cast<int64_t>(batch[i]);
          break;
        case DATATYPE::DOUBLE:
          KDouble64(rec_helper->columnAddr(col_idx, rec)) = std::any_cast<double>(batch[i]);
          break;
        default:
          fprintf(stderr, "unsupported data type:%d \n", d_type);
      }
      col_idx++;
    }
    for (; col_idx < num_col; ++col_idx) {
      if (rec_helper->stringToColumn(col_idx, const_cast<char*>(kwdbts::s_emptyString.c_str()),
                                     rec) < 0)
        break;
    }
    return rec;
  }

  const static int header_size_ = kwdbts::Payload::header_size_;  // NOLINT
  static char* GenSomePayloadData(const std::vector<AttributeInfo>& schema, const std::vector<uint32_t>& actual_cols,
                                  uint32_t count, uint32_t& payload_length, KTimestamp start_ts, bool set_null = false,
                                  int insert_value = 11) {
    uint32_t ms_interval = 10;
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    int32_t tag_value_len = 8;
    payload_length = 0;
    for (int i = 0; i < schema.size(); i++) {
      payload_length += schema[i].size;
      if (schema[i].type == DATATYPE::VARSTRING || schema[i].type == DATATYPE::VARBINARY) {
        payload_length += (test_str.size() + 2);
      }
    }

    uint32_t header_len = header_size_;
    int32_t primary_tag_len = 8;
    int16_t primary_len_len = 2;
    int32_t tag_len_len = 4;
    int32_t data_len_len = 4;
    int32_t bitmap_len = (count + 7) / 8;
    tag_value_len += (1 + 7) / 8;  // tag bitmap
    int32_t data_len = payload_length * count + bitmap_len * schema.size();
    uint32_t data_length =
        header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
    payload_length = data_length;
    char* value = new char[data_length];
    memset(value, 0, data_length);
    KInt32(value + kwdbts::Payload::row_num_offset_) = count;
    KUint32(value + kwdbts::Payload::ts_version_offset_) = 1;
    // set primary_len_len
    KInt16(value + header_size_) = primary_tag_len;
    // set tag_len_len
    KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
    // set data_len_len
    KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
    kwdbts::Payload p(schema, actual_cols, {value, data_length});
    int16_t len = 0;
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::TIMESTAMP64_LSN:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::INT64:
          if (set_null) {
            for (int j = 0; j < count; j++) {
              int row_id = j;
              auto bitmap = (unsigned char*)(p.GetNullBitMapAddr(i));
              unsigned char bit_pos = (1 << (j & 7));
              row_id = row_id >> 3;
              bitmap[row_id] |= bit_pos;
            }
          } else {
            for (int j = 0; j < count; j++) {
              KInt64(p.GetColumnAddr(j, i)) = insert_value;
            }
          }
          break;
        case DATATYPE::DOUBLE:
          for (int j = 0; j < count; j++) {
            KDouble64(p.GetColumnAddr(j, i)) = 2222.2;
          }
          break;
        case DATATYPE::CHAR:
          for (int j = 0; j < count; j++) {
            strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
          }
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY: {
          if (set_null) {
            for (int j = 0; j < count; j++) {
              int row_id = j;
              auto bitmap = (unsigned char*) (p.GetNullBitMapAddr(i));
              unsigned char bit_pos = (1 << (j & 7));
              row_id = row_id >> 3;
              bitmap[row_id] |= bit_pos;
            }
          } else {
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
        }
          break;
        default:
          break;
      }
    }
    return value;
  }

  static int CreateDB(const std::string& db, size_t life_cycle, ErrorInfo& err_info) {
    string db_path = normalizePath(db);
    string ws = rmPathSeperator(db_path);
    if (ws == "")
      return err_info.setError(KWEINVALIDNAME, db);
    int err_code = IsDbNameValid(ws);
    if (err_code != 0)
      err_info.setError(err_code, db);
    string dir_path = makeDirectoryPath(kwdbts::EngineOptions::home() + ws);
    MakeDirectory(dir_path, err_info);
    return err_info.errcode;
  }

  static MMapMetricsTable* CreateRootTable(const std::string& tbl_sub_path1, const std::string& db_path, std::string&
  table_name, int encoding, ErrorInfo& err_info) {
    encoding = encoding | NO_DEFAULT_TABLE;
    Def_Column(col_1, 1, "k_ts", DATATYPE::TIMESTAMP64, 0, 0, 1, 0, AINFO_NOT_NULL, 3, 1, 0);
    Def_Column(col_2, 2, "v1_value", DATATYPE::INT64, 0, 0, 1, 0, 0, 0, 1, 0);
    Def_Column(col_3, 3, "v2_value", DATATYPE::DOUBLE, 0, 0, 1, 0, 0, 0, 1, 0);

    vector<AttributeInfo> schema = {std::move(col_1), std::move(col_2), std::move(col_3)};
    kwdbts::KTableKey cur_table_id = 123456789;
    string tbl_sub_path = std::to_string(cur_table_id) + "/";

    string dir_path = db_path + tbl_sub_path;
    if (access(dir_path.c_str(), 0)) {
      std::string cmd = "mkdir -p " + dir_path;
      system(cmd.c_str());
    }


    MMapMetricsTable* root_bt = new MMapMetricsTable();
    string bt_path = nameToEntityBigTablePath(std::to_string(cur_table_id));
    if (root_bt->open(bt_path, db_path, tbl_sub_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
        err_info.errcode == KWECORR) {
      root_bt->create(schema, 1, tbl_sub_path, kwdbts::EngineOptions::iot_interval, encoding, err_info, true);
    }

    if (err_info.errcode < 0) {
      root_bt->setObjectReady();
      root_bt->remove();
      delete root_bt;
      root_bt = nullptr;
    }
    return root_bt;
  }

  static MMapMetricsTable* CreateLsnTable(const std::string& tbl_sub_path1, const std::string& db_path, std::string&
  table_name, int encoding, ErrorInfo& err_info) {
    encoding = encoding | NO_DEFAULT_TABLE;
;
    Def_Column(col_1, 1, "k_ts", DATATYPE::TIMESTAMP64_LSN, 0, 0, 1, 0, AINFO_NOT_NULL, 3, 1, 0);
    Def_Column(col_2, 2, "v1_value", DATATYPE::INT64, 0, 0, 1, 0, 0, 0, 1, 0);
    Def_Column(col_3, 3, "v2_value", DATATYPE::DOUBLE, 0, 0, 1, 0, 0, 0, 1, 0);
    //  Def_Column(col_4, kwdbts::s_deletable(), DATATYPE::INT32, 0, 0, 1, 0, 0, 0, 1, 0);

    vector<AttributeInfo> schema = {std::move(col_1), std::move(col_2), std::move(col_3)};
    kwdbts::KTableKey cur_table_id = 123456789;
    string tbl_sub_path = std::to_string(cur_table_id) + "/";

    string dir_path = db_path + tbl_sub_path;
    if (access(dir_path.c_str(), 0)) {
      std::string cmd = "mkdir -p " + dir_path;
      system(cmd.c_str());
    }

    MMapMetricsTable* root_bt = new MMapMetricsTable();
    string bt_path = nameToEntityBigTablePath(std::to_string(cur_table_id));
    if (root_bt->open(bt_path, db_path, tbl_sub_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
        err_info.errcode == KWECORR) {
      root_bt->create(schema, 1, tbl_sub_path, 86400, encoding, err_info, true);
    }

    if (err_info.errcode < 0) {
      root_bt->setObjectReady();
      root_bt->remove();
      delete root_bt;
      root_bt = nullptr;
    }
    return root_bt;
  }

  static MMapMetricsTable* CreateRootTableVarCol(const std::string& tbl_sub_path1, const std::string& db_path, std::string&
  table_name, int encoding, ErrorInfo& err_info) {
    encoding = encoding | NO_DEFAULT_TABLE;

    Def_Column(col_1, 1, "k_ts", DATATYPE::TIMESTAMP64, 0, 0, 1, 0, AINFO_NOT_NULL, 3, 1, 0);
    Def_Column(col_2, 2, "v1_value", DATATYPE::INT64, 0, 0, 1, 0, 0, 0, 1, 0);
    Def_Column(col_3, 3, "v2_value", DATATYPE::VARSTRING, 0, 0, 1, 0, 0, 0, 1, 0);
    //  Def_Column(col_4, kwdbts::s_deletable(), DATATYPE::INT32, 0, 0, 1, 0, 0, 0, 1, 0);

    vector<AttributeInfo> schema = {std::move(col_1), std::move(col_2), std::move(col_3)};
    kwdbts::KTableKey cur_table_id = 123456789;
    string tbl_sub_path = std::to_string(cur_table_id) + "/";

    string dir_path = db_path + tbl_sub_path;
    if (access(dir_path.c_str(), 0)) {
      std::string cmd = "mkdir -p " + dir_path;
      system(cmd.c_str());
    }

    MMapMetricsTable* root_bt = new MMapMetricsTable();
    string bt_path = nameToEntityBigTablePath(std::to_string(cur_table_id));
    if (root_bt->open(bt_path, db_path, tbl_sub_path, MMAP_CREAT_EXCL, err_info) >= 0 ||
        err_info.errcode == KWECORR) {
      root_bt->create(schema, 1, tbl_sub_path, kwdbts::EngineOptions::iot_interval, encoding, err_info, true);
    }

    if (err_info.errcode < 0) {
      root_bt->setObjectReady();
      root_bt->remove();
      delete root_bt;
      root_bt = nullptr;
    }
    return root_bt;
  }

  static int CheckError(const char* msg, ErrorInfo err_info) {
    if (err_info.errcode != 0) {
      fprintf(stderr, "%s : %s\n", msg, err_info.errmsg.c_str());
    }
    return err_info.errcode;
  }


  static void PrintTable(TsTimePartition* bt, uint32_t entity_id) {
    fprintf(stdout, "----------Print Table Data----------\n");
    for (int r = 1; r <= bt->size(entity_id); r++) {
      bt->printRecord(entity_id, std::cout, r);
      fprintf(stdout, "\n");
    }
  }
};

class TestBigTableInstance : public ::testing::Test {
 public:
  static const string kw_home_;
  static const string db_name_;
  static const uint64_t iot_interval_;

 protected:
  void SetUp() override {
    setenv("KW_HOME", kw_home_.c_str(), 1);
    setenv("KW_IOT_INTERVAL", std::to_string(iot_interval_).c_str(), 1);

    kwdbts::EngineOptions::init();
    ErrorInfo err_info;
    ASSERT_NE(kw_home_, ".");
    ASSERT_NE(kw_home_, "");
    system(("rm -rf " + kw_home_ + "/*").c_str());
    system(("rm -rf " + kw_home_ + DIR_SEP + db_name_ + "*").c_str());

    err_info.clear();
    BtUtil::CreateDB(TestBigTableInstance::db_name_, iot_interval_ * 10, err_info);
    BtUtil::CheckError("createDB failed :", err_info);
  }

  void TearDown() override {
  }

  static void initData2(kwdbts::kwdbContext_p ctx, TsTimePartition* bt, uint32_t entity_id, timestamp64 start_ts, int
  row_num, DedupResult* dedup_result, kwdbts::DedupRule dedup_rule = kwdbts::DedupRule::KEEP, int value = 11) {
    timestamp64 ts = start_ts;
    int count = row_num;
    do {
      int insert_num = std::min(100, count);
      ErrorInfo err_info;
      uint32_t payload_len = 0;
      std::vector<BlockSpan> cur_alloc_spans;
      std::vector<MetricRowID> del_rows;
      char* data = BtUtil::GenSomePayloadData(bt->getSchemaInfoIncludeDropped(), bt->getColsIdxExcludeDropped(), insert_num, payload_len, ts, false, value);
      kwdbts::Payload pd(bt->getSchemaInfoIncludeDropped(), bt->getColsIdxExcludeDropped(), {data, payload_len});
      uint32_t inc_unordered_cnt;
      pd.dedup_rule_ = dedup_rule;
      bt->push_back_payload(ctx, entity_id, &pd, 0, insert_num, &cur_alloc_spans, &del_rows, err_info, &inc_unordered_cnt, dedup_result);
      if (err_info.errcode >= 0) {
        bt->publish_payload_space(cur_alloc_spans, del_rows, entity_id, true);
      }
      delete[]data;
      ts += 10 * row_num;
      count -= insert_num;
    } while (count > 0);
  }

  static void initDataWithNull(kwdbts::kwdbContext_p ctx, TsTimePartition* bt, uint32_t entity_id,
                               timestamp64 start_ts, int row_num, DedupResult* dedup_result,
                               kwdbts::DedupRule dedup_rule = kwdbts::DedupRule::KEEP) {
    timestamp64 ts = start_ts;
    int count = row_num;
    do {
      int insert_num = std::min(100, count);
      ErrorInfo err_info;
      uint32_t payload_len = 0;
      std::vector<BlockSpan> cur_alloc_spans;
      std::vector<MetricRowID> del_rows;
      char* data = BtUtil::GenSomePayloadData(bt->getSchemaInfoIncludeDropped(), bt->getColsIdxExcludeDropped(), insert_num, payload_len, ts, true);
      kwdbts::Payload pd(bt->getSchemaInfoIncludeDropped(), bt->getColsIdxExcludeDropped(), {data, payload_len});
      uint32_t inc_unordered_cnt;
      pd.dedup_rule_ = dedup_rule;
      bt->push_back_payload(ctx, entity_id, &pd, 0, insert_num, &cur_alloc_spans, &del_rows, err_info, &inc_unordered_cnt, dedup_result);
      if (err_info.errcode >= 0) {
        bt->publish_payload_space(cur_alloc_spans, del_rows, entity_id, true);
      }
      delete[]data;
      ts += 10 * row_num;
      count -= insert_num;
    } while (count > 0);
  }

  static void insertBaseData(BigTable* bt, timestamp64 start_ts, int row_num, int64_t begin_v, int interval = 1) {
    timestamp64 ts = start_ts;
    char* rec = nullptr;
    for (int i = 0; i < row_num; i++) {
      std::vector<std::any> batch = {ts, (int64_t) begin_v + i, static_cast<double>(1.1 + i)};
      rec = BtUtil::GenRawData(bt, batch);
      bt->push_back(rec);
      delete[] rec;
      ts = (ts + interval);
    }
  }

  static void printTable(TsTimePartition* bt, uint32_t entity_id) {
    fprintf(stdout, "----------Print Table Data----------\n");
    for (int r = 1; r <= bt->size(entity_id); r++) {
      bt->printRecord(entity_id, std::cout, r);
      fprintf(stdout, "\n");
    }
  }

  static int countRecords(TsTimePartition* bt, uint32_t entity_id) {
    int count = 0;
    std::deque<BlockItem*> block_item_queue;
    bt->GetAllBlockItems(entity_id, block_item_queue);
    while (!block_item_queue.empty()) {
      BlockItem* block_item = block_item_queue.front();
      block_item_queue.pop_front();
      if (!block_item || !block_item->publish_row_count) {
        continue;
      }
      // all rows in block is deleted.
      if (block_item->isBlockEmpty()) {
        continue;
      }
      count += block_item->getNonNullRowCount();
    }
    return count;
  }
};
