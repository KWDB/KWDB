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

#pragma once
namespace kwdbts {
using namespace kwdbts;

struct zTableColumnMeta {
  roachpb::DataType type;  // column type
  k_uint32 storage_len;  // bytes of column storage
  k_uint32 actual_len;   // bytes of actual len
  roachpb::VariableLengthType storage_type;
};

inline void constructColumnMetas(std::vector<zTableColumnMeta>* metas, BenchParams& params) {
  // Construct all column types
  metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
  int j = 0;
  for (int i = 0 ; i < params.meta_param.COLUMN_NUM ; i++) {
    switch (params.data_types[j]) {
      case 2: // int
        metas->push_back({roachpb::DataType::INT, 4, 4, roachpb::VariableLengthType::ColStorageTypeTuple});
        break;
      case 3: // bigint
        metas->push_back({roachpb::DataType::BIGINT, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
        break;
      case 5: // double
        metas->push_back({roachpb::DataType::DOUBLE, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
        break;
      case 7: // char
        metas->push_back({roachpb::DataType::CHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
        break;
      case 9: // varchar
        metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
        break;

      default:
        break;
    }
    j++;
    if (j >= params.data_types.size()) {
      j = 0;
    }
  }
}

inline void constructTagMetas(std::vector<zTableColumnMeta>* metas) {
  // Construct all tag column types
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
  metas->push_back({roachpb::DataType::VARCHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
}

class StMetaBuilder {
 public:

  static void constructRoachpbTable(roachpb::CreateTsTable* meta, KTableKey table_id, BenchParams& params) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)

    roachpb::KWDBTsTable *table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id);
    std::string prefix_table_name = "bench_table";
    table->set_table_name(prefix_table_name + std::to_string(table_id));
    table->set_partition_interval(params.meta_param.PARTITION_INTERVAL);
    table->set_hash_num(2000);
    meta->set_allocated_ts_table(table);

    std::vector<zTableColumnMeta> col_meta;
    constructColumnMetas(&col_meta, params);

    uint32_t col_id = 1;
    for (int i = 0; i < col_meta.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType)(col_meta[i].type));
      column->set_storage_len(col_meta[i].storage_len);
      column->set_column_id(col_id++);
      if (i == 0) {
        column->set_name("k_timestamp");  // first column name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag columns
    std::vector<zTableColumnMeta> tag_metas;
    constructTagMetas(&tag_metas);
    for (int i = 0; i< tag_metas.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType)(tag_metas[i].type));
      column->set_storage_len(tag_metas[i].storage_len);
      column->set_column_id(col_id++);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }
  }
};

}