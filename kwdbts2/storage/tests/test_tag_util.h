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
#include <any>
#include <cstring>
#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include "utils/big_table_utils.h"
#include "mmap/mmap_tag_column_table.h"
#include "ts_table.h"

// change char* memory to KTimestamp.
#define KTimestamp(buf) (*(static_cast<timestamp64 *>(static_cast<void *>(buf))))

// change char* memory to k_int64
#define KInt64(buf) (*(static_cast<int64_t *>(static_cast<void *>(buf))))

// change char* memory to k_double64
#define KDouble64(buf) (*(static_cast<double *>(static_cast<void *>(buf))))

#define  KUInt32(buf) (*(static_cast<uint32_t *>(static_cast<void *>(buf))))

class TagBtUtil {
 public:
  static size_t GetVarTagColumnSize(MMapTagColumnTable* bt) {
    size_t total_size = 0;
    std::vector<TagColumn*> tag_schemas = bt->getSchemaInfo();
    for (int i = 0; i < tag_schemas.size(); i++) {
      if (tag_schemas[i]->attributeInfo().m_tag_type == GENERAL_TAG &&
          tag_schemas[i]->attributeInfo().m_data_type == DATATYPE::VARSTRING) {
        // len + value
        total_size += 4;
        total_size += tag_schemas[i]->attributeInfo().m_length;
      }
    }
    return total_size;
  }

  static ErrorInfo
  InsertTagData(MMapTagColumnTable* bt, int rownum, uint64_t ts, int64_t begin_v, uint32_t entity_id,
                uint32_t group_id) {
    int rec_size = bt->recordSize() + TagBtUtil::GetVarTagColumnSize(bt);
    // bitmap + primary_tag + tag
    char* rec = new char[rec_size];
    memset(rec, 0x00, rec_size);
    int num_col = bt->numColumn();
    int ts_col = -1;
    int64_t row_num;
    int col_idx = 0;
    char* ptr = rec + (num_col + 7) / 8;
    char* end_ptr = ptr;
    std::vector<TagInfo> var_general_tags;
    for (int i = 0; i < bt->getSchemaInfo().size(); i++) {
      TagColumn* tag_col = bt->getSchemaInfo()[col_idx];
      if (col_idx >= num_col) {
        break;
      }
      int d_type = tag_col->attributeInfo().m_data_type;
      switch (d_type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(reinterpret_cast<void*>((intptr_t) ptr + tag_col->attributeInfo().m_offset)) = (uint64_t) ts;

          break;
        case DATATYPE::INT64:
          KInt64(reinterpret_cast<void*>((intptr_t) ptr + tag_col->attributeInfo().m_offset)) = (int64_t) begin_v;
          break;
        case DATATYPE::DOUBLE:
          KDouble64(reinterpret_cast<void*>((intptr_t) ptr + tag_col->attributeInfo().m_offset)) =
              static_cast<double>(1.1 + rownum);
          break;
        case DATATYPE::CHAR:
          memset(ptr + tag_col->attributeInfo().m_offset, '1', tag_col->attributeInfo().m_size);
          break;
        case DATATYPE::VARSTRING:
          if (tag_col->attributeInfo().m_tag_type == PRIMARY_TAG) {
            std::string tmp_str(std::move(std::to_string(ts)));
            strncpy(ptr + tag_col->attributeInfo().m_offset, tmp_str.c_str(), tmp_str.length());
          } else {
            var_general_tags.push_back(tag_col->attributeInfo());
          }
          break;
        default:
          fprintf(stderr, "unsupported data type:%d \n", d_type);
      }
      end_ptr = ptr + tag_col->attributeInfo().m_offset + tag_col->attributeInfo().m_size;
      col_idx++;
    }
    for (int i = 0; i < var_general_tags.size(); i++) {
      KUInt32(reinterpret_cast<void*>((intptr_t) ptr + var_general_tags[i].m_offset)) = (end_ptr - rec);
      // len + value
      std::string ts_str = std::to_string(ts);
      *reinterpret_cast<int16_t*>(end_ptr) = ((int16_t) ts_str.length());
      // KInt32((void*)end_ptr) = ((int32_t)ts_str.length());
      end_ptr += sizeof(int16_t);
      strncpy(end_ptr, ts_str.c_str(), ts_str.length());
      end_ptr += ts_str.length();
    }
    ErrorInfo err_info(false);
    uint32_t hashpoint;
    hashpoint = GetConsistentHashId(rec, rec_size, g_testcase_hash_num);
    size_t row_no = 0;
    row_num = bt->insert(entity_id, group_id, hashpoint, rec, &row_no);
    if (row_num < 0) {  // $$ push_back is the function to write table in table
      fprintf(stderr, "Fail to insert TS table,error_code:%ld \n", row_num);
      err_info.errcode = row_num;
    }
    delete[] rec;
    return err_info;
  }

  static void
  InitTagData2(MMapTagColumnTable* bt, uint64_t start_ts, int row_num, int64_t begin_v, int interval = 1) {
    uint64_t ts = start_ts;
    for (int i = 0; i < row_num; i++) {
      TagBtUtil::InsertTagData(bt, i, ts, begin_v + i, i, i);
      ts = (ts + interval);
    }
  }
};
