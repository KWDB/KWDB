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

#include <string>
#include <cstdint>


/**
 * @brief mark row deleted.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] row_index     row num of bitmap. start from 1.
 * 
*/
void setRowDeleted(char* delete_flags, size_t row_index);

/**
 * @brief mark row valid.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] row_index     row num of bitmap. start from 1.
 * 
*/
void setRowValid(char* delete_flags, size_t row_index);

/**
 * @brief check row is set deleted.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] row_index     row num of bitmap. start from 1.
 * 
*/
inline bool isRowDeleted(char* delete_flags, size_t row_index) {
  // 0 ~ config_block_rows_ - 1
  size_t row = row_index - 1;
  size_t byte = row >> 3;
  size_t bit = 1 << (row & 7);
  return delete_flags[byte] & bit;
}

/**
 * @brief check column is deleted or column value is null.
 * @param[in] delete_flags    bitmap addr of blockItem.
 * @param[in] col_nullbitmap  bitmap addr of column block.
 * @param[in] row_index       row num of bitmap. start from 1.
 * 
*/
bool isColDeleted(char* row_delete_flags, char* col_nullbitmap, size_t row_index);

/**
 * @brief mark rows that continuous deleted.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] start_row     row num of bitmap. start from 1.
 * @param[in] del_rows_count     deleted count.
 * 
*/
void setBatchDeleted(char* delete_flags, size_t start_row, size_t del_rows_count);

/**
 * @brief mark rows that continuous valid.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] start_row     row num of bitmap. start from 1.
 * @param[in] del_rows_count     row count.
 *
*/
void setBatchValid(char* delete_flags, size_t start_row, size_t del_rows_count);

/**
 * @brief check if all rows that continuous deleted.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] start_row     row num of bitmap. start from 1.
 * @param[in] rows_count    check count.
 * 
*/
bool isAllDeleted(char* delete_flags, size_t start_row, size_t rows_count);

/**
 * @brief check if has deleted rows.
 * @param[in] delete_flags  bitmap addr of blockItem.
 * @param[in] start_row     row num of bitmap. start from 1.
 * @param[in] rows_count    check count.
 *
*/
bool hasDeleted(char* delete_flags, size_t start_row, size_t rows_count);

bool hasNonZeroBit(char* delete_flags, size_t start_row, size_t rows_count);
