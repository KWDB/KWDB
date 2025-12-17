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

#include <string.h>
#include "bitmap_utils.h"

void setRowDeleted(char* delete_flags, size_t row_index) {
  size_t byte = (row_index - 1) >> 3;
  size_t bit = (row_index - 1) & 7;
  delete_flags[byte] |= (1 << bit);
}

void setRowValid(char* delete_flags, size_t row_index) {
  size_t byte = (row_index - 1) >> 3;
  size_t bit = (row_index - 1) & 7;
  delete_flags[byte] -= (1 << bit);
}

bool isColDeleted(char* row_delete_flags, char* col_nullbitmap, size_t row_index) {
  return isRowDeleted(row_delete_flags, row_index) || isRowDeleted(col_nullbitmap, row_index);
}

void setBatchDeleted(char* delete_flags, size_t start_row, size_t del_rows_count) {
  size_t byte_start = (start_row - 1) >> 3;
  size_t bit_start = (start_row - 1) & 7;
  size_t byte_end = (start_row - 1 + del_rows_count - 1) >> 3;
  size_t bit_end = (start_row - 1 + del_rows_count - 1) & 7;
  uint8_t del_flag = 0;
  // in same byte
  if (byte_start == byte_end) {
    del_flag = 0;
    for (size_t i = bit_start; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_start] |= (del_flag << bit_start);
    return;
  }

  size_t memset_start = byte_start + 1;
  size_t memset_length = byte_end - byte_start - 1;
  // process first byte
  if (bit_start == 0) {
    memset_start = byte_start;
    memset_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = bit_start; i < 8; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_start] |= (del_flag << bit_start);
  }
  // process last byte
  if (bit_end == 7) {
    memset_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = 0; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_end] |= del_flag;
  }
  // process other bytes
  memset(delete_flags + memset_start, 0xFF, memset_length);
}

void setBatchValid(char* delete_flags, size_t start_row, size_t del_rows_count) {
  size_t byte_start = (start_row - 1) >> 3;
  size_t bit_start = (start_row - 1) & 7;
  size_t byte_end = (start_row - 1 + del_rows_count - 1) >> 3;
  size_t bit_end = (start_row - 1 + del_rows_count - 1) & 7;
  uint8_t del_flag = 0;
  // in same byte
  if (byte_start == byte_end) {
    del_flag = 0;
    for (size_t i = bit_start; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_start] &= ~(del_flag << bit_start);
    return;
  }

  size_t memset_start = byte_start + 1;
  size_t memset_length = byte_end - byte_start - 1;
  // process first byte
  if (bit_start == 0) {
    memset_start = byte_start;
    memset_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = bit_start; i < 8; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_start] &= ~(del_flag << bit_start);
  }
  // process last byte
  if (bit_end == 7) {
    memset_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = 0; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    delete_flags[byte_end] &= ~del_flag;
  }
  // process other bytes
  memset(delete_flags + memset_start, 0, memset_length);
}

bool isAllDeleted(char* delete_flags, size_t start_row, size_t rows_count) {
  size_t byte_start = (start_row - 1) >> 3;
  size_t bit_start = (start_row - 1) & 7;
  size_t byte_end = (start_row - 1 + rows_count - 1) >> 3;
  size_t bit_end = (start_row - 1 + rows_count - 1) & 7;
  uint8_t del_flag = 0;
  // in same byte
  if (byte_start == byte_end) {
    del_flag = 0;
    for (size_t i = bit_start; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((delete_flags[byte_start] >> bit_start) & del_flag) != del_flag) {
      return false;
    }
    return true;
  }

  size_t bytes_start = byte_start + 1;
  size_t bytes_length = byte_end - byte_start - 1;
  // check first byte
  if (bit_start == 0) {
    bytes_start = byte_start;
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = bit_start; i < 8; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((delete_flags[byte_start] >> bit_start) & del_flag) != del_flag) {
      return false;
    }
  }
  // check last byte
  if (bit_end == 7) {
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = 0; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if ((delete_flags[byte_end] & del_flag) != del_flag) {
      return false;
    }
  }
  // check other bytes
  for (size_t i = 0; i < bytes_length; i++) {
    if (delete_flags[bytes_start + i] != static_cast<char>(0xFF)) {
      return false;
    }
  }
  return true;
}

bool hasDeleted(char* delete_flags, size_t start_row, size_t rows_count) {
  return hasNonZeroBit(delete_flags, start_row, rows_count);
}

bool hasNonZeroBit(char* delete_flags, size_t start_row, size_t rows_count) {
  size_t byte_start = (start_row - 1) >> 3;
  size_t bit_start = (start_row - 1) & 7;
  size_t byte_end = (start_row - 1 + rows_count - 1) >> 3;
  size_t bit_end = (start_row - 1 + rows_count - 1) & 7;
  uint8_t del_flag = 0;
  // in same byte
  if (byte_start == byte_end) {
    del_flag = 0;
    for (size_t i = bit_start; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((delete_flags[byte_start] >> bit_start) & del_flag) != 0) {
      return true;
    }
    return false;
  }

  size_t bytes_start = byte_start + 1;
  size_t bytes_length = byte_end - byte_start - 1;
  // check first byte
  if (bit_start == 0) {
    bytes_start = byte_start;
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = bit_start; i < 8; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if (((delete_flags[byte_start] >> bit_start) & del_flag) != 0) {
      return true;
    }
  }
  // check last byte
  if (bit_end == 7) {
    bytes_length += 1;
  } else {
    del_flag = 0;
    for (size_t i = 0; i <= bit_end; i++) {
      del_flag <<= 1;
      del_flag += 1;
    }
    if ((delete_flags[byte_end] & del_flag) != 0) {
      return true;
    }
  }
  // check other bytes
  for (size_t i = 0; i < bytes_length; i++) {
    if (delete_flags[bytes_start + i] != 0) {
      return true;
    }
  }
  return false;
}
