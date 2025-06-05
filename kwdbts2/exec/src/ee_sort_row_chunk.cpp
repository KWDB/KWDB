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

#include "ee_sort_row_chunk.h"
#include <algorithm>
#include <vector>
namespace kwdbts {

/**
 * @brief Overloaded function call operator for comparing two constant row data pointers.
 * 
 * This function uses `std::memcmp` to compare the memory contents pointed to by `a_ptr` and `b_ptr`
 * up to `sort_row_size_` bytes. It returns `true` if the memory block pointed to by `a_ptr`
 * is lexicographically less than that pointed to by `b_ptr`; otherwise, it returns `false`.
 * 
 * @param a_ptr Pointer to the first constant row data.
 * @param b_ptr Pointer to the second constant row data.
 * @return bool Returns `true` if `a_ptr` is less than `b_ptr`, `false` otherwise.
 */
bool AllConstantColumnCompare::operator()(DatumPtr a_ptr, DatumPtr b_ptr) {
  return std::memcmp(a_ptr, b_ptr, sort_row_size_) < 0;
}

/**
 * @brief Overloaded function call operator for comparing two row data pointers with non-constant columns.
 * 
 * This function iterates through the ordering information and compares the columns of two rows.
 * For string columns, it handles null values, compares the string contents, and considers the sorting order.
 * For non-string columns, it directly compares the column values.
 * 
 * @param a_ptr Pointer to the first row data.
 * @param b_ptr Pointer to the second row data.
 * @return bool Returns `true` if `a_ptr` should be ordered before `b_ptr`, `false` otherwise.
 */
bool HasNonConstantColumnCompare::operator()(DatumPtr a_ptr, DatumPtr b_ptr) {
  // Iterate through the ordering column
  for (auto order : *(order_info_)) {
    if (col_info_[order.col_idx].is_string) {
      auto order_direction = order.direction;

      // Get the pointers to the actual string data
      DatumPtr a_data = *reinterpret_cast<DatumPtr*>(
          a_ptr + col_offset_[order.col_idx] + NULL_INDECATOR_WIDE);
      DatumPtr b_data = *reinterpret_cast<DatumPtr*>(
          b_ptr + col_offset_[order.col_idx] + NULL_INDECATOR_WIDE);
      if (a_data == nullptr) {
        return true;
      }
      if (b_data == nullptr) {
        return false;
      }

      // Check null indicators
      k_bool* a_null = reinterpret_cast<k_bool*>(a_ptr);
      k_bool* b_null = reinterpret_cast<k_bool*>(b_ptr);
      if (*a_null) {
        if (*b_null) {
          continue;
        }
        return true;
      } else if (*b_null) {
        return false;
      }
      auto a_len = *reinterpret_cast<k_uint16*>(a_data);
      auto b_len = *reinterpret_cast<k_uint16*>(b_data);

      // Compare the string contents
      auto ret = std::memcmp(a_data + STRING_WIDE, b_data + STRING_WIDE,
                             std::min(a_len, b_len));
      if (ret != 0) {
        return order_direction == TSOrdering_Column_Direction_ASC ? ret < 0
                                                                  : ret > 0;
      }

      // Compare the string lengths if contents are equal
      if (a_len != b_len) {
        return order_direction == TSOrdering_Column_Direction_ASC
                   ? a_len < b_len
                   : a_len > b_len;
      }
    } else {
      // Compare non-string columns directly
      auto ret = std::memcmp(a_ptr + col_offset_[order.col_idx],
                             b_ptr + col_offset_[order.col_idx],
                             col_info_[order.col_idx].fixed_storage_len);
      if (ret != 0) {
        return ret < 0;
      }
    }
  }
  return false;
}

SortRowChunk::~SortRowChunk() {
  SafeDeleteArray(col_offset_);
  SafeDeleteArray(is_encoded_col_);
  SafeDeleteArray(data_);
  SafeDeleteArray(non_constant_data_);
}

/**
 * @brief Initializes the SortRowChunk object.
 * 
 * This function allocates memory for various buffers, calculates the row size based on column information,
 * and determines the storage mode for non-constant columns. It also initializes the data buffer and 
 * non-constant data buffer if necessary.
 * 
 * @return k_bool Returns true if the initialization is successful, false otherwise.
 */
k_bool SortRowChunk::Initialize() {
  col_offset_ = KNEW k_uint32[col_num_];
  is_encoded_col_ = KNEW k_int8[col_num_];
  if (col_offset_ == nullptr || is_encoded_col_ == nullptr) {
    LOG_ERROR("Allocate buffer in SortRowChunk failed.");
    return false;
  }
  non_constant_max_row_size_ = 0;
  memset(is_encoded_col_, NOT_ENCODED_COL, col_num_ * sizeof(k_int8));
  sort_row_size_ = 0;
  for (int i = 0; i < col_num_; ++i) {
    if (col_info_[i].is_string) {
      col_info_[i].fixed_storage_len = col_info_[i].storage_len + STRING_WIDE;
    } else if (col_info_[i].storage_type == roachpb::DataType::DECIMAL) {
      col_info_[i].fixed_storage_len = col_info_[i].storage_len + BOOL_WIDE;
    } else {
      col_info_[i].fixed_storage_len = col_info_[i].storage_len;
    }
  }
  for (auto& sort_col : order_info_) {
    is_encoded_col_[sort_col.col_idx] = sort_col.direction;
    col_offset_[sort_col.col_idx] = sort_row_size_;
    sort_row_size_ += NULL_INDECATOR_WIDE;  // null indicator
    if (col_info_[sort_col.col_idx].is_string) {
      if (force_constant_) {
        sort_row_size_ +=
            col_info_[sort_col.col_idx].max_string_len + STRING_WIDE;
      } else {
        all_constant_ = false;
        all_constant_in_order_col_ = false;
        non_constant_col_offsets_.push_back(sort_row_size_ -
                                            NULL_INDECATOR_WIDE);
        sort_row_size_ += NON_CONSTANT_PLACE_HOLDER_WIDE;  // string offset
        non_constant_max_row_size_ +=
            (col_info_[sort_col.col_idx].fixed_storage_len);
      }

    } else {
      sort_row_size_ += col_info_[sort_col.col_idx].fixed_storage_len;
    }
  }
  sort_row_size_ += LINE_NUMBER_WIDE;  // row index
  row_size_ = sort_row_size_;
  for (int i = 0; i < col_num_; ++i) {
    if (is_encoded_col_[i] != NOT_ENCODED_COL) {
      continue;
    }
    /**
     * Row size adjustment for string type column and decimal type column. Add
     * 1 byte null indicator for all types. Add 2 byte for string length and 1
     * byte indicator for decimal type. Ideally storage length of the field
     * (FieldDecimal, FieldVarChar, FieldVarBlob, etc.) should take extra
     * bytes into account, and it needs to make necessary changes on all
     * derived Field classes. Add 4 byte for row index at the end of row.
     * Now we temporarily make row size adjustment in
     * several places.
     */
    col_offset_[i] = row_size_;
    row_size_ += NULL_INDECATOR_WIDE;  // null indicator
    if (col_info_[i].is_string) {
      if (force_constant_) {
        row_size_ += col_info_[i].max_string_len + STRING_WIDE;
      } else {
        all_constant_ = false;
        non_constant_col_offsets_.push_back(row_size_ - NULL_INDECATOR_WIDE);
        row_size_ += NON_CONSTANT_PLACE_HOLDER_WIDE;  // string offset
        non_constant_max_row_size_ += (col_info_[i].fixed_storage_len);
      }
    } else {
      row_size_ += col_info_[i].fixed_storage_len;
    }
  }
  if (capacity_ == 0) {
    capacity_ = chunk_size_ / (row_size_ + non_constant_max_row_size_);
  }
  data_size_ = row_size_ * capacity_;
  if (!all_constant_ && !force_constant_) {
    non_constant_max_size_ = non_constant_max_row_size_ * capacity_;
    non_constant_data_ = KNEW char[non_constant_max_size_];
    if (non_constant_data_ == nullptr) {
      LOG_ERROR("Allocate buffer in SortRowChunk failed.");
      return false;
    }
  }

  data_ = KNEW char[data_size_];
  if (data_ == nullptr) {
    LOG_ERROR("Allocate buffer in SortRowChunk failed.");
    return false;
  }

  if (all_constant_in_order_col_) {
    non_constant_save_mode_ = OFFSET_MODE;
  } else {
    non_constant_save_mode_ = POINTER_MODE;
  }
  return true;
}

/**
 * @brief Appends rows from a DataChunk to the SortRowChunk.
 * 
 * This function iterates through the specified range of rows in the given DataChunk and appends them
 * to the current SortRowChunk. It handles memory expansion if necessary, manages null values,
 * and encodes column data according to the ordering information. For string columns, it also
 * manages non-constant data storage.
 * 
 * @param data_chunk_ptr Pointer to the DataChunk from which rows will be appended.
 * @param begin_row Reference to the starting row index in the DataChunk. This value will be updated
 *                  as rows are appended.
 * @param end_row The ending row index (exclusive) in the DataChunk.
 * @return KStatus Returns KStatus::SUCCESS if the operation is successful, otherwise returns an 
 *                 appropriate error status.
 */
KStatus SortRowChunk::Append(DataChunkPtr& data_chunk_ptr, k_uint32& begin_row,
                             k_uint32 end_row) {
  KStatus ret = KStatus::SUCCESS;
  if (data_chunk_ptr == nullptr) {
    return KStatus::FAIL;
  }
  for (; begin_row < end_row; begin_row++) {
    if (count_ >= capacity_ && !all_constant_) {
      ret = Expand(count_ * 2);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
    if (non_constant_data_size_ + non_constant_max_row_size_ >
        non_constant_max_size_) {
      return KStatus::FAIL;
    }
    DatumRowPtr row_data_ptr = data_ + count_ * row_size_;
    // Append Sort Data
    for (auto& order_i : order_info_) {
      const auto col = order_i.col_idx;
      const k_bool is_null = data_chunk_ptr->IsNull(begin_row, col);
      row_data_ptr[col_offset_[col]] = is_null ? 1 : 0;
      k_uint8* col_ptr = reinterpret_cast<k_uint8*>(
          row_data_ptr + col_offset_[col] + NULL_INDECATOR_WIDE);
      k_uint8* val_ptr =
          reinterpret_cast<k_uint8*>(data_chunk_ptr->GetData(begin_row, col));
      if (!is_null) {
        if (col_info_[col].is_string) {
          auto len = reinterpret_cast<k_uint16*>(val_ptr);
          col_info_[col].max_string_len =
              std::max(col_info_[col].max_string_len, *len);
          DatumPtr non_constant_col_data =
              non_constant_data_ + non_constant_data_size_;
          memcpy(non_constant_col_data, val_ptr, STRING_WIDE + *len);
          if (non_constant_save_mode_ == POINTER_MODE) {
            memcpy(col_ptr, &(non_constant_col_data),
                   NON_CONSTANT_PLACE_HOLDER_WIDE);
          } else {
            memcpy(col_ptr, &non_constant_data_size_,
                   NON_CONSTANT_PLACE_HOLDER_WIDE);
          }
          non_constant_data_size_ += (STRING_WIDE + *len);
        } else {
          if (order_i.direction == TSOrdering_Column_Direction_DESC) {
            ret = EncodeColDataInvert(col, col_ptr, val_ptr);
          } else {
            ret = EncodeColData(col, col_ptr, val_ptr);
          }

          if (ret != KStatus::SUCCESS) {
            return ret;
          }
        }
      }
    }
    Radix::EncodeData<k_uint64>(
        reinterpret_cast<k_uint8*>(row_data_ptr + sort_row_size_ -
                                   LINE_NUMBER_WIDE),
        &base_count_);

    // Append Payload Data
    for (k_uint32 col = 0; col < col_num_; col++) {
      if (is_encoded_col_[col] != NOT_ENCODED_COL) {
        continue;
      }
      const k_bool is_null = data_chunk_ptr->IsNull(begin_row, col);
      row_data_ptr[col_offset_[col]] = is_null ? 1 : 0;
      char* col_ptr = row_data_ptr + col_offset_[col] + NULL_INDECATOR_WIDE;
      if (!is_null) {
        k_uint8* val_ptr =
            reinterpret_cast<k_uint8*>(data_chunk_ptr->GetData(begin_row, col));
        if (col_info_[col].is_string) {
          auto len = reinterpret_cast<k_uint16*>(val_ptr);
          col_info_[col].max_string_len =
              std::max(col_info_[col].max_string_len, *len);
          DatumPtr non_constant_col_data =
              non_constant_data_ + non_constant_data_size_;

          memcpy(non_constant_col_data, val_ptr, STRING_WIDE + *len);
          if (non_constant_save_mode_ == POINTER_MODE) {
            memcpy(col_ptr, &(non_constant_col_data),
                   NON_CONSTANT_PLACE_HOLDER_WIDE);
          } else {
            memcpy(col_ptr, &non_constant_data_size_,
                   NON_CONSTANT_PLACE_HOLDER_WIDE);
          }
          non_constant_data_size_ += (STRING_WIDE + *len);
        } else {
          std::memcpy(row_data_ptr + col_offset_[col] + 1, val_ptr,
                      col_info_[col].fixed_storage_len);
        }
      }
    }
    ++count_;
    ++base_count_;
  }

  return SUCCESS;
}

KStatus SortRowChunk::Append(SortRowChunkPtr& data_chunk_ptr,
                             DatumPtr input_row_data_ptr) {
  KStatus ret = KStatus::SUCCESS;
  if (data_chunk_ptr == nullptr) {
    return FAIL;
  }
  if (all_constant_ && data_chunk_ptr->all_constant_) {
    // Append Data
    std::memcpy(data_ + count_ * row_size_, input_row_data_ptr, row_size_);
    ++count_;

    return SUCCESS;
  } else if (!all_constant_ && !data_chunk_ptr->all_constant_) {
    if (count_ >= capacity_ && !all_constant_) {
      KStatus ret = Expand(count_ * 2);
      if (ret != KStatus::SUCCESS) {
        return ret;
      }
    }
    if (non_constant_data_size_ + non_constant_max_row_size_ >
        non_constant_max_size_) {
      return KStatus::FAIL;
    }
    DatumRowPtr row_data_ptr = data_ + count_ * row_size_;
    std::memcpy(row_data_ptr, input_row_data_ptr, row_size_);
    ReplaceNonConstantRowData(input_row_data_ptr,
                              data_chunk_ptr->GetNonConstantData(), count_,
                              data_chunk_ptr->non_constant_save_mode_);
    ++count_;
    return KStatus::SUCCESS;
  } else if (all_constant_ && !data_chunk_ptr->all_constant_) {
    // Change Variable Length Column To Constant
    if (count_ >= capacity_) {
      return KStatus::FAIL;
    }
    DatumRowPtr row_data_ptr = data_ + count_ * row_size_;
    for (int col = 0; col < col_num_; col++) {
      DatumPtr input_col_data_ptr =
          input_row_data_ptr + data_chunk_ptr->col_offset_[col];
      if (col_info_[col].is_string) {
        k_bool is_null;
        memcpy(&is_null, input_col_data_ptr, NULL_INDECATOR_WIDE);
        if (is_null) {
          row_data_ptr[col_offset_[col]] = 1;
          memset(row_data_ptr + col_offset_[col] + NULL_INDECATOR_WIDE, 0,
                 col_info_[col].max_string_len + STRING_WIDE);
        } else {
          row_data_ptr[col_offset_[col]] = 0;
          k_uint8 *input_non_constant_data;
          if (data_chunk_ptr->non_constant_save_mode_ == POINTER_MODE) {
            input_non_constant_data = *reinterpret_cast<k_uint8**>(
                input_col_data_ptr + NULL_INDECATOR_WIDE);
          } else {
            auto input_non_constant_offset = reinterpret_cast<k_uint64*>(
                input_col_data_ptr + NULL_INDECATOR_WIDE);
            input_non_constant_data =
                reinterpret_cast<k_uint8*>(data_chunk_ptr->non_constant_data_) + *input_non_constant_offset;
          }
          k_uint8 *col_ptr = reinterpret_cast<k_uint8*>(row_data_ptr + col_offset_[col] + NULL_INDECATOR_WIDE);
          if (is_encoded_col_[col] == TSOrdering_Column_Direction_DESC) {
            ret = EncodeColDataInvert(col, col_ptr, input_non_constant_data);
          } else if (is_encoded_col_[col] == TSOrdering_Column_Direction_ASC) {
            ret = EncodeColData(col, col_ptr, input_non_constant_data);
          } else {
            auto len = reinterpret_cast<k_uint16*>(input_non_constant_data);
            memcpy(col_ptr,
                  input_non_constant_data, *len + STRING_WIDE);
          }
        }

      } else {
        memcpy(row_data_ptr + col_offset_[col], input_col_data_ptr,
               col_info_[col].fixed_storage_len + NULL_INDECATOR_WIDE);
      }
    }
    ++count_;
    return KStatus::SUCCESS;
  }
  return KStatus::FAIL;
}

DatumPtr SortRowChunk::GetData(k_uint32 row, k_uint32 col) {
  if (!all_constant_ && col_info_[col].is_string) {
    auto non_constant_offset = reinterpret_cast<k_uint64*>(
        data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE);
    return non_constant_data_ + *non_constant_offset;
  }
  return data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE;
}

DatumPtr SortRowChunk::GetData(k_uint32 col) {
  return GetData(current_line_, col);
}
DatumPtr SortRowChunk::GetRowData(k_uint32 row) {
  return data_ + row * row_size_;
}
DatumPtr SortRowChunk::GetRowData() {
  return data_ + current_line_ * row_size_;
}

DatumPtr SortRowChunk::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  if (!col_info_[col].is_string) {
    return nullptr;
  }
  if (force_constant_) {
    std::memcpy(&len,
                data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE,
                STRING_WIDE);
    return data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE + STRING_WIDE;
  }
  if (non_constant_save_mode_ == OFFSET_MODE) {
    auto non_constant_offset = reinterpret_cast<k_uint64*>(
        data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE);
    std::memcpy(&len, non_constant_data_ + *non_constant_offset, STRING_WIDE);
    return non_constant_data_ + *non_constant_offset + STRING_WIDE;
  } else {
    DatumPtr non_constant_ptr = *reinterpret_cast<DatumPtr*>(
        data_ + row * row_size_ + col_offset_[col] + NULL_INDECATOR_WIDE);
    std::memcpy(&len, non_constant_ptr, STRING_WIDE);
    return non_constant_ptr + STRING_WIDE;
  }
}

void SortRowChunk::Reset(k_bool force_constant) {
  if (is_ordered_) {
    is_ordered_ = false;
  }
  non_constant_data_size_ = 0;
  count_ = 0;
  current_line_ = -1;
  if (force_constant && !all_constant_ && force_constant_ != force_constant) {
    force_constant_ = force_constant;
    SafeDeleteArray(col_offset_);
    SafeDeleteArray(is_encoded_col_);
    SafeDeleteArray(non_constant_data_);
    non_constant_col_offsets_.clear();
    all_constant_ = true;
    all_constant_in_order_col_ = true;

    Initialize();
  }
}

/**
 * rows count
 */
k_uint32 SortRowChunk::Count() { return count_; }

/**
 *  nextline
 */
k_int32 SortRowChunk::NextLine() {
  if (current_line_ + 1 >= count_) {
    return -1;
  }
  current_line_++;
  return current_line_;
}

/**
 *  ResetLine
 */
void SortRowChunk::ResetLine() { current_line_ = -1; }

bool SortRowChunk::IsNull(k_uint32 row, k_uint32 col) {
  return (data_ + row * row_size_ + col_offset_[col])[0] != 0;
}

bool SortRowChunk::IsNull(k_uint32 col) {
  return (data_ + current_line_ * row_size_ + col_offset_[col])[0] != 0;
}

bool SortRowChunk::SetCurrentLine(k_int32 line) {
  if (line >= count_) {
    return false;
  }
  current_line_ = line;

  return true;
}

KStatus SortRowChunk::DecodeData() {
  for (k_uint32 i = 0; i < col_num_; i++) {
    if (is_encoded_col_[i] == NOT_ENCODED_COL || (!all_constant_ && col_info_[i].is_string)) {
      continue;
    }
    if (is_encoded_col_[i] == TSOrdering_Column_Direction_DESC) {
      for (k_uint32 row = 0; row < count_; row++) {
        if (IsNull(row, i)) {
          continue;
        }
        k_uint8* col_ptr = reinterpret_cast<k_uint8*>(GetData(row, i));
        DecodeColDataInvert(i, GetData(row, i), col_ptr);
      }
    } else {
      for (k_uint32 row = 0; row < count_; row++) {
        if (IsNull(row, i)) {
          continue;
        }
        k_uint8* col_ptr = reinterpret_cast<k_uint8*>(GetData(row, i));
        DecodeColData(i, GetData(row, i), col_ptr);
      }
    }
  }
  for (k_uint32 row = 0; row < count_; row++) {
    k_uint8* col_ptr = reinterpret_cast<k_uint8*>(
        data_ + row * row_size_ + sort_row_size_ - LINE_NUMBER_WIDE);
    Radix::DecodeData<k_uint64>(col_ptr, reinterpret_cast<k_uint64*>(col_ptr));
  }
  return SUCCESS;
}
KStatus ISortableChunk::EncodeColData(k_uint32 col, k_uint8* col_ptr,
                                      k_uint8* val_ptr) {
  switch (col_info_[col].storage_type) {
    case roachpb::DataType::SMALLINT:
      Radix::EncodeData<k_int16>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::INT:
      Radix::EncodeData<k_int32>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::DATE:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      Radix::EncodeData<k_int64>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::FLOAT:
      Radix::EncodeData<k_float32>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::DOUBLE:
      Radix::EncodeData<k_double64>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      if (col_info_[col].max_string_len > 0) {
        Radix::EncodeVarData(col_ptr, val_ptr, col_info_[col].max_string_len + STRING_WIDE);
      } else {
        Radix::EncodeVarData(col_ptr, val_ptr, col_info_[col].fixed_storage_len);
      }
      break;
    case roachpb::DataType::DECIMAL:
      memcpy(col_ptr, val_ptr, BOOL_WIDE);
      if (*reinterpret_cast<k_bool*>(val_ptr)) {
        Radix::EncodeData<k_double64>(col_ptr + BOOL_WIDE, val_ptr + BOOL_WIDE);
      } else {
        Radix::EncodeData<k_int64>(col_ptr + BOOL_WIDE, val_ptr + BOOL_WIDE);
      }
      break;
    case roachpb::DataType::BOOL:
      Radix::EncodeData<k_bool>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::NULLVAL:
      Radix::EncodeData<k_bool>(col_ptr, val_ptr);
      break;
    default:
      LOG_ERROR("Type(%d) is not supported in order by",
                col_info_[col].storage_type);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus ISortableChunk::DecodeColData(k_uint32 col, DatumPtr col_ptr,
                                      k_uint8* val_ptr) {
  k_uint8* cp = reinterpret_cast<k_uint8*>(col_ptr);
  switch (col_info_[col].storage_type) {
    case roachpb::DataType::SMALLINT:
      Radix::DecodeData<k_int16>(cp, val_ptr);
      break;
    case roachpb::DataType::INT:
      Radix::DecodeData<k_int32>(cp, val_ptr);
      break;
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::DATE:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      Radix::DecodeData<k_int64>(cp, val_ptr);
      break;
    case roachpb::DataType::FLOAT:
      Radix::DecodeData<k_float32>(cp, val_ptr);
      break;
    case roachpb::DataType::DOUBLE:
      Radix::DecodeData<k_double64>(cp, val_ptr);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      if (col_info_[col].max_string_len > 0) {
        Radix::DecodeVarData(cp, val_ptr, col_info_[col].max_string_len + STRING_WIDE);
      } else {
        Radix::DecodeVarData(cp, val_ptr, col_info_[col].fixed_storage_len);
      }
      break;
    case roachpb::DataType::DECIMAL:
      memcpy(col_ptr, val_ptr, BOOL_WIDE);
      if (*reinterpret_cast<k_bool*>(val_ptr)) {
        Radix::DecodeData<k_double64>(cp + BOOL_WIDE, val_ptr + BOOL_WIDE);
      } else {
        Radix::DecodeData<k_int64>(cp + BOOL_WIDE, val_ptr + BOOL_WIDE);
      }
      break;
    case roachpb::DataType::BOOL:
      Radix::DecodeData<k_bool>(cp, val_ptr);
      break;
    case roachpb::DataType::NULLVAL:
      Radix::DecodeData<k_bool>(cp, val_ptr);
      break;
    default:
      LOG_ERROR("Type(%d) is not supported in order by",
                col_info_[col].storage_type);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus ISortableChunk::EncodeColDataInvert(k_uint32 col, k_uint8* col_ptr,
                                            k_uint8* val_ptr) {
  switch (col_info_[col].storage_type) {
    case roachpb::DataType::SMALLINT:
      Radix::EncodeDataInvert<k_int16>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::INT:
      Radix::EncodeDataInvert<k_int32>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::DATE:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      Radix::EncodeDataInvert<k_int64>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::FLOAT:
      Radix::EncodeDataInvert<k_float32>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::DOUBLE:
      Radix::EncodeDataInvert<k_double64>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      if (col_info_[col].max_string_len > 0) {
        Radix::EncodeVarDataInvert(col_ptr, val_ptr, col_info_[col].max_string_len + STRING_WIDE);
      } else {
        Radix::EncodeVarDataInvert(col_ptr, val_ptr, col_info_[col].fixed_storage_len);
      }
      break;
    case roachpb::DataType::DECIMAL:
      memcpy(col_ptr, val_ptr, BOOL_WIDE);
      if (*reinterpret_cast<k_bool*>(val_ptr)) {
        Radix::EncodeDataInvert<k_double64>(col_ptr + BOOL_WIDE,
                                            val_ptr + BOOL_WIDE);
      } else {
        Radix::EncodeDataInvert<k_int64>(col_ptr + BOOL_WIDE,
                                         val_ptr + BOOL_WIDE);
      }
      break;
    case roachpb::DataType::BOOL:
      Radix::EncodeDataInvert<k_bool>(col_ptr, val_ptr);
      break;
    case roachpb::DataType::NULLVAL:
      Radix::EncodeDataInvert<k_bool>(col_ptr, val_ptr);
      break;
    default:
      LOG_ERROR("Type(%d) is not supported in order by",
                col_info_[col].storage_type);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus ISortableChunk::DecodeColDataInvert(k_uint32 col, DatumPtr col_ptr,
                                            k_uint8* val_ptr) {
  k_uint8* cp = reinterpret_cast<k_uint8*>(col_ptr);
  switch (col_info_[col].storage_type) {
    case roachpb::DataType::SMALLINT:
      Radix::DecodeDataInvert<k_int16>(cp, val_ptr);
      break;
    case roachpb::DataType::INT:
      Radix::DecodeDataInvert<k_int32>(cp, val_ptr);
      break;
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::DATE:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      Radix::DecodeDataInvert<k_int64>(cp, val_ptr);
      break;
    case roachpb::DataType::FLOAT:
      Radix::DecodeDataInvert<k_float32>(cp, val_ptr);
      break;
    case roachpb::DataType::DOUBLE:
      Radix::DecodeDataInvert<k_double64>(cp, val_ptr);
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      if (col_info_[col].max_string_len > 0) {
        Radix::DecodeVarDataInvert(cp, val_ptr, col_info_[col].max_string_len + STRING_WIDE);
      } else {
        Radix::DecodeVarDataInvert(cp, val_ptr, col_info_[col].fixed_storage_len);
      }
      break;
    case roachpb::DataType::DECIMAL:
      memcpy(col_ptr, val_ptr, BOOL_WIDE);
      if (*reinterpret_cast<k_bool*>(val_ptr)) {
        Radix::DecodeDataInvert<k_double64>(cp + BOOL_WIDE,
                                            val_ptr + BOOL_WIDE);
      } else {
        Radix::DecodeDataInvert<k_int64>(cp + BOOL_WIDE, val_ptr + BOOL_WIDE);
      }
      break;
    case roachpb::DataType::BOOL:
      Radix::DecodeDataInvert<k_bool>(cp, val_ptr);
      break;
    case roachpb::DataType::NULLVAL:
      Radix::DecodeDataInvert<k_bool>(cp, val_ptr);
      break;
    default:
      LOG_ERROR("Type(%d) is not supported in order by",
                col_info_[col].storage_type);
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

k_uint32 SortRowChunk::ComputeRowSize(ColumnInfo* col_info,
                                      std::vector<ColumnOrderInfo> order_info,
                                      k_uint32 col_num) {
  k_uint32 row_size = 0;
  for (k_uint32 i = 0; i < col_num; i++) {
    row_size += NULL_INDECATOR_WIDE;  // null indicator
    if (col_info[i].is_string) {
      row_size += NON_CONSTANT_PLACE_HOLDER_WIDE + STRING_WIDE;
    }
    row_size += col_info[i].fixed_storage_len;
  }
  row_size += LINE_NUMBER_WIDE;  // row index
  return row_size;
}

k_uint32 SortRowChunk::ComputeSortRowSize(
    ColumnInfo* col_info, std::vector<ColumnOrderInfo>& order_info,
    k_uint32 col_num) {
  k_uint32 row_size = 0;
  for (auto& col : order_info) {
    row_size += NULL_INDECATOR_WIDE;
    if (col_info[col.col_idx].is_string) {
      row_size += NON_CONSTANT_PLACE_HOLDER_WIDE;
    } else {
      row_size += col_info[col.col_idx].fixed_storage_len;
    }
  }
  row_size += LINE_NUMBER_WIDE;  // row index
  return row_size;
}

KStatus SortRowChunk::Expand(k_uint32 new_count, k_bool copy) {
  if (all_constant_ || force_constant_) {
    return KStatus::FAIL;
  }
  auto estimate_row_size = ComputeRowSizeIfAllConstant();
  if (non_constant_data_size_ + (new_count - count_) * estimate_row_size >
      non_constant_max_size_) {
    capacity_ = count_;
    return KStatus::FAIL;
  } else {
    capacity_ = new_count;
    auto new_data = KNEW char[capacity_ * row_size_];
    if (new_data == nullptr) {
      capacity_ = count_;
      return KStatus::FAIL;
    }
    if (copy) {
      memcpy(new_data, data_, data_size_);
    }
    data_size_ = capacity_ * row_size_;
    SafeDeleteArray(data_);
    data_ = new_data;
    return SUCCESS;
  }
}

k_uint32 SortRowChunk::ComputeRowSizeIfAllConstant() {
  if (all_constant_) {
    return row_size_;
  }
  k_uint32 estimate_row_size_ = 0;
  for (int i = 0; i < col_num_; i++) {
    if (col_info_[i].is_string) {
      estimate_row_size_ += NON_CONSTANT_PLACE_HOLDER_WIDE + STRING_WIDE +
                            col_info_[i].max_string_len;
    } else {
      estimate_row_size_ += col_info_[i].fixed_storage_len;
    }
  }
  return estimate_row_size_;
}

KStatus SortRowChunk::CopyWithSortFrom(SortRowChunkPtr& data_chunk_ptr) {
  count_ = *(data_chunk_ptr->GetCount());
  auto src_data = data_chunk_ptr->GetData();
  all_constant_ = data_chunk_ptr->IsAllConstant();
  all_constant_in_order_col_ = data_chunk_ptr->IsAllConstantInOrderCol();
  is_ordered_ = data_chunk_ptr->IsOrdered();
  if (count_ == 0) {
    return KStatus::SUCCESS;
  }
  if (count_ > capacity_) {
    KStatus ret = Expand(count_, false);
  }
  if (count_ == 1 || is_ordered_) {
    memcpy(data_, src_data, data_chunk_ptr->Size());
    if (!all_constant_) {
      memcpy(non_constant_data_, data_chunk_ptr->GetNonConstantData(),
             data_chunk_ptr->GetNonConstantDataSize());
    }
    return KStatus::SUCCESS;
  }

  if (all_constant_in_order_col_) {
    if (count_ >= max_output_count_) {
      RadixSortMSD(src_data, data_, 0, count_, 0);
    } else {
      RadixSortLSD(src_data, data_);
    }
    if (!all_constant_) {
      if (count_ < max_output_count_ && non_constant_save_mode_ == OFFSET_MODE) {
        memcpy(non_constant_data_, data_chunk_ptr->GetNonConstantData(),
               data_chunk_ptr->GetNonConstantDataSize());
      } else {
        //  Reorder non-constant data
        count_ = std::min(count_, max_output_count_);
        for (k_uint32 i = 0; i < count_; ++i) {
          ReplaceNonConstantRowData(data_ + i * row_size_,
                                    data_chunk_ptr->GetNonConstantData(), i,
                                    data_chunk_ptr->non_constant_save_mode_);
        }
      }
      is_ordered_ = true;
      return KStatus::SUCCESS;
    }
  } else {
    data_chunk_ptr->ChangeOffsetToPointer();
    auto compare = HasNonConstantColumnCompare(this);
    std::vector<DatumPtr> row_ptrs;
    row_ptrs.reserve(count_);
    for (k_uint32 i = 0; i < count_; ++i) {
      row_ptrs.push_back(data_chunk_ptr->GetRowData(i));
    }
    if (count_ >= max_output_count_) {
      count_ = max_output_count_;
      std::partial_sort(row_ptrs.begin(), row_ptrs.begin() + count_,
                        row_ptrs.end(), compare);
    } else {
      std::sort(row_ptrs.begin(), row_ptrs.end(), compare);
    }
    for (k_uint32 i = 0; i < count_; ++i) {
      memcpy(data_ + i * row_size_, row_ptrs[i], row_size_);
      ReplaceNonConstantRowData(data_ + i * row_size_,
                                data_chunk_ptr->GetNonConstantData(), i,
                                data_chunk_ptr->non_constant_save_mode_);
    }
  }
  is_ordered_ = true;
  return KStatus::SUCCESS;
}

void SortRowChunk::RadixSortLSD(DatumPtr src_data, DatumPtr dest_data) {
  k_uint32 counts[256];
  bool swap = false;
  unsigned char* source = nullptr;
  unsigned char* target = nullptr;
  for (int byte_pos = sort_row_size_ - 1; byte_pos >= 0; --byte_pos) {
    std::memset(counts, 0, sizeof(counts));

    if (swap) {
      source = reinterpret_cast<unsigned char*>(dest_data);
      target = reinterpret_cast<unsigned char*>(src_data);
    } else {
      source = reinterpret_cast<unsigned char*>(src_data);
      target = reinterpret_cast<unsigned char*>(dest_data);
    }
    for (int i = 0; i < count_; ++i) {
      ++counts[*(source + i * row_size_ + byte_pos)];
    }

    k_uint32 max_count = counts[0];
    for (int i = 1; i < 256; ++i) {
      max_count = std::max(max_count, counts[i]);
      counts[i] += counts[i - 1];
    }
    if (max_count == count_) {
      continue;
    }

    for (int i = count_ - 1; i >= 0; --i) {
      memcpy(
          target + (--counts[*(source + i * row_size_ + byte_pos)]) * row_size_,
          source + i * row_size_, row_size_);
    }
    swap = !swap;
  }
  if (!swap) {
    memcpy(dest_data, src_data, row_size_ * count_);
  }
}

void SortRowChunk::RadixSortMSD(DatumPtr src_data, DatumPtr dest_data,
                                k_uint32 start, k_uint32 end,
                                k_uint32 byte_pos) {
  if (start >= end || byte_pos >= sort_row_size_ ||
      start >= max_output_count_) {
    return;
  }

  k_uint32 counts[256] = {0};
  unsigned char* source = reinterpret_cast<unsigned char*>(src_data);

  for (k_uint32 i = start; i < end; ++i) {
    ++counts[source[i * row_size_ + byte_pos]];
  }

  k_uint32 temp[256];
  temp[0] = start;
  for (int i = 1; i < 256; ++i) {
    temp[i] = temp[i - 1] + counts[i - 1];
  }

  unsigned char* target = reinterpret_cast<unsigned char*>(dest_data);
  for (k_uint32 i = start; i < end; ++i) {
    unsigned char byte_value = source[i * row_size_ + byte_pos];
    memcpy(target + temp[byte_value] * row_size_, source + i * row_size_,
           row_size_);
    ++temp[byte_value];
  }

  memcpy(source + start * row_size_, target + start * row_size_,
         (end - start) * row_size_);

  for (int i = 0; i < 256; ++i) {
    if (counts[i] > 1) {
      k_uint32 new_start = start + temp[i] - counts[i];
      k_uint32 new_end = start + temp[i];
      if (new_start >= max_output_count_) {
        break;
      }
      RadixSortMSD(src_data, dest_data, new_start, new_end, byte_pos + 1);
    }
  }
}

void SortRowChunk::ReplaceNonConstantRowData(DatumPtr src_row_data,
                                             DatumPtr src_non_const_data,
                                             k_uint32 row_index,
                                             NonConstantSaveMode mode) {
  for (auto& offset : non_constant_col_offsets_) {
    k_bool is_null;
    std::memcpy(&is_null, src_row_data + offset, NULL_INDECATOR_WIDE);
    if (is_null) {
      continue;
    }
    DatumPtr non_constant_ptr;
    if (mode == POINTER_MODE) {
      non_constant_ptr = *reinterpret_cast<DatumPtr*>(src_row_data + offset +
                                                      NULL_INDECATOR_WIDE);
    } else {
      auto non_constant_offset = reinterpret_cast<k_uint64*>(
          src_row_data + offset + NULL_INDECATOR_WIDE);
      non_constant_ptr = src_non_const_data + *non_constant_offset;
    }

    auto len = reinterpret_cast<k_uint16*>(non_constant_ptr);
    std::memcpy(non_constant_data_ + non_constant_data_size_, non_constant_ptr,
                *len + STRING_WIDE);
    if (non_constant_save_mode_ == POINTER_MODE) {
      auto now_nc_ptr = non_constant_data_ + non_constant_data_size_;
      std::memcpy(data_ + row_index * row_size_ + offset + NULL_INDECATOR_WIDE,
                  &now_nc_ptr, NON_CONSTANT_PLACE_HOLDER_WIDE);
    } else {
      std::memcpy(data_ + row_index * row_size_ + offset + NULL_INDECATOR_WIDE,
                  &non_constant_data_size_, NON_CONSTANT_PLACE_HOLDER_WIDE);
    }
    non_constant_data_size_ += (STRING_WIDE + *len);
  }
}

void SortRowChunk::ChangeOffsetToPointer() {
  if (non_constant_save_mode_ == POINTER_MODE) {
    return;
  }
  non_constant_save_mode_ = POINTER_MODE;
  for (k_uint32 i = 0; i < count_; ++i) {
    for (auto& offset : non_constant_col_offsets_) {
      auto col_ptr = data_ + i * row_size_ + offset;
      auto is_null = reinterpret_cast<k_bool*>(col_ptr);
      if (*is_null) {
        continue;
      }
      auto non_constant_offset =
          reinterpret_cast<k_uint64*>(col_ptr + NULL_INDECATOR_WIDE);
      DatumPtr non_constant_ptr = non_constant_data_ + *non_constant_offset;
      std::memcpy(col_ptr + NULL_INDECATOR_WIDE, &non_constant_ptr,
                  NON_CONSTANT_PLACE_HOLDER_WIDE);
    }
  }
}

void SortRowChunk::ChangePointerToOffset() {
  if (non_constant_save_mode_ == OFFSET_MODE) {
    return;
  }
  non_constant_save_mode_ = OFFSET_MODE;
  for (k_uint32 i = 0; i < count_; ++i) {
    for (auto& offset : non_constant_col_offsets_) {
      auto col_ptr = data_ + i * row_size_ + offset;
      auto is_null = reinterpret_cast<k_bool*>(col_ptr);
      if (*is_null) {
        continue;
      }
      DatumPtr non_constant_ptr =
          *reinterpret_cast<DatumPtr*>(col_ptr + NULL_INDECATOR_WIDE);
      k_uint64 non_constant_offset = non_constant_ptr - non_constant_data_;
      std::memcpy(col_ptr + NULL_INDECATOR_WIDE, &non_constant_offset,
                  NON_CONSTANT_PLACE_HOLDER_WIDE);
    }
  }
}

EEIteratorErrCode SortRowChunk::VectorizeData(kwdbContext_p ctx,
                                              DataInfo* data_info) {
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  bitmap_size_ = (capacity_ + 7) / 8;

  do {
    TsColumnInfo* ColInfo =
        static_cast<TsColumnInfo*>(malloc(col_num_ * sizeof(TsColumnInfo)));
    if (nullptr == ColInfo) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      break;
    }

    TsColumnData* ColData =
        static_cast<TsColumnData*>(malloc(col_num_ * sizeof(TsColumnData)));
    if (nullptr == ColData) {
      SafeFreePointer(ColInfo);
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      break;
    }
    std::vector<k_uint32> col_offsets;
    col_offsets.resize(col_num_);

    k_uint32 constant_col_data_size = 0;

    k_uint32 constant_col_row_size = 0;

    k_uint32 bitmap_offset = 0;

    if (bitmap_offset_ == nullptr) {
      bitmap_offset_ = KNEW k_uint32[col_num_];
    }
    for (k_int32 i = 0; i < col_num_; i++) {
      bitmap_offset_[i] = bitmap_offset;
      col_offsets[i] = bitmap_offset + bitmap_size_;
      bitmap_offset += bitmap_size_;
      if (col_info_[i].is_string) {
        continue;
      }
      bitmap_offset += col_info_[i].fixed_storage_len * capacity_;
      constant_col_row_size += col_info_[i].fixed_storage_len;
    }
    constant_col_data_size = (capacity_ + 7) / 8 * col_num_ + capacity_ * constant_col_row_size;

    DatumPtr col_data = EE_MemPoolMalloc(g_pstBufferPoolInfo, constant_col_data_size);
    if (nullptr == col_data) {
      SafeFreePointer(ColInfo);
      SafeFreePointer(ColData);
      LOG_ERROR("Failed to allocate memory for col_data\n");
      return EEIteratorErrCode::EE_ERROR;
    }
    for (k_int32 i = 0; i < col_num_; ++i) {
      ColInfo[i].fixed_len_ = col_info_[i].fixed_storage_len;
      ColInfo[i].return_type_ = col_info_[i].return_type;
      ColInfo[i].storage_len_ = col_info_[i].storage_len;
      ColInfo[i].storage_type_ = col_info_[i].storage_type;
      if (ColInfo[i].return_type_ == KWDBTypeFamily::StringFamily ||
          ColInfo[i].return_type_ == KWDBTypeFamily::BytesFamily) {
        k_int32* offset =
            static_cast<k_int32*>(malloc((count_ + 1) * sizeof(k_int32)));
        if (nullptr == offset) {
          SafeFreePointer(ColInfo);
          SafeFreePointer(ColData);
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                        "Insufficient memory");
          break;
        }
        memset(offset, 0, count_ * sizeof(k_int32));
        DatumPtr bitmap = col_data + bitmap_offset_[i];
        memset(bitmap, 0, bitmap_size_);
        k_int32 total_len = 0;
        std::vector<k_uint16> vec_len;
        vec_len.reserve(count_);
        for (k_uint32 j = 0; j < count_; ++j) {
          k_uint16 len = 0;
          GetData(j, i, len);
          offset[j] = total_len;
          total_len += len;
          vec_len.push_back(len);
        }
        offset[count_] = total_len;
        ColData[i].offset_ = offset;
        char* ptr = static_cast<char*>(malloc(total_len));
        if (nullptr == ptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                        "Insufficient memory");
          SafeFreePointer(ColInfo);
          SafeFreePointer(ColData);
          SafeFreePointer(offset);
          break;
        }
        memset(ptr, 0, total_len);
        ColData[i].data_ptr_ = ptr;
        for (k_uint32 j = 0; j < count_; ++j) {
          memcpy(ptr + offset[j], GetData(j, i, vec_len[j]), vec_len[j]);
          bitmap[j >> 3] |= (IsNull(j, i) ? 1 : 0) << (j & 7);
        }
        ColData[i].data_ptr_ = ptr;
        ColData[i].bitmap_ptr_ = bitmap;
      } else {
        DatumPtr col_ptr = col_data + col_offsets[i];
        DatumPtr bitmap = col_data + bitmap_offset_[i];
        memset(bitmap, 0, bitmap_size_);
        for (k_uint32 j = 0; j < count_; ++j) {
          memcpy(col_ptr + j * ColInfo[i].fixed_len_, GetData(j, i),
                 ColInfo[i].fixed_len_);
          bitmap[j >> 3] |= (IsNull(j, i) ? 1 : 0) << (j & 7);
        }
        ColData[i].data_ptr_ = col_ptr;
        ColData[i].bitmap_ptr_ = bitmap;
      }
    }

    data_info->column_num_ = col_num_;
    data_info->column_ = ColInfo;
    data_info->column_data_ = ColData;
    data_info->data_count_ = count_;
    data_info->bitmap_size_ = bitmap_size_;
    data_info->row_size_ = row_size_;
    data_info->capacity_ = capacity_;
    data_info->data_ = col_data;
    data_info->is_data_owner_ = is_data_owner_;

    ret = EEIteratorErrCode::EE_OK;
  } while (0);

  return ret;
}

}  // namespace kwdbts
