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

#include "lg_api.h"
#include "ee_data_chunk.h"
#include "cm_func.h"
#include "ee_common.h"
#include "ee_timestamp_utils.h"
#include "ee_sort_compare.h"
namespace kwdbts {

template <typename T>
static inline k_int32 fastIntToString(T value, char* buffer) {
  // Handle zero case first
  if (value == 0) {
    buffer[0] = '0';
    buffer[1] = '\0';
    return 1;
  }

  k_int32 pos = 0;

  // Handle negative numbers
  using UnsignedT = typename std::make_unsigned<T>::type;
  UnsignedT abs_value;

  if constexpr (std::is_signed_v<T>) {
    if (value < 0) {
      buffer[pos++] = '-';
      // Handle INT_MIN overflow safely
      abs_value = static_cast<UnsignedT>(0) - static_cast<UnsignedT>(value);
    } else {
      abs_value = static_cast<UnsignedT>(value);
    }
  } else {
    abs_value = value;
  }

  // Find the number of digits first to avoid array reversal
  UnsignedT temp_val = abs_value;
  k_int32 digit_count = 0;
  while (temp_val > 0) {
    digit_count++;
    temp_val /= 10;
  }

  // Write digits directly in correct order
  k_int32 digit_pos = pos + digit_count - 1;
  temp_val = abs_value;

  while (temp_val > 0) {
    buffer[digit_pos--] = '0' + (temp_val % 10);
    temp_val /= 10;
  }

  pos += digit_count;
  buffer[pos] = '\0';
  return pos;
}

class MemCompare {
 public:
  explicit MemCompare(DataChunk *chunk, bool is_reverse): chunk_(chunk), is_reverse_(is_reverse) {}

  inline bool operator()(k_uint32 l1, k_uint32 r1) {
    void *lptr = chunk_->GetData(l1, chunk_->ColumnNum() - 1);
    void *rptr = chunk_->GetData(r1, chunk_->ColumnNum() - 1);
    k_int64 l_ts = *(static_cast<k_int64*>(lptr));
    k_int64 r_ts = *(static_cast<k_int64*>(rptr));
    if (is_reverse_) {
      return l_ts > r_ts;
    } else {
      return l_ts < r_ts;
    }
  }

  DataChunk *chunk_{nullptr};
  bool is_reverse_{false};
};

DataChunk::DataChunk(ColumnInfo *col_info, k_int32 col_num, k_uint32 capacity) :
    col_info_(col_info), col_num_(col_num), capacity_(capacity) {}

DataChunk::DataChunk(ColumnInfo* col_info, k_int32 col_num, const char* buf,
                     k_uint32 count, k_uint32 capacity)
    : data_(const_cast<char*>(buf)),
      col_info_(col_info),
      col_num_(col_num),
      capacity_(capacity),
      count_(count) {
  is_data_owner_ = false;
}

DataChunk::~DataChunk() {
  SafeDeleteArray(col_offset_);
  SafeDeleteArray(bitmap_offset_);
  if (is_data_owner_) {
    kwdbts::EE_MemPoolFree(g_pstBufferPoolInfo, data_);
    data_ = nullptr;
  }
  if (is_buf_owner_) {
    SafeFreePointer(encoding_buf_);
  }
}

k_bool DataChunk::Initialize() {
  // null bitmap
  // calculate row width and length
  row_size_ = ComputeRowSize(col_info_, col_num_);

  if (capacity_ == 0) {
    // (capacity_ + 7)/8 * col_num_ + capacity_ * row_size_ <= DataChunk::SIZE_LIMIT
    capacity_ = EstimateCapacity(col_info_, col_num_);
  }
  data_size_ = (capacity_ + 7) / 8 * col_num_ + capacity_ * row_size_;

  if (is_data_owner_) {
    if (capacity_ * row_size_ > 0) {
      k_uint64 data_len = Size();
      if (data_len <= DataChunk::SIZE_LIMIT) {
        data_ = kwdbts::EE_MemPoolMalloc(g_pstBufferPoolInfo, ROW_BUFFER_SIZE);
        data_len = ROW_BUFFER_SIZE;
      } else {
        data_ = kwdbts::EE_MemPoolMalloc(g_pstBufferPoolInfo, data_len);
      }
      // allocation failure
      if (data_ == nullptr) {
        LOG_ERROR("Allocate buffer in DataChunk failed.");
        return false;
      }
      std::memset(data_, 0, data_len);
    }
  }

  bitmap_size_ = (capacity_ + 7) / 8;

  k_uint32 bitmap_offset = 0;
  bitmap_offset_ = KNEW k_uint32[col_num_];
  col_offset_ = KNEW k_uint32[col_num_];
  if (bitmap_offset_ == nullptr || col_offset_ == nullptr) {
    LOG_ERROR("Allocate buffer in DataChunk failed.");
    return false;
  }
  for (k_int32 i = 0; i < col_num_; i++) {
    bitmap_offset_[i] = bitmap_offset;
    col_offset_[i] = bitmap_offset + bitmap_size_;
    bitmap_offset += col_info_[i].fixed_storage_len * capacity_ + bitmap_size_;
  }

  return true;
}

char* DataChunk::GetBitmapPtr(k_uint32 col) {
  return data_ + bitmap_offset_[col];
}

KStatus DataChunk::InsertData(k_uint32 row, k_uint32 col, DatumPtr value, k_uint16 len, bool set_not_null) {
  k_uint32 col_offset = row * col_info_[col].fixed_storage_len + col_offset_[col];

  if (col_info_[col].is_string) {
    std::memcpy(data_ + col_offset, &len, STRING_WIDE);
    std::memcpy(data_ + col_offset + STRING_WIDE, value, len);
  } else {
    std::memcpy(data_ + col_offset, value, len);
  }
  if (set_not_null) {
    SetNotNull(row, col);
  }
  return SUCCESS;
}

KStatus DataChunk::InsertDecimal(k_uint32 row, k_uint32 col, DatumPtr value, k_bool is_double) {
  if (col_info_[col].storage_type != roachpb::DataType::DECIMAL) {
    return FAIL;
  }
  k_uint32 col_offset = row * col_info_[col].fixed_storage_len + col_offset_[col];

  std::memcpy(data_ + col_offset, &is_double, BOOL_WIDE);
  std::memcpy(data_ + col_offset + BOOL_WIDE, value, sizeof(k_double64));
  SetNotNull(row, col);
  return SUCCESS;
}

KStatus DataChunk::PutData(kwdbContext_p ctx, DatumPtr value, k_uint32 count) {
  EnterFunc();
  if (value == nullptr) {
    Return(KStatus::FAIL);
  }
  memcpy(data_, value, bitmap_size_ * col_num_ + capacity_ * row_size_);
  count_ = count;
  Return(KStatus::SUCCESS);
}

KStatus DataChunk::InsertData(kwdbContext_p ctx, IChunk* value, Field** renders) {
  EnterFunc();

  if (nullptr == renders) {
    if (value == nullptr) {
      Return(KStatus::FAIL);
    }

    for (uint32_t col_idx = 0; col_idx < col_num_; col_idx++) {
      if (value->IsNull(col_idx)) {
        SetNull(count_, col_idx);
      } else {
        SetNotNull(count_, col_idx);
        std::memcpy(data_ + count_ * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx],
                    value->GetData(col_idx), col_info_[col_idx].fixed_storage_len);
      }
    }
    AddCount();
    Return(KStatus::SUCCESS);
  }

  for (k_uint32 col = 0; col < col_num_; ++col) {
    Field* field = renders[col];

    // dispose null
    if (field->is_nullable()) {
      SetNull(count_, col);
      continue;
    }

    k_uint32 len = field->get_storage_length();
    switch (field->get_storage_type()) {
      case roachpb::DataType::BOOL: {
        bool val = field->ValInt() > 0 ? 1 : 0;
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        k_int64 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::INT: {
        k_int32 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::SMALLINT: {
        k_int16 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::FLOAT: {
        k_float32 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::DOUBLE: {
        k_double64 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::VARBINARY: {
        kwdbts::String val = field->ValStr();
        if (val.isNull()) {
          SetNull(count_, col);
          break;
        }
        char* mem = const_cast<char*>(val.c_str());
        InsertData(count_, col, mem, val.length());
        break;
      }
      case roachpb::DataType::DECIMAL: {
        if (field->get_field_type() == Field::Type::FIELD_AGG ||
            field->get_field_type() == Field::Type::FIELD_ITEM) {
          DatumPtr src = field->get_ptr();
          InsertData(count_, col, src, len + BOOL_WIDE);
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  AddCount();

  Return(KStatus::SUCCESS);
}

KStatus DataChunk::InsertData(kwdbContext_p ctx, IChunk* value, std::vector<Field*> &output_fields) {
  EnterFunc();

  for (k_uint32 col = 0; col < col_num_; ++col) {
    Field* field = output_fields[col];

    // dispose null
    if (field->is_nullable()) {
      SetNull(count_, col);
      continue;
    }

    k_uint32 len = field->get_storage_length();
    switch (field->get_storage_type()) {
      case roachpb::DataType::BOOL: {
        bool val = field->ValInt() > 0 ? 1 : 0;
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        k_int64 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::INT: {
        k_int32 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::SMALLINT: {
        k_int16 val = field->ValInt();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::FLOAT: {
        k_float32 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::DOUBLE: {
        k_double64 val = field->ValReal();
        InsertData(count_, col, reinterpret_cast<char*>(&val), len);
        break;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::VARBINARY: {
        kwdbts::String val = field->ValStr();
        if (val.isNull()) {
          SetNull(count_, col);
          break;
        }
        char* mem = const_cast<char*>(val.c_str());
        InsertData(count_, col, mem, val.length());
        break;
      }
      case roachpb::DataType::DECIMAL: {
        if (field->get_field_type() == Field::Type::FIELD_AGG ||
            field->get_field_type() == Field::Type::FIELD_ITEM) {
          DatumPtr src = field->get_ptr();
          InsertData(count_, col, src, len + BOOL_WIDE);
        }
        break;
      }
      default: {
        break;
      }
    }
  }
  AddCount();

  Return(KStatus::SUCCESS);
}

DatumPtr DataChunk::GetDataPtr(k_uint32 row, k_uint32 col) {
  if (IsNull(row, col)) {
    return nullptr;
  }
  DatumPtr p = data_ + row * col_info_[col].fixed_storage_len + col_offset_[col];
  if (col_info_[col].is_string) {
    p += STRING_WIDE;
  } else if (col_info_[col].storage_type == roachpb::DataType::DECIMAL) {
    p += BOOL_WIDE;
  }
  return p;
}

DatumPtr DataChunk::GetData(k_uint32 row, k_uint32 col) {
  return data_ + row * col_info_[col].fixed_storage_len + col_offset_[col];
}

DatumPtr DataChunk::GetData(k_uint32 row, k_uint32 col, k_uint16& len) {
  k_uint32 col_offset = row * col_info_[col].fixed_storage_len + col_offset_[col];
  std::memcpy(&len, data_ + col_offset, sizeof(k_uint16));
  return data_ + col_offset + sizeof(k_uint16);
}

DatumPtr DataChunk::GetVarData(k_uint32 col, k_uint16& len) {
  k_uint32 col_offset = current_line_ * col_info_[col].fixed_storage_len + col_offset_[col];
  std::memcpy(&len, data_ + col_offset, sizeof(k_uint16));
  return data_ + col_offset + sizeof(k_uint16);
}

DatumPtr DataChunk::GetData(k_uint32 col) {
  return data_ + current_line_ * col_info_[col].fixed_storage_len + col_offset_[col];
}

DatumPtr DataChunk::GetDataPtr(k_uint32 col) {
  return data_ + col_offset_[col];
}

bool DataChunk::IsNull(k_uint32 row, k_uint32 col) {
  if (!col_info_[col].allow_null) {
    return false;
  }
  char* bitmap = data_ + bitmap_offset_[col];
  if (bitmap == nullptr) {
    return true;
  }

  // return (bitmap[row >> 3] & ((1 << 7) >> (row & 7))) != 0;  // (bitmap[row / 8] & ((1 << 7) >> (row % 8))) != 0;
  return bitmap[row >> 3] & (1 << (row & 7));
}

bool DataChunk::IsNull(k_uint32 col) {
  return IsNull(current_line_, col);
}

// 1 null，0 not null
void DataChunk::SetNull(k_uint32 row, k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(data_ + bitmap_offset_[col]);
  if (bitmap == nullptr) {
    return;
  }

  // bitmap[row >> 3] |= (1 << 7) >> (row & 7);
  bitmap[row >> 3] |= 1 << (row & 7);
}

void DataChunk::SetNotNull(k_uint32 row, k_uint32 col) {
  char* bitmap = reinterpret_cast<char*>(data_ + bitmap_offset_[col]);
  if (bitmap == nullptr) {
    return;
  }

  // k_uint32 index = row >> 3;     // row / 8
  // unsigned int pos = 1 << 7;    // binary 1000 0000
  // unsigned int mask = pos >> (row & 7);     // pos >> (row % 8)
  // bitmap[index] &= ~mask;

  bitmap[row >> 3] &= ~(1 << (row & 7));
}

void DataChunk::SetAllNull() {
  for (int col_idx = 0; col_idx < col_num_; col_idx++) {
    char* bitmap = reinterpret_cast<char*>(data_ + bitmap_offset_[col_idx]);
    if (bitmap == nullptr) {
      return;
    }
    std::memset(bitmap, 0xFF, bitmap_size_);
  }
}

KStatus DataChunk::Append(DataChunk* chunk) {
  for (uint32_t col_idx = 0; col_idx < col_num_; col_idx++) {
    size_t col_data_length = col_info_[col_idx].fixed_storage_len * chunk->Count();
    std::memcpy(data_ + count_ * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx],
                chunk->GetData(0, col_idx), col_data_length);

    for (k_uint32 row = 0; row < chunk->Count(); row++) {
      if (chunk->IsNull(row, col_idx)) {
        SetNull(count_ + row, col_idx);
      }
    }
  }
  count_ += chunk->Count();
  return SUCCESS;
}

KStatus DataChunk::Append(std::queue<DataChunkPtr>& buffer) {
  KStatus ret = SUCCESS;
  while (!buffer.empty()) {
    auto& buf = buffer.front();
    ret = Append(buf.get());
    if (ret != SUCCESS) {
      return ret;
    }
    buffer.pop();
  }
  return SUCCESS;
}

KStatus DataChunk::Append(DataChunk* chunk, k_uint32 begin_row, k_uint32 end_row) {
  k_uint32 row_num = end_row - begin_row;
  for (uint32_t col_idx = 0; col_idx < col_num_; col_idx++) {
    size_t col_data_length = col_info_[col_idx].fixed_storage_len * row_num;
    std::memcpy(data_ + count_ * col_info_[col_idx].fixed_storage_len + col_offset_[col_idx],
                chunk->GetData(begin_row, col_idx), col_data_length);

    for (k_uint32 row = begin_row; row < end_row; row++) {
      if (chunk->IsNull(row, col_idx)) {
        SetNull(count_ + row - begin_row, col_idx);
      }
    }
  }

  count_ += row_num;
  return SUCCESS;
}

KStatus DataChunk::Append_Selective(DataChunk* src, const k_uint32* indexes, k_uint32 size) {
  for (size_t i = 0; i < size; i++) {
    k_int32 row = indexes[i];
    for (uint32_t col_idx = 0; col_idx < col_num_; col_idx++) {
      size_t col_data_length = col_info_[col_idx].fixed_storage_len;
      std::memcpy(data_ + count_ * col_info_[col_idx].fixed_storage_len +
                      col_offset_[col_idx],
                  src->GetData(row, col_idx), col_data_length);

        if (src->IsNull(row, col_idx)) {
          SetNull(count_ + i, col_idx);
        }
    }
    count_++;
  }
  return SUCCESS;
}

KStatus DataChunk::Append_Selective(DataChunk* src, k_uint32 row) {
  for (uint32_t col_idx = 0; col_idx < col_num_; col_idx++) {
    size_t col_data_length = col_info_[col_idx].fixed_storage_len;
    std::memcpy(data_ + count_ * col_info_[col_idx].fixed_storage_len +
                    col_offset_[col_idx],
                src->GetData(row, col_idx), col_data_length);

    if (src->IsNull(row, col_idx)) {
      SetNull(count_, col_idx);
    }
  }
  count_++;
  return SUCCESS;
}

k_int32 DataChunk::Compare(size_t left, size_t right, k_uint32 col_idx,
                           DataChunk* rhs) {
  // dispose Null
  k_bool is_a_null = IsNull(left, col_idx);
  k_bool is_b_null = rhs->IsNull(right, col_idx);

  // a,b is null
  if (is_a_null && is_b_null) {
    return 0;
  }

  if (is_a_null && !is_b_null) {
    return -1;
  } else if (!is_a_null && is_b_null) {
    return 1;
  }

  // compare not null
  switch (col_info_[col_idx].storage_type) {
    case roachpb::DataType::BOOL: {
      auto* a_data = reinterpret_cast<k_bool*>(GetData(left, col_idx));
      auto* b_data = reinterpret_cast<k_bool*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_bool>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::SMALLINT: {
      auto* a_data = reinterpret_cast<k_int16*>(GetData(left, col_idx));
      auto* b_data = reinterpret_cast<k_int16*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_int16>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::INT: {
      auto* a_data = reinterpret_cast<k_int32*>(GetData(left, col_idx));
      auto* b_data = reinterpret_cast<k_int32*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_int32>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT: {
      auto* a_data = reinterpret_cast<k_int64*>(GetData(left, col_idx));
      auto* b_data = reinterpret_cast<k_int64*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_int64>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::FLOAT: {
      auto* a_data = reinterpret_cast<k_float32*>(GetData(left, col_idx));
      auto* b_data = reinterpret_cast<k_float32*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_float32>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::DOUBLE: {
      auto* a_data = reinterpret_cast<k_double64*>(GetData(left, col_idx));
      auto* b_data =
          reinterpret_cast<k_double64*>(rhs->GetData(right, col_idx));
      return SorterComparator<k_double64>::compare(*a_data, *b_data);
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      auto* a_data = reinterpret_cast<char*>(GetData(left, col_idx));
      k_uint16 a_len;
      std::memcpy(&a_len, a_data, sizeof(k_uint16));
      std::string a_str = std::string{a_data + sizeof(k_uint16), a_len};

      auto* b_data = reinterpret_cast<char*>(rhs->GetData(right, col_idx));
      k_uint16 b_len;
      std::memcpy(&b_len, b_data, sizeof(k_uint16));
      std::string b_str = std::string{b_data + sizeof(k_uint16), b_len};
      return SorterComparator<std::string>::compare(a_str, b_str);
    }
    case roachpb::DataType::DECIMAL: {
      DatumPtr a_data = GetData(left, col_idx);
      DatumPtr b_data = rhs->GetData(right, col_idx);
      k_bool src_is_double = *reinterpret_cast<k_bool*>(a_data);
      k_bool dest_is_double = *reinterpret_cast<k_bool*>(b_data);

      if (!src_is_double && !dest_is_double) {
        k_int64 src_val = *reinterpret_cast<k_int64*>(a_data + sizeof(k_bool));
        k_int64 dest_val = *reinterpret_cast<k_int64*>(b_data + sizeof(k_bool));
        return SorterComparator<k_int64>::compare(src_val, dest_val);
      } else {
        k_double64 src_val, dest_val;
        if (src_is_double) {
          src_val = *reinterpret_cast<k_double64*>(a_data + sizeof(k_bool));
        } else {
          k_int64 src_ival = *reinterpret_cast<k_int64*>(a_data + sizeof(k_bool));
          src_val = (k_double64)src_ival;
        }

        if (dest_is_double) {
          dest_val = *reinterpret_cast<k_double64*>(b_data + sizeof(k_bool));
        } else {
          k_int64 dest_ival =
              *reinterpret_cast<k_int64*>(b_data + sizeof(k_bool));
          dest_val = (k_double64)dest_ival;
        }
        return SorterComparator<k_double64>::compare(src_val, dest_val);
      }
    }
    default:
      return 1;
  }
  return -1;
}

// Encode datachunk
KStatus DataChunk::Encoding(kwdbContext_p ctx, TsNextRetState nextState, bool use_query_short_circuit,
                            k_int64* command_limit,
                            std::atomic<k_int64>* count_for_limit) {
  KStatus st = KStatus::SUCCESS;

  if (DML_VECTORIZE_NEXT == nextState) {
    return st;
  }

  if (encoding_buf_ != nullptr) {
    return st;
  }

  EE_StringInfo msgBuffer = ee_makeStringInfo();
  if (msgBuffer == nullptr) {
    return KStatus::FAIL;
  }

  if (DML_PG_RESULT == nextState || use_query_short_circuit) {
    k_uint32 row = 0;
    for (; row < Count(); ++row) {
      if (*command_limit > 0) {
        k_int64 current_value = count_for_limit->load();
        while (current_value < (*command_limit) &&
               !count_for_limit->compare_exchange_weak(current_value,
                                                       current_value + 1)) {
          current_value = count_for_limit->load();
        }
        if (current_value >= (*command_limit)) {
          break;
        }
      }
      st = PgResultData(ctx, row, msgBuffer);
      if (st != SUCCESS) {
        break;
      }
    }
    count_ = row;
  } else {
    for (k_uint32 row = 0; row < Count(); ++row) {
      for (k_uint32 col = 0; col < ColumnNum(); ++col) {
        st = EncodingValue(ctx, row, col, msgBuffer);
        if (st != SUCCESS) {
          break;
        }
      }
      if (st != SUCCESS) {
        break;
      }
    }
  }
  if (st == SUCCESS) {
    encoding_buf_ = msgBuffer->data;
    encoding_len_ = msgBuffer->len;
  } else {
    free(msgBuffer->data);
  }
  delete msgBuffer;
  if (is_data_owner_) {
    kwdbts::EE_MemPoolFree(g_pstBufferPoolInfo, data_);
    data_ = nullptr;
  }
  return st;
}

void DataChunk::DebugPrintData() {
  char buffer[4096] = {0};
  memset(buffer, 0, sizeof(buffer));
  for (k_uint32 col = 0; col < ColumnNum(); ++col) {
    ColumnInfo info = col_info_[col];
    snprintf(buffer + strlen(buffer), sizeof(buffer), "%s,   ", KWDBTypeFamilyToString(info.return_type).c_str());
  }

  LOG_ERROR("%s, col count : %d, row count : %d", buffer, ColumnNum(), Count());

  for (k_uint32 row = 0; row < Count(); ++row) {
    memset(buffer, 0, sizeof(buffer));
    for (k_uint32 col = 0; col < ColumnNum(); ++col) {
      if (IsNull(row, col)) {
        snprintf(buffer + strlen(buffer), sizeof(buffer), "null,   ");
        break;
      }

      switch (col_info_[col].return_type) {
      case KWDBTypeFamily::BoolFamily: {
        DatumPtr raw = GetData(row, col);
        k_bool val;
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%d,   ", val);
        break;
      }
      case KWDBTypeFamily::BytesFamily:
      case KWDBTypeFamily::StringFamily: {
        k_uint16 val_len;
        DatumPtr raw = GetData(row, col, val_len);
        std::string val = std::string{static_cast<char*>(raw), val_len};
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%s,   ", val.c_str());
        break;
      }
      case KWDBTypeFamily::TimestampFamily:
      case KWDBTypeFamily::TimestampTZFamily: {
        DatumPtr raw = GetData(row, col);
        k_int64 val;
        std::memcpy(&val, raw, sizeof(k_int64));
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%ld,   ", val);
        break;
      }
      case KWDBTypeFamily::IntFamily: {
        DatumPtr raw = GetData(row, col);
        k_int64 val;
        switch (col_info_[col].storage_type) {
          case roachpb::DataType::BIGINT:
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
            std::memcpy(&val, raw, sizeof(k_int64));
            break;
          case roachpb::DataType::SMALLINT:
            k_int16 val16;
            std::memcpy(&val16, raw, sizeof(k_int16));
            val = val16;
            break;
          default:
            k_int32 val32;
            std::memcpy(&val32, raw, sizeof(k_int32));
            val = val32;
            break;
        }
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%ld,   ", val);
        break;
      }
      case KWDBTypeFamily::FloatFamily: {
        DatumPtr raw = GetData(row, col);
        k_double64 val;
        if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
          k_float32 val32;
          std::memcpy(&val32, raw, sizeof(k_float32));
          val = val32;
        } else {
          std::memcpy(&val, raw, sizeof(k_double64));
        }

        snprintf(buffer + strlen(buffer), sizeof(buffer), "%lf,   ", val);
        break;
      }
      case KWDBTypeFamily::DecimalFamily: {
        break;
      }
      case KWDBTypeFamily::IntervalFamily: {
        DatumPtr raw = GetData(row, col);
        k_int64 val;
        std::memcpy(&val, raw, sizeof(k_int64));
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%ld,   ", val);
        break;
      }
      case KWDBTypeFamily::DateFamily: {
        const int secondOfDay = 24 * 3600;
        DatumPtr raw = GetData(row, col);
        std::string date_str = std::string{static_cast<char*>(raw)};
        snprintf(buffer + strlen(buffer), sizeof(buffer), "%s,   ", date_str.c_str());
        break;
      }
      default: {
        break;
      }
      }
    }

    LOG_ERROR("%s", buffer);
  }
}

KStatus DataChunk::EncodingValue(kwdbContext_p ctx, k_uint32 row, k_uint32 col, const EE_StringInfo& info) {
  EnterFunc();
  KStatus ret = KStatus::SUCCESS;

  // dispose null
  if (IsNull(row, col)) {
    k_int32 len = ValueEncoding::EncodeComputeLenNull(0);
    ret = ee_enlargeStringInfo(info, len);
    if (ret != SUCCESS) {
      Return(ret);
    }

    CKSlice slice{info->data + info->len, len};
    ValueEncoding::EncodeNullValue(&slice, 0);
    info->len = info->len + len;
    Return(ret);
  }

  switch (col_info_[col].return_type) {
    case KWDBTypeFamily::BoolFamily: {
      DatumPtr raw = GetData(row, col);
      k_bool val;
      std::memcpy(&val, raw, sizeof(k_bool));
      k_int32 len = ValueEncoding::EncodeComputeLenBool(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeBoolValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::BytesFamily:
    case KWDBTypeFamily::StringFamily: {
      k_uint16 val_len;
      DatumPtr raw = GetData(row, col, val_len);
      std::string val = std::string{static_cast<char*>(raw), val_len};
      k_int32 len = ValueEncoding::EncodeComputeLenString(0, val.size());
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeBytesValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::TimestampFamily:
    case KWDBTypeFamily::TimestampTZFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));
      CKTime ck_time = getCKTime(val, col_info_[col].storage_type, ctx->timezone);
      k_int32 len = ValueEncoding::EncodeComputeLenTime(0, ck_time);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeTimeValue(&slice, 0, ck_time);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::IntFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      switch (col_info_[col].storage_type) {
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
          std::memcpy(&val, raw, sizeof(k_int64));
          break;
        case roachpb::DataType::SMALLINT:
          k_int16 val16;
          std::memcpy(&val16, raw, sizeof(k_int16));
          val = val16;
          break;
        default:
          k_int32 val32;
          std::memcpy(&val32, raw, sizeof(k_int32));
          val = val32;
          break;
      }
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::FloatFamily: {
      DatumPtr raw = GetData(row, col);
      k_double64 val;
      if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
        k_float32 val32;
        std::memcpy(&val32, raw, sizeof(k_float32));
        val = val32;
      } else {
        std::memcpy(&val, raw, sizeof(k_double64));
      }

      k_int32 len = ValueEncoding::EncodeComputeLenFloat(0);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeFloatValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::DecimalFamily: {
      switch (col_info_[col].storage_type) {
        case roachpb::DataType::SMALLINT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int16>(ptr, info);
          break;
        }
        case roachpb::DataType::INT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int32>(ptr, info);
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_int64>(ptr, info);
          break;
        }
        case roachpb::DataType::FLOAT: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_float32>(ptr, info);
          break;
        }
        case roachpb::DataType::DOUBLE: {
          DatumPtr ptr = GetData(row, col);
          EncodeDecimal<k_double64>(ptr, info);
          break;
        }
        case roachpb::DataType::DECIMAL: {
          DatumPtr ptr = GetData(row, col);
          k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
          if (is_double) {
            EncodeDecimal<k_double64>(ptr + sizeof(k_bool), info);
          } else {
            EncodeDecimal<k_int64>(ptr + sizeof(k_bool), info);
          }
          break;
        }
        default: {
          LOG_ERROR("Unsupported Decimal type for encoding: %d ", col_info_[col].storage_type)
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
          break;
        }
      }
      break;
    }
    case KWDBTypeFamily::IntervalFamily: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));

      struct KWDuration duration;
      switch (col_info_[col].storage_type) {
        case roachpb::TIMESTAMP_MICRO:
        case roachpb::TIMESTAMPTZ_MICRO:
          duration.format(val, 1000);
          break;
        case roachpb::TIMESTAMP_NANO:
        case roachpb::TIMESTAMPTZ_NANO:
          duration.format(val, 1);
          break;
        default:
          duration.format(val, 1000000);
          break;
      }
      k_int32 len = ValueEncoding::EncodeComputeLenDuration(0, duration);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeDurationValue(&slice, 0, duration);
      info->len = info->len + len;
      break;
    }
    case KWDBTypeFamily::DateFamily: {
      const int secondOfDay = 24 * 3600;
      DatumPtr raw = GetData(row, col);
      std::string date_str = std::string{static_cast<char*>(raw)};
      struct tm stm {
        0
      };
      int year, mon, day;
      k_int64 msec;
      std::memcpy(&msec, raw, sizeof(k_int64));
      k_int64 seconds = msec / 1000;
      time_t rawtime = (time_t) seconds;
      tm timeinfo;
      ToGMT(rawtime, timeinfo);

      stm.tm_year = timeinfo.tm_year;
      stm.tm_mon = timeinfo.tm_mon;
      stm.tm_mday = timeinfo.tm_mday;
      time_t val = timelocal(&stm);
      val += ctx->timezone * 60 * 60;
      val /= secondOfDay;
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
    default: {
      DatumPtr raw = GetData(row, col);
      k_int64 val;
      std::memcpy(&val, raw, sizeof(k_int64));
      k_int32 len = ValueEncoding::EncodeComputeLenInt(0, val);
      ret = ee_enlargeStringInfo(info, len);
      if (ret != SUCCESS) {
        break;
      }

      CKSlice slice{info->data + info->len, len};
      ValueEncoding::EncodeIntValue(&slice, 0, val);
      info->len = info->len + len;
      break;
    }
  }
  Return(ret);
}

static inline k_uint8 format_timestamp(const CKTime& ck_time, KWDBTypeFamily return_type, kwdbContext_p ctx,
                                       char* buf) {
  // copy
  CKTime adjusted_time = ck_time;
  if (return_type == KWDBTypeFamily::TimestampTZFamily) {
    adjusted_time.t_timespec.tv_sec += adjusted_time.t_abbv;
  }

  // sec
  tm ts{};
  ToGMT(adjusted_time.t_timespec.tv_sec, ts);

  // buf
  char* p = buf;

  // YYYY-MM-DD HH:MM:SS
  const k_int32 year = ts.tm_year + 1900;
  const k_int32 month = ts.tm_mon + 1;
  const k_int32 day = ts.tm_mday;
  const k_int32 hour = ts.tm_hour;
  const k_int32 min = ts.tm_min;
  const k_int32 sec = ts.tm_sec;

  // year(4 bit)
  *p++ = '0' + (year / 1000);
  *p++ = '0' + ((year / 100) % 10);
  *p++ = '0' + ((year / 10) % 10);
  *p++ = '0' + (year % 10);
  *p++ = '-';

  // mon(2 bit)
  *p++ = '0' + (month / 10);
  *p++ = '0' + (month % 10);
  *p++ = '-';

  // day (2 bit)
  *p++ = '0' + (day / 10);
  *p++ = '0' + (day % 10);
  *p++ = ' ';

  // hour (2 bit)
  *p++ = '0' + (hour / 10);
  *p++ = '0' + (hour % 10);
  *p++ = ':';

  // min (2 bit)
  *p++ = '0' + (min / 10);
  *p++ = '0' + (min % 10);
  *p++ = ':';

  // sec (2 bit)
  *p++ = '0' + (sec / 10);
  *p++ = '0' + (sec % 10);

  // ns .123456789
  if (adjusted_time.t_timespec.tv_nsec != 0) {
    *p++ = '.';
    k_int64 nsec = adjusted_time.t_timespec.tv_nsec;

    *p++ = '0' + (nsec / 100000000);
    nsec %= 100000000;
    *p++ = '0' + (nsec / 10000000);
    nsec %= 10000000;
    *p++ = '0' + (nsec / 1000000);
    nsec %= 1000000;
    *p++ = '0' + (nsec / 100000);
    nsec %= 100000;
    *p++ = '0' + (nsec / 10000);
    nsec %= 10000;
    *p++ = '0' + (nsec / 1000);
    nsec %= 1000;
    *p++ = '0' + (nsec / 100);
    nsec %= 100;
    *p++ = '0' + (nsec / 10);
    *p++ = '0' + (nsec % 10);
  }

  // timezone +-HH:00
  if (return_type == KWDBTypeFamily::TimestampTZFamily) {
    const k_int32 timezone = ctx->timezone;
    const k_int32 tz_hour = std::abs(timezone);

    *p++ = (timezone >= 0) ? '+' : '-';
    *p++ = '0' + (tz_hour / 10);
    *p++ = '0' + (tz_hour % 10);
    *p++ = ':';
    *p++ = '0';
    *p++ = '0';
  }

  // return length
  return static_cast<k_uint8>(p - buf);
}

KStatus DataChunk::PgResultData(kwdbContext_p ctx, k_uint32 row, const EE_StringInfo& info) {
  EnterFunc();
  k_uint32 temp_len = info->len;
  char* temp_addr = nullptr;

  if (ee_appendBinaryStringInfo(info, "D0000", 5) != SUCCESS) {
    Return(FAIL);
  }

  // write column quantity
  if (ee_sendint(info, col_num_, 2) != SUCCESS) {
    Return(FAIL);
  }

  for (k_uint32 col = 0; col < col_num_; ++col) {
    if (IsNull(row, col)) {
      // write a negative value to indicate that the column is NULL
      if (ee_sendint(info, -1, 4) != SUCCESS) {
        Return(FAIL);
      }
      continue;
    }

    // get col value
    KWDBTypeFamily return_type = col_info_[col].return_type;
    switch (return_type) {
      case KWDBTypeFamily::BoolFamily: {
        // get col value
        DatumPtr raw = GetData(row, col);
        k_bool val;
        std::memcpy(&val, raw, sizeof(k_bool));
        // get col value
        std::string val_str;
        if (val == 0) {
          val_str = "f";
        } else {
          val_str = "t";
        }
        // write the length of col value
        if (ee_sendint(info, val_str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string
        if (ee_appendBinaryStringInfo(info, val_str.data(), val_str.length()) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::StringFamily: {
        k_uint16 val_len;
        DatumPtr raw = GetData(row, col, val_len);
        std::string val_str = std::string{static_cast<char*>(raw), val_len};

        // write the length of col value
        if (ee_sendint(info, val_str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string
        if (ee_appendBinaryStringInfo(info, val_str.c_str(), val_str.length()) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::BytesFamily: {
        k_uint16 len;
        DatumPtr raw = GetData(row, col, len);
        std::string val_str = std::string{static_cast<char*>(raw), len};

        // use format of varbinary
        std::string bytes_f;
        bytes_f.append("\\x");
        char tmp[3] = {0};
        for (u_char c : val_str) {
          snprintf(tmp, sizeof(tmp), "%02x", c);
          bytes_f.append(tmp, 2);
        }
        if (ee_sendint(info, bytes_f.size(), 4) != SUCCESS) {
          Return(FAIL);
        }
        if (ee_appendBinaryStringInfo(info, bytes_f.c_str(), bytes_f.size()) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::TimestampFamily:
      case KWDBTypeFamily::TimestampTZFamily: {
        char ts_format_buf[64] = {0};
        // format timestamps as strings
        k_int64 val;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&val, raw, sizeof(k_int64));
        CKTime ck_time = getCKTime(val, col_info_[col].storage_type, ctx->timezone);
        k_uint8 format_len = format_timestamp(ck_time, return_type, ctx, ts_format_buf);
        // write the length of column value
        if (ee_sendint(info, format_len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, ts_format_buf, format_len) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::FloatFamily: {
        k_char buf[50] = {0};
        k_int32 n = 0;

        DatumPtr raw = GetData(row, col);
        k_double64 d;
        if (col_info_[col].storage_type == roachpb::DataType::FLOAT) {
          k_float32 val32;
          std::memcpy(&val32, raw, sizeof(k_float32));
          d = (k_double64)val32;
          n = snprintf(buf, sizeof(buf), "%.6f", d);
        } else {
          std::memcpy(&d, raw, sizeof(k_double64));
          n = snprintf(buf, sizeof(buf), "%.17g", d);
        }

        if (std::isnan(d)) {
          buf[0] = 'N';
          buf[1] = 'a';
          buf[2] = 'N';
          n = 3;
        }
        // write the length of column value
        if (ee_sendint(info, n, 4) != SUCCESS) {
          Return(FAIL);
        }

        // write string format
        if (ee_appendBinaryStringInfo(info, buf, n) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::DecimalFamily: {
        switch (col_info_[col].storage_type) {
          case roachpb::DataType::SMALLINT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int16>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::INT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int32>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
          case roachpb::DataType::BIGINT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_int64>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::FLOAT: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_float32>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::DOUBLE: {
            DatumPtr ptr = GetData(row, col);
            if (PgEncodeDecimal<k_double64>(ptr, info) != SUCCESS) {
              Return(FAIL);
            }
            break;
          }
          case roachpb::DataType::DECIMAL: {
            DatumPtr ptr = GetData(row, col);
            k_bool is_double = *reinterpret_cast<k_bool*>(ptr);
            if (is_double) {
              PgEncodeDecimal<k_double64>(ptr + sizeof(k_bool), info);
            } else {
              PgEncodeDecimal<k_int64>(ptr + sizeof(k_bool), info);
            }
            break;
          }
          default: {
            LOG_ERROR("Unsupported Decimal type for encoding: %d ", col_info_[col].storage_type)
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
            break;
          }
        }
      } break;
      case KWDBTypeFamily::IntervalFamily: {
        time_t ms;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&ms, raw, sizeof(k_int64));
        char buf[32] = {0};
        struct KWDuration duration;
        size_t n;
        switch (col_info_[col].storage_type) {
          case roachpb::TIMESTAMP_MICRO:
          case roachpb::TIMESTAMPTZ_MICRO:
            n = duration.format_pg_result(ms, buf, 32, 1000);
            break;
          case roachpb::TIMESTAMP_NANO:
          case roachpb::TIMESTAMPTZ_NANO:
            n = duration.format_pg_result(ms, buf, 32, 1);
            break;
          default:
            n = duration.format_pg_result(ms, buf, 32, 1000000);
            break;
        }

        // write the length of column value
        if (ee_sendint(info, n, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, buf, n) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::DateFamily: {
        char ts_format_buf[64] = {0};
        // format timestamps as strings
        k_int64 val;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&val, raw, sizeof(k_int64));
        CKTime ck_time = getCKTime(val, col_info_[col].storage_type, ctx->timezone);
        tm ts{};
        ToGMT(ck_time.t_timespec.tv_sec, ts);
        strftime(ts_format_buf, 32, "%F %T", &ts);
        k_uint8 format_len = strlen(ts_format_buf);
        format_len = strlen(ts_format_buf);
        // write the length of column value
        if (ee_sendint(info, format_len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, ts_format_buf, format_len) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      case KWDBTypeFamily::IntFamily: {
        DatumPtr raw = GetData(row, col);
        k_int64 val;
        switch (col_info_[col].storage_type) {
          case roachpb::DataType::BIGINT:
          case roachpb::DataType::TIMESTAMP:
          case roachpb::DataType::TIMESTAMPTZ:
          case roachpb::DataType::TIMESTAMP_MICRO:
          case roachpb::DataType::TIMESTAMP_NANO:
          case roachpb::DataType::TIMESTAMPTZ_MICRO:
          case roachpb::DataType::TIMESTAMPTZ_NANO:
          case roachpb::DataType::DATE:
            std::memcpy(&val, raw, sizeof(k_int64));
            break;
          case roachpb::DataType::SMALLINT:
            k_int16 val16;
            std::memcpy(&val16, raw, sizeof(k_int16));
            val = val16;
            break;
          default:
            k_int32 val32;
            std::memcpy(&val32, raw, sizeof(k_int32));
            val = val32;
            break;
        }

        char val_char[32];
        k_int32 len = fastIntToString(val, val_char);
        if (ee_sendint(info, len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, len) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      default: {
        // write the length of column value
        k_int64 val;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&val, raw, sizeof(k_int64));
        char val_char[32];
        k_int32 len = fastIntToString(val, val_char);
        if (ee_sendint(info, len, 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, len) != SUCCESS) {
          Return(FAIL);
        }
      } break;
    }
  }

  temp_addr = &info->data[temp_len + 1];
  k_uint32 n32 = be32toh(info->len - temp_len - 1);
  memcpy(temp_addr, &n32, 4);
  Return(SUCCESS);
}

EEIteratorErrCode DataChunk::VectorizeData(kwdbContext_p ctx, DataInfo *data_info) {
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    TsColumnInfo *ColInfo = static_cast<TsColumnInfo *>(malloc(col_num_ * sizeof(TsColumnInfo)));
    if (nullptr == ColInfo) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      break;
    }

    TsColumnData *ColData = static_cast<TsColumnData *>(malloc(col_num_ * sizeof(TsColumnData)));
    if (nullptr == ColData) {
      SafeFreePointer(ColInfo);
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      break;
    }
    for (k_int32 i = 0; i < col_num_; ++i) {
      ColInfo[i].fixed_len_     = col_info_[i].fixed_storage_len;
      ColInfo[i].return_type_   = col_info_[i].return_type;
      ColInfo[i].storage_len_   = col_info_[i].storage_len;
      ColInfo[i].storage_type_  = col_info_[i].storage_type;
      ColData[i].data_ptr_      = GetDataPtr(i);
      ColData[i].bitmap_ptr_    = GetBitmapPtr(i);
      if (ColInfo[i].return_type_ == KWDBTypeFamily::StringFamily ||
                ColInfo[i].return_type_ == KWDBTypeFamily::BytesFamily) {
        k_int32 *offset = static_cast<k_int32 *>(malloc((count_ + 1) * sizeof(k_int32)));
        if (nullptr == offset) {
          for (k_int32 j = 0; j < i; ++j) {
            if (ColInfo[i].return_type_ == KWDBTypeFamily::StringFamily ||
                ColInfo[i].return_type_ == KWDBTypeFamily::BytesFamily) {
                SafeFreePointer(ColData[i].data_ptr_);
                SafeFreePointer(ColData[i].offset_);
            }
          }
          SafeFreePointer(ColInfo);
          SafeFreePointer(ColData);
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          break;
        }
        memset(offset, 0, count_ * sizeof(k_int32));
        char *data_ptr = GetDataPtr(i);
        k_int32 total_len = 0;
        std::vector<k_uint16> vec_len;
        vec_len.reserve(count_);
        for (k_uint32 j = 0; j < count_; ++j) {
          k_uint16 len = 0;
          memcpy(&len, data_ptr + j * col_info_[i].fixed_storage_len, sizeof(k_uint16));
          offset[j] = total_len;
          total_len += len;
          vec_len.push_back(len);
        }
        offset[count_] = total_len;
        ColData[i].offset_ = offset;
        char *ptr = static_cast<char*>(malloc(total_len));
        if (nullptr == ptr) {
          for (k_int32 j = 0; j < i; ++j) {
            if (ColInfo[i].return_type_ == KWDBTypeFamily::StringFamily ||
                ColInfo[i].return_type_ == KWDBTypeFamily::BytesFamily) {
                SafeFreePointer(ColData[i].data_ptr_);
                SafeFreePointer(ColData[i].offset_);
            }
          }
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
          SafeFreePointer(ColInfo);
          SafeFreePointer(ColData);
          SafeFreePointer(offset);
          break;
        }
        memset(ptr, 0, total_len);
        ColData[i].data_ptr_ = ptr;
        for (k_uint32 j = 0; j < count_; ++j) {
          memcpy(ptr + offset[j], data_ptr + j * col_info_[i].fixed_storage_len + sizeof(k_uint16), vec_len[j]);
        }
        ColData[i].data_ptr_ = ptr;
      }
    }

    data_info->column_num_ = col_num_;
    data_info->column_ = ColInfo;
    data_info->column_data_ = ColData;
    data_info->data_count_ = count_;
    data_info->bitmap_size_ = bitmap_size_;
    data_info->row_size_ = row_size_;
    data_info->capacity_ = capacity_;
    data_info->data_ = data_;
    data_info->is_data_owner_ = is_data_owner_;
    is_data_owner_ = false;
    ret = EEIteratorErrCode::EE_OK;
  } while (0);

  return ret;
}

void DataChunk::AddRecordByRow(kwdbContext_p ctx, RowBatch* row_batch, k_uint32 col, Field* field) {
  k_uint32 len = field->get_storage_length();
  switch (field->get_storage_type()) {
    case roachpb::DataType::BOOL: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }

        bool val = field->ValInt() > 0 ? 1 : 0;
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BIGINT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }

        k_int64 val = field->ValInt();
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::INT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        k_int32 val = field->ValInt();
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::SMALLINT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        k_int16 val = field->ValInt();
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::FLOAT: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        k_float32 val = field->ValReal();
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::DOUBLE: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        k_double64 val = field->ValReal();
        InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }

        kwdbts::String val = field->ValStr();
        if (val.isNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        char* mem = const_cast<char*>(val.c_str());
        InsertData(count_ + row, col, mem, val.length());
        row_batch->NextLine();
      }
      break;
    }
    case roachpb::DataType::DECIMAL: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
          continue;
        }
        k_bool overflow = field->is_over_flow();
        if (field->get_sql_type() == roachpb::DataType::DOUBLE ||
            field->get_sql_type() == roachpb::DataType::FLOAT || overflow) {
          k_double64 val = field->ValReal();
          InsertDecimal(count_ + row, col, reinterpret_cast<char*>(&val),
                        true);
        } else {
          k_int64 val = field->ValInt();
          InsertDecimal(count_ + row, col, reinterpret_cast<char*>(&val),
                        false);
        }
        row_batch->NextLine();
      }
      break;
    }
    default: {
      for (int row = 0; row < row_batch->Count(); ++row) {
        if (field->CheckNull()) {
          SetNull(count_ + row, col);
          row_batch->NextLine();
        }
      }
      break;
    }
  }
}

KStatus DataChunk::AddRecordByColumn(kwdbContext_p ctx, RowBatch* row_batch, Field** renders) {
  EnterFunc()
  for (k_uint32 col = 0; col < col_num_; ++col) {
    Field* field = renders[col];
    row_batch->ResetLine();
    if (field->get_field_type() == Field::Type::FIELD_ITEM) {
      switch (field->get_storage_type()) {
        case roachpb::DataType::BOOL:
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          k_uint32 len = field->get_storage_length();
          k_uint32 col_offset = count_ * col_info_[col].fixed_storage_len + col_offset_[col];
          row_batch->CopyColumnData(field->getColIdxInRs(), data_ + col_offset, len, field->get_column_type(),
                                    field->get_storage_type());
          if (field->is_allow_null()) {
            for (int row = 0; row < row_batch->Count(); ++row) {
              if (field->is_nullable()) {
                SetNull(count_ + row, col);
              }
              row_batch->NextLine();
            }
          }

          break;
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          for (int row = 0; row < row_batch->Count(); ++row) {
            if (field->CheckNull()) {
              SetNull(count_ + row, col);
              row_batch->NextLine();
              continue;
            }

            kwdbts::String val = field->ValStr();
            if (val.isNull()) {
              SetNull(count_ + row, col);
              row_batch->NextLine();
              continue;
            }
            char* mem = const_cast<char*>(val.c_str());
            InsertData(count_ + row, col, mem, val.length());
            row_batch->NextLine();
          }
          break;
        }
        case roachpb::DataType::DECIMAL: {
          Return(KStatus::FAIL)
        }
        default: {
          break;
        }
      }
    } else {
      AddRecordByRow(ctx, row_batch, col, field);
    }
  }
  Return(KStatus::SUCCESS)
}

KStatus DataChunk::AddRecordByColumnWithSelection(kwdbContext_p ctx, RowBatch* row_batch, Field** renders) {
  const k_uint32 start_row = count_;
  for (k_uint32 col = 0; col < col_num_; ++col) {
    Field* field = renders[col];
    row_batch->ResetLine();
    const k_uint32 total_rows = row_batch->Count();
    const auto storage_len = field->get_storage_length();
    const auto offset_storage_len = storage_len;
    const auto col_idx_in_rs = field->getColIdxInRs();
    const auto column_type = field->get_column_type();
    const bool is_tag =
        (column_type == roachpb::KWDBKTSColumn::TYPE_PTAG) || (column_type == roachpb::KWDBKTSColumn::TYPE_TAG);
    const auto storage_type = field->get_storage_type();
    const bool not_null = !field->is_allow_null();
    const bool is_string = IsStringType(storage_type);

    // Handle tag type (special logic)
    if (is_tag) {
      if (!not_null && row_batch->IsNull(col_idx_in_rs, column_type)) {
        // If it's a tag column, nullable, and the current column is null, set the entire column to NULL.
        for (k_uint32 row = 0; row < total_rows; ++row) {
          SetNull(start_row + row, col);
        }
        continue;
      }

      if (is_string) {
        // Tag and string type: all rows use the same value
        kwdbts::String val = field->ValStr();
        const char* mem = val.c_str();
        const size_t len = val.length();

        for (k_uint32 row = 0; row < total_rows; ++row) {
          InsertData(start_row + row, col, const_cast<char*>(mem), len);
        }
      } else {
        // non-string type: batch copy the entire column of data
        k_uint32 col_offset = start_row * col_info_[col].fixed_storage_len + col_offset_[col];
        row_batch->CopyColumnData(col_idx_in_rs, data_ + col_offset, storage_len, column_type, storage_type);
      }
      continue;
    }

    // Handle non-tag type
    if (not_null) {
      // not null
      if (is_string) {
        for (k_uint32 row = 0; row < total_rows; ++row) {
          kwdbts::String val = field->ValStr();
          InsertData(start_row + row, col, const_cast<char*>(val.c_str()), val.length());
          row_batch->NextLine();
        }
      } else {
        for (k_uint32 row = 0; row < total_rows; ++row) {
          char* ptr = row_batch->GetData(col_idx_in_rs, offset_storage_len, column_type, storage_type);
          InsertData(start_row + row, col, ptr, storage_len);
          row_batch->NextLine();
        }
      }
    } else {
      // maybe null
      if (is_string) {
        for (k_uint32 row = 0; row < total_rows; ++row) {
          if (row_batch->IsNull(col_idx_in_rs, column_type)) {
            SetNull(start_row + row, col);
          } else {
            kwdbts::String val = field->ValStr();
            InsertData(start_row + row, col, const_cast<char*>(val.c_str()), val.length());
          }
          row_batch->NextLine();
        }
      } else {
        for (k_uint32 row = 0; row < total_rows; ++row) {
          if (row_batch->IsNull(col_idx_in_rs, column_type)) {
            SetNull(start_row + row, col);
          } else {
            char* ptr = row_batch->GetData(col_idx_in_rs, offset_storage_len, column_type, storage_type);
            InsertData(start_row + row, col, ptr, storage_len);
          }
          row_batch->NextLine();
        }
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus DataChunk::AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch, Field** renders, bool batch_copy) {
  EnterFunc();
  KStatus status = KStatus::SUCCESS;
  if (row_batch == nullptr) {
    Return(KStatus::FAIL);
  }
  if (batch_copy) {
    if (row_batch->HasFilter()) {
      status = AddRecordByColumnWithSelection(ctx, row_batch, renders);
    } else {
      status = AddRecordByColumn(ctx, row_batch, renders);
    }
  } else {
    for (k_uint32 col = 0; col < col_num_; ++col) {
      row_batch->ResetLine();
      Field* field = renders[col];
      AddRecordByRow(ctx, row_batch, col, field);
    }
  }
  count_ += row_batch->Count();
  Return(status);
}

/**
 * reset current_line_ to the first record
*/
void DataChunk::ResetLine() {
  current_line_ = -1;
}

/**
 * NextLine
*/
k_int32 DataChunk::NextLine() {
  if (current_line_ + 1 >= count_) {
    return -1;
  }
  ++current_line_;
  return current_line_;
}

/**
 * return the number of valid rows in the result set
 */
k_uint32 DataChunk::Count() {
  return count_;
}

k_uint32 DataChunk::EstimateCapacity(ColumnInfo* column_info, k_int32 col_num) {
  auto row_size = (k_int32) ComputeRowSize(column_info, col_num);

  // (capacity_ + 7)/8 * col_num_ + capacity_ * row_size_ <= DataChunk::SIZE_LIMIT
  k_int32 capacity = (DataChunk::SIZE_LIMIT * 8 - 7 * col_num) / (col_num + 8 * row_size);

  if (capacity <= 0) {
    capacity = MIN_CAPACITY;
  }
  return capacity;
}

k_uint32 DataChunk::ComputeRowSize(ColumnInfo *column_info, k_int32 col_num) {
  k_uint32 row_size = 0;
  for (int i = 0; i < col_num; ++i) {
    /**
     * Row size adjustment for string type column and decimal type column. Add
     * 2 byte for string length and 1 byte indicator for decimal type. Ideally
     * storage length of the field (FieldDecimal, FieldVarChar, FieldVarBlob,
     * etc.) should take extra bytes into account, and it needs to make
     * necessary changes on all derived Field classes. Now we temporarily make
     * row size adjustment in several places.
     */
    if (column_info[i].is_string) {
      row_size += STRING_WIDE;
      column_info[i].fixed_storage_len = column_info[i].storage_len + STRING_WIDE;
    } else if (column_info[i].storage_type == roachpb::DataType::DECIMAL) {
      row_size += BOOL_WIDE;
      column_info[i].fixed_storage_len = column_info[i].storage_len + BOOL_WIDE;
    } else {
      column_info[i].fixed_storage_len = column_info[i].storage_len;
    }
    row_size += column_info[i].storage_len;
  }
  // In order to be consistent with the bigtable format, an additional byte of
  // delete is required
  row_size += 1;

  return row_size;
}

KStatus DataChunk::ConvertToTagData(kwdbContext_p ctx, k_uint32 row, k_uint32 col,
                                    TagRawData& tag_raw_data, DatumPtr &rel_data_ptr) {
  EnterFunc();
  tag_raw_data.tag_data = rel_data_ptr;
  // get the original rel data pointer
  DatumPtr p = data_ + row * col_info_[col].fixed_storage_len + col_offset_[col];
  if (col_info_[col].is_string) {
    std::memcpy(&tag_raw_data.size, p, STRING_WIDE);
    p += STRING_WIDE;
    // copy data into the buffer
    memcpy(rel_data_ptr, p, tag_raw_data.size + 1);
    rel_data_ptr += tag_raw_data.size + 1;
  } else if (col_info_[col].storage_type == roachpb::DataType::DECIMAL) {
    std::memcpy(&tag_raw_data.size, p, BOOL_WIDE);
    p += BOOL_WIDE;
    // copy data into the buffer
    memcpy(rel_data_ptr, p, tag_raw_data.size);
    rel_data_ptr += tag_raw_data.size;
  } else {
    tag_raw_data.size = GetColumnInfo()[col].storage_len;
    // copy data into the buffer
    memcpy(rel_data_ptr, p, tag_raw_data.size);
    rel_data_ptr += tag_raw_data.size;
  }
  tag_raw_data.is_null = IsNull(row, col);
  Return(KStatus::SUCCESS);
}

bool DataChunk::SetEncodingBuf(const unsigned char *buf, k_uint32 len) {
  encoding_buf_ = static_cast<char *>(malloc(len));
  if (nullptr == encoding_buf_) {
    return false;
  }

  memcpy(encoding_buf_, buf, len);
  encoding_len_ = len;

  return true;
}

KStatus DataChunk::InsertEntities(TagRowBatch* tag_row_batch) {
  return tag_row_batch->GetEntities(&entity_indexs_);
}

EntityResultIndex& DataChunk::GetEntityIndex(k_uint32 row) {
  return entity_indexs_[row];
}

KStatus DataChunk::OffsetSort(std::vector<k_uint32> &selection, bool is_reverse) {
  for (k_uint32 i = 0; i < count_; ++i) {
    selection.push_back(i);
  }
  const auto it_begin = selection.begin();
  auto it_end = selection.end();

  MemCompare cmp(this, is_reverse);
  std::sort(it_begin, it_end, cmp);

  return KStatus::SUCCESS;
}

}   // namespace kwdbts
