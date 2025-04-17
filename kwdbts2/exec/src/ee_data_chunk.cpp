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
namespace kwdbts {

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
  char* bitmap = reinterpret_cast<char*>(data_ + bitmap_offset_[col]);
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

// Encode datachunk
KStatus DataChunk::Encoding(kwdbContext_p ctx, TsNextRetState nextState,
                            k_int64* command_limit,
                            std::atomic<k_int64>* count_for_limit) {
  KStatus st = KStatus::SUCCESS;

  if (DML_VECTORIZE_NEXT == nextState) {
    return st;
  }

  EE_StringInfo msgBuffer = ee_makeStringInfo();
  if (msgBuffer == nullptr) {
    return KStatus::FAIL;
  }
  if (DML_PG_RESULT == nextState) {
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
      gmtime_r(&rawtime, &timeinfo);

      stm.tm_year = timeinfo.tm_year;
      stm.tm_mon = timeinfo.tm_mon;
      stm.tm_mday = timeinfo.tm_mday;
      time_t val = timelocal(&stm);
      val += ctx->timezone * 60 * 60;
      val /= secondOfDay;
      if (val < 0 && val % secondOfDay) {
        val -= 1;
      }
      val += 1;
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

  for (k_uint16 col = 0; col < col_num_; ++col) {
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

        std::string val = std::string{static_cast<char*>(raw)};
        k_int32 len = ValueEncoding::EncodeComputeLenString(0, val.size());
        // write the length of col value
        if (ee_sendint(info, val.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string
        if (ee_appendBinaryStringInfo(info, val.c_str(), val.length()) != SUCCESS) {
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
        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          ck_time.t_timespec.tv_sec += ck_time.t_abbv;
        }
        tm ts{};
        gmtime_r(&ck_time.t_timespec.tv_sec, &ts);
        strftime(ts_format_buf, 32, "%F %T", &ts);
        k_uint8 format_len = strlen(ts_format_buf);
        if (ck_time.t_timespec.tv_nsec != 0) {
          snprintf(&ts_format_buf[format_len], sizeof(char[11]), ".%09ld", ck_time.t_timespec.tv_nsec);
        }
        format_len = strlen(ts_format_buf);
        // encode the time Zone Information
        if (return_type == KWDBTypeFamily::TimestampTZFamily) {
          const char* timezoneFormat;
          auto timezoneAbs = std::abs(ctx->timezone);
          if (ctx->timezone >= 0) {
            timezoneFormat = "+%02d:00";
            timezoneAbs = ctx->timezone;
          } else {
            timezoneFormat = "-%02d:00";
          }
          snprintf(&ts_format_buf[format_len], sizeof(char[7]), timezoneFormat, timezoneAbs);
        }
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
      case KWDBTypeFamily::FloatFamily: {
        k_char buf[30] = {0};
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

        // write the length of column value
        if (ee_sendint(info, n, 4) != SUCCESS) {
          Return(FAIL);
        }
        if (std::isnan(d)) {
          buf[0] = 'N';
          buf[1] = 'a';
          buf[2] = 'N';
          n = 3;
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
        DatumPtr raw = GetData(row, col);
        std::string str = std::string{static_cast<char*>(raw)};
        // write the length of column value
        if (ee_sendint(info, str.length(), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, str.c_str(), str.length()) != SUCCESS) {
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
        snprintf(val_char, sizeof(val_char), "%ld", val);
        if (ee_sendint(info, strlen(val_char), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, strlen(val_char)) != SUCCESS) {
          Return(FAIL);
        }
      } break;
      default: {
        // write the length of column value
        k_int64 val;
        DatumPtr raw = GetData(row, col);
        std::memcpy(&val, raw, sizeof(k_int64));
        char val_char[32];
        snprintf(val_char, sizeof(val_char), "%ld", val);
        if (ee_sendint(info, strlen(val_char), 4) != SUCCESS) {
          Return(FAIL);
        }
        // write string format
        if (ee_appendBinaryStringInfo(info, val_char, strlen(val_char)) != SUCCESS) {
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
          if (0 == field->get_num()) {
            for (int row = 0; row < row_batch->Count(); ++row) {
              k_int64 val = field->ValInt();
              InsertData(count_ + row, col, reinterpret_cast<char*>(&val), len);
              row_batch->NextLine();
            }
          } else {
            k_uint32 col_offset = count_ * col_info_[col].fixed_storage_len + col_offset_[col];
            row_batch->CopyColumnData(field->getColIdxInRs(), data_ + col_offset, len, field->get_column_type(),
                                      field->get_storage_type());

            for (int row = 0; row < row_batch->Count(); ++row) {
              if (field->CheckNull()) {
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

KStatus DataChunk::AddRowBatchData(kwdbContext_p ctx, RowBatch* row_batch, Field** renders, bool batch_copy) {
  EnterFunc()
  KStatus status = KStatus::SUCCESS;
  if (row_batch == nullptr) {
    Return(KStatus::FAIL)
  }
  if (batch_copy) {
    status = AddRecordByColumn(ctx, row_batch, renders);
  } else {
    for (k_uint32 col = 0; col < col_num_; ++col) {
      row_batch->ResetLine();
      Field* field = renders[col];
      AddRecordByRow(ctx, row_batch, col, field);
    }
  }
  count_ += row_batch->Count();
  Return(status)
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
