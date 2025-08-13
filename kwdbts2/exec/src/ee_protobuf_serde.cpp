
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "br_internal_service_recoverable_stub.h"
#include "ee_global.h"
#include "ee_protobuf_serde.h"
#include "lg_api.h"
namespace kwdbts {

ChunkPB ProtobufChunkSerrialde::SerializeChunk(DataChunk* src,
                                               k_bool* is_first_chunk) {
  ChunkPB dst = Serialize(src);
  auto* column_info = src->GetColumnInfo();
  dst.set_data_size(src->IsEncoding() ? src->GetEncodingBufferLength()
                                      : src->Size());
  if (*is_first_chunk) {
    int col_num = src->ColumnNum();
    dst.mutable_column_info()->Reserve(col_num);

    for (int i = 0; i < col_num; i++) {
      auto* column = dst.add_column_info();
      column->set_allow_null(column_info[i].allow_null);
      column->set_fixed_storage_len(column_info[i].fixed_storage_len);
      column->set_return_type(column_info[i].return_type);
      column->set_storage_len(column_info[i].storage_len);
      column->set_storage_type(column_info[i].storage_type);
      column->set_is_string(column_info[i].is_string);
    }

    *is_first_chunk = false;
  }

  return std::move(dst);
}

ChunkPB ProtobufChunkSerrialde::SerializeColumn(DataChunk* src,
                                                k_bool* is_first_chunk) {
  ChunkPB dst;  // = SerializeColumnData(src);
  auto* column_info = src->GetColumnInfo();
  dst.set_data_size(src->Size());
  if (*is_first_chunk) {
    int col_num = src->ColumnNum();
    dst.mutable_column_info()->Reserve(col_num);

    for (int i = 0; i < col_num; i++) {
      auto* column = dst.add_column_info();
      column->set_allow_null(column_info[i].allow_null);
      column->set_fixed_storage_len(column_info[i].fixed_storage_len);
      column->set_return_type(column_info[i].return_type);
      column->set_storage_len(column_info[i].storage_len);
      column->set_storage_type(column_info[i].storage_type);
      column->set_is_string(column_info[i].is_string);
    }

    *is_first_chunk = false;
  }
  return std::move(dst);
}

ChunkPB ProtobufChunkSerrialde::SerializeColumnData(DataChunk* src) {
  ChunkPB chunk_pb;
  int col_num = src->ColumnNum();
  size_t col_size = 0;
  k_int64 offset = 0;
  auto* column_info = src->GetColumnInfo();
  k_uint32 bitmap_size = src->BitmapSize();
  for (int i = 0; i < col_num; i++) {
    ColumnPB* column_pb = chunk_pb.add_columns();
    std::string* serialized_column = column_pb->mutable_data();
    col_size = column_info[i].fixed_storage_len * src->Capacity() + bitmap_size;
    serialized_column->resize(col_size);
    column_pb->set_uncompressed_size(col_size);

    auto* buff = reinterpret_cast<uint8_t*>(serialized_column->data());
    memcpy(buff, src->GetData() + offset, col_size);
    offset += col_size;
  }

  return std::move(chunk_pb);
}

ChunkPB ProtobufChunkSerrialde::Serialize(DataChunk* src) {
  ChunkPB chunk_pb;
  // chunk_pb.set_compress_type(CompressionTypePB::NO_COMPRESSION);
  std::string* serialized_data = chunk_pb.mutable_data();
  k_uint32 max_serialized_size = 0;
  if (src->IsEncoding()) {
    max_serialized_size = src->GetEncodingBufferLength() + 20;
  } else {
    max_serialized_size = src->Size() + 20;
  }

  serialized_data->resize(max_serialized_size);
  auto* buff = reinterpret_cast<uint8_t*>(serialized_data->data());

  encode_fixed32(buff + 0, 1);
  encode_fixed32(buff + 4, src->Count());
  encode_fixed32(buff + 8, src->Capacity());
  encode_fixed32(buff + 12, src->IsEncoding() ? src->GetEncodingBufferLength()
                                              : src->Size());
  buff = buff + 20;

  if (src->IsEncoding()) {
    memcpy(buff, src->GetEncodingBuffer(), src->GetEncodingBufferLength());
    buff += src->GetEncodingBufferLength();
  } else {
    memcpy(buff, src->GetData(), src->Size());
    buff += src->Size();
  }
  chunk_pb.set_serialized_size(
      buff - reinterpret_cast<const uint8_t*>(serialized_data->data()));
  serialized_data->resize(chunk_pb.serialized_size());
  chunk_pb.set_is_encoding(src->IsEncoding());
  return std::move(chunk_pb);
}

bool ProtobufChunkSerrialde::Deserialize(DataChunkPtr& chunk,
                                         std::string_view buff,
                                         bool is_encoding, ColumnInfo* col_info,
                                         k_int32 num) {
  auto* cur = reinterpret_cast<const uint8_t*>(buff.data());

  k_uint32 version = decode_fixed32(cur);
  if (version != 1) {
    LOG_ERROR("invalid version {%u}", version);
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_DATA_EXCEPTION, "invalid version");
    return false;
  }
  cur += 4;
  k_uint32 num_rows = decode_fixed32(cur);
  // LOG_ERROR("ProtobufChunkSerrialde::Deserialize numrows(%d)", num_rows);
  cur += 4;
  k_uint32 capacity = decode_fixed32(cur);
  cur += 4;
  k_uint32 real_data_size = decode_fixed64(cur);
  cur += 8;
  chunk = std::make_unique<DataChunk>(col_info, num, capacity);
  if (chunk == nullptr) {
    LOG_ERROR("Deserialize make unique data chunk failed");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    return false;
  }

  if (is_encoding) {
    chunk->SetEncodingBuf(cur, real_data_size);
  } else {
    if (!chunk->Initialize()) {
      LOG_ERROR("Deserialize data chunk, initialize failed");
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      return false;
    }
    memcpy(chunk->GetData(), cur, real_data_size);
  }

  chunk->SetCount(num_rows);

  return true;
}

void ProtobufChunkSerrialde::DeserializeColumn(DataChunkPtr& chunk,
                                               std::string_view buff,
                                               ColumnInfo* col_info,
                                               k_int32 num) {
  // auto* cur = reinterpret_cast<const uint8_t*>(buff.data());
  // if (chunk == nullptr) {
  //   chunk = std::make_unique<DataChunk>(col_info, num, capacity);
  //   if (chunk == nullptr) {
  //     LOG_ERROR("Deserialize make unique data chunk failed");
  //     return;
  //   }
  //   chunk->Initialize();
  // }

  // memcpy(chunk->GetData(), cur, real_data_size);
  // chunk->SetCount(num_rows);
}
}  // namespace kwdbts
