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
#include <cstdint>
#include <string>
#include <vector>

#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_coding.h"

namespace kwdbts {

struct TsLastSegmentBlockIndex {
  uint64_t info_offset, length;
  uint64_t table_id;
  uint32_t table_version, n_entity;
  int64_t min_ts, max_ts;
  int64_t first_ts, last_ts;
  uint64_t min_osn, max_osn;
  uint64_t first_osn, last_osn;
  uint64_t min_entity_id, max_entity_id;
};

struct TsLastSegmentBlockInfo {
  uint64_t block_offset = 0;
  uint32_t entity_id_len = 0;
  uint32_t osn_len = 0;
  uint32_t nrow = 0;
  uint32_t ncol = 0;
  struct ColInfo {
    uint32_t offset = 0;
    uint16_t bitmap_len = 0;
    uint32_t fixdata_len = 0;
    uint32_t vardata_len = 0;
  };
  std::vector<ColInfo> col_infos;
};

struct TsLastSegmentBlockInfoWithData : TsLastSegmentBlockInfo {
  TsSliceGuard data;
};

// first 8 byte of `md5 -s kwdbts::TsLastSegment`
// TODO(zzr) fix endian
static constexpr uint64_t FOOTER_MAGIC = 0xcb2ffe9321847271;

struct TsLastSegmentFooter {
  uint64_t block_info_idx_offset, n_data_block;
  uint64_t meta_block_idx_offset, n_meta_block;
  uint8_t padding[16] = {0};
  uint64_t file_version;
  uint64_t magic_number;
};
static_assert(sizeof(TsLastSegmentFooter) == 64);
// static_assert(std::has_unique_object_representations_v<TsLastSegmentFooter>);

inline void EncodeBlockInfo(std::string* buf, const TsLastSegmentBlockInfo& info) {
  PutFixed64(buf, info.block_offset);
  PutFixed32(buf, info.entity_id_len);
  PutFixed32(buf, info.osn_len);
  PutFixed32(buf, info.nrow);
  PutFixed32(buf, info.ncol);
  for (int i = 0; i < info.ncol; ++i) {
    PutFixed32(buf, info.col_infos[i].offset);
    PutFixed16(buf, info.col_infos[i].bitmap_len);
    PutFixed32(buf, info.col_infos[i].fixdata_len);
    PutFixed32(buf, info.col_infos[i].vardata_len);
  }
}

inline KStatus DecodeBlockInfo(TSSlice slice, TsLastSegmentBlockInfo* info) {
  if (slice.len < 24) {
    return FAIL;
  }
  GetFixed64(&slice, &info->block_offset);
  GetFixed32(&slice, &info->entity_id_len);
  GetFixed32(&slice, &info->osn_len);
  GetFixed32(&slice, &info->nrow);
  GetFixed32(&slice, &info->ncol);
  if (slice.len != info->ncol * 14) {
    return FAIL;
  }
  info->col_infos.resize(info->ncol);
  for (int i = 0; i < info->ncol; ++i) {
    GetFixed32(&slice, &info->col_infos[i].offset);
    GetFixed16(&slice, &info->col_infos[i].bitmap_len);
    GetFixed32(&slice, &info->col_infos[i].fixdata_len);
    GetFixed32(&slice, &info->col_infos[i].vardata_len);
  }
  return SUCCESS;
}

inline void EncodeBlockIndex(std::string* buf, const TsLastSegmentBlockIndex& index) {
  PutFixed64(buf, index.info_offset);
  PutFixed64(buf, index.length);
  PutFixed64(buf, index.table_id);
  PutFixed32(buf, index.table_version);
  PutFixed32(buf, index.n_entity);
  PutFixed64(buf, index.min_ts);
  PutFixed64(buf, index.max_ts);
  PutFixed64(buf, index.first_ts);
  PutFixed64(buf, index.last_ts);
  PutFixed64(buf, index.min_osn);
  PutFixed64(buf, index.max_osn);
  PutFixed64(buf, index.first_osn);
  PutFixed64(buf, index.last_osn);
  PutFixed64(buf, index.min_entity_id);
  PutFixed64(buf, index.max_entity_id);
}

inline KStatus DecodeBlockIndex(TSSlice slice, TsLastSegmentBlockIndex* index) {
  if (slice.len != 112) {
    return FAIL;
  }
  GetFixed64(&slice, &index->info_offset);
  GetFixed64(&slice, &index->length);
  GetFixed64(&slice, &index->table_id);
  GetFixed32(&slice, &index->table_version);
  GetFixed32(&slice, &index->n_entity);
  uint64_t ts;
  GetFixed64(&slice, &ts);
  index->min_ts = ts;
  GetFixed64(&slice, &ts);
  index->max_ts = ts;
  GetFixed64(&slice, &ts);
  index->first_ts = ts;
  GetFixed64(&slice, &ts);
  index->last_ts = ts;
  GetFixed64(&slice, &index->min_osn);
  GetFixed64(&slice, &index->max_osn);
  GetFixed64(&slice, &index->first_osn);
  GetFixed64(&slice, &index->last_osn);
  GetFixed64(&slice, &index->min_entity_id);
  GetFixed64(&slice, &index->max_entity_id);
  return SUCCESS;
}

inline void EncodeFooter(std::string* buf, const TsLastSegmentFooter& footer) {
  PutFixed64(buf, footer.block_info_idx_offset);
  PutFixed64(buf, footer.n_data_block);
  PutFixed64(buf, footer.meta_block_idx_offset);
  PutFixed64(buf, footer.n_meta_block);
  PutFixed64(buf, 0);  // padding
  PutFixed64(buf, 0);  // padding
  PutFixed64(buf, footer.file_version);
  PutFixed64(buf, footer.magic_number);
}

inline KStatus DecodeFooter(TSSlice slice, TsLastSegmentFooter* footer) {
  if (slice.len != 64) {
    return FAIL;
  }
  GetFixed64(&slice, &footer->block_info_idx_offset);
  GetFixed64(&slice, &footer->n_data_block);
  GetFixed64(&slice, &footer->meta_block_idx_offset);
  GetFixed64(&slice, &footer->n_meta_block);
  RemovePrefix(&slice, 16);  // skip padding
  GetFixed64(&slice, &footer->file_version);
  GetFixed64(&slice, &footer->magic_number);
  return SUCCESS;
}
}  // namespace kwdbts
