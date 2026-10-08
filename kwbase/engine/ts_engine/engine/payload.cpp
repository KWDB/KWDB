// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.

#include "payload.h"


Payload::Payload(const std::vector<AttributeInfo>& schema, const std::vector<uint32_t>& valid_cols, TSSlice data)
    : schema_(schema), idx_for_valid_cols_(valid_cols), slice_(data) {
  start_row_ = 0;
  count_ = *reinterpret_cast<int32_t*> (slice_.data + row_num_offset_);
  flag_ = *reinterpret_cast<uint8_t*> (slice_.data + row_type_offset_);
  primary_len_ = KInt16(slice_.data + header_size_);
  primary_offset_ = header_size_ + 2;
  tag_len_ = KInt32(slice_.data + primary_offset_ + primary_len_);
  tag_offset_ = primary_offset_ + primary_len_ + 4;
  if (flag_ != Payload::TAG_ONLY) {
    data_len_ = KInt32(slice_.data + tag_offset_ + tag_len_);
    data_offset_ = tag_offset_ + tag_len_ + 4;
  } else {
    data_len_ = 0;
    data_offset_ = 0;
  }
  bitmap_len_ = (count_ + 7) / 8;

  col_offsets_ = new int32_t[schema_.size()];
  // update column offsets
  int32_t col_len = data_offset_;
  for (int i = 0; i < schema_.size(); i++) {
    col_offsets_[i] = col_len;
    col_len += (bitmap_len_ + schema_[i].size * count_);
  }
}

Payload::Payload(const std::vector<AttributeInfo>& schema, TSSlice data) : schema_(schema), slice_(data) {
  std::vector<uint32_t> valid_cols;
  for (size_t i = 0; i <= schema_.size(); i++) {
    idx_for_valid_cols_.push_back(i);
  }
  start_row_ = 0;
  count_ = *reinterpret_cast<int32_t*> (slice_.data + row_num_offset_);
  flag_ = *reinterpret_cast<uint8_t*> (slice_.data + row_type_offset_);
  primary_len_ = KInt16(slice_.data + header_size_);
  primary_offset_ = header_size_ + 2;
  tag_len_ = KInt32(slice_.data + primary_offset_ + primary_len_);
  tag_offset_ = primary_offset_ + primary_len_ + 4;
  if (flag_ != Payload::TAG_ONLY) {
    data_len_ = KInt32(slice_.data + tag_offset_ + tag_len_);
    data_offset_ = tag_offset_ + tag_len_ + 4;
  }
  bitmap_len_ = (count_ + 7) / 8;

  col_offsets_ = new int32_t[schema_.size()];
  // update column offsets
  int32_t col_len = data_offset_;
  for (int i = 0; i < schema_.size(); i++) {
    col_offsets_[i] = col_len;
    col_len += (bitmap_len_ + schema_[i].size * count_);
  }
}
