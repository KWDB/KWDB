#pragma once
#include <memory>
#include <string>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"

namespace kwdbts {

struct TsColumnCompressInfo {
  //   bool has_bitmap;   // optimize later
  int bitmap_len;
  int fixdata_len;
  int vardata_len;
  int row_count;
};

class TsColumnBlock {
  friend class TsColumnBlockBuilder;

 private:
  const AttributeInfo col_schema_;
  int count_ = 0;
  TsBitmap bitmap_;
  std::string fixlen_data_;
  std::string varchar_data_;

  TsColumnBlock(const AttributeInfo& col_schema, int count, const TsBitmap& bitmap,
                const std::string& fixlen_data, const std::string& varchar_data)
      : col_schema_(col_schema),
        count_(count),
        bitmap_(bitmap),
        fixlen_data_(fixlen_data),
        varchar_data_(varchar_data) {}

 public:
  static KStatus ParseCompressedColumnData(const AttributeInfo& col_schema,
                                           TSSlice compressed_data,
                                           const TsColumnCompressInfo& info,
                                           std::unique_ptr<TsColumnBlock>* colblock);

  bool GetCompressedData(std::string*, TsColumnCompressInfo*);

  size_t GetRowNum() const { return count_; }
  const AttributeInfo& GetColSchama() const { return col_schema_; }
  KStatus GetColAddr(char** value);
  KStatus GetColBitmap(TsBitmap& bitmap);
  KStatus GetValueSlice(int row_num, TSSlice& value);
};

class TsColumnBlockBuilder {
 private:
  const AttributeInfo& col_schema_;
  int count_ = 0;
  TsBitmap bitmap_;
  std::string fixlen_data_;
  std::string varchar_data_;

 public:
  explicit TsColumnBlockBuilder(const AttributeInfo& col_schema) : col_schema_(col_schema) {}
  void AppendFixLenData(TSSlice data, int count, const TsBitmap& bmap);
  void AppendVarLenData(TSSlice data, DataFlags flag);

  // TODO(zzr) make it const ref
  void AppendColumnBlock(TsColumnBlock& col);

  std::unique_ptr<TsColumnBlock> GetColumnBlock() const {
    return std::unique_ptr<TsColumnBlock>{
        new TsColumnBlock(col_schema_, count_, bitmap_, fixlen_data_, varchar_data_)};
  }
};
}  // namespace kwdbts