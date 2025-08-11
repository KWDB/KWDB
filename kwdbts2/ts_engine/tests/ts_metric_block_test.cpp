#if false
#include "ts_metric_block.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <random>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_block.h"

using namespace kwdbts;

static std::string RandomString(std::default_random_engine& gen, size_t len) {
  std::uniform_int_distribution<int> dis(0, 255);
  std::string s(len, 0);
  std::generate(s.begin(), s.end(), std::bind(dis, std::ref(gen)));
  return s;
}

class UnitTestBlock : public TsBlock {
 private:
  int row_count_;

  const std::vector<AttributeInfo> schema_;

  std::vector<std::string> data_;
  std::vector<std::vector<std::string>> var_data_;

  std::vector<TS_LSN> lsn_data_;

  std::vector<TsBitmap> bitmaps_;

  std::default_random_engine gen_;

 public:
  explicit UnitTestBlock(int row_count, const std::vector<AttributeInfo>& schema, size_t seed = 0)
      : row_count_(row_count), schema_(schema), gen_(seed) {}

  void Init() {
    lsn_data_.resize(row_count_);
    std::iota(lsn_data_.begin(), lsn_data_.end(), 0);

    bitmaps_.resize(schema_.size());
    data_.resize(schema_.size());
    var_data_.resize(schema_.size());
    for (int icol = 0; icol < schema_.size(); icol++) {
      bitmaps_[icol].Reset(row_count_);
      for (int i = 0; i < row_count_; i++) {
        bitmaps_[icol][i] = gen_() % 2 == 0 ? DataFlags::kValid : DataFlags::kNull;
      }
      if (isVarLenType(schema_[icol].type)) {
        var_data_[icol].resize(row_count_);
        for (int i = 0; i < row_count_; i++) {
          int len = gen_() % 256;
          var_data_[icol][i] = RandomString(gen_, len);
        }
      } else {
        std::uniform_int_distribution<char> dis(0, 255u);
        data_[icol].resize(row_count_ * schema_[icol].size);
        std::generate(data_[icol].begin(), data_[icol].end(), std::bind(dis, std::ref(gen_)));
      }
    }
  }

  uint32_t GetTableVersion() override { return 1; }
  TSTableID GetTableId() override { return 12345; }
  size_t GetRowNum() override { return row_count_; }
  uint64_t* GetLSNAddr(int row_num) override { return &lsn_data_[row_num]; }
  timestamp64 GetTS(int row_num) override {
    assert(false);
    return -1;
  }

  KStatus GetColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema, char** value) override {
    assert(col_id < schema_.size());
    assert(!isVarLenType(schema_[col_id].type));
    *value = data_[col_id].data();
    return KStatus::SUCCESS;
  }
  KStatus GetColBitmap(uint32_t col_id, const std::vector<AttributeInfo>& schema, TsBitmap& bitmap) override {
    bitmap = bitmaps_[col_id];
    return KStatus::SUCCESS;
  }
  KStatus GetValueSlice(int row_num, int col_id, const std::vector<AttributeInfo>& schema, TSSlice& value) override {
    assert(col_id < schema_.size());
    if (isVarLenType(schema_[col_id].type)) {
      value.data = var_data_[col_id][row_num].data();
      value.len = var_data_[col_id][row_num].size();
    } else {
      value.data = data_[col_id].data() + row_num * schema_[col_id].size;
      value.len = schema_[col_id].size;
    }
    return SUCCESS;
  }
  bool IsColNull(int row_num, int col_id, const std::vector<AttributeInfo>& schema) override {
    assert(false);
    return false;
  }
};

std::shared_ptr<TsBlock> CreateBlock(int row_count, const std::vector<AttributeInfo>& schema, size_t seed) {
  auto block = std::make_shared<UnitTestBlock>(row_count, schema, seed);
  assert(block != nullptr);
  block->Init();
  return block;
}

static std::vector<AttributeInfo> GetSchema() {
  std::vector<AttributeInfo> schema;
  std::vector<DATATYPE> types{TIMESTAMP64, INT64, FLOAT, DOUBLE, BYTE, CHAR, VARSTRING};
  std::vector<int> sizes{16, 8, 4, 8, 23, 34, 255};
  for (int i = 0; i < types.size(); i++) {
    AttributeInfo attr;
    attr.id = i;
    attr.type = types[i];
    attr.size = sizes[i];
    schema.push_back(attr);
  }
  return schema;
};

TEST(UnitTestBlock, BasicRW) {
  auto schema = GetSchema();
  auto block = CreateBlock(100, schema, 0);
  TsBlockSpan span{1, block, 0, 100, nullptr, 0};
  char* data;
  TsBitmap bitmap;
  auto s = span.GetFixLenColAddr(0, &data, bitmap);
  EXPECT_EQ(s, KStatus::SUCCESS);
  DataFlags flag;
  TSSlice slice;
  s = span.GetVarLenTypeColAddr(2, 6, flag, slice);
  EXPECT_EQ(s, KStatus::SUCCESS);
}

TEST(MetricBlockBuilder, build) {
  auto schema = GetSchema();
  auto block = CreateBlock(100, schema, 0);
  auto span = std::make_shared<TsBlockSpan>(1, block, 0, 100);
  TsMetricBlockBuilder builder{schema};
  for (int i = 0; i < 10; i++) {
    auto s = builder.PutBlockSpan(span);
    ASSERT_EQ(s, KStatus::SUCCESS);
  }
  auto metric_block = builder.GetMetricBlock();

  std::string output;
  TsMetricCompressInfo info;
  auto s = metric_block->GetCompressedData(&output, &info, true);
  ASSERT_EQ(s, KStatus::SUCCESS);

  auto lsn = metric_block->GetLSNAddr();
  auto expected_lsn = block->GetLSNAddr(0);
  for (int i = 0; i < 1000; ++i) {
    EXPECT_EQ(lsn[i], expected_lsn[i % 100]);
  }
}
#endif