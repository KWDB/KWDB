#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <memory>

#include "me_metadata.pb.h"
#include "test_util.h"
#include "ts_coding.h"
#include "ts_engine_schema_manager.h"
#include "ts_env.h"
#include "ts_io.h"
#include "ts_last_segment_manager.h"
#include "ts_payload.h"
#include "ts_slice.h"

void Tester(TSTableID table_id, int nrow, const std::string &filename) {
  auto compressor = kwdbts::TsEnvInstance::GetInstance().Compressor();
  compressor->ResetPolicy(LIGHT_COMRESS);
  using namespace roachpb;
  std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::INT, DataType::INT,
                                        DataType::INT, DataType::INT};
  {
    System("rm -rf schema");
    CreateTsTable meta;
    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    auto mgr = std::make_unique<TsEngineSchemaManager>("schema");
    auto s = mgr->CreateTable(nullptr, table_id, &meta);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::shared_ptr<TsTableSchemaManager> schema_mgr;
    s = mgr->GetTableSchemaMgr(table_id, schema_mgr);
    ASSERT_EQ(s, KStatus::SUCCESS);

    std::vector<AttributeInfo> metric_schema;
    s = schema_mgr->GetMetricMeta(1, metric_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    ASSERT_EQ(s, KStatus::SUCCESS);

    TsLastSegmentManager last_segment_mgr("./");
    std::unique_ptr<TsLastSegment> last_segment;
    last_segment_mgr.NewLastSegment(&last_segment);
    TsLastSegmentBuilder builder(mgr.get(), std::move(last_segment));
    auto payload = GenRowPayload(metric_schema, tag_schema, table_id, 1, 1, nrow, 123);
    TsRawPayloadRowParser parser{metric_schema};
    TsRawPayload p{payload, metric_schema};

    for (int i = 0; i < p.GetRowCount(); ++i) {
      s = builder.PutRowData(table_id, 1, 1, i, p.GetRowData(i));
      EXPECT_EQ(s, KStatus::SUCCESS);
    }
    builder.Finalize();
    builder.Flush();
    free(payload.data);
  }

  auto file = std::make_unique<TsMMapFile>(filename, true);
  TsLastSegmentFooter footer;
  auto sz = file->GetFileSize();
  ASSERT_TRUE(sz >= sizeof(footer));
  TSSlice slice;
  file->Read(sz - sizeof(footer), sizeof(footer), &slice, reinterpret_cast<char *>(&footer));
  ASSERT_EQ(footer.magic_number, FOOTER_MAGIC);

  auto nblock = footer.n_data_block;
  EXPECT_EQ(nblock, (nrow + 4095) / 4096);
  int expected_nrows = nrow;
  nrow = 0;
  for (int i = 0; i < nblock; ++i) {
    TsLastSegmentBlockIndex idx_block;
    file->Read(footer.block_info_idx_offset + i * sizeof(idx_block), sizeof(idx_block), &slice,
               reinterpret_cast<char *>(&idx_block));
    EXPECT_EQ(idx_block.table_id, table_id);

    char buf[10240];
    TSSlice result;
    file->Read(idx_block.offset, idx_block.length, &result, buf);
    TsLastSegmentBlockInfo info;
    const char *ptr = buf;
    info.block_offset = DecodeFixed64(ptr);
    ptr += 8;
    info.nrow = DecodeFixed32(ptr);
    ptr += 4;
    info.ncol = DecodeFixed32(ptr);
    ptr += 4;
    info.var_offset = DecodeFixed32(ptr);
    ptr += 4;
    info.var_len = DecodeFixed32(ptr);
    ptr += 4;

    ASSERT_EQ(info.ncol, dtypes.size() + 2);
    info.col_infos.resize(info.ncol);
    for (int j = 0; j < info.ncol; ++j) {
      info.col_infos[j].offset = DecodeFixed32(ptr);
      ptr += 4;
      info.col_infos[j].bitmap_len = DecodeFixed16(ptr);
      ptr += 2;
      info.col_infos[j].data_len = DecodeFixed32(ptr);
      ptr += 4;

      EXPECT_NE(info.col_infos[j].bitmap_len, 1);
    }
    ASSERT_EQ(ptr, buf + idx_block.length);
    for (int j = 0; j < info.ncol; ++j) {
      if (j < 2) {
        ASSERT_EQ(info.col_infos[j].bitmap_len, 0) << "At Column " << j;
      }
      if (info.col_infos[j].bitmap_len != 0) {
        file->Read(info.block_offset + info.col_infos[j].offset, info.col_infos[j].bitmap_len,
                   &result, buf);
        ASSERT_EQ(buf[0], 0) << "At block: " << i << ", Column: " << j;
      }
    }
  }
}

TEST(LastSegmentBuilder, WriteAndRead1) {
  Tester(13, 1, "last.ver-0001");
  std::filesystem::remove_all("schema");
  std::filesystem::remove("last.ver-0001");
}

TEST(LastSegmentBuilder, WriteAndRead2) {
  Tester(14, 12345, "last.ver-0001");
  std::filesystem::remove_all("schema");
  std::filesystem::remove("last.ver-0001");
}

TEST(LastSegmentBuilder, WriteAndRead3) {
  Tester(15, 4096, "last.ver-0001");
  std::filesystem::remove_all("schema");
  std::filesystem::remove("last.ver-0001");
}