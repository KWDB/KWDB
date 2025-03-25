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

void Tester(int nrow, const std::string &filename) {
  auto compressor = kwdbts::TsEnvInstance::GetInstance().Compressor();
  compressor->ResetPolicy(LIGHT_COMRESS);
  using namespace roachpb;
  {
    System("rm -rf schema");
    CreateTsTable meta;
    TSTableID table_id = 123;
    ConstructRoachpbTableWithTypes(
        &meta, table_id,
        {DataType::TIMESTAMP, DataType::INT, DataType::INT, DataType::INT, DataType::INT});
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

    auto file = std::make_unique<TsMMapFile>(filename, false);
    TsLastSegmentBuilder builder(mgr.get(), std::move(file));
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
    TsLastSegmentMetricIndexBlock idx_block;
    file->Read(footer.block_info_idx_offset + i * sizeof(idx_block), sizeof(idx_block), &slice,
               reinterpret_cast<char *>(&idx_block));
    // char buf[32];
    // file->Read(idx_block.offset, 32, &slice, buf);
    // auto table_id = DecodeFixed<64>(buf + 8);
    EXPECT_EQ(idx_block.table_id, 123);
  }
}

TEST(LastSegmentBuilder, WriteAndRead) {
  Tester(1, "last1");
  std::filesystem::remove_all("schema");
  std::filesystem::remove("last1");
  Tester(12345, "last2");
  // std::filesystem::remove_all("schema");
  // std::filesystem::remove("last2");
}