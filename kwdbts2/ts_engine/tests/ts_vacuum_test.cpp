#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>

#include "cm_kwdb_context.h"
#include "data_type.h"
#include "me_metadata.pb.h"
#include "settings.h"
#include "sys_utils.h"
#include "test_util.h"
#include "ts_engine_schema_manager.h"
#include "ts_table.h"
#include "ts_vgroup.h"

kwdbContext_t g_ctx_;
class Stopper {
 public:
  Stopper() { KWDBDynamicThreadPool::GetThreadPool().Init(8, &g_ctx_); }
  ~Stopper() { KWDBDynamicThreadPool::GetThreadPool().Stop(); }
};

static Stopper s;
using namespace roachpb;
using namespace kwdbts;
std::vector<roachpb::DataType> dtypes{DataType::TIMESTAMP, DataType::DOUBLE};
class VacuumTest : public testing::Test {
 protected:
  kwdbContext_p ctx_ = &g_ctx_;
  EngineOptions opts_;

  TSTableID table_id = 12315;
  roachpb::CreateTsTable meta;

  VacuumTest() {
    EngineOptions::vgroup_max_num = 1;
    EngineOptions::g_dedup_rule = DedupRule::KEEP;
    EngineOptions::mem_segment_max_size = INT32_MAX;
    EngineOptions::max_last_segment_num = 0;
    EngineOptions::max_compact_num = 2;
    EngineOptions::min_rows_per_block = 1;
    opts_.db_path = "./tsdb";
  }

  std::unique_ptr<TsEngineSchemaManager> schema_mgr_;
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_;
  const std::vector<AttributeInfo>* metric_schema_{nullptr};
  std::vector<TagInfo> tag_schema_;

  std::shared_ptr<TsVGroup> vgroup_;

  void SetUp() override {
    fs::remove_all("./tsdb");
    fs::remove_all("./schema");

    InitKWDBContext(ctx_);
    schema_mgr_ = std::make_unique<TsEngineSchemaManager>(opts_.db_path + "/schema");

    ConstructRoachpbTableWithTypes(&meta, table_id, dtypes);
    std::shared_ptr<TsTable> ts_table;

    ASSERT_EQ(schema_mgr_->CreateTable(ctx_, 1, table_id, &meta), SUCCESS);
    ASSERT_EQ(schema_mgr_->GetTableSchemaMgr(table_id, table_schema_mgr_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetMetricMeta(1, &metric_schema_), KStatus::SUCCESS);
    ASSERT_EQ(table_schema_mgr_->GetTagMeta(1, tag_schema_), KStatus::SUCCESS);

    std::shared_mutex wal_level_mutex;
    vgroup_ = std::make_shared<TsVGroup>(&opts_, 1, schema_mgr_.get(), &wal_level_mutex, nullptr, false);
    ASSERT_EQ(vgroup_->Init(ctx_), KStatus::SUCCESS);

    InitKWDBContext(ctx_);
  }
  void TearDown() override { vgroup_.reset(); }

  ~VacuumTest() {}
};

TEST_F(VacuumTest, ZDP49302) {
  setenv("KW_VACUUM_TIME", "0", true);
  for (int i = 0; i < 2; ++i) {
    auto payload = GenRowPayload(*metric_schema_, tag_schema_, table_id, 1, 1, 100, 1864000000 * i, 1);
    TsRawPayloadRowParser parser{metric_schema_};
    TsRawPayload p{metric_schema_};
    p.ParsePayLoadStruct(payload);
    auto ptag = p.GetPrimaryTag();
    vgroup_->PutData(ctx_, table_schema_mgr_, 0, &ptag, 1, &payload, false);
    free(payload.data);
  }

  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back({0, 0});
  vgroup_->DeleteData(ctx_, table_id, 1, 5000, ts_spans);

  vgroup_->Flush();
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);
  vgroup_->Vacuum(ctx_, false);

  int nblock_file = 0;

  for (auto ent : fs::directory_iterator("tsdb/vg_001/db00001_+0000000000000")) {
    std::string name = ent.path().filename();
    nblock_file += name.find("block.ver") != std::string::npos;
  }

  EXPECT_LE(nblock_file, 1);
}
