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

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#define private public
#define protected public
#include "ee_dml_exec.h"
#include "ee_hash_tag_scan_op.h"
#include "ee_storage_handler.h"
#undef protected
#undef private

#include "ee_common.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_internal_type.h"
#include "ee_kwthd_context.h"
#include "ee_mempool.h"
#include "engine.h"

namespace kwdbts {
namespace {

class FakeStorageHandler : public StorageHandler {
 public:
  explicit FakeStorageHandler(TABLE* table) : StorageHandler(table) {}

  EEIteratorErrCode NewTagIterator(kwdbContext_p) override {
    ++new_iterator_calls_;
    return iterator_code_;
  }

  EEIteratorErrCode iterator_code_{EEIteratorErrCode::EE_OK};
  int new_iterator_calls_{0};
};

struct IteratorResult {
  IteratorResult() = default;
  IteratorResult(IteratorResult&&) = default;
  IteratorResult& operator=(IteratorResult&&) = default;
  IteratorResult(const IteratorResult&) = delete;
  IteratorResult& operator=(const IteratorResult&) = delete;
  ~IteratorResult() {
    for (auto& column_batches : batches) {
      for (auto*& batch : column_batches) {
        delete batch;
        batch = nullptr;
      }
    }
  }

  std::vector<EntityResultIndex> entities;
  k_uint32 count{0};
  k_uint32 col_num{0};
  std::vector<std::vector<Batch*>> batches;
};

class FakeEntityIterator : public BaseEntityIterator {
 public:
  explicit FakeEntityIterator(std::vector<IteratorResult> results)
      : results_(std::move(results)) {}

  KStatus Init() override { return KStatus::SUCCESS; }

  KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res,
               k_uint32* count) override {
    if (cursor_ >= results_.size()) {
      entity_id_list->clear();
      res->clear();
      *count = 0;
      return KStatus::SUCCESS;
    }
    *entity_id_list = std::move(results_[cursor_].entities);
    res->clear();
    res->setColumnNum(results_[cursor_].col_num);
    for (k_uint32 col = 0; col < results_[cursor_].col_num; ++col) {
      for (auto*& batch : results_[cursor_].batches[col]) {
        res->push_back(col, batch);
        batch = nullptr;
      }
    }
    *count = results_[cursor_].count;
    ++cursor_;
    return KStatus::SUCCESS;
  }

  KStatus Close() override {
    closed_ = true;
    return KStatus::SUCCESS;
  }

  bool closed_{false};

 private:
  std::vector<IteratorResult> results_;
  size_t cursor_{0};
};

class IteratorBackedStorageHandler : public StorageHandler {
 public:
  explicit IteratorBackedStorageHandler(TABLE* table) : StorageHandler(table) {}

  EEIteratorErrCode NewTagIterator(kwdbContext_p) override {
    ++new_iterator_calls_;
    if (iterator_code_ != EEIteratorErrCode::EE_OK) {
      return iterator_code_;
    }
    tag_iterator = iterator_;
    iterator_ = nullptr;
    return EEIteratorErrCode::EE_OK;
  }

  BaseEntityIterator* iterator_{nullptr};
  EEIteratorErrCode iterator_code_{EEIteratorErrCode::EE_OK};
  int new_iterator_calls_{0};
};

#define TS_FAIL_METHOD(...) KStatus __VA_ARGS__ override { return KStatus::FAIL; }
#define TS_VOID_METHOD(...) void __VA_ARGS__ override {}

class StartOnlyTSEngine : public TSEngine {
 public:
  KStatus GetTsTable(kwdbContext_p, const KTableKey&, std::shared_ptr<TsTable>& ts_table,
                     bool& is_dropped, bool, uint32_t) override {
    ++get_ts_table_calls_;
    is_dropped = false;
    ts_table.reset();
    return get_ts_table_status_;
  }

  TS_FAIL_METHOD(CreateTsTable(kwdbContext_p, const KTableKey&, roachpb::CreateTsTable*,
                               std::vector<RangeGroup>, bool));
  TS_FAIL_METHOD(DropTsTable(kwdbContext_p, const KTableKey&));
  TS_FAIL_METHOD(DropResidualTsTable(kwdbContext_p));
  TS_FAIL_METHOD(CreateNormalTagIndex(kwdbContext_p, const KTableKey&, const uint64_t, const char*, bool&,
                                      const uint32_t, const uint32_t, const std::vector<uint32_t>&));
  TS_FAIL_METHOD(DropNormalTagIndex(kwdbContext_p, const KTableKey&, const uint64_t, const char*, bool&,
                                    const uint32_t, const uint32_t));
  TS_FAIL_METHOD(AlterNormalTagIndex(kwdbContext_p, const KTableKey&, const uint64_t, const char*, bool&,
                                     const uint32_t, const uint32_t, const std::vector<uint32_t>&));
  TS_FAIL_METHOD(CompressTsTable(kwdbContext_p, const KTableKey&, KTimestamp));
  TS_FAIL_METHOD(GetMetaData(kwdbContext_p, const KTableKey&, RangeGroup, roachpb::CreateTsTable*, bool&));
  TS_FAIL_METHOD(PutEntity(kwdbContext_p, const KTableKey&, uint64_t, TSSlice*, int, uint64_t, bool&));
  TS_FAIL_METHOD(PutData(kwdbContext_p, const KTableKey&, uint64_t, TSSlice*, int, uint64_t, uint16_t*,
                         uint32_t*, DedupResult*, bool, const char*));
  TS_FAIL_METHOD(DeleteRangeData(kwdbContext_p, const KTableKey&, uint64_t, HashIdSpan&,
                                 const std::vector<KwTsSpan>&, uint64_t*, uint64_t, uint64_t, bool&));
  TS_FAIL_METHOD(DeleteData(kwdbContext_p, const KTableKey&, uint64_t, std::string&,
                            const std::vector<KwTsSpan>&, uint64_t*, uint64_t, uint64_t, bool&));
  TS_FAIL_METHOD(DeleteEntities(kwdbContext_p, const KTableKey&, uint64_t, std::vector<std::string>,
                                uint64_t*, uint64_t, bool&, uint64_t));
  TS_FAIL_METHOD(DeleteEntityByTag(kwdbContext_p, const KTableKey&, bool&, const std::vector<uint32_t>&,
                                   std::vector<std::string>, uint64_t*, uint64_t, const HashIdSpan&, uint64_t));
  TS_FAIL_METHOD(DeleteMetricByTag(kwdbContext_p, const KTableKey&, bool&, const std::vector<uint32_t>&,
                                   std::vector<std::string>, const std::vector<KwTsSpan>&, uint64_t*,
                                   uint64_t, const HashIdSpan&, uint64_t));
  TS_FAIL_METHOD(CountRangeData(kwdbContext_p, const KTableKey&, uint64_t, HashIdSpan&,
                                const std::vector<KwTsSpan>&, uint64_t*, uint64_t, uint64_t));
  TS_FAIL_METHOD(GetBatchRepr(kwdbContext_p, TSSlice*));
  TS_FAIL_METHOD(ApplyBatchRepr(kwdbContext_p, TSSlice*));
  TS_FAIL_METHOD(FlushBuffer(kwdbContext_p));
  TS_FAIL_METHOD(CreateCheckpoint(kwdbContext_p));
  TS_FAIL_METHOD(CreateCheckpointForTable(kwdbContext_p, TSTableID, bool&));
  TS_FAIL_METHOD(Recover(kwdbContext_p));
  TS_FAIL_METHOD(TSMtrBegin(kwdbContext_p, const KTableKey&, uint64_t, uint64_t, uint64_t, uint64_t&, const char*));
  TS_FAIL_METHOD(TSMtrCommit(kwdbContext_p, const KTableKey&, uint64_t, uint64_t, const char*));
  TS_FAIL_METHOD(TSMtrRollback(kwdbContext_p, const KTableKey&, uint64_t, uint64_t, bool, const char*));
  TS_FAIL_METHOD(TSxBegin(kwdbContext_p, const KTableKey&, char*, bool&));
  TS_FAIL_METHOD(TSxCommit(kwdbContext_p, const KTableKey&, char*, bool&));
  TS_FAIL_METHOD(TSxRollback(kwdbContext_p, const KTableKey&, char*, bool&));
  TS_VOID_METHOD(GetTableIDList(kwdbContext_p, std::vector<KTableKey>&));
  TS_FAIL_METHOD(UpdateSetting(kwdbContext_p));
  TS_FAIL_METHOD(AddColumn(kwdbContext_p, const KTableKey&, char*, bool&, TSSlice, uint32_t, uint32_t,
                           std::string&));
  TS_FAIL_METHOD(DropColumn(kwdbContext_p, const KTableKey&, char*, bool&, TSSlice, uint32_t, uint32_t,
                            std::string&));
  TS_FAIL_METHOD(AlterPartitionInterval(kwdbContext_p, const KTableKey&, uint64_t));
  TS_FAIL_METHOD(AlterLifetime(kwdbContext_p, const KTableKey&, uint64_t, bool&));
  TS_FAIL_METHOD(AlterColumn(kwdbContext_p, const KTableKey&, char*, bool&, TSSlice, TSSlice,
                             uint32_t, uint32_t, AlterType, std::string&));
  TS_FAIL_METHOD(GetTsWaitThreadNum(kwdbContext_p, void*));
  TS_FAIL_METHOD(GetTableVersion(kwdbContext_p, TSTableID, uint32_t*, bool&));
  TS_FAIL_METHOD(GetWalLevel(kwdbContext_p, uint8_t*));
  TS_FAIL_METHOD(SetUseRaftLogAsWAL(kwdbContext_p, bool));
  TS_FAIL_METHOD(FlushVGroups(kwdbContext_p));
  TS_VOID_METHOD(AlterTableCacheCapacity(int));
  TS_FAIL_METHOD(GetTableBlocksDistribution(TSTableID, TSSlice*));
  TS_FAIL_METHOD(GetDBBlocksDistribution(uint32_t, TSSlice*));

  int get_ts_table_calls_{0};
  KStatus get_ts_table_status_{KStatus::SUCCESS};
};

#undef TS_FAIL_METHOD
#undef TS_VOID_METHOD

class WhiteBoxHashTagScanOperator : public HashTagScanOperator {
 public:
  WhiteBoxHashTagScanOperator(TSTagReaderSpec* spec, PostProcessSpec* post,
                              TABLE* table)
      : HashTagScanOperator(nullptr, spec, post, table, 0) {}

  using HashTagScanOperator::BuildTagIndex;
  using HashTagScanOperator::CreateDynamicHashIndexes;
  using HashTagScanOperator::BuildRelIndex;
  using HashTagScanOperator::GetJoinColumnValues;
  using HashTagScanOperator::InitRelJointIndexes;
};

class StubNextHashTagScanOperator : public WhiteBoxHashTagScanOperator {
 public:
  StubNextHashTagScanOperator(TSTagReaderSpec* spec, PostProcessSpec* post,
                              TABLE* table)
      : WhiteBoxHashTagScanOperator(spec, post, table) {}

  EEIteratorErrCode Next(kwdbContext_p ctx) override {
    ++next_calls_;
    rel_tag_rowbatch_ = std::make_shared<RelTagRowBatch>();
    rel_tag_rowbatch_->Init(table_);
    rel_tag_rowbatch_->entity_indexs_.push_back(EntityResultIndex(1, 11, 2));
    rel_tag_rowbatch_->entity_indexs_.push_back(EntityResultIndex(1, 22, 3));
    rel_tag_rowbatch_->SetPipeEntityNum(ctx, current_thd->GetDegree());
    return EEIteratorErrCode::EE_OK;
  }

  int next_calls_{0};
};

class EntityIndexChunk : public DataChunk {
 public:
  using DataChunk::DataChunk;

  void SetEntityIndex(k_uint32 row, const EntityResultIndex& entity_index) {
    if (entity_indexs_.size() <= row) {
      entity_indexs_.resize(row + 1);
    }
    entity_indexs_[row] = entity_index;
  }
};

class OwnedColumnInfoChunk : public DataChunk {
 public:
  OwnedColumnInfoChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedColumnInfoChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

class OwnedEntityIndexChunk : public EntityIndexChunk {
 public:
  OwnedEntityIndexChunk(ColumnInfo* col_info, k_uint32 col_num,
                        k_uint32 capacity)
      : EntityIndexChunk(col_info, col_num, capacity),
        owned_col_info_(col_info) {}

  ~OwnedEntityIndexChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

std::unique_ptr<DataChunk> MakeChunk(const std::vector<ColumnInfo>& columns,
                                     const std::vector<bool>& nulls,
                                     k_int64 bigint_value,
                                     const std::string& varchar_value) {
  auto* col_info = new ColumnInfo[columns.size()];
  for (size_t i = 0; i < columns.size(); ++i) {
    col_info[i] = columns[i];
  }
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(
      col_info, columns.size(), 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  if (!nulls[0]) {
    chunk->InsertData(0, 0, reinterpret_cast<char*>(&bigint_value),
                      sizeof(bigint_value));
  } else {
    chunk->SetNull(0, 0);
  }
  if (!nulls[1]) {
    chunk->InsertData(0, 1, const_cast<char*>(varchar_value.data()),
                      varchar_value.size());
  } else {
    chunk->SetNull(0, 1);
  }
  return chunk;
}

std::unique_ptr<DataChunk> MakeStringIntChunk(const std::string& string_value,
                                              k_int32 int_value,
                                              bool string_is_null = false,
                                              bool int_is_null = false) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  col_info[1] =
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 2, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  if (string_is_null) {
    chunk->SetNull(0, 0);
  } else {
    chunk->InsertData(0, 0, const_cast<char*>(string_value.data()),
                      string_value.size());
  }
  if (int_is_null) {
    chunk->SetNull(0, 1);
  } else {
    chunk->InsertData(0, 1, reinterpret_cast<char*>(&int_value),
                      sizeof(int_value));
  }
  return chunk;
}

std::unique_ptr<DataChunk> MakeSingleStringChunk(const std::string& string_value,
                                                 bool string_is_null = false) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  if (string_is_null) {
    chunk->SetNull(0, 0);
  } else {
    chunk->InsertData(0, 0, const_cast<char*>(string_value.data()),
                      string_value.size());
  }
  return chunk;
}

std::unique_ptr<DataChunk> MakeSingleIntChunk(k_int32 int_value,
                                              bool int_is_null = false) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  if (int_is_null) {
    chunk->SetNull(0, 0);
  } else {
    chunk->InsertData(0, 0, reinterpret_cast<char*>(&int_value),
                      sizeof(int_value));
  }
  return chunk;
}

DataChunkPtr MakeTagChunk(k_int64 ptag_value, const std::string& tag_value,
                          const EntityResultIndex& entity_index) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  col_info[1] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  auto* chunk = new OwnedEntityIndexChunk(col_info, 2, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&ptag_value),
                    sizeof(ptag_value));
  chunk->InsertData(0, 1, const_cast<char*>(tag_value.data()), tag_value.size());
  chunk->SetEntityIndex(0, entity_index);
  return DataChunkPtr(chunk);
}

IteratorResult MakeSingleVarcharTagResult(const std::string& tag_value,
                                          const EntityResultIndex& entity_index,
                                          bool is_null = false) {
  IteratorResult output;
  output.entities.push_back(entity_index);
  output.count = 1;
  output.col_num = 1;
  output.batches.resize(1);

  auto* mem = static_cast<char*>(malloc(64));
  EXPECT_NE(mem, nullptr);
  auto* batch = new VarTagBatch(64, mem, 1);
  if (is_null) {
    EXPECT_EQ(batch->setNull(0), KStatus::SUCCESS);
  } else {
    std::string encoded(sizeof(k_uint16) + tag_value.size(), '\0');
    *reinterpret_cast<k_uint16*>(&encoded[0]) =
        static_cast<k_uint16>(tag_value.size());
    memcpy(&encoded[sizeof(k_uint16)], tag_value.data(), tag_value.size());
    EXPECT_EQ(batch->writeDataIncludeLen(
                  0, const_cast<char*>(encoded.data()),
                  static_cast<uint16_t>(encoded.size())),
              0);
  }
  output.batches[0].push_back(batch);
  return output;
}

IteratorResult MakeSingleIntTagResult(k_int32 tag_value,
                                      const EntityResultIndex& entity_index,
                                      bool is_null = false) {
  IteratorResult output;
  output.entities.push_back(entity_index);
  output.count = 1;
  output.col_num = 1;
  output.batches.resize(1);

  auto* mem = static_cast<char*>(malloc(8));
  EXPECT_NE(mem, nullptr);
  auto* batch = new TagBatch(5, mem, 1);
  if (is_null) {
    EXPECT_EQ(batch->setNull(0), KStatus::SUCCESS);
  } else {
    EXPECT_EQ(batch->setNotNull(0), KStatus::SUCCESS);
    memcpy(batch->getRowAddr(0), &tag_value, sizeof(tag_value));
  }
  output.batches[0].push_back(batch);
  return output;
}

BatchDataContainerPtr MakeBatchDataContainer(DataChunkPtr chunk,
                                             const std::vector<RowIndice>& next) {
  auto container = std::make_shared<BatchDataContainer>();
  LinkedListPtr linked_list = std::make_unique<BatchDataLinkedList>();
  EXPECT_EQ(linked_list->init(next.size()), 0);
  for (size_t i = 0; i < next.size(); ++i) {
    linked_list->row_indice_list_[i] = next[i];
  }
  container->rel_data_linked_lists_.AddLinkedList(std::move(linked_list));
  EXPECT_EQ(container->rel_data_container_.AddDataChunk(chunk), KStatus::SUCCESS);
  return container;
}

void InitTestContext(kwdbContext_p ctx) {
  InitServerKWDBContext(ctx);
  current_thd = KNEW KWThdContext();
  current_thd->SetPgEncode(TsNextRetState::DML_NEXT);
}

class HashTagScanWhiteBoxTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
  }

  static void TearDownTestCase() {
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }

  void SetUp() override {
    InitTestContext(&context_);

    table_ = new TABLE(1, 1);
    table_->field_num_ = 2;
    table_->min_tag_id_ = 0;
    table_->tag_num_ = 2;
    table_->fields_ =
        static_cast<Field**>(malloc(sizeof(Field*) * table_->field_num_));
    ASSERT_NE(table_->fields_, nullptr);

    auto* ptag_field = new FieldLonglong(0, roachpb::DataType::BIGINT, 8);
    ptag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);
    table_->fields_[0] = ptag_field;

    auto* tag_field = new FieldVarchar(1, roachpb::DataType::VARCHAR, 16);
    tag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
    table_->fields_[1] = tag_field;

    auto* rel_bigint = new FieldLonglong(0, roachpb::DataType::BIGINT, 8);
    auto* rel_varchar = new FieldVarchar(1, roachpb::DataType::VARCHAR, 16);
    table_->GetRelFields().push_back(rel_bigint);
    table_->GetRelFields().push_back(rel_varchar);
    post_.add_output_columns(0);
    post_.add_output_columns(1);
    post_.add_output_columns(2);
    post_.add_output_columns(3);
  }

  void TearDown() override {
    SafeDeletePointer(current_thd);
    delete table_;
  }

  kwdbContext_t context_{};
  TABLE* table_{nullptr};
  PostProcessSpec post_;
};

TEST_F(HashTagScanWhiteBoxTest, InitMapsTagsRelColsAndCachesBatchQueue) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  spec.set_tableid(0);
  spec.set_tableversion(7);

  DmlExec dml_exec;
  dml_exec.rel_batch_queue_ = new RelBatchQueue();
  context_.dml_exec_handle = &dml_exec;

  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  ASSERT_EQ(op.Init(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.Init(&context_), EEIteratorErrCode::EE_OK);

  ASSERT_EQ(table_->scan_rel_cols_.size(), 2U);
  EXPECT_EQ(table_->scan_rel_cols_[0], 0U);
  EXPECT_EQ(table_->scan_rel_cols_[1], 1U);
  EXPECT_EQ(op.scan_tag_to_output[0], 0U);
  EXPECT_EQ(op.scan_tag_to_output[1], 1U);
  EXPECT_NE(op.rel_batch_queue_, nullptr);
  EXPECT_NE(op.batch_data_container_, nullptr);
}

TEST_F(HashTagScanWhiteBoxTest, InitWithObjectIdBuildsRendersAndOutputFields) {
  PostProcessSpec post;
  post.add_output_columns(0);
  post.add_output_columns(1);
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 16));

  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  spec.set_tableid(9);
  spec.set_tableversion(11);

  DmlExec dml_exec;
  dml_exec.rel_batch_queue_ = new RelBatchQueue();
  context_.dml_exec_handle = &dml_exec;

  WhiteBoxHashTagScanOperator op(&spec, &post, table_);
  ASSERT_EQ(op.Init(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.num_, 2U);
  ASSERT_NE(op.renders_, nullptr);
  ASSERT_EQ(op.output_fields_.size(), 2U);
  EXPECT_EQ(op.output_fields_[0]->get_storage_type(), roachpb::DataType::BIGINT);
  EXPECT_EQ(op.output_fields_[1]->get_storage_type(), roachpb::DataType::VARCHAR);
}

TEST_F(HashTagScanWhiteBoxTest, InitRelJoinIndexesAndJoinValueExtractionHandleNulls) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::primaryHashTagScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  op.scan_tag_to_output[0] = 0;
  op.scan_tag_to_output[1] = 1;
  table_->GetRelTagJoinColumnIndexes().push_back({0, 0});
  table_->GetRelTagJoinColumnIndexes().push_back({1, 1});

  ASSERT_EQ(op.InitRelJointIndexes(), EEIteratorErrCode::EE_OK);
  ASSERT_EQ(op.primary_rel_cols_.size(), 1U);
  EXPECT_EQ(op.primary_rel_cols_[0], 0U);
  ASSERT_EQ(op.rel_other_join_cols_.size(), 1U);
  EXPECT_EQ(op.rel_other_join_cols_[0], 1U);
  ASSERT_EQ(op.tag_other_join_cols_.size(), 1U);
  EXPECT_EQ(op.tag_other_join_cols_[0], 1U);
  ASSERT_EQ(op.join_column_lengths_.size(), 1U);
  EXPECT_EQ(op.join_column_lengths_[0], 8U);
  EXPECT_EQ(op.total_join_column_length_, 8U);
  EXPECT_EQ(op.other_join_column_length_, 16U + STRING_WIDE);

  std::vector<ColumnInfo> rel_columns{
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};
  auto rel_chunk = MakeChunk(rel_columns, {false, false}, 123, "tag-1");

  std::vector<void*> primary_values(1);
  char primary_storage[8] = {0};
  primary_values[0] = primary_storage;
  std::vector<void*> other_values(1, nullptr);
  char other_storage[32] = {0};
  k_bool has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, rel_chunk, 0, primary_values,
                                   other_values, other_storage, has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(has_null);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(primary_storage), 123);
  ASSERT_NE(other_values[0], nullptr);
  EXPECT_EQ(std::string(static_cast<char*>(other_values[0]), 5), "tag-1");

  auto null_primary_chunk = MakeChunk(rel_columns, {true, false}, 123, "tag-1");
  has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, null_primary_chunk, 0,
                                   primary_values, other_values,
                                   other_storage, has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_TRUE(has_null);

  auto null_chunk = MakeChunk(rel_columns, {false, true}, 123, "ignored");
  has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, null_chunk, 0, primary_values,
                                   other_values, other_storage, has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_TRUE(has_null);

  char key_storage[32] = {0};
  has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, rel_chunk, 0, {0}, key_storage,
                                   has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(has_null);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(key_storage), 123);

  has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, null_chunk, 0, {1}, key_storage,
                                   has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_TRUE(has_null);

  TSTagReaderSpec bad_spec;
  bad_spec.set_accessmode(TSTableReadMode::primaryHashTagScan);
  WhiteBoxHashTagScanOperator bad_op(&bad_spec, &post_, table_);
  bad_op.scan_tag_to_output[1] = 1;
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({1, 1});
  EXPECT_EQ(bad_op.InitRelJointIndexes(),
            EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED);
}

TEST_F(HashTagScanWhiteBoxTest, CreateDynamicHashIndexesBuildsFromRelSideAndNextHandlesErrors) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  op.scan_tag_to_output[0] = 0;
  table_->scan_tags_ = {0};
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({0, 0});
  table_->SetAccessMode(TSTableReadMode::hashRelScan);

  FakeStorageHandler* handler = new FakeStorageHandler(table_);
  op.handler_ = handler;
  op.batch_data_container_ = std::make_shared<BatchDataContainer>();

  RelBatchQueue rel_batch_queue;
  std::vector<Field*> output_fields{table_->GetRelFields()[0]};
  ASSERT_EQ(rel_batch_queue.Init(output_fields), KStatus::SUCCESS);
  auto* queue_col_info = new ColumnInfo[1];
  queue_col_info[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  auto rel_chunk = std::make_unique<OwnedColumnInfoChunk>(queue_col_info, 1, 1);
  ASSERT_TRUE(rel_chunk->Initialize());
  rel_chunk->AddCount();
  k_int64 join_key = 77;
  rel_chunk->InsertData(0, 0, reinterpret_cast<char*>(&join_key),
                        sizeof(join_key));
  ASSERT_EQ(rel_batch_queue.Add(&context_, rel_chunk->GetData(), 1),
            KStatus::SUCCESS);
  ASSERT_EQ(rel_batch_queue.Done(&context_), EEIteratorErrCode::EE_OK);
  op.rel_batch_queue_ = &rel_batch_queue;

  ASSERT_EQ(op.CreateDynamicHashIndexes(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(handler->new_iterator_calls_, 1);
  ASSERT_NE(op.dynamic_hash_index_, nullptr);
  RowIndice row_indice;
  char key_buffer[8] = {0};
  *reinterpret_cast<k_int64*>(key_buffer) = 77;
  EXPECT_EQ(op.dynamic_hash_index_->get(key_buffer, sizeof(key_buffer),
                                        row_indice),
            0);
  EXPECT_EQ(row_indice.batch_no, 1U);
  EXPECT_EQ(row_indice.offset_in_batch, 1U);

  DataChunkPtr ignored_chunk = nullptr;
  EXPECT_EQ(op.Next(&context_, ignored_chunk), EEIteratorErrCode::EE_ERROR);

  WhiteBoxHashTagScanOperator no_queue_op(&spec, &post_, table_);
  EXPECT_EQ(no_queue_op.Next(&context_), EEIteratorErrCode::EE_ERROR);

  RelBatchQueue invalid_mode_queue;
  ASSERT_EQ(invalid_mode_queue.Init(output_fields), KStatus::SUCCESS);
  auto* invalid_col_info = new ColumnInfo[1];
  invalid_col_info[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  auto invalid_chunk =
      std::make_unique<OwnedColumnInfoChunk>(invalid_col_info, 1, 1);
  ASSERT_TRUE(invalid_chunk->Initialize());
  invalid_chunk->AddCount();
  invalid_chunk->InsertData(0, 0, reinterpret_cast<char*>(&join_key),
                            sizeof(join_key));
  ASSERT_EQ(invalid_mode_queue.Add(&context_, invalid_chunk->GetData(), 1),
            KStatus::SUCCESS);
  ASSERT_EQ(invalid_mode_queue.Done(&context_), EEIteratorErrCode::EE_OK);

  WhiteBoxHashTagScanOperator invalid_mode_op(&spec, &post_, table_);
  invalid_mode_op.rel_batch_queue_ = &invalid_mode_queue;
  invalid_mode_op.table_->SetAccessMode(999);
  invalid_mode_op.total_join_column_length_ = 1;
  invalid_mode_op.other_join_column_length_ = 1;
  EXPECT_EQ(invalid_mode_op.Next(&context_), EEIteratorErrCode::EE_ERROR);
}

TEST_F(HashTagScanWhiteBoxTest, BuildRelIndexStoresRowsInHashIndex) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  op.primary_rel_cols_ = {0};
  op.join_column_lengths_ = {8};
  op.total_join_column_length_ = 8;
  op.dynamic_hash_index_ = new DynamicHashIndex();
  ASSERT_EQ(op.dynamic_hash_index_->init(8), 0);
  op.batch_data_container_ = std::make_shared<BatchDataContainer>();

  RelBatchQueue rel_batch_queue;
  std::vector<Field*> output_fields{table_->GetRelFields()[0], table_->GetRelFields()[1]};
  ASSERT_EQ(rel_batch_queue.Init(output_fields), KStatus::SUCCESS);
  auto rel_chunk = MakeChunk(
      {ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily),
       ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)},
      {false, false}, 314, "rel-build");
  ASSERT_EQ(rel_batch_queue.Add(&context_, rel_chunk->GetData(), 1), KStatus::SUCCESS);
  ASSERT_EQ(rel_batch_queue.Done(&context_), EEIteratorErrCode::EE_OK);
  op.rel_batch_queue_ = &rel_batch_queue;

  ASSERT_EQ(op.BuildRelIndex(&context_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op.batch_data_container_->GetRelDataChunk(0), nullptr);
  EXPECT_EQ(op.batch_data_container_->GetRelDataChunk(0)->Count(), 1U);

  RowIndice row_indice;
  char key_buffer[8] = {0};
  *reinterpret_cast<k_int64*>(key_buffer) = 314;
  ASSERT_EQ(op.dynamic_hash_index_->get(key_buffer, sizeof(key_buffer), row_indice), 0);
  EXPECT_EQ(row_indice.batch_no, 1U);
  EXPECT_EQ(row_indice.offset_in_batch, 1U);
}

TEST_F(HashTagScanWhiteBoxTest, CreateDynamicHashIndexesBuildsTagSideIndexFromIterator) {
  delete table_->fields_[1];
  auto* tag_field = new FieldInt(1, roachpb::DataType::INT, 4);
  tag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
  table_->fields_[1] = tag_field;

  delete table_->GetRelFields()[1];
  table_->GetRelFields()[1] = new FieldInt(1, roachpb::DataType::INT, 4);

  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashTagScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  table_->SetAccessMode(TSTableReadMode::hashTagScan);
  table_->scan_tags_ = {1};
  table_->scan_rel_cols_.clear();
  op.scan_tag_to_output[1] = 0;
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({1, 1});
  op.batch_data_container_ = std::make_shared<BatchDataContainer>();
  op.tag_col_size_ = 1;
  op.tag_renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
  ASSERT_NE(op.tag_renders_, nullptr);
  op.tag_renders_[0] = table_->fields_[1];
  op.tag_col_info_ = KNEW ColumnInfo[1];
  op.tag_col_info_[0] =
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);

  std::vector<IteratorResult> build_results;
  build_results.reserve(1);
  build_results.push_back(MakeSingleIntTagResult(314, EntityResultIndex(2, 22, 3)));
  auto* iterator = new FakeEntityIterator(std::move(build_results));
  auto* handler = new IteratorBackedStorageHandler(table_);
  handler->iterator_ = iterator;
  op.handler_ = handler;

  ASSERT_EQ(op.CreateDynamicHashIndexes(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(handler->new_iterator_calls_, 1);
  ASSERT_NE(op.dynamic_hash_index_, nullptr);
  ASSERT_NE(op.batch_data_container_->GetRelDataChunk(0), nullptr);
  EXPECT_EQ(op.batch_data_container_->GetRelDataChunk(0)->Count(), 1U);

  char key_buffer[4] = {0};
  *reinterpret_cast<k_int32*>(key_buffer) = 314;
  RowIndice row_indice;
  ASSERT_EQ(op.dynamic_hash_index_->get(key_buffer, sizeof(key_buffer), row_indice),
            0);
  EXPECT_EQ(row_indice.batch_no, 1U);
  EXPECT_EQ(row_indice.offset_in_batch, 1U);
}

TEST_F(HashTagScanWhiteBoxTest, NextInHashRelModeJoinsRowsFromTagIterator) {
  delete table_->fields_[1];
  auto* tag_field = new FieldInt(1, roachpb::DataType::INT, 4);
  tag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
  table_->fields_[1] = tag_field;

  delete table_->GetRelFields()[1];
  table_->GetRelFields()[1] = new FieldInt(1, roachpb::DataType::INT, 4);

  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  table_->SetAccessMode(TSTableReadMode::hashRelScan);
  table_->scan_tags_ = {1};
  table_->scan_rel_cols_.clear();
  table_->fields_[1]->setColIdxInRs(0);
  op.scan_tag_to_output[1] = 0;
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({0, 1});
  op.primary_rel_cols_ = {0};
  op.primary_tag_cols_ = {0};
  op.join_column_lengths_ = {4};
  op.total_join_column_length_ = 4;
  op.dynamic_hash_index_ = new DynamicHashIndex();
  ASSERT_EQ(op.dynamic_hash_index_->init(4), 0);
  op.batch_data_container_ = std::make_shared<BatchDataContainer>();
  op.tag_col_size_ = 1;
  op.tag_renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
  ASSERT_NE(op.tag_renders_, nullptr);
  op.tag_renders_[0] = table_->fields_[1];
  op.tag_col_info_ = KNEW ColumnInfo[1];
  op.tag_col_info_[0] =
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);

  RelBatchQueue rel_batch_queue;
  std::vector<Field*> output_fields{table_->GetRelFields()[1]};
  ASSERT_EQ(rel_batch_queue.Init(output_fields), KStatus::SUCCESS);
  auto rel_chunk = MakeSingleIntChunk(314);
  ASSERT_EQ(rel_batch_queue.Add(&context_, rel_chunk->GetData(), 1),
            KStatus::SUCCESS);
  ASSERT_EQ(rel_batch_queue.Done(&context_), EEIteratorErrCode::EE_OK);
  op.rel_batch_queue_ = &rel_batch_queue;
  ASSERT_EQ(op.BuildRelIndex(&context_), EEIteratorErrCode::EE_OK);

  std::vector<IteratorResult> probe_results;
  probe_results.reserve(1);
  probe_results.push_back(
      MakeSingleIntTagResult(314, EntityResultIndex(7, 77, 5)));
  auto* iterator = new FakeEntityIterator(std::move(probe_results));
  auto* handler = new IteratorBackedStorageHandler(table_);
  handler->iterator_ = iterator;
  op.handler_ = handler;
  ASSERT_EQ(handler->NewTagIterator(&context_), EEIteratorErrCode::EE_OK);

  ASSERT_EQ(op.Next(&context_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op.rel_tag_rowbatch_, nullptr);
  EXPECT_EQ(op.rel_tag_rowbatch_->Count(), 1U);
  ASSERT_EQ(op.rel_tag_rowbatch_->entity_indexs_.size(), 1U);
  EXPECT_TRUE(op.rel_tag_rowbatch_->entity_indexs_[0].equalsWithoutMem(
      EntityResultIndex(7, 77, 5)));
  EXPECT_EQ(op.total_read_row_, 1U);
}

TEST_F(HashTagScanWhiteBoxTest, StartAndCreateDynamicHashIndexesCoverFailureBranches) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  spec.set_tableid(12);
  table_->scan_tags_ = {0, 1};

  auto* start_op = new WhiteBoxHashTagScanOperator(&spec, &post_, table_);
  EXPECT_EQ(start_op->Start(&context_), EEIteratorErrCode::EE_ERROR);
  EXPECT_TRUE(start_op->started_);
  EXPECT_EQ(start_op->start_code_, EEIteratorErrCode::EE_ERROR);
  delete start_op;

  auto* cleanup_op = new WhiteBoxHashTagScanOperator(&spec, &post_, table_);
  cleanup_op->handler_ = new FakeStorageHandler(table_);
  cleanup_op->dynamic_hash_index_ = new DynamicHashIndex();
  ASSERT_EQ(cleanup_op->dynamic_hash_index_->init(8), 0);
  cleanup_op->tag_renders_ =
      static_cast<Field**>(malloc(sizeof(Field*) * table_->scan_tags_.size()));
  ASSERT_NE(cleanup_op->tag_renders_, nullptr);
  memset(cleanup_op->tag_renders_, 0, sizeof(Field*) * table_->scan_tags_.size());
  cleanup_op->tag_col_info_ = KNEW ColumnInfo[table_->scan_tags_.size()];
  delete cleanup_op;

  WhiteBoxHashTagScanOperator iterator_fail_op(&spec, &post_, table_);
  iterator_fail_op.scan_tag_to_output[0] = 0;
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({0, 0});
  auto* iterator_fail_handler = new FakeStorageHandler(table_);
  iterator_fail_handler->iterator_code_ = EEIteratorErrCode::EE_ERROR;
  iterator_fail_op.handler_ = iterator_fail_handler;
  EXPECT_EQ(iterator_fail_op.CreateDynamicHashIndexes(&context_),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(iterator_fail_handler->new_iterator_calls_, 1);
  EXPECT_EQ(iterator_fail_op.dynamic_hash_index_, nullptr);

  WhiteBoxHashTagScanOperator invalid_mode_op(&spec, &post_, table_);
  invalid_mode_op.scan_tag_to_output[0] = 0;
  invalid_mode_op.handler_ = new FakeStorageHandler(table_);
  table_->SetAccessMode(999);
  EXPECT_EQ(invalid_mode_op.CreateDynamicHashIndexes(&context_),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_NE(invalid_mode_op.dynamic_hash_index_, nullptr);
}

TEST_F(HashTagScanWhiteBoxTest, StartSucceedsForPrimaryHashTagScanWithStubEngine) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::primaryHashTagScan);
  spec.set_tableid(18);
  spec.set_tableversion(3);

  PostProcessSpec post;
  post.add_output_columns(0);
  post.add_output_columns(1);
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 16));

  DmlExec dml_exec;
  dml_exec.rel_batch_queue_ = new RelBatchQueue();
  context_.dml_exec_handle = &dml_exec;

  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({0, 0});
  table_->GetRelTagJoinColumnIndexes().push_back({1, 1});

  StartOnlyTSEngine engine;
  context_.ts_engine = &engine;

  WhiteBoxHashTagScanOperator op(&spec, &post, table_);
  ASSERT_EQ(op.Init(&context_), EEIteratorErrCode::EE_OK);
  ASSERT_EQ(op.Start(&context_), EEIteratorErrCode::EE_OK);

  EXPECT_TRUE(op.started_);
  EXPECT_EQ(op.start_code_, EEIteratorErrCode::EE_OK);
  EXPECT_EQ(engine.get_ts_table_calls_, 1);
  EXPECT_NE(op.handler_, nullptr);
  ASSERT_NE(op.tag_renders_, nullptr);
  EXPECT_EQ(op.tag_col_size_, 2);
  ASSERT_NE(op.tag_col_info_, nullptr);
  EXPECT_EQ(op.total_join_column_length_, 8U);
  EXPECT_GT(op.other_join_column_length_, 0U);

  EXPECT_EQ(op.Start(&context_), EEIteratorErrCode::EE_OK);
}

TEST_F(HashTagScanWhiteBoxTest, JoinValueExtractionCoversStringAndDefaultBranches) {
  delete table_->fields_[0];
  auto* ptag_field = new FieldVarchar(0, roachpb::DataType::VARCHAR, 16);
  ptag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);
  table_->fields_[0] = ptag_field;

  delete table_->fields_[1];
  auto* tag_field = new FieldInt(1, roachpb::DataType::INT, 4);
  tag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
  table_->fields_[1] = tag_field;

  delete table_->GetRelFields()[0];
  table_->GetRelFields()[0] = new FieldVarchar(0, roachpb::DataType::VARCHAR, 16);
  delete table_->GetRelFields()[1];
  table_->GetRelFields()[1] = new FieldInt(1, roachpb::DataType::INT, 4);

  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::primaryHashTagScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  op.scan_tag_to_output[0] = 0;
  op.scan_tag_to_output[1] = 1;
  table_->GetRelTagJoinColumnIndexes().clear();
  table_->GetRelTagJoinColumnIndexes().push_back({0, 0});
  table_->GetRelTagJoinColumnIndexes().push_back({1, 1});
  ASSERT_EQ(op.InitRelJointIndexes(), EEIteratorErrCode::EE_OK);

  auto rel_chunk = MakeStringIntChunk("ptag-key", 42);
  std::vector<void*> primary_values(1);
  char primary_storage[32] = {0};
  primary_values[0] = primary_storage;
  std::vector<void*> other_values(1, nullptr);
  char other_storage[16] = {0};
  k_bool has_null = false;
  ASSERT_EQ(op.GetJoinColumnValues(&context_, rel_chunk, 0, primary_values,
                                   other_values, other_storage, has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(has_null);
  EXPECT_EQ(std::string(primary_storage, 8), "ptag-key");
  ASSERT_NE(other_values[0], nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(other_values[0]), 42);

  char key_storage[32] = {0};
  ASSERT_EQ(op.GetJoinColumnValues(&context_, rel_chunk, 0, {0}, key_storage,
                                   has_null),
            EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(has_null);
  EXPECT_EQ(std::string(key_storage, 8), "ptag-key");
}

TEST_F(HashTagScanWhiteBoxTest, NextInPrimaryHashTagModeSkipsRowsWithoutCommonPrimaryTags) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::primaryHashTagScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  table_->SetAccessMode(TSTableReadMode::primaryHashTagScan);
  table_->scan_tags_ = {1};
  table_->scan_rel_cols_.clear();
  op.scan_tag_to_output[1] = 0;
  op.primary_rel_cols_ = {0};
  op.rel_other_join_cols_ = {1};
  op.tag_other_join_cols_ = {0};
  op.join_column_lengths_ = {8};
  op.total_join_column_length_ = 8;
  op.other_join_column_length_ = 32;
  op.handler_ = new StorageHandler(table_);

  k_int64 primary_tag = 111;
  op.primary_tags_.push_back(&primary_tag);

  RelBatchQueue rel_batch_queue;
  std::vector<Field*> output_fields{table_->GetRelFields()[0], table_->GetRelFields()[1]};
  ASSERT_EQ(rel_batch_queue.Init(output_fields), KStatus::SUCCESS);
  auto rel_chunk = MakeChunk(
      {ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily),
       ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)},
      {false, false}, 222, "secondary");
  ASSERT_EQ(rel_batch_queue.Add(&context_, rel_chunk->GetData(), 1), KStatus::SUCCESS);
  ASSERT_EQ(rel_batch_queue.Done(&context_), EEIteratorErrCode::EE_OK);
  op.rel_batch_queue_ = &rel_batch_queue;

  EXPECT_EQ(op.Next(&context_), EEIteratorErrCode::EE_END_OF_RECORD);
  ASSERT_NE(op.rel_tag_rowbatch_, nullptr);
  EXPECT_EQ(op.rel_tag_rowbatch_->Count(), 0U);
}

TEST_F(HashTagScanWhiteBoxTest, NextInHashTagModeProducesJoinedRows) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashTagScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  table_->SetAccessMode(TSTableReadMode::hashTagScan);
  table_->scan_tags_ = {0, 1};
  table_->scan_rel_cols_ = {0, 1};
  table_->fields_[0]->setColIdxInRs(0);
  table_->fields_[1]->setColIdxInRs(1);
  table_->GetRelFields()[0]->setColIdxInRs(2);
  table_->GetRelFields()[1]->setColIdxInRs(3);

  RelBatchQueue rel_batch_queue;
  std::vector<Field*> output_fields{table_->GetRelFields()[0], table_->GetRelFields()[1]};
  ASSERT_EQ(rel_batch_queue.Init(output_fields), KStatus::SUCCESS);
  auto rel_chunk = MakeChunk(
      {ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily),
       ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)},
      {false, false}, 77, "rel-77");
  ASSERT_EQ(rel_batch_queue.Add(&context_, rel_chunk->GetData(), 1), KStatus::SUCCESS);
  op.rel_batch_queue_ = &rel_batch_queue;

  auto tag_chunk = MakeTagChunk(77, "tag-77", EntityResultIndex(5, 55, 7));
  op.batch_data_container_ =
      MakeBatchDataContainer(std::move(tag_chunk), {RowIndice{0, 0}});
  op.dynamic_hash_index_ = new DynamicHashIndex();
  ASSERT_EQ(op.dynamic_hash_index_->init(8), 0);
  char key_buffer[8] = {0};
  *reinterpret_cast<k_int64*>(key_buffer) = 77;
  ASSERT_EQ(op.dynamic_hash_index_->put(key_buffer, sizeof(key_buffer),
                                        RowIndice{1, 1}),
            0);
  op.primary_rel_cols_ = {0};
  op.join_column_lengths_ = {8};
  op.total_join_column_length_ = 8;
  op.other_join_column_length_ = 1;

  ASSERT_EQ(op.Next(&context_), EEIteratorErrCode::EE_OK);
  ASSERT_NE(op.rel_tag_rowbatch_, nullptr);
  EXPECT_EQ(op.rel_tag_rowbatch_->Count(), 1U);
  ASSERT_EQ(op.rel_tag_rowbatch_->entity_indexs_.size(), 1U);
  EXPECT_TRUE(op.rel_tag_rowbatch_->entity_indexs_[0].equalsWithoutMem(
      EntityResultIndex(5, 55, 7)));
  EXPECT_EQ(op.total_read_row_, 1U);
}

TEST_F(HashTagScanWhiteBoxTest, GetEntitiesCloseResetAndAccessorUseVirtualState) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  table_->scan_tags_ = {0, 1};
  StubNextHashTagScanOperator op(&spec, &post_, table_);

  TagRowBatchPtr batch;
  std::vector<EntityResultIndex> entities;
  ASSERT_EQ(op.GetEntities(&context_, &entities, &batch), KStatus::SUCCESS);
  ASSERT_NE(batch, nullptr);
  EXPECT_EQ(op.next_calls_, 1);
  ASSERT_EQ(entities.size(), 2U);
  EXPECT_TRUE(entities[0].equalsWithoutMem(EntityResultIndex(1, 11, 2)));
  EXPECT_TRUE(entities[1].equalsWithoutMem(EntityResultIndex(1, 22, 3)));
  EXPECT_EQ(op.GetRowBatch(&context_), batch.get());
  EXPECT_EQ(op.Reset(&context_), EEIteratorErrCode::EE_OK);

  op.started_ = true;
  op.start_code_ = EEIteratorErrCode::EE_OK;
  EXPECT_EQ(op.Start(&context_), EEIteratorErrCode::EE_OK);

  op.total_read_row_ = 9;
  op.tag_index_once_ = false;
  EXPECT_EQ(op.Close(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.total_read_row_, 0U);
  EXPECT_FALSE(op.started_);
  EXPECT_TRUE(op.tag_index_once_);
}

TEST_F(HashTagScanWhiteBoxTest, GetEntitiesFailsWhenDistributedCursorSwitchesToEmptyBatch) {
  TSTagReaderSpec spec;
  spec.set_accessmode(TSTableReadMode::hashRelScan);
  WhiteBoxHashTagScanOperator op(&spec, &post_, table_);
  op.is_first_entity_ = false;
  op.rel_tag_rowbatch_ = std::make_shared<RelTagRowBatch>();
  op.rel_tag_rowbatch_->Init(table_);
  op.rel_tag_rowbatch_->valid_pipe_no_ = 1;
  op.rel_tag_rowbatch_->current_pipe_no_ = 0;

  TagRowBatchPtr row_batch_ptr = std::make_shared<RelTagRowBatch>();
  row_batch_ptr->Init(table_);
  row_batch_ptr->valid_pipe_no_ = 0;
  row_batch_ptr->current_pipe_no_ = 0;

  std::vector<EntityResultIndex> entities;
  EXPECT_EQ(op.GetEntities(&context_, &entities, &row_batch_ptr), KStatus::FAIL);
}

}  // namespace
}  // namespace kwdbts
