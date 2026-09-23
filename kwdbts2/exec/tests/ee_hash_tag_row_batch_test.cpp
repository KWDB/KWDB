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

#include "ee_batch_data_container.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_hash_tag_row_batch.h"
#include "ee_kwthd_context.h"
#include "ee_mempool.h"
#include "ee_rel_tag_row_batch.h"
#include "ee_table.h"
#include "gtest/gtest.h"

namespace kwdbts {
namespace {

class HashTagRowBatchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    table_ = new TABLE(1, 1);
    table_->field_num_ = 2;
    table_->min_tag_id_ = 0;
    table_->tag_num_ = 2;
    table_->scan_rel_cols_.push_back(0);
    table_->scan_tags_.push_back(0);
    table_->scan_tags_.push_back(1);

    table_->fields_ =
        static_cast<Field**>(malloc(sizeof(Field*) * table_->field_num_));
    ASSERT_NE(table_->fields_, nullptr);

    auto* ptag_field = new FieldChar(0, roachpb::DataType::CHAR, 4);
    ptag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);
    table_->fields_[0] = ptag_field;

    auto* tag_field = new FieldVarchar(1, roachpb::DataType::VARCHAR, 16);
    tag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
    table_->fields_[1] = tag_field;

    batch_.Init(table_);
    batch_.count_ = 1;
  }

  void TearDown() override { delete table_; }

  TABLE* table_{nullptr};
  HashTagRowBatch batch_;
};

class TestDataChunk : public DataChunk {
 public:
  using DataChunk::DataChunk;

  void SetEntityIndex(k_uint32 row, const EntityResultIndex& entity_index) {
    if (entity_indexs_.size() <= row) {
      entity_indexs_.resize(row + 1);
    }
    entity_indexs_[row] = entity_index;
  }
};

class OwnedJoinChunk : public TestDataChunk {
 public:
  OwnedJoinChunk(ColumnInfo* columns, k_uint32 col_num, k_uint32 capacity)
      : TestDataChunk(columns, col_num, capacity), owned_columns_(columns) {}

  ~OwnedJoinChunk() override { delete[] owned_columns_; }

 private:
  ColumnInfo* owned_columns_;
};

struct JoinRow {
  k_int64 ptag_value;
  std::string tag_varchar;
  std::string tag_char;
  bool varchar_is_null{false};
  bool char_is_null{false};
  EntityResultIndex entity_index{};
};

DataChunkPtr MakeJoinChunk(const std::vector<JoinRow>& rows) {
  auto* columns = new ColumnInfo[3];
  columns[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  columns[1] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  columns[2] =
      ColumnInfo(8, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily);
  columns[1].allow_null = true;
  columns[2].allow_null = true;

  auto* chunk = new OwnedJoinChunk(columns, 3, rows.size());
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount(rows.size());

  for (size_t i = 0; i < rows.size(); ++i) {
    k_int64 ptag_value = rows[i].ptag_value;
    chunk->InsertData(static_cast<k_uint32>(i), 0,
                      reinterpret_cast<char*>(&ptag_value),
                      sizeof(ptag_value));
    if (rows[i].varchar_is_null) {
      chunk->SetNull(static_cast<k_uint32>(i), 1);
    } else {
      chunk->InsertData(static_cast<k_uint32>(i), 1,
                        const_cast<char*>(rows[i].tag_varchar.data()),
                        rows[i].tag_varchar.size());
    }
    if (rows[i].char_is_null) {
      chunk->SetNull(static_cast<k_uint32>(i), 2);
    } else {
      chunk->InsertData(static_cast<k_uint32>(i), 2,
                        const_cast<char*>(rows[i].tag_char.data()),
                        rows[i].tag_char.size());
    }
    chunk->SetEntityIndex(static_cast<k_uint32>(i), rows[i].entity_index);
  }

  return DataChunkPtr(chunk);
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

k_int64 ReadIntValue(const TagRawData& data) {
  return *reinterpret_cast<const k_int64*>(data.tag_data);
}

std::string ReadVarString(const TagRawData& data) {
  return std::string(data.tag_data + sizeof(k_uint16), data.size);
}

std::string ReadFixedString(const TagRawData& data) {
  return std::string(data.tag_data, data.size);
}

class RelTagRowBatchTest : public ::testing::Test {
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
    InitServerKWDBContext(&context_);
    current_thd = KNEW KWThdContext();
    current_thd->SetPgEncode(TsNextRetState::DML_NEXT);

    table_ = new TABLE(1, 1);
    table_->field_num_ = 3;
    table_->min_tag_id_ = 0;
    table_->tag_num_ = 3;
    table_->scan_tags_ = {0, 1, 2};
    table_->scan_rel_cols_ = {0, 1, 2};

    table_->fields_ =
        static_cast<Field**>(malloc(sizeof(Field*) * table_->field_num_));
    ASSERT_NE(table_->fields_, nullptr);

    auto* ptag_field = new FieldLonglong(0, roachpb::DataType::BIGINT, 8);
    ptag_field->set_column_type(roachpb::KWDBKTSColumn::TYPE_PTAG);
    ptag_field->setColIdxInRs(0);
    table_->fields_[0] = ptag_field;

    auto* tag_varchar = new FieldVarchar(1, roachpb::DataType::VARCHAR, 16);
    tag_varchar->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
    tag_varchar->setColIdxInRs(1);
    table_->fields_[1] = tag_varchar;

    auto* tag_char = new FieldChar(2, roachpb::DataType::CHAR, 8);
    tag_char->set_column_type(roachpb::KWDBKTSColumn::TYPE_TAG);
    tag_char->setColIdxInRs(2);
    table_->fields_[2] = tag_char;

    auto* rel_bigint = new FieldLonglong(0, roachpb::DataType::BIGINT, 8);
    rel_bigint->setColIdxInRs(3);
    table_->GetRelFields().push_back(rel_bigint);

    auto* rel_varchar = new FieldVarchar(1, roachpb::DataType::VARCHAR, 16);
    rel_varchar->setColIdxInRs(4);
    table_->GetRelFields().push_back(rel_varchar);

    auto* rel_char = new FieldChar(2, roachpb::DataType::CHAR, 8);
    rel_char->setColIdxInRs(5);
    table_->GetRelFields().push_back(rel_char);

    batch_.Init(table_);
  }

  void TearDown() override {
    SafeDeletePointer(current_thd);
    delete table_;
  }

  static void ExpectJoinedRow(const TagData& row, k_int64 tag_id,
                              const std::string& tag_var,
                              const std::string& tag_char,
                              k_int64 rel_id,
                              const std::string& rel_var,
                              const std::string& rel_char,
                              bool tag_var_is_null = false,
                              bool rel_var_is_null = false,
                              bool tag_char_is_null = false,
                              bool rel_char_is_null = false) {
    ASSERT_EQ(row.size(), 6U);

    EXPECT_FALSE(row[0].is_null);
    EXPECT_EQ(ReadIntValue(row[0]), tag_id);

    EXPECT_EQ(row[1].is_null, tag_var_is_null);
    if (!tag_var_is_null) {
      EXPECT_EQ(row[1].size, tag_var.size());
      EXPECT_EQ(ReadVarString(row[1]), tag_var);
    }

    EXPECT_EQ(row[2].is_null, tag_char_is_null);
    if (!tag_char_is_null) {
      EXPECT_EQ(row[2].size, tag_char.size());
      EXPECT_EQ(ReadFixedString(row[2]), tag_char);
    }

    EXPECT_FALSE(row[3].is_null);
    EXPECT_EQ(ReadIntValue(row[3]), rel_id);

    EXPECT_EQ(row[4].is_null, rel_var_is_null);
    if (!rel_var_is_null) {
      EXPECT_EQ(row[4].size, rel_var.size());
      EXPECT_EQ(ReadVarString(row[4]), rel_var);
    }

    EXPECT_EQ(row[5].is_null, rel_char_is_null);
    if (!rel_char_is_null) {
      EXPECT_EQ(row[5].size, rel_char.size());
      EXPECT_EQ(ReadFixedString(row[5]), rel_char);
    }
  }

  TABLE* table_{nullptr};
  RelTagRowBatch batch_;
  kwdbContext_t context_{};
};

TEST_F(HashTagRowBatchTest, InitAndAccessorsAdjustRelativeIndexes) {
  auto* ptag_mem = static_cast<char*>(malloc(16));
  ASSERT_NE(ptag_mem, nullptr);
  std::memset(ptag_mem, 0, 16);
  std::memcpy(ptag_mem, "PTAG", 4);
  auto* ptag_batch = new TagBatch(16, ptag_mem, 1);
  ASSERT_EQ(ptag_batch->setNotNull(0), KStatus::SUCCESS);
  batch_.res_.push_back(0, ptag_batch);

  auto* var_mem = static_cast<char*>(malloc(32));
  ASSERT_NE(var_mem, nullptr);
  auto* tag_batch = new VarTagBatch(32, var_mem, 1);
  ASSERT_EQ(tag_batch->writeDataExcludeLen(0, const_cast<char*>("name"), 4), 0);
  batch_.res_.push_back(1, tag_batch);

  EXPECT_FALSE(batch_.IsNull(1, roachpb::KWDBKTSColumn::TYPE_PTAG));
  char* ptag_data =
      batch_.GetData(1, 0, roachpb::KWDBKTSColumn::TYPE_PTAG,
                     roachpb::DataType::CHAR);
  ASSERT_NE(ptag_data, nullptr);
  EXPECT_EQ(static_cast<unsigned char>(ptag_data[0]), 1U);

  EXPECT_FALSE(batch_.IsNull(2, roachpb::KWDBKTSColumn::TYPE_TAG));
  EXPECT_EQ(batch_.GetDataLen(2, 0, roachpb::KWDBKTSColumn::TYPE_TAG), 5);
  char* tag_data =
      batch_.GetData(2, 0, roachpb::KWDBKTSColumn::TYPE_TAG,
                     roachpb::DataType::VARCHAR);
  ASSERT_NE(tag_data, nullptr);
  EXPECT_EQ(std::string(tag_data + sizeof(k_uint16), 4), "name");

  ASSERT_EQ(tag_batch->setNull(0), KStatus::SUCCESS);
  EXPECT_TRUE(batch_.IsNull(2, roachpb::KWDBKTSColumn::TYPE_TAG));
  EXPECT_EQ(batch_.GetDataLen(2, 0, roachpb::KWDBKTSColumn::TYPE_TAG), 0);
}

TEST_F(RelTagRowBatchTest, PrimaryJoinAddsRowsAndEntityIndexes) {
  TagData row;
  void* bitmap = nullptr;
  EXPECT_EQ(batch_.GetTagData(&row, &bitmap, 0), KStatus::FAIL);

  auto rel_chunk = MakeJoinChunk({{500, "rel-a", "RC1", false, false, {7, 70, 1}}});
  auto tag_chunk = MakeJoinChunk({
      {101, "tag-a", "TC1", false, false, {1, 11, 2}},
      {202, "", "TC2", true, false, {1, 22, 2}},
  });

  ASSERT_EQ(batch_.AddPrimaryTagRelJoinRecord(&context_, rel_chunk, 0, tag_chunk),
            KStatus::SUCCESS);
  ASSERT_EQ(batch_.Count(), 2U);

  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 0), KStatus::SUCCESS);
  ExpectJoinedRow(row, 101, "tag-a", "TC1", 500, "rel-a", "RC1");
  EXPECT_TRUE(batch_.GetEntityIndex(0).equalsWithoutMem(EntityResultIndex(1, 11, 2)));

  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 1), KStatus::SUCCESS);
  ExpectJoinedRow(row, 202, "", "TC2", 500, "rel-a", "RC1", true);
  EXPECT_TRUE(batch_.GetEntityIndex(1).equalsWithoutMem(EntityResultIndex(1, 22, 2)));
}

TEST_F(RelTagRowBatchTest, TagRelJoinTraversesLinkedListAndUsesBuildSideEntities) {
  auto build_chunk = MakeJoinChunk({
      {1001, "build-a", "BA1", false, false, {9, 91, 1}},
      {1002, "", "BA2", true, true, {9, 92, 1}},
  });
  auto container = MakeBatchDataContainer(
      std::move(build_chunk), {{1, 2}, {0, 0}});
  auto rel_chunk = MakeJoinChunk({{700, "probe-r", "PR1", false, false, {3, 30, 1}}});

  RowIndice row_indice{1, 1};
  ASSERT_EQ(batch_.AddTagRelJoinRecord(&context_, container, rel_chunk, 0, row_indice),
            KStatus::SUCCESS);
  ASSERT_EQ(batch_.Count(), 2U);

  TagData row;
  void* bitmap = nullptr;
  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 0), KStatus::SUCCESS);
  ExpectJoinedRow(row, 1001, "build-a", "BA1", 700, "probe-r", "PR1");
  EXPECT_TRUE(batch_.GetEntityIndex(0).equalsWithoutMem(EntityResultIndex(9, 91, 1)));

  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 1), KStatus::SUCCESS);
  ExpectJoinedRow(row, 1002, "", "", 700, "probe-r", "PR1", true, false, true);
  EXPECT_TRUE(batch_.GetEntityIndex(1).equalsWithoutMem(EntityResultIndex(9, 92, 1)));
}

TEST_F(RelTagRowBatchTest, RelTagJoinTraversesLinkedListAndConsumesProbeEntity) {
  auto build_chunk = MakeJoinChunk({
      {3001, "rel-1", "R11", false, false, {4, 41, 1}},
      {3002, "", "", true, true, {4, 42, 1}},
  });
  auto container = MakeBatchDataContainer(
      std::move(build_chunk), {{1, 2}, {0, 0}});
  auto tag_chunk = MakeJoinChunk({{801, "tag-p", "TP1", false, false, {8, 81, 1}}});

  RowIndice row_indice{1, 1};
  ASSERT_EQ(batch_.AddRelTagJoinRecord(&context_, container, tag_chunk, 0, row_indice),
            KStatus::SUCCESS);
  EXPECT_EQ(row_indice.batch_no, 0);
  EXPECT_EQ(row_indice.offset_in_batch, 0);
  ASSERT_EQ(batch_.Count(), 2U);

  TagData row;
  void* bitmap = nullptr;
  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 0), KStatus::SUCCESS);
  ExpectJoinedRow(row, 801, "tag-p", "TP1", 3001, "rel-1", "R11");
  EXPECT_TRUE(batch_.GetEntityIndex(0).equalsWithoutMem(EntityResultIndex(8, 81, 1)));

  ASSERT_EQ(batch_.GetTagData(&row, &bitmap, 1), KStatus::SUCCESS);
  ExpectJoinedRow(row, 801, "tag-p", "TP1", 3002, "", "", false, true, false, true);
  EXPECT_TRUE(batch_.GetEntityIndex(1).equalsWithoutMem(EntityResultIndex(8, 81, 1)));
}

TEST_F(RelTagRowBatchTest, DestructorCleansAllocatedJoinBuffers) {
  {
    RelTagRowBatch local_batch;
    local_batch.Init(table_);
    auto rel_chunk =
        MakeJoinChunk({{901, "rel-x", "RX1", false, false, {5, 51, 1}}});
    auto tag_chunk =
        MakeJoinChunk({{902, "tag-x", "TX1", false, false, {6, 61, 1}}});
    ASSERT_EQ(local_batch.AddPrimaryTagRelJoinRecord(&context_, rel_chunk, 0,
                                                     tag_chunk),
              KStatus::SUCCESS);
    ASSERT_EQ(local_batch.Count(), 1U);
  }
}

}  // namespace
}  // namespace kwdbts
