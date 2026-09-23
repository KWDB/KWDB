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

#include "ee_dml_exec.h"
#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"
#include "gtest/gtest.h"

#define private public
#define protected public
#include "ee_noop_op.h"
#include "ee_noop_parser.h"
#undef protected
#undef private

#include "ee_field_const.h"
#include "ee_mempool.h"

namespace kwdbts {

namespace {

class OwnedColumnDataChunk : public DataChunk {
 public:
  OwnedColumnDataChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedColumnDataChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

std::unique_ptr<DataChunk> MakeSingleIntChunk(k_int32 value) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<OwnedColumnDataChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&value), sizeof(value));
  return chunk;
}

std::unique_ptr<DataChunk> MakeIntChunk(const std::vector<k_int32>& values) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk =
      std::make_unique<OwnedColumnDataChunk>(col_info, 1,
                                             static_cast<k_uint32>(values.size()));
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < values.size(); ++i) {
    chunk->AddCount();
    auto value = values[i];
    chunk->InsertData(static_cast<k_uint32>(i), 0, reinterpret_cast<char*>(&value),
                      sizeof(value));
  }
  return chunk;
}

class StubNoopChildOperator : public BaseOperator {
 public:
  StubNoopChildOperator()
      : BaseOperator(nullptr, nullptr, nullptr, 0) {
    renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
    renders_[0] = new FieldInt(0, roachpb::DataType::INT, 4);
    num_ = 1;
    output_fields_.push_back(renders_[0]);
  }

  ~StubNoopChildOperator() override {
    if (renders_ != nullptr) {
      if (renders_[0] != nullptr) {
        delete renders_[0];
        renders_[0] = nullptr;
      }
      free(renders_);
      renders_ = nullptr;
    }
    output_fields_.clear();
  }

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    ++init_calls_;
    return init_code_;
  }

  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    return start_code_;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr& chunk) override {
    ++next_calls_;
    if (!queued_chunks_.empty()) {
      chunk = std::move(queued_chunks_.front());
      queued_chunks_.pop_front();
      return EEIteratorErrCode::EE_OK;
    }
    chunk = nullptr;
    return next_code_;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override {
    ++reset_calls_;
    return reset_code_;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return close_code_;
  }

  BaseOperator* Clone() override {
    auto* cloned = new StubNoopChildOperator();
    cloned->next_code_ = next_code_;
    return cloned;
  }

  int init_calls_{0};
  int start_calls_{0};
  int next_calls_{0};
  int reset_calls_{0};
  int close_calls_{0};
  EEIteratorErrCode init_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode start_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode next_code_{EEIteratorErrCode::EE_END_OF_RECORD};
  EEIteratorErrCode reset_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode close_code_{EEIteratorErrCode::EE_OK};
  std::deque<DataChunkPtr> queued_chunks_;

 private:
};

class CloneableNoopChildOperator : public StubNoopChildOperator {
 public:
  explicit CloneableNoopChildOperator(std::vector<k_int32> values = {}) {
    if (!values.empty()) {
      queued_chunks_.push_back(MakeIntChunk(values));
      next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
    }
  }

  BaseOperator* Clone() override {
    auto* cloned = new CloneableNoopChildOperator();
    cloned->next_code_ = next_code_;
    for (auto* field : output_fields_) {
      cloned->output_fields_.clear();
      delete cloned->renders_[0];
      cloned->renders_[0] = field->field_to_copy();
      cloned->output_fields_.push_back(cloned->renders_[0]);
      break;
    }
    return cloned;
  }
};

class NonCloneableNoopChildOperator : public StubNoopChildOperator {
 public:
  BaseOperator* Clone() override { return nullptr; }
};

class MockRowBatch : public RowBatch {
 public:
  explicit MockRowBatch(k_uint32 col_num) : col_num_(col_num) {
    typ_ = RowBatchTypeScan;
  }

  void AddRow() {
    rows_.emplace_back(col_num_);
    count_ = rows_.size();
  }

  char* GetData(k_uint32 col, k_uint32, roachpb::KWDBKTSColumn::ColumnType,
                roachpb::DataType) override {
    return rows_[cursor_][col].empty() ? nullptr : rows_[cursor_][col].data();
  }

  bool IsNull(k_uint32, roachpb::KWDBKTSColumn::ColumnType) override {
    return false;
  }

  k_uint32 Count() override { return count_; }

  k_int32 NextLine() override {
    if (cursor_ + 1 < count_) {
      ++cursor_;
    }
    return cursor_;
  }

  void ResetLine() override { cursor_ = 0; }

  KStatus Sort(Field**, const std::vector<k_uint32>&,
               const std::vector<k_int32>&) override {
    return KStatus::SUCCESS;
  }

  void SetLimitOffset(k_uint32 limit, k_uint32 offset) override {
    limit_calls_++;
    last_limit_ = limit;
    last_offset_ = offset;
  }

  void AddSelection() override { selection_count_++; }

  void EndSelection() override { end_selection_count_++; }

  k_uint32 selection_count_{0};
  k_uint32 end_selection_count_{0};
  k_uint32 limit_calls_{0};
  k_uint32 last_limit_{0};
  k_uint32 last_offset_{0};

 private:
  k_uint32 col_num_;
  k_uint32 count_{0};
  k_uint32 cursor_{0};
  std::vector<std::vector<std::vector<char>>> rows_;
};

class NoopWhiteBoxTest : public ::testing::Test {
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
    InitServerKWDBContext(&ctx_);
    current_thd = KNEW KWThdContext();
  }

  void TearDown() override { SafeDeletePointer(current_thd); }

  kwdbContext_t ctx_{};
};

TEST_F(NoopWhiteBoxTest, NoopParserHandlesReferencesRenderSizeAndErrors) {
  PostProcessSpec post;
  post.add_output_columns(0);

  StubNoopChildOperator child;
  TsNoopParser parser(&post, nullptr);
  parser.input_ = &child;

  k_uint32 render_num = 0;
  parser.RenderSize(&ctx_, &render_num);
  EXPECT_EQ(render_num, 1U);

  auto ref = std::make_shared<VirtualField>(std::list<k_uint32>{1, 1});
  Field* chained = nullptr;
  EXPECT_EQ(parser.ParserReference(&ctx_, ref, &chained), EEIteratorErrCode::EE_OK);
  ASSERT_NE(chained, nullptr);
  EXPECT_EQ(chained, child.OutputFields()[0]);
  EXPECT_EQ(chained->next_, child.OutputFields()[0]);

  Field* render[1] = {nullptr};
  EXPECT_EQ(parser.HandleRender(&ctx_, render, 1), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(render[0], child.OutputFields()[0]);

  child.output_fields_[0] = nullptr;
  EXPECT_EQ(parser.HandleRender(&ctx_, render, 1), EEIteratorErrCode::EE_ERROR);
}

TEST_F(NoopWhiteBoxTest, NoopParserHandlesRenderExpressions) {
  PostProcessSpec post;
  post.add_render_exprs()->set_expr("@1");

  StubNoopChildOperator child;
  TsNoopParser parser(&post, nullptr);
  parser.input_ = &child;

  k_uint32 render_num = 0;
  parser.RenderSize(&ctx_, &render_num);
  EXPECT_EQ(render_num, 1U);

  Field* render[1] = {nullptr};
  EXPECT_EQ(parser.HandleRender(&ctx_, render, 1), EEIteratorErrCode::EE_OK);
  ASSERT_NE(render[0], nullptr);
  EXPECT_EQ(render[0]->get_storage_type(), roachpb::DataType::INT);
}

TEST_F(NoopWhiteBoxTest, NoopOperatorPassThroughLifecycleAndClone) {
  NoopCoreSpec spec;
  PostProcessSpec post;
  NoopOperator op(nullptr, &spec, &post, nullptr, 0);
  CloneableNoopChildOperator child({7});
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_TRUE(op.is_pass_through_);
  EXPECT_EQ(op.Start(&ctx_), EEIteratorErrCode::EE_OK);

  DataChunkPtr result;
  EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_OK);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 1U);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(result->GetData(0, 0)), 7);

  EXPECT_EQ(op.Reset(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.reset_calls_, 1);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.close_calls_, 1);

  BaseOperator* cloned = op.Clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->Type(), OperatorType::OPERATOR_NOOP);
  EXPECT_TRUE(cloned->HasChildren());
  delete cloned;

  NoopOperator no_clone(nullptr, &spec, &post, nullptr, 0);
  NonCloneableNoopChildOperator nonclone_child;
  no_clone.AddDependency(&nonclone_child);
  EXPECT_EQ(no_clone.Clone(), nullptr);
}

TEST_F(NoopWhiteBoxTest, NoopOperatorAppliesFilterOffsetLimitAndHelperMethods) {
  NoopCoreSpec spec;
  PostProcessSpec post;
  post.add_output_columns(0);
  post.set_limit(1);
  post.set_offset(1);

  NoopOperator op(nullptr, &spec, &post, nullptr, 0);
  StubNoopChildOperator child;
  child.queued_chunks_.push_back(MakeIntChunk({10, 20, 30}));
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_FALSE(op.is_pass_through_);
  SafeDeletePointer(op.filter_);
  op.filter_ = new FieldConstInt(roachpb::DataType::INT, 1, sizeof(k_int64));

  DataChunkPtr result;
  op.make_noop_data_chunk(&ctx_, &result, 2);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Capacity(), 2U);

  EXPECT_EQ(op.GetRender(0)->get_storage_type(), roachpb::DataType::INT);
  EXPECT_EQ(op.GetRender(99), nullptr);

  auto row_batch = std::make_shared<MockRowBatch>(1);
  row_batch->AddRow();
  row_batch->AddRow();
  EXPECT_EQ(op.ResolveFilter(&ctx_, row_batch), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(row_batch->selection_count_, 2U);
  EXPECT_EQ(row_batch->end_selection_count_, 1U);

  op.limit_ = 3;
  op.offset_ = 2;
  op.ResolveLimitOffset(&ctx_, row_batch);
  EXPECT_EQ(row_batch->limit_calls_, 1U);
  EXPECT_EQ(row_batch->last_limit_, 3U);
  EXPECT_EQ(row_batch->last_offset_, 2U);

  op.limit_ = 0;
  op.offset_ = 0;
  row_batch->SetCount(0);
  op.ResolveLimitOffset(&ctx_, row_batch);
  EXPECT_EQ(row_batch->limit_calls_, 2U);
  SafeDeletePointer(op.filter_);
}

TEST_F(NoopWhiteBoxTest, PassThroughNoopOperatorForwardsChildAndLimitState) {
  NoopCoreSpec spec;
  PostProcessSpec post;
  post.set_limit(1);
  PassThroughNoopOperaotr op(nullptr, &spec, &post, nullptr, 0);
  CloneableNoopChildOperator child({33});
  op.AddDependency(&child);

  EXPECT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.Start(&ctx_), EEIteratorErrCode::EE_OK);

  DataChunkPtr result;
  EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_OK);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 1U);

  op.examined_rows_ = 1;
  EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(op.Reset(&ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.Close(&ctx_), EEIteratorErrCode::EE_OK);
}

TEST_F(NoopWhiteBoxTest, NoopOperatorHandlesEarlyLimitAndChildFailures) {
  NoopCoreSpec spec;
  PostProcessSpec post;
  post.add_output_columns(0);

  {
    NoopOperator op(nullptr, &spec, &post, nullptr, 0);
    StubNoopChildOperator child;
    child.init_code_ = EEIteratorErrCode::EE_ERROR;
    op.AddDependency(&child);
    EXPECT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_ERROR);
  }

  {
    NoopOperator op(nullptr, &spec, &post, nullptr, 0);
    StubNoopChildOperator child;
    child.queued_chunks_.push_back(MakeSingleIntChunk(9));
    op.AddDependency(&child);
    ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
    op.limit_ = 1;
    op.examined_rows_ = 1;

    DataChunkPtr result;
    EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
    EXPECT_EQ(result, nullptr);
  }

  {
    NoopOperator op(nullptr, &spec, &post, nullptr, 0);
    StubNoopChildOperator child;
    child.next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
    op.AddDependency(&child);
    ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
    op.is_pass_through_ = false;
    op.filter_ = nullptr;

    DataChunkPtr result;
    EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
    EXPECT_EQ(result, nullptr);
  }

  {
    NoopCoreSpec pass_spec;
    PostProcessSpec pass_post;
    PassThroughNoopOperaotr op(nullptr, &pass_spec, &pass_post, nullptr, 0);
    StubNoopChildOperator child;
    child.next_code_ = EEIteratorErrCode::EE_END_OF_RECORD;
    op.AddDependency(&child);

    ASSERT_EQ(op.Init(&ctx_), EEIteratorErrCode::EE_OK);
    DataChunkPtr result;
    EXPECT_EQ(op.Next(&ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
    EXPECT_EQ(result, nullptr);
  }
}

}  // namespace

class TestNoopOperator : public OperatorTestBase {
 public:
  TestNoopOperator() : OperatorTestBase() {}
  virtual void SetUp() {
    OperatorTestBase::SetUp();
  }

  virtual void TearDown() {
    OperatorTestBase::TearDown();
  }
};

TEST_F(TestNoopOperator, NoopIterTest) {
  TSFlowSpec flow;
  TestNoopSpec noop_spec(table_id_);
  noop_spec.PrepareFlowSpec(flow);
  noop_spec.PrepareInputOutputSpec(flow);

  size_t size = flow.ByteSizeLong();

  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));
  auto message = make_unique<char[]>(size);
  flow.SerializeToArray(message.get(), size);

  auto* info = reinterpret_cast<QueryInfo*>(req.get());
  auto* info2 = reinterpret_cast<QueryInfo*>(resp.get());
  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = size;
  info->id = 3;
  info->unique_id = 34716;
  info->handle = nullptr;
  info->value = message.get();
  info->ret = 0;
  info->time_zone = 0;
  info->relBatchData = nullptr;
  info->relRowCount = 0;

  KStatus status = DmlExec::ExecQuery(ctx_, info, info2);
  ASSERT_EQ(status, KStatus::SUCCESS);

  auto* result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));
  ASSERT_EQ(result->ret, SUCCESS);

  // next
  info->tp = EnMqType::MQ_TYPE_DML_NEXT;

  do {
    ASSERT_EQ(DmlExec::ExecQuery(ctx_, info, info2), KStatus::SUCCESS);
    result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));

    if (result->value) {
      free(result->value);
      result->value = nullptr;
    }
  } while (result->code != -1);

  ASSERT_EQ(result->ret, SUCCESS);

  info->handle = result->handle;
  info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
  DmlExec::ExecQuery(ctx_, info, info2);
}

}  // namespace kwdbts
