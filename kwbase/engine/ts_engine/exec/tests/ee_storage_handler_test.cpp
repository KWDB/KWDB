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

#include "ee_storage_handler.h"

#include <memory>
#include <vector>

#include "ee_field.h"
#include "ee_kwthd_context.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"

#include "ts_engine.h"
#include "../../ts_engine/tests/test_util.h"
#include "../../ts_engine/tests/ts_test_base.h"
#include "ee_internal_type.h"
#include "ee_scan_row_batch.h"
#include "ee_field_compare.h"
#include "ee_op_engine_utils.h"

namespace kwdbts {
const string engine_root_path = "./tsdb";

class TestStorageHandler : public TsEngineTestBase {
 public:
  TestStorageHandler() {
    InitContext();
    InitEngine(engine_root_path);
  }
};

// Test constructor and destructor
TEST_F(TestStorageHandler, TestGetData) {
  TSTableID table_id = 999;
  roachpb::CreateTsTable pb_meta;
  ConstructRoachpbTable(&pb_meta, table_id, 1,
                        {roachpb::DataType::TIMESTAMP, roachpb::DataType::INT,
                         roachpb::DataType::DOUBLE},
                        {roachpb::DataType::TIMESTAMP, roachpb::DataType::VARCHAR,
                         roachpb::DataType::INT, roachpb::DataType::TIMESTAMP},
                        true);
  CreateTableAndPrepareData(ctx_, table_id, pb_meta, engine_);

  // convert pb_meta to tagspec.colmetas
  TSTagReaderSpec spec;
  for (int i = 0; i < pb_meta.k_column_size(); i++) {
    auto& col = pb_meta.k_column(i);
    auto ts_col = spec.add_colmetas();
    ts_col->set_storage_type(col.storage_type());
    ts_col->set_storage_len(col.storage_len());
    ts_col->set_column_type(col.col_type());
    ts_col->set_nullable(col.nullable());
  }
  spec.set_accessmode(TSTableReadMode::tableTableMeta);
  spec.set_tableid(table_id);
  spec.set_tableversion(pb_meta.ts_table().ts_version());
  spec.set_only_tag(false);
  // init tag post
  PostProcessSpec post;
  for (int i = 0; i < pb_meta.k_column_size(); i++) {
    auto& col = pb_meta.k_column(i);
    if (col.col_type() != roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA) {
      post.add_output_columns(i);
      if (col.storage_type() == roachpb::DataType::TIMESTAMP) {
        post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 10));
      } else {
        post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));
      }
    }
  }
  // add tag filter
  Expression *expr = new Expression();
  expr->set_expr("@3>=(0:::INT8)");
  post.set_allocated_filter(expr);
  TABLE table(1, table_id);
  KStatus s = table.Init(ctx_, &spec);
  ASSERT_EQ(s, KStatus::SUCCESS);
  TsFetcherCollection collection;
  TagScanOperator tag_op(&collection, &spec, &post, &table, 1);
  EEIteratorErrCode ecode = tag_op.Init(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);

  ctx_->ts_engine = engine_;
  StorageHandler handle(&table);
  ecode = handle.Init(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);

  handle.SetTagScan(&tag_op);
  ecode = tag_op.Start(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);

  std::vector<KwTsSpan> ts_kwspans;
  KwTsSpan ts_span;
  ts_span.begin = kInt64Min;
  ts_span.end = kInt64Max;
  ts_kwspans.push_back(ts_span);
  handle.SetSpans(&ts_kwspans);
  table.scan_cols_ = {0,1,2};
  table.table_version_ = 1;
  table.hash_spans_ = {{kUint64Min, kUint64Max}};

  TsScanStats stats;
  ScanRowBatch *row_batch = KNEW ScanRowBatch(&table);
  KWThdContext thd;
  current_thd = &thd;
  thd.SetRowBatch(row_batch);
  // init filter
  FieldConst* field_const = new FieldConstInt(roachpb::DataType::BIGINT, 3600, 8);
  Field* filter = new FieldFuncGt(table.fields_[0], field_const);
  // test TsNext and Filter
  k_uint32 cur_offset = 1;
  k_int32 limit = 1000;
  k_uint32 total_read_row = 0, examined_rows = 0;
  ecode = handle.TsNextAndFilter(ctx_, filter, &cur_offset, limit, row_batch, &total_read_row, &examined_rows, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  row_batch->ResetLine();
  ASSERT_EQ(row_batch->Count(), 8);
  for (auto i = 0; i < row_batch->Count(); i++) {
    bool is_null = row_batch->IsNull(0, roachpb::KWDBKTSColumn::TYPE_DATA);
    if (!is_null) {
      auto* ptr = row_batch->GetData(0, 0, roachpb::KWDBKTSColumn::TYPE_DATA, roachpb::DataType::TIMESTAMP);
      ASSERT_EQ(ptr != 0, true);
      auto len = row_batch->GetDataLen(0, 0, roachpb::KWDBKTSColumn::TYPE_DATA);
      ASSERT_EQ(len == 0, true);
      bool overflow = row_batch->IsOverflow(0, roachpb::KWDBKTSColumn::TYPE_DATA);
      ASSERT_EQ(overflow, false);
    }
    is_null = row_batch->IsNull(3, roachpb::KWDBKTSColumn::TYPE_TAG);
    if (!is_null) {
      auto* ptr = row_batch->GetData(3, 0, roachpb::KWDBKTSColumn::TYPE_TAG, roachpb::DataType::TIMESTAMP);
      ASSERT_EQ(ptr != 0, true);
      auto len = row_batch->GetDataLen(3, 0, roachpb::KWDBKTSColumn::TYPE_TAG);
      ASSERT_EQ(len == 0, true);
      bool overflow = row_batch->IsOverflow(3, roachpb::KWDBKTSColumn::TYPE_TAG);
      ASSERT_EQ(overflow, false);
    }
    row_batch->NextLine();
  }
  row_batch->SetCurrentLine(5);
  row_batch->Reset();
  ecode = handle.TsNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ASSERT_EQ(row_batch->Count(), 10);
  ecode = handle.TsNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_END_OF_RECORD);
  handle.Close();
  tag_op.Reset(ctx_);
  // test TsOffsetNext
  ecode = tag_op.Init(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ecode = tag_op.Start(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  table.ordered_scan_ = true;
  table.offset_ = 1;
  table.limit_ = 1000;
  ecode = handle.TsOffsetNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ecode = handle.TsOffsetNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ASSERT_EQ(handle.isDisorderedMetrics(), false);
  ecode = handle.TsOffsetNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_END_OF_RECORD);
  handle.Close();
  tag_op.Reset(ctx_);

  // test TsStatisticCacheNext
  ecode = tag_op.Init(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ecode = tag_op.Start(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  table.scan_real_agg_types_ = {kwdbts::LAST_ROW, kwdbts::LAST_ROW, kwdbts::LAST_ROW};
  ecode = handle.TsStatisticCacheNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ecode = handle.TsStatisticCacheNext(ctx_, &stats);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_END_OF_RECORD);
  handle.Close();
  tag_op.Reset(ctx_);

  ecode = tag_op.Init(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ecode = tag_op.Start(ctx_);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  DataChunkPtr chunk;
  ecode = tag_op.Next(ctx_, chunk);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_OK);
  ASSERT_EQ(chunk->Count(), 2);
  ecode = tag_op.Next(ctx_, chunk);
  ASSERT_EQ(ecode, EEIteratorErrCode::EE_END_OF_RECORD);
  tag_op.Reset(ctx_);
  tag_op.Close(ctx_);

  SafeDeletePointer(row_batch);
  SafeDeletePointer(field_const);
  SafeDeletePointer(filter);

}


}  // namespace kwdbts
