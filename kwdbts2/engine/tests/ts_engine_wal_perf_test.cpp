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

#include <unistd.h>
#include "engine.h"
#include "test_util.h"
#include "st_wal_table.h"

using namespace kwdbts;  // NOLINT
extern DedupRule g_dedup_rule = kwdbts::DedupRule::KEEP;

std::string kDbPath = "./test_db";  // NOLINT

const string TestBigTableInstance::kw_home_ = kDbPath;  // NOLINT big table dir
const string TestBigTableInstance::db_name_ = "tsdb";  // NOLINT database name
const uint64_t TestBigTableInstance::iot_interval_ = 3600;

class TestEngineWALPerf : public TestBigTableInstance {
 public:
  kwdbContext_t context_;
  kwdbContext_p ctx_;
  EngineOptions opts_;
  TSEngine* ts_engine_;


  TestEngineWALPerf() {
    ctx_ = &context_;
    InitServerKWDBContext(ctx_);
    ts_engine_ = nullptr;
    setenv("KW_HOME", kDbPath.c_str(), 1);
    // clear data dir
    system(("rm -rf " + kDbPath + "/*").c_str());

  }

  ~TestEngineWALPerf() {
    // CLOSE engine
    KWDBDynamicThreadPool::GetThreadPool().Stop();
  }

  void ConstructTestColumnMetas(std::vector<ZTableColumnMeta>* metas) {
    // construct column metas
    metas->push_back({roachpb::DataType::TIMESTAMP, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::BIGINT, 8, 8, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  void ConstructTestTagMetas(std::vector<ZTableColumnMeta>* metas) {
    // construct tag metas
    for (int i = 0; i < 10; i++) {
      metas->push_back({roachpb::DataType::CHAR, 30, 30, roachpb::VariableLengthType::ColStorageTypeTuple});
    }
  }

  void ConstructTestRoachpbTable(roachpb::CreateTsTable* meta, const KString& prefix_table_name, KTableKey table_id,
                                 uint64_t partition_interval = kwdbts::EngineOptions::iot_interval) {
    // create table :  TIMESTAMP | FLOAT | INT | CHAR(char_len) | BOOL | BINARY(binary_len)
    roachpb::KWDBTsTable* table = KNEW roachpb::KWDBTsTable();
    table->set_ts_table_id(table_id);
    table->set_table_name(prefix_table_name + std::to_string(table_id));
    table->set_partition_interval(partition_interval);
    table->set_hash_num(g_testcase_hash_num);
    meta->set_allocated_ts_table(table);

    std::vector<ZTableColumnMeta> col_meta;
    ConstructTestColumnMetas(&col_meta);

    for (int i = 0; i < col_meta.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (col_meta[i].type));
      column->set_storage_len(col_meta[i].storage_len);
      column->set_column_id(i + 1);
      if (i == 0) {
        column->set_name("k_timestamp");  // first column name: k_timestamp
      } else {
        column->set_name("column" + std::to_string(i + 1));
      }
    }
    // add tag metas
    std::vector<ZTableColumnMeta> tag_metas;
    ConstructTestTagMetas(&tag_metas);
    for (int i = 0; i < tag_metas.size(); i++) {
      roachpb::KWDBKTSColumn* column = meta->mutable_k_column()->Add();
      column->set_storage_type((roachpb::DataType) (tag_metas[i].type));
      column->set_storage_len(tag_metas[i].storage_len);
      column->set_column_id(tag_metas.size() + 1 + i);
      if (i == 0) {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);
      } else {
        column->set_col_type(::roachpb::KWDBKTSColumn_ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
      }
      column->set_name("tag" + std::to_string(i + 1));
    }

  }

  const static int header_size_ = Payload::header_size_;  // NOLINT

  void GenTestPayloadTagData(Payload& payload, std::vector<AttributeInfo>& tag_schema,
                             KTimestamp start_ts, bool fix_primary_tag = true) {
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    if (fix_primary_tag) {
      start_ts = 100;
    }
    char* primary_start_ptr = payload.GetPrimaryTagAddr();
    char* tag_data_start_ptr = payload.GetTagAddr() + (tag_schema.size() + 7) / 8;
    for (int i = 0; i < tag_schema.size(); i++) {
      // generate primary tag
      if (tag_schema[i].isAttrType(COL_PRIMARY_TAG)) {
        switch (tag_schema[i].type) {
          case DATATYPE::TIMESTAMP64:
            KTimestamp(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT8:
            *(static_cast<k_int8*>(static_cast<void*>(primary_start_ptr))) = 10;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::INT32:
            KInt32(primary_start_ptr) = start_ts;
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::CHAR:
            memset(primary_start_ptr, '1', tag_schema[i].size);
            primary_start_ptr += tag_schema[i].size;
            break;
          case DATATYPE::VARSTRING: {
            strncpy(primary_start_ptr, test_str.c_str(), test_str.length());
            primary_start_ptr += tag_schema[i].size;
          }
            break;
          default:
            break;
        }
      }
      // generate normal tag
      switch (tag_schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          KTimestamp(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT8:
          *(static_cast<k_int8*>(static_cast<void*>(tag_data_start_ptr))) = 10;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::INT32:
          KInt32(tag_data_start_ptr) = start_ts;
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::CHAR:
          memset(tag_data_start_ptr, '1', tag_schema[i].size);
          tag_data_start_ptr += tag_schema[i].size;
          break;
        case DATATYPE::VARSTRING: {
          *reinterpret_cast<int16_t*>(tag_data_start_ptr) = ((int16_t) test_str.length());
          tag_data_start_ptr += sizeof(int16_t);
          strncpy(tag_data_start_ptr, test_str.c_str(), test_str.length());
          tag_data_start_ptr += test_str.length();
        }
          break;
        default:
          break;
      }
    }
  }

  char* GenTestPayloadData(kwdbContext_p ctx, k_uint32 count, k_uint32& payload_length, KTimestamp start_ts,
                           roachpb::CreateTsTable meta,
                           k_uint32 ms_interval = 10, int test_value = 0, bool fix_entity_id = true) {
    vector<AttributeInfo> schema;
    vector<AttributeInfo> tag_schema;
    vector<uint32_t> actual_cols;
    k_int32 tag_value_len = 0;
    payload_length = 0;
    string test_str = "abcdefghijklmnopqrstuvwxyz";
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i==0);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_value_len += col_var.size;
        tag_schema.emplace_back(std::move(col_var));
      } else {
        payload_length += col_var.size;
        if (col_var.type == DATATYPE::VARSTRING || col_var.type == DATATYPE::VARBINARY) {
          payload_length += (test_str.size() + 2);
        }
        actual_cols.push_back(schema.size());
        schema.push_back(std::move(col_var));
      }
    }

    k_uint32 header_len = header_size_;
    k_int32 primary_tag_len = 30;
    k_int16 primary_len_len = 2;
    k_int32 tag_len_len = 4;
    k_int32 data_len_len = 4;
    k_int32 bitmap_len = (count + 7) / 8;
    tag_value_len += (tag_schema.size() + 7) / 8;  // tag bitmap
    k_int32 data_len = payload_length * count + bitmap_len * schema.size();
    k_uint32 data_length =
        header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len + data_len_len + data_len;
    payload_length = data_length;
    char* value = new char[data_length];
    memset(value, 0, data_length);
    KInt32(value + Payload::row_num_offset_) = count;
    KUint32(value + Payload::ts_version_offset_) = 1;
    // set primary_len_len
    KInt16(value + header_size_) = primary_tag_len;
    // set tag_len_len
    KInt32(value + header_len + primary_len_len + primary_tag_len) = tag_value_len;
    // set data_len_len
    KInt32(value + header_len + primary_len_len + primary_tag_len + tag_len_len + tag_value_len) = data_len;
    Payload p(schema, actual_cols, {value, data_length});
    int16_t len = 0;
    GenPayloadTagData(p, tag_schema, start_ts, fix_entity_id);
    uint64_t var_exist_len = 0;
    for (int i = 0; i < schema.size(); i++) {
      switch (schema[i].type) {
        case DATATYPE::TIMESTAMP64:
          for (int j = 0; j < count; j++) {
            KTimestamp(p.GetColumnAddr(j, i)) = start_ts;
            start_ts += ms_interval;
          }
          break;
        case DATATYPE::INT16:
          for (int j = 0; j < count; j++) {
            KInt16(p.GetColumnAddr(j, i)) = 11;
          }
          break;
        case DATATYPE::INT32:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = 2222;
          }
          break;
        case DATATYPE::INT64:
          for (int j = 0; j < count; j++) {
            KInt32(p.GetColumnAddr(j, i)) = 33333;
          }
          break;
        case DATATYPE::CHAR:
          for (int j = 0; j < count; j++) {
            strncpy(p.GetColumnAddr(j, i), test_str.c_str(), schema[i].size);
          }
          break;
        case DATATYPE::VARSTRING:
        case DATATYPE::VARBINARY: {
          len = test_str.size();
          uint64_t var_type_offset = 0;
          for (int k = i; k < schema.size(); k++) {
            var_type_offset += (schema[k].size * count + bitmap_len);
          }
          for (int j = 0; j < count; j++) {
            KInt64(p.GetColumnAddr(j, i)) = var_type_offset + var_exist_len;
            // len + value
            memcpy(p.GetVarColumnAddr(j, i), &len, 2);
            strncpy(p.GetVarColumnAddr(j, i) + 2, test_str.c_str(), test_str.size());
            var_exist_len += (test_str.size() + 2);
          }
        }
          break;
        default:
          break;
      }
    }
    return value;
  }

  // header info
  long row_num_ = 1;
  long insert_num_ = 10;
//  int insert_time = 200;  // ms
  int run_num_ = 1;
  int table_num_ = 1;

  void RunTest() {
    // uint64_t total_duration = 0;
    // uint64_t total_insert_row = 0;
    std::vector<std::thread> threads;
    /*threads.emplace_back([=] {
      for (int k = 0; k < (insert_num_ * table_num_ / 100000); k++) {
        KStatus s = ts_engine_->CreateCheckpoint(ctx_);
        ASSERT_EQ(s, KStatus::SUCCESS);
        usleep(1000000 * 60);
      }
    });*/

    roachpb::CreateTsTable meta;
    KTableKey cur_table_id = 100;
    ConstructTestRoachpbTable(&meta, "perf", cur_table_id);
    RangeGroup test_range{1001, 0};
    std::vector<RangeGroup> ranges{test_range};

    KStatus s = ts_engine_->CreateTsTable(ctx_, cur_table_id, &meta, ranges);
    ASSERT_EQ(s, KStatus::SUCCESS);
    for (int k = 0; k < table_num_; k++) {
      threads.emplace_back([=] {

        kwdbContext_t ctx1;
        InitServerKWDBContext(&ctx1);
        KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::system_clock::now().time_since_epoch()).count();
        k_uint32 p_len = 0;
        char* data_value = GenTestPayloadData(&ctx1, row_num_, p_len, start_ts, meta);
        std::cout << "p_len: " << p_len << " bytes" << std::endl;
        TSSlice payload{data_value, p_len};

        auto write_start = std::chrono::system_clock::now();
        for (int i = 0 ; i < insert_num_ ; i++) {
          uint16_t inc_entity_cnt;
          uint32_t inc_unordered_cnt;
          DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
          ts_engine_->PutData(&ctx1, cur_table_id, test_range.range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
        }

        auto write_end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
        std::cout << "Duration for write is: " << duration.count() << " ms" << std::endl;
        // total_duration += duration.count();

        if (duration.count() > 0) {
          long wpr = insert_num_ * row_num_ * 1000 / duration.count();
          // total_insert_row += wpr;
          std::cout << "per second for write: " << wpr << " row" << std::endl;
        }

        delete[] data_value;
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }

    ts_engine_->DropTsTable(ctx_, cur_table_id);

    //std::cout << "Avg Duration for write is: " << total_duration / run_num_ << " ms" << std::endl;
    //std::cout << "Avg per second for write: " << total_insert_row / run_num_ << " row" << std::endl;
  }

  void PutData(KTableKey table_id, uint64_t range_group_id, roachpb::CreateTsTable meta) {
    kwdbContext_t ctx1;
    InitServerKWDBContext(&ctx1);
    KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
    k_uint32 p_len = 0;
    char* data_value = GenSomePayloadData(&ctx1, row_num_, p_len, start_ts, &meta);
    TSSlice payload{data_value, p_len};
    for (int k = 0; k < run_num_; k++) {
      for (int i = 0; i < insert_num_; i++) {
        uint16_t inc_entity_cnt;
        uint32_t inc_unordered_cnt;
        DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
        uint64_t mtr_id = 0;
        ts_engine_->TSMtrBegin(&ctx1, table_id, range_group_id, 1, 1, mtr_id);
        ts_engine_->PutData(&ctx1, table_id, range_group_id, &payload, 1, 0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
        // ts_engine_->TSMtrCommit(&ctx1, table_id, range_group_id, mtr_id);
        ts_engine_->TSMtrRollback(&ctx1, table_id, range_group_id, mtr_id);
      }
    }
    delete[] data_value;
  }

  vector<AttributeInfo> GetSchema(roachpb::CreateTsTable& meta) const {
    vector<AttributeInfo> schema;
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
        schema.push_back(std::move(col_var));
      }
    }

    return schema;
  }

  vector<uint32_t> GetActualCols(roachpb::CreateTsTable& meta) const {
    vector<uint32_t> actual_cols;
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx_, col, col_var, i == 0);
      if (!col_var.isAttrType(COL_GENERAL_TAG) && !col_var.isAttrType(COL_PRIMARY_TAG)) {
        actual_cols.push_back(actual_cols.size());
      }
    }
    return actual_cols;
  }

  void AddPutLog(KTableKey table_id, uint64_t range_group_id, roachpb::CreateTsTable meta) {
    TS_LSN mtr_id = 0;
    auto wal2 = new WALMgr(kDbPath + "/", table_id, range_group_id, &opts_);
    wal2->Init(ctx_);

    auto tsm = TSxMgr(wal2);

    for (int i = 0; i < insert_num_; i++) {
      mtr_id = 0;
      tsm.MtrBegin(ctx_, range_group_id, 1, mtr_id);
      KTimestamp start_ts = std::chrono::duration_cast<std::chrono::milliseconds>
          (std::chrono::system_clock::now().time_since_epoch()).count();
      k_uint32 p_len = 0;
      char* data_value = GenSomePayloadData(ctx_, row_num_, p_len, start_ts, &meta);
      TSSlice payload{data_value, p_len};
      auto pd = Payload(GetSchema(meta), GetActualCols(meta), payload);
      //if (i == 0)
      //  wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, payload);

      TS_LSN entry_lsn;
      wal2->WriteInsertWAL(ctx_, mtr_id, 0, 0, pd.GetPrimaryTag(), payload, entry_lsn);

      tsm.MtrCommit(ctx_, mtr_id);
      delete[] data_value;
    }

    wal2->Flush(ctx_);
    delete wal2;
  }
};

TEST_F(TestEngineWALPerf, insert0) {
  opts_.wal_level = 0;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  RunTest();

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWALPerf, insert1) {
  opts_.wal_level = 1;
  opts_.db_path = kDbPath;
  opts_.wal_file_size = 1024;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  RunTest();

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWALPerf, insert2) {
  opts_.wal_level = 2;
  opts_.wal_buffer_size = 10;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  RunTest();

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}

TEST_F(TestEngineWALPerf, recover) {
  opts_.wal_level = 2;
  opts_.db_path = kDbPath;
  KStatus s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);

  for (int i = 0; i < table_num_; i++) {
    KTableKey table_id = 1000 + i;
    uint64_t range_group_id = 10000 + i * 1000;
    roachpb::CreateTsTable meta;
    ConstructRoachpbTable(&meta, "testTable", table_id);
    // ConstructTestRoachpbTable(&meta, "testTable", table_id);
    std::vector<RangeGroup> ranges;
    ranges.push_back({range_group_id, 0});
    ranges.push_back({range_group_id + 1, 0});
    ranges.push_back({range_group_id + 2, 0});
    ts_engine_->CreateTsTable(ctx_, table_id, &meta, ranges);

    std::vector<std::thread> threads;
    auto write_start = std::chrono::system_clock::now();
    for (const auto range : ranges) {
      threads.emplace_back([=] {
        PutData(table_id, range.range_group_id, meta);
      });
    }

    for (auto& thread : threads) {
      thread.join();
    }
    auto write_end = std::chrono::system_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
    std::cout << "Duration for write is: " << duration.count() << " ms" << std::endl;
  }

  // close normally
  // s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  // EXPECT_EQ(s, KStatus::SUCCESS);

  // close abnormally
  delete ts_engine_;

  ts_engine_ = nullptr;

  // for (const auto range : ranges) {
  //   AddPutLog(table_id, range.range_group_id, meta);
  // }

  auto write_start = std::chrono::system_clock::now();
  s = TSEngineImpl::OpenTSEngine(ctx_, kDbPath, opts_, &ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  auto write_end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(write_end - write_start);
  std::cout << "Duration for recover is: " << duration.count() << " ms" << std::endl;

  s = TSEngineImpl::CloseTSEngine(ctx_, ts_engine_);
  EXPECT_EQ(s, KStatus::SUCCESS);
  ts_engine_ = nullptr;
}