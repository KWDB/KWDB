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
#if false
#include "gtest/gtest.h"
#include "ee_dml_exec.h"
#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"
#include "ee_data_chunk.h"

namespace kwdbts {

// time-series data for test cases
vector<vector<vector<string>>> tsTableData = {
  // case 0
  {
    {"001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011",
        "01234567890123456789012345678", "01334567890123456789012345678", "014", "015", "016", "017", "018", "019", "020", "021"},
    {"101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111",
        "11234567890123456789012345678", "11334567890123456789012345678", "114", "115", "116", "117", "118", "119", "120", "121"}
  },
  // case 1
  {
    {"001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011",
        "01234567890123456789012345678", "01334567890123456789012345678", "014", "015", "016", "017", "018", "019", "020", "021"},
    {"101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111",
        "11234567890123456789012345678", "11334567890123456789012345678", "114", "115", "116", "117", "118", "119", "120", "121"},
    {"201", "202", "203", "204", "205", "206", "207", "208", "209", "210", "211",
        "21234567890123456789012345678", "21334567890123456789012345678", "214", "215", "216", "217", "218", "219", "220", "221"},
    {"301", "302", "303", "304", "305", "306", "307", "308", "309", "310", "311",
        "31234567890123456789012345678", "31334567890123456789012345678", "314", "315", "316", "317", "318", "319", "320", "321"}
  },
  // case 2
  {
    {"001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011",
        "01234567890123456789012345678", "01334567890123456789012345678", "014", "015", "016", "017", "018", "019", "020", "021"},
    {"101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111",
        "11234567890123456789012345678", "11334567890123456789012345678", "114", "115", "116", "117", "118", "119", "120", "121"}
  },
  // case 3
  {
    {"001", "002", "003", "004", "005", "006", "007", "008", "009", "010", "011",
        "01234567890123456789012345678", "01334567890123456789012345678", "014", "015", "016", "017", "018", "019", "020", "021"},
    {"101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111",
        "11234567890123456789012345678", "11334567890123456789012345678", "114", "115", "116", "117", "118", "119", "120", "121"}
  }
};

// relational data type for test cases
vector<vector<ColumnInfo>> relTableColumnType = {
  // case 0
  {
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 1
  {
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 2
  {
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 3
  {
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  }
};

// relational data for test cases
vector<vector<vector<vector<char*>>>> relTableData = {
  // case 0
  {
    // batch 0
    {
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_1"}
    }
  },
  // case 1
  {
    // batch 0
    {
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_1"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_2"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_3"}
    }
  },
  // case 2
  {
    // batch 0
    {
      {nullptr, "11334567890123456789012345678", "host_1"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_2"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_3"}
    }
  },
  // case 3
  {
    // batch 0
    {
      {nullptr, "col_01", "col_02"},
      {"col_10", "col_11", "col_12"},
      {"col_20", "col_20", "col_20"}
    },
    // batch 1
    {
      {nullptr, "11334567890123456789012345678", "host_1"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_2"},
      {"11234567890123456789012345678", "11334567890123456789012345678", "host_3"}
    }
  }
};

// query result data type for test cases
vector<vector<ColumnInfo>> resultType = {
  // case 0
  {
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 1
  {
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 2
  {
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  },
  // case 3
  {
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily},
    {30, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily}
  }
};

// query result data for test cases
vector<vector<vector<char*>>> resultData = {
  // case 0
  {
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"}
  },
  // case 1
  {
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"},
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"},
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"}
  },
  // case 2
  {
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"},
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"}
  },
  // case 3
  {
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"},
    {"102", "103", "104", "105", "11234567890123456789012345678", "11334567890123456789012345678", "114"}
  }
};

// generate relational data for multiple model processing
void GenerateRelData(vector<ColumnInfo>& data_type, vector<vector<char*>>& batch_data, DataChunkPtr& chunk) {
  if (batch_data.size() > 0) {
    ASSERT_EQ(batch_data[0].size(), data_type.size());
  }
  k_uint32 capacity = batch_data.size();
  k_int32 col_num = data_type.size();
  ColumnInfo *col_info = new ColumnInfo[col_num];
  for (int i = 0; i < data_type.size(); i++) {
    col_info[i] = data_type[i];
  }
  chunk = std::make_unique<kwdbts::DataChunk>(col_info, col_num, capacity);
  chunk->Initialize();

  for (int i = 0; i < capacity; i++) {
    chunk->AddCount();
    for (int j = 0; j < batch_data[i].size(); ++j) {
      if (batch_data[i][j] == nullptr) {
        chunk->SetNull(i, j);
        continue;
      }
      switch (data_type[j].storage_type) {
        case roachpb::DataType::BIGINT: {
              k_int64 converted_value = std::stoll(batch_data[i][j]);
              chunk->InsertData(i, j, reinterpret_cast<char*>(&converted_value), sizeof(k_int64));
              break;
            }
        case roachpb::DataType::CHAR: {
              chunk->InsertData(i, j, const_cast<char*>(batch_data[i][j]), strlen(batch_data[i][j]));
              break;
            }
        default:
              // data type not supported yet
              break;
      }
    }
  }
  return;
}

// TestHashTagScanOp for multiple model processing
class TestHashTagScanOp : public OperatorTestBase {
 public:
  TestHashTagScanOp() : OperatorTestBase() {

  }

 protected:
  roachpb::CreateTsTable meta_;
  ColumnInfo col_info_[3];
  void SetUp() override {
    // OperatorTestBase::SetUp();
    ExecPool::GetInstance().Init(ctx_);
    EngineOptions::is_single_node_ = true;
    test_range.range_group_id = default_entitygroup_id_in_dist_v2;
    TSBSSchema::constructTableMetadata(meta_, "test_table", table_id_);
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
  }

  void CreateTable() {
    ASSERT_EQ(engine_->CreateTsTable(ctx_, table_id_, &meta_, {test_range}), KStatus::SUCCESS);
  }

  void InsertRecords(k_uint32 case_num) {
    for (int i = 0; i < tsTableData[case_num].size(); ++i) {
      k_uint32 p_len = 0;
      KTimestamp start_ts = 0;
      auto data_value = TSBSSchema::genPayloadData(ctx_, row_num_per_payload, p_len, start_ts, meta_, tsTableData[case_num][i]);
      TSSlice payload{data_value.get(), p_len};
      DedupResult dedup_result{0, 0, 0, TSSlice{nullptr, 0}};
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      engine_->PutData(ctx_, table_id_, test_range.range_group_id, &payload, 1,
                       0, &inc_entity_cnt, &inc_unordered_cnt, &dedup_result);
    }
  }

  void DeleteTable() {
    engine_->DropTsTable(ctx_, table_id_);
  }

  void SetupHashTagScan(QueryInfo* request, QueryInfo* response) {
    request->tp = EnMqType::MQ_TYPE_DML_SETUP;
    KStatus status = DmlExec::ExecQuery(ctx_, request, response);
    ASSERT_EQ(status, KStatus::SUCCESS);
    ASSERT_EQ(response->ret, SUCCESS);
  }

  void PushRelData(void* rel_data, k_uint32 row_count, QueryInfo* request, QueryInfo* response) {
    request->tp = EnMqType::MQ_TYPE_DML_PUSH;
    request->relBatchData = rel_data;
    request->relRowCount = row_count;

    ASSERT_EQ(DmlExec::ExecQuery(ctx_, request, response), KStatus::SUCCESS);
    ASSERT_EQ(response->ret, SUCCESS);
    ASSERT_EQ(response->code, 1);
  }

  void NextHashTagScan(QueryInfo* request, QueryInfo* response) {
    // Next to fetch results
    request->tp = EnMqType::MQ_TYPE_DML_NEXT;
    request->relBatchData = nullptr;
    request->relRowCount = 0;
    ASSERT_EQ(DmlExec::ExecQuery(ctx_, request, response), KStatus::SUCCESS);
    ASSERT_EQ(response->ret, SUCCESS);
  }

  void CloseHashTagScan(QueryInfo* request, QueryInfo* response) {
    request->tp = EnMqType::MQ_TYPE_DML_CLOSE;
    DmlExec::ExecQuery(ctx_, request, response);
    ASSERT_EQ(response->ret, SUCCESS);
  }

  void RunTestCase(k_uint32 case_num, TSTableReadMode access_mode) {
    CreateTable();
    InsertRecords(case_num);

    TSFlowSpec flow;
    HashTagScanSpec hash_tag_scan_spec(table_id_, access_mode);
    hash_tag_scan_spec.PrepareFlowSpec(flow);
    hash_tag_scan_spec.PrepareInputOutputSpec(flow);

    size_t size = flow.ByteSizeLong();

    auto req = make_unique<char[]>(sizeof(QueryInfo));
    auto resp = make_unique<char[]>(sizeof(QueryInfo));
    auto message = make_unique<char[]>(size);
    flow.SerializeToArray(message.get(), size);

    auto* request = reinterpret_cast<QueryInfo*>(req.get());
    auto* response = reinterpret_cast<QueryInfo*>(resp.get());
    request->tp = EnMqType::MQ_TYPE_DML_SETUP;
    request->len = size;
    request->id = 3;
    request->unique_id = 34716;
    request->handle = nullptr;
    request->value = message.get();
    request->ret = 0;
    request->time_zone = 0;
    request->relBatchData = nullptr;
    request->relRowCount = 0;

    SetupHashTagScan(request, response);

    // Generate rel data and push it down
    DataChunkPtr rel_data_chunk = nullptr;
    for (int i = 0; i < relTableData[case_num].size(); ++i) {
      GenerateRelData(relTableColumnType[i], relTableData[case_num][i], rel_data_chunk);
      PushRelData(rel_data_chunk->GetData(), rel_data_chunk->Count(), request, response);
      delete[](rel_data_chunk->GetColumnInfo());
    }

    // Complete rel data push down
    PushRelData(nullptr, 0, request, response);

    NextHashTagScan(request, response);
    ASSERT_NE(response->value, nullptr);
    ASSERT_EQ(response->code, 1);

    DataChunkPtr resultChunk = nullptr;
    GenerateRelData(resultType[case_num], resultData[case_num], resultChunk);
    kwdbts::EE_StringInfo tmp_info = nullptr;
    tmp_info = kwdbts::ee_makeStringInfo();
    for (k_uint32 row = 0; row < resultChunk->Count(); ++row) {
      for (k_uint32 col = 0; col < resultChunk->ColumnNum(); ++col) {
        resultChunk->EncodingValue(ctx_, row, col, tmp_info);
      }
    }
    char* serialized_data = static_cast<char*>(response->value);
    ASSERT_EQ(response->len, tmp_info->len);
    ASSERT_TRUE(std::memcmp(serialized_data, tmp_info->data, tmp_info->len) == 0) << "The data buffers do not match!";
    free(tmp_info->data);
    delete tmp_info;

    free(response->value);
    response->value = nullptr;

    NextHashTagScan(request, response);
    ASSERT_EQ(response->value, nullptr);
    ASSERT_EQ(response->code, -1);
    CloseHashTagScan(request, response);
    delete[](resultChunk->GetColumnInfo());
  }
};

// HashTagScan op test cases for multiple model processing
TEST_F(TestHashTagScanOp, TestDmlExecHashTagScan) {
  RunTestCase(0, TSTableReadMode::primaryHashTagScan);

  DeleteTable();

  RunTestCase(0, TSTableReadMode::hashTagScan);

  DeleteTable();

  RunTestCase(0, TSTableReadMode::hashRelScan);

  DeleteTable();

  RunTestCase(1, TSTableReadMode::primaryHashTagScan);

  DeleteTable();

  RunTestCase(1, TSTableReadMode::hashRelScan);

  DeleteTable();

  RunTestCase(1, TSTableReadMode::hashTagScan);

  DeleteTable();

  RunTestCase(2, TSTableReadMode::primaryHashTagScan);

  DeleteTable();

  RunTestCase(2, TSTableReadMode::hashRelScan);

  DeleteTable();

  RunTestCase(2, TSTableReadMode::hashTagScan);

  DeleteTable();

  RunTestCase(3, TSTableReadMode::primaryHashTagScan);

  DeleteTable();

  RunTestCase(3, TSTableReadMode::hashRelScan);

  DeleteTable();

  RunTestCase(3, TSTableReadMode::hashTagScan);

  DeleteTable();
}

}  // namespace kwdbts
#endif