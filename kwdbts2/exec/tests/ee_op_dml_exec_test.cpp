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
// #if 0
#include "gtest/gtest.h"
#include "ee_dml_exec.h"
#include "ee_op_test_base.h"
#include "ee_op_spec_utils.h"

namespace kwdbts {

class TestDmlExec : public OperatorTestBase {
 public:
  TestDmlExec() : OperatorTestBase() {}

 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
  }

  void TearDown() override {
    OperatorTestBase::TearDown();
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
vector<vector<vector<vector<const char*>>>> relTableData = {
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

void GenerateRelData(vector<ColumnInfo>& data_type, vector<vector<const char*>>& batch_data, DataChunkPtr& chunk) {
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
#if 0
TEST_F(TestDmlExec, TestDmlExecSelectAndSort) {
  TSFlowSpec flow;
  SpecSelectWithSort select_spec(table_id_);
  select_spec.PrepareFlowSpec(flow);

  size_t size = flow.ByteSizeLong();

  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));
  auto message = make_unique<char[]>(size);
  flow.SerializeToArray(message.get(), size);

  auto* info = reinterpret_cast<QueryInfo*>(req.get());

  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = size;
  info->id = 2;
  info->unique_id = 34715;
  info->handle = nullptr;
  info->value = message.get();
  info->ret = 0;
  info->time_zone = 0;
  info->relBatchData = nullptr;
  info->relRowCount = 0;

  KStatus status = DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get()));
  ASSERT_EQ(status, KStatus::SUCCESS);

  auto* result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));
  ASSERT_EQ(result->ret, SUCCESS);

  // next
  info->tp = EnMqType::MQ_TYPE_DML_NEXT;

  do {
    ASSERT_EQ(DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get())), KStatus::SUCCESS);
    result = static_cast<QueryInfo*>(static_cast<void*>(resp.get()));

    if (result->value) {
      free(result->value);
      result->value = nullptr;
    }
  } while (result->code != -1);

  ASSERT_EQ(result->ret, SUCCESS);

  info->handle = result->handle;
  info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
  DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get()));
}
#endif

TEST_F(TestDmlExec, TestDmlExecInit) {
  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));

  auto* info = reinterpret_cast<QueryInfo*>(req.get());
  info->tp = EnMqType::MQ_TYPE_DML_INIT;
  KStatus status = DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get()));
  ASSERT_EQ(status, KStatus::FAIL);

  info->tp = EnMqType::MQ_TYPE_DML_CANCEL;
  status = DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get()));
  ASSERT_EQ(status, KStatus::FAIL);
  
  // info->tp = EnMqType::MQ_TYPE_DML_PUSH;
  // status = DmlExec::ExecQuery(ctx_, info, reinterpret_cast<QueryInfo*>(resp.get()));
  // ASSERT_EQ(status, KStatus::SUCCESS);
}

TEST_F(TestDmlExec, TestDmlExecPush) {
  TSFlowSpec flow;
  HashTagScanSpec hash_tag_scan_spec(table_id_, TSTableReadMode::hashTagScan);
  hash_tag_scan_spec.PrepareFlowSpec(flow);
  hash_tag_scan_spec.PrepareInputOutputSpec(flow);

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

  // get dml exec handle
  DmlExec *handle = reinterpret_cast<DmlExec*>(info2->handle);
  ASSERT_NE(handle, nullptr);

  // set brpc info
  flow.set_queryid(34716);
  flow.add_brpcaddrs("127.0.0.1:6000");
  flow.add_brpcaddrs("");
  handle->SetBrpcInfo(&flow);

  // PUSH
  DataChunkPtr rel_data_chunk = nullptr;
  GenerateRelData(relTableColumnType[0], relTableData[0][0], rel_data_chunk);
  info->tp = EnMqType::MQ_TYPE_DML_PUSH;
  info->relBatchData = rel_data_chunk.get();
  info->relRowCount = rel_data_chunk->Count();
  status = DmlExec::ExecQuery(ctx_, info, info2);
  ASSERT_EQ(status, KStatus::SUCCESS);
}

TEST_F(TestDmlExec, TestDmlExecSetupFail) {
  // 测试MQ_TYPE_DML_SETUP失败的情况，使得resp->ret == 0，调用DisposeError函数
  auto req = make_unique<char[]>(sizeof(QueryInfo));
  auto resp = make_unique<char[]>(sizeof(QueryInfo));
  
  // 使用无效的proto消息（随机字符串）
  std::string invalid_message = "invalid proto message";
  
  auto* info = reinterpret_cast<QueryInfo*>(req.get());
  auto* info2 = reinterpret_cast<QueryInfo*>(resp.get());
  info->tp = EnMqType::MQ_TYPE_DML_SETUP;
  info->len = invalid_message.size();
  info->id = 4;
  info->unique_id = 45678;
  info->handle = nullptr;
  info->value = const_cast<char*>(invalid_message.c_str());
  info->ret = 0;
  info->time_zone = 0;
  info->relBatchData = nullptr;
  info->relRowCount = 0;
  
  KStatus status = DmlExec::ExecQuery(ctx_, info, info2);
  
  // 验证resp->ret == 0，说明处理失败
  ASSERT_EQ(info2->ret, 0);
  
  // 验证错误码被设置
  ASSERT_NE(info2->code, 0);
  
  // 清理
  if (info->handle) {
    info->tp = EnMqType::MQ_TYPE_DML_CLOSE;
    DmlExec::ExecQuery(ctx_, info, info2);
  }
}

}  // namespace kwdbts
// #endif