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

#pragma once

#include <string>

#include "test_util.h"
#include "ts_engine.h"

namespace kwdbts {

class TsEngineTestBase : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    KWDBDynamicThreadPool::GetThreadPool().InitImplicitly();
  }

  static void TearDownTestCase() {
    auto& pool = KWDBDynamicThreadPool::GetThreadPool();
    if (!pool.IsStop()) {
      pool.Stop();
    }
    KWDBDynamicThreadPool::Destroy();
  }

 protected:
  TsEngineTestBase() = default;

  ~TsEngineTestBase() override {
    DestroyEngine();
  }

  void InitContext() { InitContextInternal(InitKWDBContext); }

  void InitEngine(const std::string& db_path, bool reset_db_path = true) {
    DestroyEngine();
    opts_.db_path = db_path;
    if (reset_db_path) {
      Remove(db_path);
      MakeDirectory(db_path);
    }
    engine_ = new TSEngineImpl(opts_);
    auto s = engine_->Init(ctx_);
    EXPECT_EQ(s, KStatus::SUCCESS);
  }

  std::string GetPrimaryKey(TSTableID table_id, TSEntityID dev_id) {
    std::shared_ptr<kwdbts::TsTableSchemaManager> schema_mgr;
    bool is_dropped = false;
    KStatus s = engine_->GetTableSchemaMgr(ctx_, table_id, is_dropped, schema_mgr);
    EXPECT_EQ(s, KStatus::SUCCESS);
    std::vector<TagInfo> tag_schema;
    s = schema_mgr->GetTagMeta(1, tag_schema);
    EXPECT_EQ(s, KStatus::SUCCESS);
    uint64_t pkey_len = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        pkey_len += tag_schema[i].m_size;
      }
    }
    char* mem = reinterpret_cast<char*>(malloc(pkey_len));
    memset(mem, 0, pkey_len);
    std::string dev_str = intToString(dev_id);
    size_t offset = 0;
    for (size_t i = 0; i < tag_schema.size(); i++) {
      if (tag_schema[i].isPrimaryTag()) {
        if (tag_schema[i].m_data_type == DATATYPE::VARSTRING) {
          memcpy(mem + offset, dev_str.data(), dev_str.length());
        } else {
          memcpy(mem + offset, reinterpret_cast<char*>(&dev_id), tag_schema[i].m_size);
        }
        offset += tag_schema[i].m_size;
      }
    }
    auto ret = std::string{mem, pkey_len};
    free(mem);
    return ret;
  }

  void DestroyEngine() {
    if (engine_ != nullptr) {
      delete engine_;
      engine_ = nullptr;
    }
  }

 private:
  void InitContextInternal(KStatus (*context_init)(kwdbContext_p)) {
    ctx_ = &g_ctx_;
    auto ctx_status = context_init(ctx_);
    EXPECT_EQ(ctx_status, KStatus::SUCCESS);
  }

 protected:
  EngineOptions opts_{};
  TSEngineImpl* engine_{nullptr};
  kwdbContext_t g_ctx_{};
  kwdbContext_p ctx_{&g_ctx_};
};

}  // namespace kwdbts



