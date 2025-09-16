// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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

#include <cm_kwdb_context.h>
#include <engine.h>
#include <settings.h>
#include <cm_trace_plugin.h>
#include <utility>
#include <memory>
#include <string>
#include <vector>
#include "../util.h"
#include "../worker.h"

namespace kwdbts {

void constructRoachpbTable(roachpb::CreateTsTable* meta, uint64_t table_id, const BenchParams& params,
                           uint64_t partition_interval = EngineOptions::iot_interval);

void genPayloadData(std::vector<TagInfo> tag_schema, std::vector<AttributeInfo> data_schema,
                    int32_t primary_tag, KTimestamp start_ts, int count, int time_inc, TSSlice *payload);

void FillPayloaderBuilderData(TSRowPayloadBuilder& pay_build, int32_t primary_tag, KTimestamp start_ts, int count, int time_inc);

void genRowBasedPayloadData(std::vector<TagInfo> tag_schema, std::vector<AttributeInfo> data_schema,
 TSTableID table_id, uint32_t version,
 int32_t primary_tag, KTimestamp start_ts, int count, int time_inc, TSSlice *payload);

bool checkColValue(const std::vector<AttributeInfo>& data_schema, const ResultSet& res, int ret_cnt, int batch_offset);

class StInstance {
  static StInstance* st_inst_;

 public:
  StInstance() = default;
  ~StInstance();

  static StInstance*& Get() {
    static std::mutex mutex;
    std::lock_guard<std::mutex> lk(mutex);
    if (st_inst_ == nullptr) {
      st_inst_ = new StInstance();
    }
    return st_inst_;
  }

  static void Stop() {
    delete st_inst_;
    st_inst_ = nullptr;
  }

  void ParseInputParams();

  void SetInputParams(const std::string& key, const std::string& value);

  KBStatus Init(BenchParams params, std::vector<uint32_t> table_ids_);

  kwdbts::kwdbContext_p GetContext() { return g_contet_p; }

  TSEngine* GetTSEngine() { return ts_engine_; }

  bool IsV2() {
    return params_.engine_version == "2";
  }

  vector<roachpb::CreateTsTable>& tableMetas() { return table_metas; };

  uint64_t rangeGroup();

  uint32_t GetSnapShotTableId() { return snapshot_desc_table_id; }

  KStatus GetSchemaInfo(kwdbContext_p ctx, uint32_t table_id, std::vector<TagInfo>* tag_schema,
           std::vector<AttributeInfo>* data_schema);

  DedupRule GetDedupRule() {
    return dedup_rule_;
  }

  void SetDedupRule(DedupRule dedup_rule) {
    dedup_rule_ = dedup_rule;
  }

 private:
  BenchParams params_;
  kwdbts::kwdbContext_t g_context;
  kwdbts::kwdbContext_p g_contet_p;;

  std::mutex mutex_;  // control the concurrency of engine initialization
  TSEngine* ts_engine_{nullptr};
  TSOptions ts_opts_;
  vector<roachpb::CreateTsTable> table_metas;
  DedupRule dedup_rule_ = DedupRule::OVERRIDE;
  uint32_t snapshot_desc_table_id{32};
};


}  // namespace kwdbts
