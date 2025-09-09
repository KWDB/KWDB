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
/*
 * @author jiadx
 * @date 2022/10/31
 * @version 1.0
 */
#pragma once

#include <cm_kwdb_context.h>
#include <settings.h>
#include <cm_trace_plugin.h>
#include <utility>
#include "../util.h"
#include "../worker.h"
#include "st_instance.h"

namespace kwdbts {
void RegisterEmbedWorker(const BenchParams& params);

using namespace kwdbts;

class StWorker : public Worker {
 public:
  StWorker(const std::string& pname, const BenchParams& params,
           const std::vector<uint32_t>& tbl_ids) : Worker(pname, params, tbl_ids) {
    st_inst_ = StInstance::Get();
    ctx = &context;
    kwdbts::InitKWDBContext(ctx);
  }

  // complete metadata initialization and other tasks
  virtual KBStatus Init() override {
    KBStatus ks = st_inst_->Init(params_, table_ids_);
    if (ks.isNotOK()) {
      return ks;
    }

    vector<AttributeInfo> schema;
    vector<AttributeInfo> tag_schema;

    roachpb::CreateTsTable& meta = st_inst_->tableMetas()[0];  // all tables have the same schema
    for (int i = 0; i < meta.k_column_size(); i++) {
      const auto& col = meta.k_column(i);
      struct AttributeInfo col_var;
      TsEntityGroup::GetColAttributeInfo(ctx, col, col_var, i==0);
      if (col_var.isAttrType(COL_GENERAL_TAG) || col_var.isAttrType(COL_PRIMARY_TAG)) {
        tag_schema.emplace_back(std::move(col_var));
      } else {
        schema.push_back(std::move(col_var));
      }
    }
    schema_ = std::move(schema);
    tag_schema_ = std::move(tag_schema);
    return KBStatus::OK();
  }

  bool IsTableCreated(uint32_t tbl_id, int table_i);


 protected:
  kwdbts::kwdbContext_t context;
  kwdbts::kwdbContext_p ctx;
  StInstance* st_inst_;

  // cache schema information
  vector<AttributeInfo> schema_;
  vector<AttributeInfo> tag_schema_;

  void getTableSchema(vector<AttributeInfo>*& schema, vector<AttributeInfo>*& tag_schema) {
    schema = &schema_;
    tag_schema = &tag_schema_;
  }
};

class StWriteWorker : public StWorker {
 public:
  StWriteWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      StWorker(pname, params, tbl_ids) {
  }

  KBStatus InitData(KTimestamp& new_ts) override;

  KBStatus Destroy() override {
    return KBStatus::OK();
  }

  virtual std::string show_extra() override;

 protected:
  // ts_now is the simulated business data time
  KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::AvgStat _row_prepare_time; // row data prepare time
  kwdbts::AvgStat _row_put_time;  // row data put time
  int64_t _row_sum = 0;
};

class StGetLastWorker : public StWorker {
 public:
  StGetLastWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids)
      : StWorker(pname, params, tbl_ids) {
  }

  KBStatus Init() override {
    return StWorker::Init();
  }

  KBStatus Destroy()  override {
    return KBStatus::OK();
  }

  virtual BenchType GetType() override {
    return Worker::BenchType::READ;
  }

  virtual std::string show_extra() override;

 protected:
  KBStatus do_work(KTimestamp new_ts) override;

  kwdbts::AvgStat _get_time;  // row data read time
};

class StScanWorker : public StWorker {
 public:
  StScanWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids)
      : StWorker(pname, params, tbl_ids),start_ts_(-1) {
  }

  KBStatus Init() override;
  KBStatus Destroy()  override {
    return KBStatus::OK();
  }

  virtual BenchType GetType() override {
    return Worker::BenchType::READ;
  }

  virtual std::string show_extra() override;

 protected:
  KBStatus do_work(KTimestamp new_ts) override;

  int64_t start_ts_;  // start time of query
  kwdbts::AvgStat _scan_rows;  // the number of rows returned per query
  kwdbts::AvgStat _scan_time;  // each query time(including traversal)
  kwdbts::AvgStat _agg_time;  // each full table aggregation query time
  kwdbts::KTimestamp ts_range_min_{0};
  kwdbts::KTimestamp  ts_range_max_;
};

class StSnapshotWorker : public StWorker {
 public:
  StSnapshotWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      StWorker(pname, params, tbl_ids) {
  }

  KBStatus InitData(KTimestamp& new_ts) override {
    return KBStatus::OK();
  }

  KBStatus Destroy() override {
    return KBStatus::OK();
  }

  virtual BenchType GetType() override {
    return Worker::BenchType::SNAPSHOT;
  }

  virtual std::string show_extra() override;

 protected:
  // ts_now is the simulated business data time
  KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::AvgStat _init_time;
  kwdbts::AvgStat _get_time;
  kwdbts::AvgStat _put_time;
  kwdbts::AvgStat _del_time;
  kwdbts::AvgStat _total_time;
  kwdbts::AvgStat _total_size;
};


class StSnapshotByBlockWorker : public StSnapshotWorker {
 public:
  StSnapshotByBlockWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      StSnapshotWorker(pname, params, tbl_ids) {
    SnapshotFactory::TestSetType(2);
  }
};

class StCompressWorker : public StWorker {
 public:
  StCompressWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      StWorker(pname, params, tbl_ids) {
  }

  KBStatus InitData(KTimestamp& new_ts) override {
    return KBStatus::OK();
  }


  KBStatus Destroy() override {
    return KBStatus::OK();
  }

  virtual BenchType GetType() override {
    return Worker::BenchType::COMPRESS;
  }

  virtual std::string show_extra() override;

 protected:
  // ts_now is the simulated business data time
  KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::AvgStat _compress_time; // data compress time
};


class StRetentionsWorker : public StWorker {
 public:
  StRetentionsWorker(const std::string& pname, const BenchParams& params, const std::vector<uint32_t>& tbl_ids) :
      StWorker(pname, params, tbl_ids) {
  }

  KBStatus InitData(KTimestamp& new_ts) override;


  KBStatus Destroy() override {
    return KBStatus::OK();
  }

  virtual BenchType GetType() override {
    return Worker::BenchType::RETENTIONS;
  }

  virtual std::string show_extra() override;

 protected:
  KBStatus do_work(KTimestamp new_ts) override;

 protected:
  kwdbts::KTimestamp start_ts_;
  kwdbts::KTimestamp retentions_ts_;
  kwdbts::AvgStat _retentions_time; // data retentions time
};
}