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
#include <cstdio>
#include <string>
#include <iostream>
#include <memory>
#include <vector>
#include <atomic>
#include "../statistics.h"
#include "st_entitygroup_worker.h"

DedupRule g_dedup_rule_;

namespace kwdbts {

KBStatus TSEntityGroupWriteWorker::do_work(KTimestamp  new_ts) {
  StEngityGroupInstance::Get()->SetMaxDataTS(new_ts);
  if (params_.ts_add_mode == 1) {
    KTimestamp max_ts = StEngityGroupInstance::Get()->GetMaxTS();
    if (new_ts < max_ts) {
      new_ts = max_ts;
    }
  }
  
  if (table_ids_.size() == 0) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  TSSlice payload;
  {
    KWDB_START();
    // traverse table, execute write
    uint32_t w_table = getCurrentTable();
    genPayloadData(tag_schema_, data_schema_, w_table, new_ts, params_.BATCH_NUM, params_.time_inc, &payload);
    KWDB_DURATION(_row_prepare_time);
  }
  KStatus stat;
  {
      KWDB_START();
      DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
      uint16_t inc_entity_cnt;
      uint32_t inc_unordered_cnt;
      stat = entity_group_->PutData(ctx, payload, 0, &inc_entity_cnt,
                                    &inc_unordered_cnt, &dedup_result,
                                    StInstance::Get()->GetDedupRule());
      KWDB_DURATION(_row_put_time);
  }
  delete[] payload.data;
  _row_sum += params_.BATCH_NUM;
  return dump_zstatus("PutData", ctx, stat);
}

std::string TSEntityGroupWriteWorker::show_extra() {
  char msg[128];
  snprintf(msg, sizeof(msg), "total rows %ld, time: preparePayload=%.3fus,putData=%.3f(%.0f)us",
           _row_sum, _row_prepare_time.avg() / 1e3,
           _row_put_time.avg() / 1e3 , _row_put_time.max() / 1e3);
  _row_prepare_time.reset();
  _row_put_time.reset();
  return msg;
}

KBStatus TSEntityGroupWriteWorkerWithScan::do_work(KTimestamp  new_ts) {
  KBStatus put_s = TSEntityGroupWriteWorker::do_work(new_ts);
  if (!put_s.isOK()) {
    return KBStatus(StatusCode::IOError, "insert failed.");
  }
  KStatus s;
  {
    std::vector<void*> primary_tags;
    primary_tags.push_back(&table_ids_[table_i]);
    std::vector<uint32_t> scan_tags;
    std::vector<EntityResultIndex> entityIdList;
    ResultSet res;
    uint32_t count;
    std::vector<uint64_t/*index_id*/> tags_index_id{};
    std::vector<void*> tags{};
    s = entity_group_->GetEntityIdList(ctx, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags,
                                       &entityIdList, &res, &count, 1);
    if (entityIdList.size() != 1) {
      return KBStatus(StatusCode::RError, "search entity failed.");
    }
    KwTsSpan ts_span = {new_ts, new_ts + params_.BATCH_NUM * params_.time_inc};
    std::vector<KwTsSpan> ts_spans;
    ts_spans.push_back(ts_span);
    std::vector<k_uint32> scan_cols;
    for (size_t i = 0; i < data_schema_.size(); i++) {
      scan_cols.push_back(i);
    }
    
    std::vector<Sumfunctype> scan_agg_types;
    TsStorageIterator* iter1;
    KWDB_START();
    vector<uint32_t> entity_ids = {entityIdList[0].entityId};
    auto ts_type = st_inst_->GetTable()->GetRootTableManager()->GetTsColDataType();
    s = entity_group_->GetIterator(ctx, entityIdList[0].subGroupId, entity_ids,
                                   ts_spans, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_,
                                   {}, false, false);
    assert(s == KStatus::SUCCESS);
    int total_rows = 0;
    k_uint32 ret_cnt;
    bool is_finished = false;
    do {
      res.clear();
      s = iter1->Next(&res, &ret_cnt, &is_finished);
      if (s != KStatus::SUCCESS) {
        return KBStatus(StatusCode::RError, "iterator next failed.");
      }
      if (ret_cnt > 0 && !checkColValue(data_schema_, res, ret_cnt, params_.time_inc)) {
        return KBStatus(StatusCode::RError, "column value check failed.");
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    if (total_rows != params_.BATCH_NUM) {
      return KBStatus(StatusCode::RError, "input data num no equal search data.");
    }
    KWDB_DURATION(_row_scan_time);
    delete iter1;
  }

  return dump_zstatus("PutAndScanData", ctx, s);
}

std::string TSEntityGroupWriteWorkerWithScan::show_extra() {
  std::string pri_show = TSEntityGroupWriteWorker::show_extra();
  char msg[256];
  snprintf(msg, sizeof(msg), "%s, scanData=%.3f(%.0f)us", pri_show.c_str(),
           _row_scan_time.avg() / 1e3 , _row_scan_time.max() / 1e3);
  _row_scan_time.reset();
  return msg;
}

KBStatus TSEntityGroupScanWorker::do_work(KTimestamp  new_ts) {
  KStatus s;
  uint32_t w_table = 0;
  std::vector<EntityResultIndex> entityIdList;
  ResultSet res;
  uint32_t count;
  {
    w_table = getCurrentTable();
    std::vector<void*> primary_tags;
    primary_tags.push_back(&w_table);
    std::vector<uint32_t> scan_tags;
    std::vector<uint64_t/*index_id*/> tags_index_id{};
    std::vector<void*> tags{};
    s = entity_group_->GetEntityIdList(ctx, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, scan_tags,
                                       &entityIdList, &res, &count, 1);
    if (s != KStatus::SUCCESS || entityIdList.size() != 1) {
      sleep(1);
      return KBStatus(StatusCode::NotFound, "GetEntityIdList");
    }
  }
  {
    std::vector<k_uint32> scan_cols;
    for (size_t i = 0; i < data_schema_.size(); i++) {
      scan_cols.push_back(i);
    }
    std::vector<Sumfunctype> scan_agg_types;
    TsStorageIterator* iter1;
    KTimestamp min_ts = StEngityGroupInstance::Get()->GetMinTS();
    KTimestamp max_ts = StEngityGroupInstance::Get()->GetMaxTS();
    KTimestamp start = 0;
    if (min_ts == max_ts) {
      start = min_ts;
    } else {
      start = min_ts + rand_r(&rand_seed_) % (max_ts - min_ts);
    }

    int64_t scan_range = static_cast<int64_t>(params_.scan_range);
    KwTsSpan ts_span = {start, start + scan_range};
    std::vector<KwTsSpan> ts_spans;
    ts_spans.push_back(ts_span);
    KWDB_START();
    vector<uint32_t> entity_ids = {entityIdList[0].entityId};
    auto ts_type = st_inst_->GetTable()->GetRootTableManager()->GetTsColDataType();
    s = entity_group_->GetIterator(ctx, entityIdList[0].subGroupId, entity_ids,
                                   ts_spans, ts_type, scan_cols, scan_cols, scan_agg_types, 1, &iter1, entity_group_,
                                    {}, false, false);
    assert(s == KStatus::SUCCESS);
    int total_rows = 0;
    k_uint32 ret_cnt;
    bool is_finished = false;
    do {
      res.clear();
      s = iter1->Next(&res, &ret_cnt, &is_finished);
      assert(s == KStatus::SUCCESS);
      if (ret_cnt > 0 && !checkColValue(data_schema_, res, ret_cnt, params_.time_inc)) {
        return KBStatus(StatusCode::RError, "column value check failed.");
      }
      total_rows += ret_cnt;
    } while (!is_finished);
    KWDB_DURATION(_scan_time);
    if (total_rows == 0) {
      return KBStatus(StatusCode::IOError, "search empty.");
    }
    _scan_rows.add(total_rows);

    delete iter1;
  }

  return dump_zstatus("ScanData", ctx, s);
}

std::string TSEntityGroupScanWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, ",Scan Rows=%.0f, Time=%.3f(%.0f) ms, AGG=%.3f(%.0f) ms",
           _scan_rows.avg(), _scan_time.avg() / 1e6 ,_scan_time.max() / 1e6
          , _agg_time.avg() / 1e6 ,_agg_time.max() / 1e6);
  _scan_time.reset();
  _agg_time.reset();
  return msg;
}

}  // namespace kwdbts

