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
#include <th_kwdb_dynamic_thread_pool.h>
#include <dlfcn.h>
#include "engine.h"
#include "../statistics.h"
#include "st_worker.h"
#include "st_meta.h"
#include "ts_table.h"

using namespace kwdbts;

namespace kwdbts {

const static int HEADER_SIZE = 16 + 2 + 4 + 4 + 8 + 4 + 1;  // NOLINT

bool StWorker::IsTableCreated(uint32_t tbl_id, int table_i) {
  // check if the table has been created
  std::shared_ptr<TsTable> ts_table;
  while (KStatus::SUCCESS != st_inst_->GetTSEngine()->GetTsTable(ctx, tbl_id, ts_table)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  return true;
}

KBStatus StWriteWorker::InitData(KTimestamp& data_ts) {
  _entity_i = _entity_begin;
  return KBStatus::OK();
}

KBStatus StWriteWorker::do_work(KTimestamp  new_ts) {
  if (table_ids_.size() == 0) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  // traverse table, execute write
  uint32_t w_table = table_ids_[table_i];
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }
  uint32_t entity_tag = _entity_i;
  if (_entity_i >= _entity_end) {
    _entity_i = _entity_begin;
  } else {
    _entity_i++;
  }
  KBStatus s;
  KStatus stat;
  KTimestamp wr_ts = new_ts;
  k_uint32 p_len = 0;
  TSSlice payload;
  {
    KWDB_START();
    std::vector<TagInfo> tag_schema;
    std::vector<AttributeInfo> data_schema;
    stat = st_inst_->GetSchemaInfo(ctx, w_table, &tag_schema, &data_schema);
    if (stat != KStatus::SUCCESS) {
      return KBStatus::NOT_FOUND("st_inst_->GetSchemaInfo failed. tbl:" + std::to_string(w_table));
    }
    genPayloadData(tag_schema, data_schema, entity_tag, wr_ts, params_.BATCH_NUM, params_.time_inc, &payload);
    KWDB_DURATION(_row_prepare_time);
  }

  {
    KWDB_START();
    DedupResult dedup_result{0, 0, 0, TSSlice {nullptr, 0}};
    uint16_t inc_entity_cnt;
    uint32_t inc_unordered_cnt;
    stat = st_inst_->GetTSEngine()->PutData(
        ctx, w_table, st_inst_->rangeGroup(), &payload, 1, 0, &inc_entity_cnt,
        &inc_unordered_cnt, &dedup_result);
    if (stat != KStatus::SUCCESS) {
      std::cout << "failed put data." << std::endl;
    }
    s = dump_zstatus("PutData", ctx, stat);
    KWDB_DURATION(_row_put_time);
  }
  delete[] payload.data;
  _row_sum += params_.BATCH_NUM;
  return s;
}

std::string StWriteWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, "total rows %ld, time: preparePayload=%.3fus,putData=%.3f(%.0f)us",
           _row_sum, _row_prepare_time.avg() / 1e3,
           _row_put_time.avg() / 1e3 , _row_put_time.max() / 1e3 );
  _row_prepare_time.reset();
  _row_put_time.reset();
  return msg;
}

KBStatus StGetLastWorker::do_work(KTimestamp new_ts) {
  if (table_ids_.empty()) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  // select the table in order and execute the last read
  uint32_t r_table = table_ids_[table_i];
  if (!IsTableCreated(r_table, table_i)) {
    log_INFO("Table[%d] not created!", r_table);
    return KBStatus::OK();
  }
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }

//  KWDB_START();
//  char* tuple = nullptr;
//  void* ref = nullptr;
//
//  if (st_inst_->GetKSchema()->RefLatestKObjectTableData(ctx, r_table, &tuple, &ref) != kwdbts::KStatus::SUCCESS) {
//    return KBStatus::InternalError("table_" + std::to_string(r_table));
//  }
//  if (tuple == nullptr || ref == nullptr) {
//    return KBStatus::NOT_FOUND("table_" + std::to_string(r_table));
//  }
//  if (st_inst_->GetKSchema()->UnrefKObjectTableData(ctx, r_table, ref) != kwdbts::KStatus::SUCCESS) {
//    return KBStatus::InternalError("table_" + std::to_string(r_table));
//  }
//  KWDB_DURATION(_get_time);

  return KBStatus::OK();
}

std::string StGetLastWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, "RefLatest Avg Time=%.3f (%.0f) us", _get_time.avg() / 1e3, _get_time.max() / 1e3 );
  _get_time.reset();
  return msg;
}

KBStatus StScanWorker::Init() {
  for (int i = 0; i < table_ids_.size(); i++) {
    KTableKey table_id = table_ids_[i];
    // construct table meta
    roachpb::CreateTsTable meta;
    StMetaBuilder::constructRoachpbTable(&meta, table_id, params_);
    st_inst_->tableMetas().push_back(meta);
  }
  return KBStatus::OK();
}

KBStatus StScanWorker::do_work(KTimestamp  new_ts) {
  if (table_ids_.empty()) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  if (start_ts_ < 0) {
    start_ts_ = new_ts - 1;
  }
  // select the table in order and execute the scan read
  uint32_t r_table = table_ids_[table_i];
  if (!IsTableCreated(r_table, table_i)) {
    log_INFO("Table[%d] not created!", r_table);
    return KBStatus::OK();
  }
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }

  KWDB_START();

  std::shared_ptr<TsTable> ts_table;
  KBStatus s;
  auto stat = st_inst_->GetTSEngine()->GetTsTable(ctx, r_table, ts_table);
  s = dump_zstatus("GetTsTable", ctx, stat);
  if (s.isNotOK()) {
    return s;
  }
  auto ts_type = ts_table->GetRootTableManager()->GetTsColDataType();
  std::shared_ptr<TsEntityGroup> tbl_range;
  stat = ts_table->GetEntityGroup(ctx, st_inst_->rangeGroup(), &tbl_range);
  uint32_t entity_index = 1;
  KwTsSpan ts_span = {int64_t(start_ts_), GetTimeNow()};
  std::vector<KwTsSpan> ts_spans;
  ts_spans.push_back(ts_span);
  std::vector<k_uint32> scan_cols;
  std::vector<AttributeInfo> data_schema;
  stat = ts_table->GetDataSchemaExcludeDropped(ctx, &data_schema);
  for (size_t i = 0; i < data_schema.size(); i++) {
    scan_cols.push_back(i);
  }

  std::vector<Sumfunctype> scan_agg_types;
  TsStorageIterator* iter;
  SubGroupID group_id = 1;

  vector<uint32_t> entity_ids = {entity_index};
  stat = tbl_range->GetIterator(ctx, group_id, entity_ids, ts_spans, ts_type, scan_cols, scan_cols, {}, scan_agg_types, 1, &iter, tbl_range,
                      {}, false, false);
  s = dump_zstatus("GetIterator", ctx, stat);
  if (s.isNotOK()) {
    return s;
  }

  ResultSet res;
  uint32_t count = 0;
  bool is_finished = false;
  do {
    stat = iter->Next(&res, &count, &is_finished);
    s = dump_zstatus("IteratorNext", ctx, stat);
    if (s.isNotOK()) {
      return s;
    }
    if (count > 0 && !checkColValue(data_schema, res, count, params_.time_inc)) {
      return KBStatus(StatusCode::RError, "colume value check failed.");
    }
    _scan_rows.add(count);
  } while (!is_finished);

  KWDB_DURATION(_scan_time);

  return KBStatus::OK();
}

std::string StScanWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, ",Scan Rows=%.0f, Time=%.3f(%.0f) ms, AGG=%.3f(%.0f) ms",
           _scan_rows.avg(), _scan_time.avg() / 1e6 ,_scan_time.max() / 1e6
      , _agg_time.avg() / 1e6 ,_agg_time.max() / 1e6);
  _scan_time.reset();
  _agg_time.reset();
  return msg;
}

KBStatus StSnapshotWorker::do_work(KTimestamp  new_ts) {
  if (table_ids_.empty()) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  // select the table in order and execute compress
  uint32_t r_table = table_ids_[table_i];
  if (!IsTableCreated(r_table, table_i)) {
    log_INFO("Table[%d] not created!", r_table);
    return KBStatus::OK();
  }
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }
  uint64_t read_snapshot_id, write_snapshot_id;
  KStatus s;
  size_t snapshot_size = 0;
  KWDB_START();
  {
    KWDB_START();
    s = st_inst_->GetTSEngine()->CreateSnapshotForRead(ctx, table_ids_[table_i], 0, UINT64_MAX,
                                                        {INT64_MIN, INT64_MAX}, &read_snapshot_id);
    if (s != KStatus::SUCCESS) {
      return dump_zstatus("CreateSnapshotForRead", ctx, s);
    }
    s = st_inst_->GetTSEngine()->CreateSnapshotForWrite(ctx, st_inst_->GetSnapShotTableId(), 0,
                                            UINT64_MAX, {INT64_MIN, INT64_MAX}, &write_snapshot_id);
    if (s != KStatus::SUCCESS) {
      return dump_zstatus("CreateSnapshotForWrite", ctx, s);
    }
    KWDB_DURATION(_init_time);
  }
  while (true) {
    TSSlice payload{nullptr, 0};
    {
      KWDB_START();
      s = st_inst_->GetTSEngine()->GetSnapshotNextBatchData(ctx, read_snapshot_id, &payload);
      if (s != KStatus::SUCCESS) {
        return dump_zstatus("GetSnapshotNextBatchData", ctx, s);
      }
      KWDB_DURATION(_get_time);
    }
    snapshot_size += payload.len;
    if (payload.data != nullptr) {
      KWDB_START();
      s = st_inst_->GetTSEngine()->WriteSnapshotBatchData(ctx, write_snapshot_id, payload);
      if (s != KStatus::SUCCESS) {
        return dump_zstatus("WriteSnapshotBatchData", ctx, s);
      }
      delete payload.data;
      KWDB_DURATION(_put_time);
    } else {
      break;
    }
  }
  {
    KWDB_START();
    st_inst_->GetTSEngine()->WriteSnapshotSuccess(ctx, write_snapshot_id);
    st_inst_->GetTSEngine()->DeleteSnapshot(ctx, read_snapshot_id);
    st_inst_->GetTSEngine()->DeleteSnapshot(ctx, write_snapshot_id);
    KWDB_DURATION(_del_time);
  }
  KWDB_DURATION(_total_time);
  _total_size.add(snapshot_size);

  return KBStatus::OK();
}

std::string StSnapshotWorker::show_extra() {
  char msg[256];
  snprintf(msg, sizeof(msg), ",init Time=%.3f(%.0f) ms , gen Time=%.3f(%.0f) ms,"
                     "write Time=%.3f(%.0f) ms, drop Time=%.3f(%.0f) ms,"
                     " total Time=%.3f(%.0f) ms, data size %.3f(%.0f)",
                    _init_time.avg() / 1e6, _init_time.max() / 1e6,
                    _get_time.avg() / 1e6, _get_time.max() / 1e6,
                    _put_time.avg() / 1e6, _put_time.max() / 1e6,
                    _del_time.avg() / 1e6, _del_time.max() / 1e6,
                    _total_time.avg() / 1e6, _total_time.max() / 1e6,
                    _total_size.avg(), _total_size.max());
  _init_time.reset();
  _get_time.reset();
  _put_time.reset();
  _del_time.reset();
  _total_time.reset();
  _total_size.reset();
  return msg;
}

KBStatus StCompressWorker::do_work(KTimestamp  new_ts) {
  if (table_ids_.empty()) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  // select the table in order and execute compress
  uint32_t r_table = table_ids_[table_i];
  if (!IsTableCreated(r_table, table_i)) {
    log_INFO("Table[%d] not created!", r_table);
    return KBStatus::OK();
  }
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }

  KWDB_START();

  KBStatus s;
  auto stat = st_inst_->GetTSEngine()->CompressTsTable(ctx, r_table, new_ts);
  s = dump_zstatus("CompressTsTable", ctx, stat);
  if (s.isNotOK()) {
    return s;
  }

  KWDB_DURATION(_compress_time);

  return KBStatus::OK();
}

std::string StCompressWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, ",Compress Time=%.3f(%.0f) ms ", _compress_time.avg() / 1e6 ,_compress_time.max() / 1e6);
  _compress_time.reset();
  return msg;
}

KBStatus StRetentionsWorker:: InitData(KTimestamp& new_ts) {
  start_ts_ = new_ts;
  retentions_ts_ = params_.meta_param.RETENTIONS_TIME * 1000;
  return KBStatus::OK();
}

KBStatus StRetentionsWorker::do_work(KTimestamp  new_ts) {
  if (table_ids_.empty()) {
    can_run_ = false;
    return KBStatus::Invalid("no table to run");
  }
  // select the table in order and execute retentions
  uint32_t r_table = table_ids_[table_i];
  if (!IsTableCreated(r_table, table_i)) {
    log_INFO("Table[%d] not created!", r_table);
    return KBStatus::OK();
  }
  table_i++;
  if (table_i >= table_ids_.size()) {
    table_i = 0;
  }

  kwdbts::KTimestamp end_ts = new_ts - retentions_ts_;
  if (end_ts < start_ts_) {
    return KBStatus::OK();
  }

  KWDB_START();

  std::shared_ptr<TsTable> ts_table;
  KBStatus s;
  auto stat = st_inst_->GetTSEngine()->GetTsTable(ctx, r_table, ts_table);
  s = dump_zstatus("GetTsTable", ctx, stat);
  if (s.isNotOK()) {
    return s;
  }
  stat = ts_table->DeleteExpiredData(ctx, end_ts);
  s = dump_zstatus("DeleteExpiredData", ctx, stat);
  if (s.isNotOK()) {
    return s;
  }

  KWDB_DURATION(_retentions_time);

  start_ts_ = end_ts;
  return KBStatus::OK();
}

std::string StRetentionsWorker::show_extra() {
  char msg[128];
  snprintf(msg, 128, ",Retentions Time=%.3f(%.0f) ms ", _retentions_time.avg() / 1e6 ,_retentions_time.max() / 1e6);
  _retentions_time.reset();
  return msg;
}
}
