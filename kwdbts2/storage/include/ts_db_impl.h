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

#include <vector>
#include <string>
#include "db/db_impl.h"
#include "ts_vgroup.h"

namespace rocksdb {

class TsDBImpl : public DBImpl {
 public:
  TsDBImpl(const DBOptions &options, const std::string &dbname, kwdbts::TsVGroup* ts_vgroup,
           const bool seq_per_batch = false, const bool batch_per_txn = true);

  static Status Open(const Options& options, const std::string& dbname, kwdbts::TsVGroup* ts_vgroup, DB** db_ptr);

  Status FlushMemTableToOutputFile(rocksdb::ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
                                   bool* made_progress, JobContext* job_context,
                                   SuperVersionContext* superversion_context,
                                   std::vector<SequenceNumber>& snapshot_seqs,
                                   SequenceNumber earliest_write_conflict_snapshot,
                                   SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
                                   Env::Priority thread_pri) override;
 private:
  kwdbts::TsVGroup* ts_vgroup_;
};

}  // namespace rocksdb
