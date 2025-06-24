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

#include <memory>
#include <list>
#include <vector>
#include "ts_mem_segment_mgr.h"
#include "ts_version.h"

namespace kwdbts {

class TsEntityPartition {
 private:
  std::shared_ptr<const TsPartitionVersion> partition_version_;
  TS_LSN scan_lsn_;
  DATATYPE ts_type_;
  TsScanFilterParams scan_filter_;
  std::list<std::shared_ptr<TsMemSegment>> mems_;
  TsBlockItemFilterParams block_data_filter_{0, 0, 0};
  bool skip_file_data_;

 public:
  TsEntityPartition(std::shared_ptr<const TsPartitionVersion> p, TS_LSN scan_lsn, DATATYPE ts_type,
                    const TsScanFilterParams& filter, bool skip_file_data = false)
      : partition_version_(p),
        scan_lsn_(scan_lsn),
        ts_type_(ts_type),
        scan_filter_(filter),
        skip_file_data_(skip_file_data) {}
  // initialize current object.
  KStatus Init(std::list<std::shared_ptr<TsMemSegment>>& mems);
  // filter all blocks, and return block span list.
  KStatus GetBlockSpan(std::list<shared_ptr<TsBlockSpan>>* ts_block_spans,
                      std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                      uint32_t scan_version);

 private:
  KStatus SetFilter();
  void AddMemSegment(std::list<std::shared_ptr<TsMemSegment>>& mems);
};

}  // namespace kwdbts
