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
#include <vector>
#include <list>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_block.h"
#include "ts_del_item_manager.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

class TsSegmentBase;
// conditions used for flitering data.
struct TsScanFilterParams {
  TsScanFilterParams(uint32_t db_id, TSTableID table_id, uint32_t vgroup_id,
                      TSEntityID entity_id, DATATYPE  table_ts_type, TS_OSN end_osn,
                      const std::vector<KwTsSpan>& ts_spans) :
                      db_id_(db_id), table_id_(table_id), vgroup_id_(vgroup_id),
                      entity_id_(entity_id), table_ts_type_(table_ts_type),
                      ts_spans_(ts_spans) {
                        osn_spans_.push_back({0, end_osn});
                      }
  TsScanFilterParams(uint32_t db_id, TSTableID table_id, uint32_t vgroup_id,
                      TSEntityID entity_id, DATATYPE  table_ts_type,
                      const std::vector<KwTsSpan>& ts_spans,
                      const std::vector<KwOSNSpan>& osn_spans) :
                      db_id_(db_id), table_id_(table_id), vgroup_id_(vgroup_id),
                      entity_id_(entity_id), table_ts_type_(table_ts_type),
                      osn_spans_(osn_spans), ts_spans_(ts_spans) {}
  uint32_t db_id_;
  TSTableID table_id_;
  uint32_t vgroup_id_;
  TSEntityID entity_id_;
  DATATYPE  table_ts_type_;
  std::vector<KwOSNSpan> osn_spans_;
  const std::vector<KwTsSpan>& ts_spans_;
};

// conditions used for filtering blockitem data.
struct TsBlockItemFilterParams {
  uint32_t db_id;
  TSTableID table_id;
  uint32_t vgroup_id;
  TSEntityID entity_id;
  std::vector<STScanRange> spans_;
};

// base class for data segment
class TsSegmentBase {
 public:
  // filter blockspans that satisfied condition.
  virtual KStatus GetBlockSpans(const TsBlockItemFilterParams& filter,
                                std::list<shared_ptr<TsBlockSpan>>& block_spans,
                                std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                std::shared_ptr<MMapMetricsTable>& scan_schema) = 0;

  virtual bool MayExistEntity(TSEntityID entity_id) const { return true; }

  virtual ~TsSegmentBase() {}
};

}  // namespace kwdbts
