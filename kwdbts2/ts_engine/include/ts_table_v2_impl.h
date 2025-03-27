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
#include "ts_table.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

class TsTableV2Impl : public TsTable {
 public:
  TsTableV2Impl(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr);
  TsTableV2Impl(kwdbContext_p ctx, const std::string &db_path,
                const KTableKey &table_id);

  ~TsTableV2Impl() override;

  KStatus GetTagIterator(kwdbContext_p ctx,
                          std::vector<uint32_t> scan_tags,
                          const vector<uint32_t> hps,
                          TagIterator** iter, k_uint32 table_version) override;

 private:
  std::shared_ptr<TsTableSchemaManager> table_schema_mgr_{nullptr};
};

}  // namespace kwdbts
