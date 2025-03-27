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

#include "ts_table_v2_impl.h"
#include "ts_tag_iterator_v2_impl.h"

namespace kwdbts {
TsTableV2Impl::TsTableV2Impl(kwdbContext_p ctx, std::shared_ptr<TsTableSchemaManager>& table_schema_mgr) {
  table_schema_mgr_ = table_schema_mgr;
}

TsTableV2Impl::TsTableV2Impl(kwdbContext_p ctx, const std::string &db_path,
        const KTableKey &table_id)
: TsTable(ctx, db_path, table_id) {}

TsTableV2Impl::~TsTableV2Impl() = default;

KStatus TsTableV2Impl::GetTagIterator(kwdbContext_p ctx, std::vector<uint32_t> scan_tags,
                                const std::vector<uint32_t> hps,
                                BaseEntityIterator** iter, k_uint32 table_version) {
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = this->table_schema_mgr_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  TagIteratorV2Impl* tag_iter = new TagIteratorV2Impl(tag_table, table_version, scan_tags);
  if (KStatus::SUCCESS != tag_iter->Init()) {
    delete tag_iter;
    tag_iter = nullptr;
    *iter = nullptr;
    return KStatus::FAIL;
  }
  *iter = tag_iter;
  return KStatus::SUCCESS;
}

}  //  namespace kwdbts
