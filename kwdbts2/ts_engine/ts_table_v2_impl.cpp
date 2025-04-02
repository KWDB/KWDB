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
TsTableV2Impl::~TsTableV2Impl() = default;


KStatus TsTableV2Impl::PutData(kwdbContext_p ctx, uint64_t range_group_id, TSSlice* payload, int payload_num,
                          uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt,
                          DedupResult* dedup_result, const DedupRule& dedup_rule) {                        
  uint32_t entity_id = 0;
  uint32_t tbl_grp_id = 0;
  std::shared_ptr<TagTable> tag_schema;
  table_schema_mgr_->GetTagSchema(ctx, &tag_schema);
  for (size_t i = 0; i < payload_num; i++) {
    TsRawPayload p{payload[i]};
    TSSlice primary_key = p.GetPrimaryTag();
    if (!tag_schema->hasPrimaryKey(primary_key.data, primary_key.len, entity_id, tbl_grp_id)) {
      LOG_ERROR("cannot found vgroup for this entity.");
      return KStatus::FAIL;
    }
    auto tbl_grp = table_grps_[tbl_grp_id - 1].get();
    assert(tbl_grp != nullptr);
    auto s = tbl_grp->PutData(ctx, GetTableId(), entity_id, &payload[i]);
    if (s != KStatus::SUCCESS) {
      // todo(liangbo01) if failed. should we need rollback all inserted data?
      LOG_ERROR("putdata failed. table id[%lu], group id[%u]", GetTableId(), tbl_grp_id);
      return s;
    }
  }
  return KStatus::SUCCESS; 
}


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
