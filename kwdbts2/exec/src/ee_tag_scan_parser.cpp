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


#include "ee_tag_scan_parser.h"
#include "ee_table.h"
#include "ee_field.h"
#include "cm_func.h"

namespace kwdbts {

TsTagScanParser::TsTagScanParser(TSTagReaderSpec* spec , TSPostProcessSpec *post, TABLE *table)
    : TsScanParser(post, table), spec_(spec) { }

KStatus TsTagScanParser::ParserTagSpec(kwdbContext_p ctx) {
  EnterFunc();
  table_->SetAccessMode(spec_->accessmode());
  if (spec_->primarytags_size() > 0) {
    table_->ptag_size_ = (spec_->primarytags(0).tagvalues_size() + 7) / 8;
  }
  object_id_ = spec_->tableid();
  table_->SetTableVersion(spec_->tableversion());
  table_->SetOnlyTag(spec_->only_tag());
  Return(KStatus::SUCCESS);
}

EEIteratorErrCode TsTagScanParser::ParserScanTags(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  for (k_uint32 i = 0; i < inputcols_count_; i++) {
    k_uint32 col_index = post_->outputcols(i);
    // skip the col_id if it exceeds table_->field_num_ which should be relational col for multiple model processing
    if (col_index < table_->field_num_) {
      table_->scan_tags_.push_back(col_index - table_->min_tag_id_);
      Field* field = table_->GetFieldWithColNum(col_index);
      field->setColIdxInRs(i);
    }
  }

  Return(code);
}

EEIteratorErrCode TsTagScanParser::ParserScanTagsRelCols(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // resolve relational cols
  table_->scan_rel_cols_.reserve(table_->GetRelFields().size());
  for (k_uint32 i = 0; i < inputcols_count_; ++i) {
    k_uint32 col_id = post_->outputcols(i);
    if (col_id >= table_->field_num_) {
      table_->scan_rel_cols_.push_back(col_id - table_->field_num_);
      Field* field = table_->GetFieldWithColNum(col_id);
      field->setColIdxInRs(i);
      field->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }
  }
  Return(code);
}

}  // namespace kwdbts
