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

#include "ee_scan_parser.h"
#include "ee_table.h"
#include "ee_field.h"
#include "cm_func.h"

namespace kwdbts {

TsTableScanParser::TsTableScanParser(TSReaderSpec* spec, TSPostProcessSpec* post, TABLE* table)
    : TsScanParser(post, table) {
      spec_ = spec;
}

EEIteratorErrCode TsTableScanParser::ParserScanCols(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  // resolve col
  table_->scan_cols_.reserve(table_->field_num_);
  if (inputcols_count_ == 0) {
    table_->scan_cols_.push_back(table_->fields_[0]->get_num());
  } else {
    for (k_uint32 i = 0; i < inputcols_count_; ++i) {
      if (input_cols_[i]->get_num() < table_->min_tag_id_)
        table_->scan_cols_.push_back(input_cols_[i]->get_num());
    }
  }

  for (int i = 0; i < table_->scan_cols_.size(); i++) {
    k_uint32 col_id = table_->scan_cols_[i];
    Field* field = table_->GetFieldWithColNum(col_id);
    field->setColIdxInRs(i);
  }

  Return(code);
}

bool string_ends_with(const std::string &str, const std::string &suffix) {
  if (suffix.size() > str.size()) return false;
  return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

EEIteratorErrCode TsTableScanParser::ResolveBlockFilter() {
  k_int32 size = spec_->blockfilter_size();
  for (k_int32 i = 0; i < size; i++) {
    const TSBlockFilter *filter = &(spec_->blockfilter(i));
    BlockFilter block_filter;
    block_filter.colID = filter->colid() - 1;
    block_filter.filterType = static_cast<BlockFilterType>(
        static_cast<k_int32>(filter->filtertype()));
    auto span_size = filter->columnspan_size();
    auto *field = table_->fields_[block_filter.colID];
    for (k_int32 j = 0; j < span_size; j++) {
      const auto *span = &(filter->columnspan(j));
      FilterSpan filter_span{};
      filter_span.startBoundary = FilterSpanBoundary::FSB_NONE;
      filter_span.endBoundary = FilterSpanBoundary::FSB_NONE;
      switch (field->get_storage_type()) {
        case roachpb::DataType::BOOL: {
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            filter_span.start.len = 1;
            filter_span.start.data = static_cast<char *>(malloc(filter_span.start.len));
            if (span->start() == "true") {
              filter_span.start.data[0] = 1;
            } else {
              filter_span.start.data[0] = 0;
            }
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            filter_span.end.len = 1;
            filter_span.end.data = static_cast<char *>(malloc(filter_span.end.len));
            if (span->end() == "true") {
              filter_span.end.data[0] = 1;
            } else {
              filter_span.end.data[0] = 0;
            }
          }
          break;
        }
        case roachpb::DataType::SMALLINT:
        case roachpb::DataType::INT:
        case roachpb::DataType::BIGINT: {
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            filter_span.start.ival = std::stoll(span->start());
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            filter_span.end.ival = std::stoll(span->end());
          }
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO: {
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            filter_span.start.ival = std::stoll(span->start());
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            filter_span.end.ival = std::stoll(span->end());
          }
          break;
        }
        case roachpb::DataType::FLOAT:
        case roachpb::DataType::DOUBLE: {
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            filter_span.start.dval = std::stod(span->start());
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            filter_span.end.dval = std::stod(span->end());
          }
          break;
        }
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          // bytes
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            std::string start_str = span->start();
            start_str = start_str.substr(1, start_str.length() - 2);
            auto str = parseHex2String(start_str);
            filter_span.start.data = static_cast<char *>(malloc(str.length()));
            memcpy(filter_span.start.data, str.c_str(), str.length());
            filter_span.start.len = str.length();
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            std::string end_str = span->end();
            end_str = end_str.substr(1, end_str.length() - 2);
            auto str = parseHex2String(end_str);
            filter_span.end.data = static_cast<char *>(malloc(str.length()));
            memcpy(filter_span.end.data, str.c_str(), str.length());
            filter_span.end.len = str.length();
          }
          break;
        }
        default: {
          // string
          if (span->has_start()) {
            filter_span.startBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->startboundary()));
            std::string start_str = span->start();
            if (start_str[0] == 'e') {
              start_str = start_str.substr(2, start_str.length() - 3);
            } else {
              start_str = start_str.substr(1, start_str.length() - 2);
            }
            auto str = parseUnicode2Utf8(start_str);
            if (string_ends_with(str, "\\x00")) {
              str = str.substr(0, str.length() - 4);
              filter_span.startBoundary = FilterSpanBoundary::FSB_EXCLUDE_BOUND;
            }
            filter_span.start.data = static_cast<char *>(malloc(str.length()));
            memcpy(filter_span.start.data, str.c_str(), str.length());
            filter_span.start.len = str.length();
          }
          if (span->has_end()) {
            filter_span.endBoundary = static_cast<FilterSpanBoundary>(
                static_cast<k_int32>(span->endboundary()));
            std::string end_str = span->end();
            if (end_str[0] == 'e') {
              end_str = end_str.substr(2, end_str.length() - 3);
            } else {
              end_str = end_str.substr(1, end_str.length() - 2);
            }
            auto str = parseUnicode2Utf8(end_str);
            if (string_ends_with(str, "\\x00")) {
              str = str.substr(0, str.length() - 4);
              filter_span.endBoundary = FilterSpanBoundary::FSB_EXCLUDE_BOUND;
            }
            filter_span.end.data = static_cast<char *>(malloc(str.length()));
            memcpy(filter_span.end.data, str.c_str(), str.length());
            filter_span.end.len = str.length();
          }
          break;
        }
      }
      block_filter.spans.push_back(filter_span);
    }
    table_->block_filters_.push_back(block_filter);
  }
  return EE_OK;
}


}  // namespace kwdbts
