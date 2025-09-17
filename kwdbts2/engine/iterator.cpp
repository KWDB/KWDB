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

#include <utility>
#include "engine.h"
#include "iterator.h"
#include "perf_stat.h"

enum NextBlkStatus {
  find_one = 1,
  scan_over = 2,
  error = 3
};


namespace kwdbts {

Batch* CreateAggBatch(void* mem, const std::shared_ptr<MMapSegmentTable>& segment_table) {
  if (mem) {
    return new AggBatch(mem, 1, segment_table);
  } else {
    return new AggBatch(nullptr, 0, segment_table);
  }
}

Batch* CreateAggBatch(const std::shared_ptr<void>& mem, const std::shared_ptr<MMapSegmentTable>& segment_table) {
  if (mem) {
    return new AggBatch(mem, 1, segment_table);
  } else {
    return new AggBatch(nullptr, 0, segment_table);
  }
}

// Agreement between storage layer and execution layer:
// 1. The SUM aggregation result of integer type returns the int64 type uniformly without overflow;
//    In case of overflow, return double type
// 2. The return type for floating-point numbers is double
// This function is used for type conversion of SUM aggregation results.
bool ChangeSumType(DATATYPE type, void* base, void** new_base) {
  if (type != DATATYPE::INT8 && type != DATATYPE::INT16 && type != DATATYPE::INT32 && type != DATATYPE::FLOAT) {
    *new_base = base;
    return false;
  }
  void* sum_base = malloc(8);
  memset(sum_base, 0, 8);
  switch (type) {
    case DATATYPE::INT8:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int8_t*>(base));
      break;
    case DATATYPE::INT16:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int16_t*>(base));
      break;
    case DATATYPE::INT32:
      *(static_cast<int64_t*>(sum_base)) = *(static_cast<int32_t*>(base));
      break;
    case DATATYPE::FLOAT:
      *(static_cast<double*>(sum_base)) = *(static_cast<float*>(base));
  }
  *new_base = sum_base;
  return true;
}

TsStorageIterator::TsStorageIterator() {
}

TsStorageIterator::TsStorageIterator(uint64_t entity_group_id,
                                     uint32_t subgroup_id, const vector<uint32_t>& entity_ids,
                                     const std::vector<KwTsSpan>& ts_spans,
                                     const std::vector<BlockFilter>& block_filter, DATATYPE ts_col_type,
                                     const std::vector<uint32_t>& kw_scan_cols,
                                     const std::vector<uint32_t>& ts_scan_cols, uint32_t table_version)
    : entity_group_id_(entity_group_id),
      subgroup_id_(subgroup_id),
      entity_ids_(entity_ids),
      ts_spans_(ts_spans),
      block_filter_(block_filter),
      ts_col_type_(ts_col_type),
      kw_scan_cols_(kw_scan_cols),
      ts_scan_cols_(ts_scan_cols),
      table_version_(table_version) { }

TsStorageIterator::~TsStorageIterator() {
  if (segment_iter_ != nullptr) {
    delete segment_iter_;
    segment_iter_ = nullptr;
  }
}

bool TsStorageIterator::matchesFilterRange(const BlockFilter& filter, SpanValue min, SpanValue max, DATATYPE datatype) {
  for (auto filter_span : filter.spans) {
    if (filter_span.startBoundary == FSB_NONE && filter_span.endBoundary == FSB_NONE) {
      return true;
    }
    int min_res = 0, max_res = 0;
    switch (datatype) {
      case DATATYPE::BYTE:
      case DATATYPE::BOOL:
      case DATATYPE::CHAR:
      case DATATYPE::BINARY:
      case DATATYPE::STRING:
      case DATATYPE::VARBINARY:
      case DATATYPE::VARSTRING: {
        k_int32 ret = 0;
        if (filter_span.endBoundary != FSB_NONE) {
          k_int32 min_len = std::min(min.len, filter_span.end.len);
          ret = memcmp(min.data, filter_span.end.data, min_len);
          min_res = (ret == 0) ? (min.len - filter_span.end.len) : ret;
        }

        if (filter_span.startBoundary != FSB_NONE) {
          k_int32 max_len = std::min(max.len, filter_span.start.len);
          ret = memcmp(max.data, filter_span.start.data, max_len);
          max_res = (ret == 0) ? (max.len - filter_span.start.len) : ret;
        }
        break;
      }
      case DATATYPE::INT8:
      case DATATYPE::INT16:
      case DATATYPE::INT32:
      case DATATYPE::INT64:
      case DATATYPE::TIMESTAMP:
      case DATATYPE::TIMESTAMP64:
      case DATATYPE::TIMESTAMP64_MICRO:
      case DATATYPE::TIMESTAMP64_NANO: {
        min_res = (min.ival > filter_span.end.ival) ? 1 : ((min.ival < filter_span.end.ival) ? -1 : 0);
        max_res = (max.ival > filter_span.start.ival) ? 1 : ((max.ival < filter_span.start.ival) ? -1 : 0);
        break;
      }
      case DATATYPE::FLOAT: {
        bool is_min_equal = FLT_EQUAL(min.dval, filter_span.end.dval);
        bool is_max_equal = FLT_EQUAL(max.dval, filter_span.start.dval);
        min_res = is_min_equal ? 0 : ((min.dval > filter_span.end.dval) ? 1 : -1);
        max_res = is_max_equal ? 0 : ((max.dval > filter_span.start.dval) ? 1 : -1);
        break;
      }
      case DATATYPE::DOUBLE: {
        min_res = (min.dval > filter_span.end.dval) ? 1 : ((min.dval < filter_span.end.dval) ? -1 : 0);
        max_res = (max.dval > filter_span.start.dval) ? 1 : ((max.dval < filter_span.start.dval) ? -1 : 0);
        break;
      }
      default:
        break;
    }
    if (filter_span.startBoundary == FSB_NONE &&
        ((filter_span.endBoundary == FSB_INCLUDE_BOUND && min_res <= 0) ||
        (filter_span.endBoundary == FSB_EXCLUDE_BOUND && min_res < 0))) {
      return true;
    } else if (filter_span.endBoundary == FSB_NONE &&
               ((filter_span.startBoundary == FSB_INCLUDE_BOUND && max_res >= 0) ||
               (filter_span.startBoundary == FSB_EXCLUDE_BOUND && max_res > 0))) {
      return true;
    } else if (!((filter_span.endBoundary == FSB_INCLUDE_BOUND && min_res > 0) ||
                (filter_span.endBoundary == FSB_EXCLUDE_BOUND && min_res >= 0) ||
                (filter_span.startBoundary == FSB_INCLUDE_BOUND && max_res < 0) ||
                (filter_span.startBoundary == FSB_EXCLUDE_BOUND && max_res <= 0))) {
      return true;
    }
  }
  return false;
}

bool TsStorageIterator::getCurBlockSpan(BlockItem* cur_block, std::shared_ptr<MMapSegmentTable>& segment_tbl,
                                        uint32_t* first_row, uint32_t* count) {
  bool has_data = false;
  *count = 0;
  // Sequential read optimization, if the maximum and minimum timestamps of a BlockItem are within the ts_span range,
  // there is no need to determine the timestamps for each BlockItem.
  if (cur_block->is_agg_res_available && cur_block->publish_row_count > 0
      && cur_block->publish_row_count == cur_block->alloc_row_count
      && cur_blockdata_offset_ == 1
      && cur_block->getDeletedCount() == 0
      && cur_block_ts_check_res_ == TimestampCheckResult::FullyContained) {
    has_data = true;
    *first_row = 1;
    *count = cur_block->publish_row_count;
    cur_blockdata_offset_ = *first_row + *count;
  }
  // If it is not achieved sequential reading optimization process,
  // the data under the BlockItem will be traversed one by one,
  // and the maximum number of consecutive data that meets the query conditions will be obtained.
  // The aggregation result of this continuous data will be further obtained in the future.
  while (cur_blockdata_offset_ <= cur_block->alloc_row_count) {
    bool is_deleted = !segment_tbl->IsRowVaild(cur_block, cur_blockdata_offset_);
    // If the data in the *blk_offset row is not within the ts_span range or has been deleted,
    // continue to verify the data in the next row.
    timestamp64 cur_ts = KTimestamp(segment_tbl->columnAddrByBlk(cur_block->block_id, cur_blockdata_offset_ - 1, 0));
    if (is_deleted || !checkIfTsInSpan(cur_ts)) {
      ++cur_blockdata_offset_;
      if (has_data) {
        break;
      }
      continue;
    }

    if (!has_data) {
      has_data = true;
      *first_row = cur_blockdata_offset_;
    }
    ++(*count);
    ++cur_blockdata_offset_;
  }
  return has_data;
}

KStatus TsTableIterator::Next(ResultSet* res, k_uint32* count, timestamp64 ts) {
  *count = 0;
  MUTEX_LOCK(&latch_);
  Defer defer{[&]() { MUTEX_UNLOCK(&latch_); }};

  KStatus s;
  bool is_finished;
  do {
    is_finished = false;
    if (current_iter_ >= iterators_.size()) {
      break;
    }

    s = iterators_[current_iter_]->Next(res, count, &is_finished, ts);
    if (s == FAIL) {
      return s;
    }
    // when is_finished is true,
    // it indicates that a TsStorageIterator iterator query has ended and continues to read the next one.
    if (is_finished) current_iter_++;
  } while (is_finished);

  return KStatus::SUCCESS;
}
}  // namespace kwdbts
