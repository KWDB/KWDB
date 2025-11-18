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

#include <cassert>
#include <memory>
#include <vector>
#include <list>
#include <utility>
#include <algorithm>
#include "ts_ts_lsn_span_utils.h"

namespace kwdbts {

void MergeTsSpans(std::list<KwTsSpan>& raw_spans, std::vector<KwTsSpan>* ret_spans) {
  raw_spans.sort([](KwTsSpan a, KwTsSpan b) -> bool {
    if (a.begin < b.begin) {
      return true;
    } else if (a.begin > b.begin) {
      return false;
    }
    if (a.end < b.end) {
      return true;
    } else if (a.end > b.end) {
      return false;
    }
    return true;
  });
  ret_spans->clear();
  for (auto it = raw_spans.begin(); it != raw_spans.end(); ++it) {
    if (ret_spans->empty()) {
      ret_spans->push_back(*it);
    } else {
      KwTsSpan& last_ts = ret_spans->back();
      if (IsTsSpanCross(last_ts, *it)) {
        last_ts.begin = std::min(last_ts.begin, (*it).begin);
        last_ts.end = std::max(last_ts.end, (*it).end);
      } else {
        ret_spans->push_back(*it);
      }
    }
  }
}

void DeplicateTsSpans(list<STDelRange>& raw_spans, list<STDelRange>* ret_spans) {
  raw_spans.sort([](STDelRange a, STDelRange b) -> bool {
    if (a.osn_span.end < b.osn_span.end) {
      return true;
    }
    return false;
  });
  ret_spans->clear();
  STDelRange last_osn{{0, 0}, {0, 0}};
  for (auto it = raw_spans.begin(); it != raw_spans.end(); ++it) {
    if (it->osn_span.begin != 0) {
      continue;
    }
    if (last_osn.osn_span.end == it->osn_span.end) {
      assert(last_osn.osn_span.begin == it->osn_span.begin);
      assert(last_osn.osn_span.end == it->osn_span.end);
      continue;
    }
    ret_spans->push_back(*it);
  }
}

}  // namespace kwdbts
