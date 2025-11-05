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
#include <algorithm>
#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"

namespace kwdbts {

inline bool IsTsSpanCross(KwTsSpan& a, KwTsSpan& b) {
  if (a.begin > b.end || a.end < b.begin) {
    return false;
  }
  return true;
}

inline bool IsTsLsnInScanSpan(timestamp64 ts, TS_OSN lsn, const STScanRange& span) {
  if (ts >= span.ts_span.begin && ts <= span.ts_span.end &&
      lsn >= span.osn_span.begin && lsn <= span.osn_span.end) {
    return true;
  }
  return false;
}

inline bool IsTsLsnInSpans(timestamp64 ts, TS_OSN lsn, const std::vector<STScanRange>& spans) {
  for (auto& span : spans) {
    if (IsTsLsnInScanSpan(ts, lsn, span)) {
      return true;
    }
  }
  return false;
}

inline bool IsTsLsnSpanInSpans(const std::vector<STScanRange>& spans,
                                KwTsSpan ts_span, KwOSNSpan osn_span) {
  for (auto& span : spans) {
    if (IsTsLsnInScanSpan(ts_span.begin, osn_span.begin, span) &&
        IsTsLsnInScanSpan(ts_span.end, osn_span.end, span)) {
      return true;
    }
  }
  return false;
}

inline bool IsLsnInSpan(const STScanRange& span, TS_OSN lsn) {
  return (span.osn_span.begin >= lsn && lsn <= span.osn_span.end);
}

inline bool IsOsnInSpan(const KwOSNSpan& osn_span, TS_OSN lsn) {
  return (osn_span.begin <= lsn && lsn <= osn_span.end);
}

inline bool IsOsnInSpans(TS_OSN a, const std::vector<KwOSNSpan>& b) {
  for (auto span : b) {
    if (IsOsnInSpan(span, a)) {
      return true;
    }
  }
  return false;
}

inline bool IsOsnBeforeSpans(TS_OSN a, const std::vector<KwOSNSpan>& b) {
  for (auto span : b) {
    if (span.begin <= a) {
      return false;
    }
  }
  return true;
}

inline bool IsTsLsnSpanCrossSpans(const std::vector<STScanRange>& spans,
                                KwTsSpan ts_span, KwOSNSpan osn_span) {
  for (auto& span : spans) {
    if (ts_span.begin <= span.ts_span.end && ts_span.end >= span.ts_span.begin &&
        osn_span.begin <= span.osn_span.end && osn_span.end >= span.osn_span.begin) {
      return true;
    }
  }
  return false;
}

void MergeTsSpans(std::list<KwTsSpan>& raw_spans, std::vector<KwTsSpan>* ret_spans);

}  // namespace kwdbts
