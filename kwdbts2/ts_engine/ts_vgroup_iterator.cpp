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

#include "ts_vgroup_iterator.h"

namespace kwdbts {
TsBlockSegmentIterator::~TsBlockSegmentIterator() {
}

KStatus TsBlockSegmentIterator::Init() {
  std::vector<TsBlockSegmentBlockItem*> blk_items;
  return KStatus::FAIL;
}

KStatus TsBlockSegmentIterator::Next(IterResultSet* res, bool* is_finished, timestamp64 ts) {
  return KStatus::FAIL;
}

}  //  namespace kwdbts

