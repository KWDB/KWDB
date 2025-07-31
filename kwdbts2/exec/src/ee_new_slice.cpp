// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_new_slice.h"

#include "ee_fast_string.h"

namespace kwdbts {
static const uint16_t SLICE_MAX_LENGTH = 65535;
static char slice_max_value_data_[SLICE_MAX_LENGTH];
static KSlice slice_max_value_;
static KSlice slice_min_value_;

class SliceInit {
 public:
  SliceInit() { KSlice::init(); }
};

static SliceInit _slice_init;

// NOTE(zc): we define this function here to make compile work.
KSlice::KSlice(const faststring& s)
    :  // NOLINT(runtime/explicit)
      data(const_cast<char *>(reinterpret_cast<const char*>(s.data()))),
      size(s.size()) {}

void KSlice::init() {
  memset(slice_max_value_data_, 0xff, sizeof(slice_max_value_data_));
  slice_max_value_ =
      KSlice(slice_max_value_data_, sizeof(slice_max_value_data_));
  slice_min_value_ = KSlice(const_cast<char*>(""), 0);
}

const KSlice& KSlice::max_value() { return slice_max_value_; }

const KSlice& KSlice::min_value() { return slice_min_value_; }
}  // namespace kwdbts
