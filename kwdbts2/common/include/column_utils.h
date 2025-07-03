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
#include <cstdint>
#include "data_type.h"
#include "me_metadata.pb.h"

class TagInfo;


bool ParseToAttributeInfo(const roachpb::KWDBKTSColumn& col, AttributeInfo& attr_info, bool first_col);

bool ParseToColumnInfo(struct AttributeInfo& attr_info, roachpb::KWDBKTSColumn& col);

bool ParseTagColumnInfo(struct TagInfo& tag_info, roachpb::KWDBKTSColumn& col);
