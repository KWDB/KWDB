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

#pragma once

#include <string>
#include "kwdb_type.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

class Field;

std::string MarshalToOutputType(KWDBTypeFamily family, k_uint32 width);

KWDBTypeFamily GetInternalOutputType(const std::string &str);

KStatus GetInternalField(const char *buf, k_uint32 length, Field **field, k_int32 seq);

}  // namespace kwdbts
