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
#include "cm_assert.h"
#include "ee_mempool.h"
#include "kwdb_type.h"
#include "settings.h"

namespace kwdbts {
// init executor func for query
KStatus InitExecutor(kwdbContext_p ctx, const EngineOptions &options);
// destory executor when the instance is destoried
KStatus DestoryExecutor();
// get thread num info
KStatus GetWaitThreadNum(kwdbContext_p ctx, void *resp);
};  // namespace kwdbts
