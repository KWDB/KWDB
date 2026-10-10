// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#ifndef COMMON_SRC_H_SV_AGENT_STATE_H_
#define COMMON_SRC_H_SV_AGENT_STATE_H_


#include "kwdb_type.h"

namespace kwdbts {

enum State {
    IDLE = 0,
    INITIALIZING,
    INITIALIZED,
    LOADING,
    LOADED,
    SANITYCHECKING,
    RECOVERCHECKING,
    RECOVERING,
    RECOVERED,
    RUNNING = 9,  // OPEN Agent.
    CLOSING,
    WAITING,
    UNLOADING,
    UNLOADED,
    RETRYMAX = 98,  // CAN not retry any more.
    HEALTHY = 99
};

}  // namespace kwdbts

#endif  // COMMON_SRC_H_SV_AGENT_STATE_H_
