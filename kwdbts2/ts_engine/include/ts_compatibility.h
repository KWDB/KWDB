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
#include <cstdint>

// block version control
constexpr static uint32_t INVALID_BLOCK_VERSION = -1;
constexpr static uint32_t CURRENT_BLOCK_VERSION = 1;
constexpr static uint32_t BLOCK_VERSION_LIMIT = CURRENT_BLOCK_VERSION + 1;

// batch version control
constexpr static uint32_t INVALID_BATCH_VERSION = -1;
constexpr static uint32_t CURRENT_BATCH_VERSION = 1;
constexpr static uint32_t BATCH_VERSION_LIMIT = CURRENT_BATCH_VERSION + 1;

constexpr static uint32_t CURRENT_SNAPSHOT_VERSION = 2;
constexpr static uint32_t SNAPSHOT_VERSION_LIMIT = CURRENT_SNAPSHOT_VERSION + 1;
