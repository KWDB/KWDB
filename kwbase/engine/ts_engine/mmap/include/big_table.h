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

#pragma once

// the highest two bytes  0xXX------ is reserved
enum TableType {
  ROW_TABLE =             0x00000000,
  NO_DEFAULT_TABLE =      0x00000002,     // auxiliary table without default
  TAG_TABLE =             0x00000008,
  NULLBITMAP_TABLE =      0x00800000,
  ENTITY_TABLE     =      0x00200000,
  UNKNOWN_TABLE    =      0xff000000,
};

#define is_deletable(x)     ((x & DELETABLE_TABLE) != 0)


namespace kwdbts {
class KeyMaker;
class KeyPair;
};
