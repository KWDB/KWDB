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

#include <sys/stat.h>
#include <cstdio>
#include <string>
#include "ts_common.h"
#include "libkwdbts2.h"
#include "settings.h"

namespace kwdbts {

enum WALLogType : uint8_t {
  INSERT = 1,
  UPDATE = 2,
  DELETE = 3,
  CHECKPOINT = 4,
  MTR_BEGIN = 5,
  MTR_COMMIT = 6,
  MTR_ROLLBACK = 7,
  TS_BEGIN = 8,
  TS_COMMIT = 9,
  TS_ROLLBACK = 10,
  DDL_CREATE = 21,
  DDL_DROP = 22,
  DDL_ALTER_COLUMN = 23,
  RANGE_SNAPSHOT = 24,
  SNAPSHOT_TMP_DIRCTORY = 25,
  PARTITION_TIER_CHANGE = 26,
//  DDL_ALTER_TAG = 11,
//  INDEX = 99,
  CREATE_INDEX = 27,
  DROP_INDEX = 28,
  DB_SETTING = 100
};

enum WALTableType : uint8_t {
  DATA = 0,
  TAG = 1
};

enum AlterType : uint8_t {
  ADD_COLUMN = 1,
  DROP_COLUMN = 2,
  ALTER_COLUMN_TYPE = 3,
  ALTER_PARTITION_INTERVAL = 4
};

struct WALMeta {
  uint16_t current_file_no;
  TS_LSN current_lsn;
  TS_LSN block_flush_to_disk_lsn;
  uint16_t checkpoint_file_no;
  uint32_t current_checkpoint_no;
  TS_LSN checkpoint_lsn;
}__attribute__((packed));

// checkpoint time partition
struct CheckpointPartition {
  int64_t time_partition;
  uint64_t offset;
}__attribute__((packed));

const uint32_t BLOCK_SIZE = 4096;
const uint32_t LOG_BLOCK_NUMBER_SIZE = 8;
const uint32_t LOG_BLOCK_DATA_LEN_SIZE = 2;
const uint32_t LOG_BLOCK_FIRST_REC_GRP_SIZE = 2;
const uint32_t LOG_BLOCK_CHECKSUM_SIZE = 4;

const uint32_t LOG_BLOCK_HEADER_SIZE = LOG_BLOCK_NUMBER_SIZE +
                                       LOG_BLOCK_DATA_LEN_SIZE +
                                       LOG_BLOCK_FIRST_REC_GRP_SIZE;

const uint32_t LOG_BLOCK_MAX_LOG_SIZE = BLOCK_SIZE - LOG_BLOCK_HEADER_SIZE - LOG_BLOCK_CHECKSUM_SIZE;
const uint32_t MIN_BLOCK_NUM = 255;  // min number of blocks that one WAL file contains.
}  // namespace kwdbts
