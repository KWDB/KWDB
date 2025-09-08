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
#include <cstdint>
#include <memory>

#include "data_type.h"
#include "libkwdbts2.h"
namespace kwdbts {

class ColumnBlock {
 public:
  virtual ~ColumnBlock() {}
  virtual bool Valid() const = 0;
  virtual void NextRow() = 0;
  virtual void SeekRowExactly(uint32_t row) = 0;

  virtual void SeekToFirstRow() { this->SeekRowExactly(0); }
  virtual void SeekToLastRow() { this->SeekRowExactly(this->NRows() - 1); }

  virtual const char *GetAddress() const = 0;
  virtual uint32_t NRows() const = 0;

  virtual const char *Begin() const = 0;
  virtual const char *End() const = 0;

  virtual timestamp64 GetTimeStamp() const = 0;
};
class TsColumnBlockIterator {
 public:
  virtual ~TsColumnBlockIterator() {}
  virtual bool Valid() const = 0;
  virtual void NextColumn() = 0;
  virtual void SeekColumn(uint32_t column_id) = 0;
  virtual uint32_t NColumn() const = 0;

  virtual void NextBlock() = 0;
  virtual void SeekToFirstBlock() = 0;
  virtual uint32_t NBlock() const = 0;

  virtual std::unique_ptr<ColumnBlock> GetColumnBlock() const = 0;
};

class TsInnerIterator {
 protected:
  // ColBlocks guards the continuous column data

  TSTableID table_id_ = -1;
  TSEntityID entity_id = -1;
  uint32_t version_ = -1;  // initialize as invalid

 public:
  virtual ~TsInnerIterator() {}
  virtual bool Valid() const = 0;

  virtual void NextEntity() = 0;
  virtual void SeekEntity(TSEntityID) = 0;
  // virtual void SeekToLastEntity() = 0;
  virtual void SeekToFirstEntity() { this->SeekEntity(0); }

  virtual void NextTable() = 0;
  virtual void SeekTable(TSTableID) = 0;
  // virtual void SeekToLastTable() = 0;
  virtual void SeekToFirstTable() { this->SeekTable(0); }

  // TODO(zhangzirui): ignore version

  // virtual void PrevVersion() = 0;
  // // Position at the version in the source that equals the target version exactly,
  // // return true iff the target version exist, otherwise return false;
  // // After call, the iterator is Valid() if return true;
  // virtual bool SeekVersionExactly(uint64_t version_id) = 0;
  // virtual void SeekToLatestVersion() = 0;

  virtual std::unique_ptr<TsColumnBlockIterator> GetColumnBlockIterator() const = 0;

  TSTableID GetTableID() const { return table_id_; }
  TSEntityID GetEntityID() const { return entity_id; }
  uint32_t GetVersion() const { return version_; }
};
}  // namespace kwdbts
