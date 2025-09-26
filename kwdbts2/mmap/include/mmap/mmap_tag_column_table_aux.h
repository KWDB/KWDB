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
#include "mmap/mmap_tag_column_table.h"

enum TagTableFileType {
    TTFT_UNKNOWN = -1,
    TTFT_META = 0,
    TTFT_HEADER = 1,
    TTFT_TAG = 2,
    TTFT_INDEX = 3,
    TTFT_STR = 4,
    TTFT_PRIMARYTAG = 5,
    TTFT_HASHPOINT = 6,
};

// void CleanTagFiles(const string dirPath, uint64_t tableId, uint64_t cutoffLsn);

class TagTuplePack
{
public:
  enum TTPFlag {
    TTPFLAG_UNKNOWN = -1,
    TTPFLAG_PRITAG = 0,
    TTPFLAG_ALL = 1,
  };

  TagTuplePack(vector<TagInfo> schema, char *val, size_t len)
    : schema_(schema),
      data_(val),
      dataLen_(len),
      dataMaxSize_(len),
      flag_(TTPFLAG_UNKNOWN),
      isMemOwner_(false) {
      initFlag();
  }

  TagTuplePack(vector<TagInfo> schema, TTPFlag flag = TTPFLAG_ALL)
    : schema_(schema),
      data_(nullptr),
      dataLen_(0),
      dataMaxSize_(0),
      flag_(flag),
      isMemOwner_(true) {
      calcDataMaxSizeAndLens();
  }

  TagTuplePack(TagTuplePack &&tags) noexcept;
  TagTuplePack& operator=(TagTuplePack &&tags) noexcept;

  ~TagTuplePack() {
    free();
  }

  int fillData(const char *val, size_t len);
  const TSSlice getData();

  const TSSlice getPrimaryTags();
  const TSSlice getTags();

  void setVersion(uint32_t id);
  uint32_t getVersion();

  void setTxn(char *val);

  void setOSN(uint64_t osn);
  uint32_t getOSN();
  void setDBId(uint32_t id);
  uint32_t getDBId();
  void setTabId(uint64_t id);
  uint64_t getTabId();
  void setEntityId(uint32_t id);
  uint32_t getEntityId();
  void setSubgroupId(uint32_t id);
  uint32_t getSubgroupId();
private:
  TagTuplePack(const TagTuplePack &) = delete;
  TagTuplePack& operator=(const TagTuplePack &) = delete;

  void calcDataMaxSizeAndLens();
  void setFlag();

  void initFlag();
  void setNullBitMap();
  void fillParimary(const char *val, size_t len, size_t defLen);
  void fillTag(const char *val, size_t len, size_t defLen, bool isVar);

  void free();
private:
  vector<TagInfo> schema_;
  char *data_;
  size_t dataLen_;
  size_t dataMaxSize_;
  TTPFlag flag_;
  bool isMemOwner_;
private:
  size_t priTagLen_ = 0;
  size_t bitMapLen_ = 0;
  size_t fixTagLen_ = 0;
  size_t varTagLen_ = 0;

  size_t curPriTagOffset_ = 0;
  size_t curBitMapOffset_ = 0;
  size_t curTagOffset_ = 0;
  size_t curVarOffset_ = 0;
  size_t curTag_ = 0;
public:
  static constexpr size_t versionOffset_ = 0;
  static constexpr size_t txnOffset_ = 4;
  static constexpr size_t dbIdOffset_ = 20;
  static constexpr size_t tabIdOffset_ = 24;
  static constexpr size_t entityIdOffset_ = 32;
  static constexpr size_t subgroupIdOffset_ = 36;
  static constexpr size_t flagOffset_ = 40;
  static constexpr size_t dataOffset_ = 42;
};


