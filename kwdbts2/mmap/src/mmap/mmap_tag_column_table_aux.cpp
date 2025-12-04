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

#include <sys/types.h>
#include <sys/mman.h>
#include <dirent.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <assert.h>
#include "utils/big_table_utils.h"
#include "sys_utils.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "ts_table.h"

/*
 * @Description: set lsn of file
 * @IN: lsn: the lsn to be set
 * @Return: void
 */
void MMapTagColumnTable::setLSN(kwdbts::TS_OSN lsn) {
  if (m_ptag_file_->memAddr()) {
    reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_lsn = lsn;
  }

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i] && !m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->setLSN(lsn);
    }
  }
  m_bitmap_file_->setLSN(lsn);
  m_meta_file_->setLSN(lsn);
}

/*
 * @Description: get min lsn of files
 * @IN:
 * @Return: min lsn
 */
// kwdbts::TS_OSN MMapTagColumnTable::getLSN() {
//   kwdbts::TS_OSN minLSN = reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_lsn;
//   for (size_t i = 0; i < m_cols_.size(); ++i) {
//     if (!m_cols_[i]->isPrimaryTag()) {
//       minLSN = (m_cols_[i]->getLSN() > minLSN) ? minLSN : m_cols_[i]->getLSN();
//     }
//   }
//   minLSN = (m_bitmap_file_->getLSN() > minLSN)
//     ? minLSN : m_bitmap_file_->getLSN();
//   minLSN = (m_index_->getLSN() > minLSN) ? minLSN : m_index_->getLSN();
//   minLSN = (m_meta_file_->getLSN() > minLSN) ? minLSN : m_meta_file_->getLSN();
// 
//   return minLSN;
// }

/*
 * @Description: set drop flag bit
 * @IN:
 * @Return: void
 */
void MMapTagColumnTable::setDropped() {
  if (m_ptag_file_->memAddr()) {
    reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_droped = true;
  }
}

bool MMapTagColumnTable::isDropped() {
  if (m_ptag_file_->memAddr()) {
    return reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr())->m_droped;
  } else {
    return false;
  }
}

/*
 * @Description: flush files
 * @IN:
 * @Return: 0
 */
void MMapTagColumnTable::sync_with_lsn(kwdbts::TS_OSN lsn) {
  setLSN(lsn);

  m_ptag_file_->sync(MS_SYNC);
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i] && !m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->sync(MS_SYNC);
    }
  }

  m_bitmap_file_->sync(MS_SYNC);
  m_meta_file_->sync(MS_SYNC);

  return ;
}

void MMapTagColumnTable::sync(int flags) {
  m_ptag_file_->sync(flags);
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->sync(flags);
    }
  }

  m_bitmap_file_->sync(flags);
  m_meta_file_->sync(flags);
}

std::unique_ptr<TagTuplePack> MMapTagColumnTable::GenTagPack(TagTableRowID row) {
  vector<TagInfo> schema;
  for (const auto& col: m_tag_info_include_dropped_) {
    if (col.isDropped()) {
     continue;
    }
    schema.push_back(col);
  }

  TagTuplePack* packer = KNEW TagTuplePack(schema);
  if (nullptr == packer) {
    LOG_ERROR("new TagTuplePack failed,out of memory");
    return nullptr;
  }
  const char *valPtr = nullptr;
  size_t valLen = 0;
  this->startRead();
  for (int col = 0; col < m_cols_.size(); ++col) {
    if (nullptr == m_cols_[col]) {
      continue;
    }
    if (m_cols_[col]->attributeInfo().m_tag_type == PRIMARY_TAG) {
      valPtr = columnValueAddr(row, col);
      valLen = m_cols_[col]->attributeInfo().m_length;
    } else {
      bool isnull = isNull(row, col);
      if (isnull) {
	      valPtr = nullptr;
	      valLen = 0;
      } else {
	      if (m_cols_[col]->attributeInfo().m_data_type == DATATYPE::VARSTRING ||
	          m_cols_[col]->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
          size_t var_start_offset = getVarOffset(row, col);
          if (UNLIKELY(var_start_offset < MMapStringColumn::startLoc())) {
            valPtr = nullptr;
            valLen = 0;
            if (packer->fillData(valPtr, valLen) < 0) {
              LOG_ERROR("TagTuplePack fillData failed.");
              delete packer;
              this->stopRead();
              return nullptr;
            }
            continue;
          }
  
          m_cols_[col]->varRdLock();
          char* var_data_ptr = m_cols_[col]->getVarValueAddrByOffset(var_start_offset);
          valLen = *reinterpret_cast<uint16_t*>(var_data_ptr) - kEndCharacterLen;
          valPtr = var_data_ptr + kStringLenLen;
          if (packer->fillData(valPtr, valLen) < 0) {
            LOG_ERROR("TagTuplePack fillData failed.");
            delete packer;
            this->stopRead();
            return nullptr;
          }
          m_cols_[col]->varUnLock();
          continue;
	      } else {
	        valPtr = m_cols_[col]->rowAddrNoNullBitmap(row);
	        valLen = m_cols_[col]->attributeInfo().m_length;
	      }
      }
    }
    if (packer->fillData(valPtr, valLen) < 0) {
      LOG_ERROR("TagTuplePack fillData failed.");
      delete packer;
      this->stopRead();
      return nullptr;
    }
  }

  uint32_t entityId = 0;
  uint32_t subgroupId = 0;
  const char* idPtr = entityIdStoreAddr(row);
  entityId = *reinterpret_cast<const uint32_t *>(idPtr);
  subgroupId = *reinterpret_cast<const uint32_t *>(idPtr + 4);
  this->stopRead();
  packer->setOSN(getTagDataInfoByRowNum(row)->osn);
  packer->setEntityId(entityId);
  packer->setSubgroupId(subgroupId);
  packer->setVersion(metaData().m_ts_version);

  return std::unique_ptr<TagTuplePack>(packer);
}





constexpr size_t TagTuplePack::versionOffset_;
constexpr size_t TagTuplePack::txnOffset_;
constexpr size_t TagTuplePack::dbIdOffset_;
constexpr size_t TagTuplePack::tabIdOffset_;
constexpr size_t TagTuplePack::entityIdOffset_;
constexpr size_t TagTuplePack::subgroupIdOffset_;
constexpr size_t TagTuplePack::flagOffset_;
constexpr size_t TagTuplePack::dataOffset_;

void TagTuplePack::calcDataMaxSizeAndLens() {
  dataMaxSize_ = dataOffset_;
  for (int i = 0; i < schema_.size(); i++) {
    if (schema_[i].m_tag_type == PRIMARY_TAG) {
      // the defined length, even if the actual length is short.
      priTagLen_ += schema_[i].m_length;
      if (flag_ == TTPFLAG_ALL) {
        fixTagLen_ += schema_[i].m_length;
      }
    } else if (schema_[i].m_tag_type == GENERAL_TAG) {
      if (flag_ == TTPFLAG_ALL) {
        if (schema_[i].m_data_type == DATATYPE::VARSTRING ||
	    schema_[i].m_data_type == DATATYPE::VARBINARY) {
	  // fixed part, offset: is from tag beginning, the first byte after
	  // tag length.
	  fixTagLen_ += 8;
	  varTagLen_ += 2 + schema_[i].m_length; // length+data
	} else {
	  fixTagLen_ += schema_[i].m_length;
	}
      }
    }
  }

  dataMaxSize_ += 2; // primary tag Len: don't contain the 2bytes length.
  dataMaxSize_ += priTagLen_;
  if (flag_ == TTPFLAG_ALL) {
    // bitMap contains the bits for primary tags.
    bitMapLen_ = (schema_.size() + 7) / 8;
    dataMaxSize_ += bitMapLen_;
    dataMaxSize_ += fixTagLen_ + varTagLen_;
    dataMaxSize_ += 4; // tagLen: don't contain the 4bytes length.
  }

  return;
}

void TagTuplePack::setVersion(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + versionOffset_) = id;
  }
}

uint32_t TagTuplePack::getVersion() {
  return *reinterpret_cast<uint32_t *>(data_ + versionOffset_);
}

void TagTuplePack::setOSN(uint64_t osn) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + txnOffset_) = osn;
  }
}

uint32_t TagTuplePack::getOSN() {
  return *reinterpret_cast<uint64_t *>(data_ + txnOffset_);
}

void TagTuplePack::setDBId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + dbIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getDBId() {
  return *reinterpret_cast<uint32_t *>(data_ + dbIdOffset_);
}

void TagTuplePack::setTabId(uint64_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint64_t *>(data_ + tabIdOffset_) = id;
  }
}

uint64_t TagTuplePack::getTabId() {
  return *reinterpret_cast<uint64_t *>(data_ + tabIdOffset_);
}

void TagTuplePack::setEntityId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + entityIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getEntityId() {
  return *reinterpret_cast<uint32_t *>(data_ + entityIdOffset_);
}

void TagTuplePack::setSubgroupId(uint32_t id) {
  if (isMemOwner_ && data_ != nullptr) {
    *reinterpret_cast<uint32_t *>(data_ + subgroupIdOffset_) = id;
  }
}

uint32_t TagTuplePack::getSubgroupId() {
  return *reinterpret_cast<uint32_t *>(data_ + subgroupIdOffset_);
}

void TagTuplePack::initFlag() {
  flag_ = static_cast<TTPFlag>(*reinterpret_cast<short *>(data_ + flagOffset_));
}

void TagTuplePack::setFlag() {
  *reinterpret_cast<short *>(data_ + flagOffset_) = static_cast<short>(flag_);
}

void TagTuplePack::setNullBitMap() {
  int bytePos = curTag_ / 8;
  int bitPos = curTag_ % 8;

  uint8_t *ptr = reinterpret_cast<uint8_t *>(data_ + curBitMapOffset_);
  ptr[bytePos] = ptr[bytePos] | static_cast<uint8_t>(1 << bitPos);
}

int TagTuplePack::fillData(const char *val, size_t len) {
  if (! isMemOwner_) {
    LOG_ERROR("TagTuplePack's isMemOwner_ is false.");
    return -1;
  }

  if (curTag_ == 0) {
    if (data_ == nullptr) {
      data_ = new char[dataMaxSize_];
      if (data_ == nullptr) {
        LOG_ERROR("malloc TagTuplePack data failed.");
	      return -1;
      }
    }
    memset(data_, 0, dataMaxSize_);
    setFlag();

    dataLen_ = 0;
    curPriTagOffset_ = dataOffset_ + 2;
    curBitMapOffset_ = curPriTagOffset_ + priTagLen_ + 4;
    curTagOffset_ = curBitMapOffset_ + bitMapLen_;
    curVarOffset_ = curTagOffset_ + fixTagLen_;
  }

  // while (curTag_ < schema_.size()) {
  //   if ((flag_ == TTPFLAG_PRITAG) && (schema_[curTag_].m_tag_type != PRIMARY_TAG)) {
  //     ++curTag_; // skip general tags
  //   } else {
  //     break;
  //   }
  // }

  if (schema_[curTag_].m_tag_type == PRIMARY_TAG) {
    fillParimary(val, len, schema_[curTag_].m_length);
    ++curTag_;
  } else if (schema_[curTag_].m_tag_type == GENERAL_TAG) {
    if ((schema_[curTag_].m_data_type == DATATYPE::VARSTRING) ||
	      (schema_[curTag_].m_data_type == DATATYPE::VARBINARY)) {
      fillTag(val, len, schema_[curTag_].m_length, true);
    } else {
      fillTag(val, len, schema_[curTag_].m_length, false);
    }
    ++curTag_;
  }

  if (curTag_ == schema_.size()) {
    curTag_ = 0;
    *reinterpret_cast<uint16_t *>(data_ + dataOffset_) =
      static_cast<uint16_t>(priTagLen_);
    if (flag_ == TTPFLAG_PRITAG) {
      dataLen_ = curPriTagOffset_;
    } else if (flag_ == TTPFLAG_ALL) {
      *reinterpret_cast<uint32_t *>(data_ + curPriTagOffset_) =
	static_cast<uint32_t>(curVarOffset_ - curPriTagOffset_ - 4);
      dataLen_ = curVarOffset_;
    }
  }

  return 0;
}

void TagTuplePack::fillParimary(const char *val, size_t len, size_t defLen) {
  size_t copyLen = (len < defLen) ? len : defLen;
  memcpy(data_ + curPriTagOffset_, val, copyLen);
  curPriTagOffset_ += defLen;

  if (flag_ == TTPFLAG_ALL) {
    memcpy(data_ + curTagOffset_, val, copyLen);
    curTagOffset_ += defLen;
  }

  return;
}

void TagTuplePack::fillTag(const char *val, size_t len, size_t defLen,
			    bool isVar) {
    size_t copyLen = (len < defLen) ? len : defLen;
    if (isVar) {
      if (val != nullptr) {
        // offset is from tag
        uint64_t offset = curVarOffset_ - curBitMapOffset_;
        memcpy(data_ + curTagOffset_, &offset, 8);
        curTagOffset_ += 8;

        uint16_t len0 = (uint16_t)copyLen;
        memcpy(data_ + curVarOffset_, &len0, 2);
        curVarOffset_ += 2;

        memcpy(data_ + curVarOffset_, val, copyLen);
        curVarOffset_ += copyLen;
      } else {
        // don't set the offset value.
        curTagOffset_ += 8;
        setNullBitMap();
      }
    } else {
      if (val != nullptr) {
        memcpy(data_ + curTagOffset_, val, copyLen);
        curTagOffset_ += defLen;
      } else {
        // Fixed-length data occupies storage space even if it is null
        curTagOffset_ += defLen;
        setNullBitMap();
      }
    }

    return;
}

const TSSlice TagTuplePack::getData() {
  return TSSlice{data_, dataLen_};
}

const TSSlice TagTuplePack::getPrimaryTags() {
  size_t len = *reinterpret_cast<short *>(data_ + dataOffset_);
  return TSSlice{data_ + dataOffset_ + 2, len};
}

const TSSlice TagTuplePack::getTags() {
  if (flag_ == TTPFLAG_ALL) {
    size_t priLen = *reinterpret_cast<short *>(data_ + dataOffset_);

    size_t tagOffset = dataOffset_ + 2 + priLen;
    size_t tagLen = *reinterpret_cast<uint32_t *>(data_ + tagOffset);
    return TSSlice{data_ + tagOffset + 4, tagLen};
  } else {
    return TSSlice{nullptr, 0};
  }
}

TagTuplePack::TagTuplePack(TagTuplePack &&tag) noexcept
    : schema_(tag.schema_),
      data_(tag.data_),
      dataLen_(tag.dataLen_),
      dataMaxSize_(tag.dataMaxSize_),
      flag_(tag.flag_),
      isMemOwner_(tag.isMemOwner_),
      priTagLen_(tag.priTagLen_),
      bitMapLen_(tag.bitMapLen_),
      fixTagLen_(tag.fixTagLen_),
      varTagLen_(tag.varTagLen_) {
  tag.data_ = nullptr;
}

TagTuplePack& TagTuplePack::operator=(TagTuplePack &&tag) noexcept {
  if (this != &tag) {
    free();
    schema_ = tag.schema_;
    data_ = tag.data_;
    dataLen_ = tag.dataLen_;
    dataMaxSize_ = tag.dataMaxSize_;
    flag_ = tag.flag_;
    isMemOwner_ = tag.isMemOwner_;
    priTagLen_ = tag.priTagLen_;
    bitMapLen_ = tag.bitMapLen_;
    fixTagLen_ = tag.fixTagLen_;
    varTagLen_ = tag.varTagLen_;

    tag.data_ = nullptr;
  }

  return *this;
}

void TagTuplePack::free() {
  if (isMemOwner_ && (data_ != nullptr)) {
    delete[] data_;
  }
}
