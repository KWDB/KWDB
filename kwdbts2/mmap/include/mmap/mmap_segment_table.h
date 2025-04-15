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

#include <sys/stat.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include "payload.h"
#include "entity_block_meta_manager.h"
#include "utils/date_time_util.h"
#include "ts_table_object.h"
#include "utils/compress_utils.h"
#include "mmap_entity_block_meta.h"
#include "ts_common.h"

using namespace std;

struct TsBlockFullData {
  uint32_t rows{0};
  BlockItem block_item;
  std::vector<TSSlice> col_block_addr;
  std::list<std::shared_ptr<void>> var_col_values;
  std::list<TSSlice> need_del_mems;

  ~TsBlockFullData() {
    for (auto& del : need_del_mems) {
      free(del.data);
    }
  }
};

// store agg result address of certain block and certain column
struct AggDataAddresses {
  void* count;
  void* min;
  void* max;
  void* sum;
};

inline void noFree(char* data) {}

class MMapSegmentTable : public TSObject, public TsTableObject {
 private:
  KRWLatch rw_latch_;

 protected:
  string name_;
  BLOCK_ID segment_id_;
  vector<MMapFile*> col_files_;
  // max row that has writen. sync to file while closing, and sync from file while opening.
  std::atomic<size_t> actual_writed_count_{0};
  uint64_t reserved_rows_ = 0;
  // block header size
  vector<uint32_t> col_block_header_size_;
  // data block size of every column.
  vector<size_t> col_block_size_;
  // varchar or varbinary values store in stringfile, which can remap size.
  MMapStringColumn* m_str_file_{nullptr};
  // pointer of partition's meta_manger
  EntityBlockMetaManager* meta_manager_{nullptr};
  // is this segment compressed.
  bool is_compressed_ = false;
  // max blocks per segment
  uint32_t max_blocks_per_segment_;
  // max rows per block
  uint16_t max_rows_per_block_;
  // null bitmap size of block
  uint16_t block_null_bitmap_size_;
  // whether this segment is the latest opened segment
  bool is_latest_opened_ = true;

  virtual int addColumnFile(int col, int flags, ErrorInfo& err_info);

  int open_(int char_code, const string& file_path, const std::string& db_path, const string& tbl_sub_path,
            int flags, ErrorInfo& err_info);

  int init(EntityBlockMetaManager* meta_manager, const vector<AttributeInfo>& schema, int encoding, ErrorInfo& err_info);

  int initColumn(int flags, ErrorInfo& err_info);

  int setAttributeInfo(vector<AttributeInfo>& info);

  virtual void push_back_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                 size_t payload_column_idx, size_t start_row, size_t num);

  int magic() { return *reinterpret_cast<const int *>("MMET"); }

  /**
   * push_back_var_payload writes variable-length data to the variable-length file: stringfile
   *
   * @param payload Record the data to be written.
   * @param row_id Specify the position of data in the table.
   * @param segment_column Indicate which column the data should be written to.
   * @param payload_column_idx The column corresponding to the payload.
   * @param payload_start_row The starting row number of the payload.
   * @param payload_num The number of data rows to be written.
   * @return Return the operation error code, 0 indicates success, and a negative value indicates failure.
 */
  int push_back_var_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                     size_t payload_column_idx, size_t payload_start_row, size_t payload_num);

  virtual void push_back_null_bitmap(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                     size_t payload_column, size_t payload_start_row, size_t payload_num);

  int putDataIntoVarFile(char* var_value, DATATYPE type, size_t* loc);

  /**
   *  @brief get datablock memory address of certain column, by block index in current segment
   *
   * @param block_idx  block id.
   * @param c          column idx
   * @return Return address
 */
  inline void* internalBlockHeader(BLOCK_ID block_idx, size_t c) const {
    return reinterpret_cast<void*>((intptr_t) col_files_[c]->memAddr() + block_idx * getDataBlockSize(c));
  }

 public:
  MMapSegmentTable();

  virtual ~MMapSegmentTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  /**
   * @brief compressed file path of current segment, file no need must exist.
  */
  inline string getCompressedFilePath() {
    string file_path = db_path_ + tbl_sub_path_;
    if (file_path.back() == '/') {
      file_path = file_path.substr(0, file_path.size() - 1);
    }
    file_path += ".sqfs";
    return file_path;
  }

  inline SegmentStatus getSegmentStatus() {
    return static_cast<SegmentStatus>(TsTableObject::status());
  }

  inline void setSegmentStatus(SegmentStatus s_status) {
    TsTableObject::setStatus(s_status);
  }

  inline void setNotLatestOpened() { is_latest_opened_ = false; }

  inline size_t getBlockMaxRows() const { return max_rows_per_block_; }

  inline uint64_t getBlockMaxNum() { return max_blocks_per_segment_; }

  inline uint64_t getBlockBitmapSize() const { return block_null_bitmap_size_; }

  inline uint64_t getReservedRows() { return max_rows_per_block_ * max_blocks_per_segment_; }

  static size_t GetBlockHeaderSize(size_t max_rows_per_block, size_t col_size) {
    size_t header_size = (max_rows_per_block + 7) / 8 + BLOCK_AGG_COUNT_SIZE + col_size * BLOCK_AGG_NUM;
    return (header_size + 63) / 64 * 64;
  }

  static size_t GetBlockSize(size_t max_rows_per_block, size_t col_size) {
    size_t header_size = (max_rows_per_block + 7) / 8 + BLOCK_AGG_COUNT_SIZE + col_size * BLOCK_AGG_NUM;
    size_t block_size = (header_size + 63) / 64 * 64 + col_size * max_rows_per_block;
    return (block_size + 63) / 64 * 64;
  }

  MMapStringColumn* GetStringFile() { return m_str_file_; }

  size_t GetColBlockHeaderSize(size_t col_size) {
    return GetBlockHeaderSize(max_rows_per_block_, col_size);
  }

  size_t GetColBlockSize(size_t col_size) {
    return GetBlockSize(max_rows_per_block_, col_size);
  }

  void CopyNullBitmap(BLOCK_ID blk_id, size_t col_id, char* desc_mem) {
    char* bitmap = static_cast<char*>(columnNullBitmapAddr(blk_id, col_id));
    memcpy(desc_mem, bitmap, getBlockBitmapSize());
  }


  /**
   * PushPayload is used to write data into files.
   *
   * @param entity_id entity ID.
   * @param start_row identifies the beginning position where the table should be inserted.
   * @param payload   The data to be written.
   * @return int The result status code returned after the operation, 0 for success, non-zero error codes for failure.
   */
  int PushPayload(uint32_t entity_id, MetricRowID start_row, kwdbts::Payload* payload,
                  size_t start_in_payload, const BlockSpan& span,
                  uint32_t* inc_unordered_cnt, kwdbts::DedupInfo& dedup_info);

  /**
   *  @brief copy block from snapshot to segment file  directly.
   *
   * @param block_item  block item.
   * @param block       snapshot block info
   * @param ts_span     used for deleting outrange rows.
   * @return Return address
 */
  int CopyColBlocks(BlockItem* blk_item, const TsBlockFullData& block, KwTsSpan ts_span);

  /**
   *  @brief reset all agg values in column block
   *
   * @param block_item         block item.
   * @param block_header       snapshot block info
   * @param col                column schema info
   * @param var_values         used for var-type column
   * @return Return address
 */
  void ResetBlockAgg(BlockItem* blk_item, char* block_header, const AttributeInfo& col, std::list<std::shared_ptr<void>> var_values);

  /**
   * pushBackToColumn writes data by column, updating the bitmap and aggregate results.
   * Among them, fixed-length data is directly copied, while variable-length data is written to a string file one line at a time,
   * and its offset is recorded in the data block.
   *
   * @param start_row        Start row ID.
   * @param segment_col_idx  Column index where the segment data is written.
   * @param payload_col_idx  Column index where the data is written.
   * @param payload          Data to be written.
   * @param start_in_payload Payload position where writing begins.
   * @param span             Range of data blocks where writing occurs.
   * @param dedup_info       Deduplication information.
   * @return Return error code, 0 indicates success.
   */
  int pushBackToColumn(MetricRowID start_row, size_t segment_col_idx, size_t payload_col_idx,
                   kwdbts::Payload* payload, size_t start_in_payload, const BlockSpan& span,
                   kwdbts::DedupInfo& dedup_info);

  /**
   * @brief .data file format
   *         The .data file is divided into blocks of the same size and distributed to different entities.
   * +---------+---------+---------+---------+---------+---------+
   * | Block 1 | Block 2 | Block 3 | Block 4 |   ...   | Block N |
   * +---------+---------+---------+---------+---------+---------+
   *
   *
   * @brief block format
   *        The beginning of each block stores a null bitmap, counting and maximum/minimum/sum statistics information,
   *        followed by config_block_rows data.
   * +-------------+-------+-----+-----+-----+---------------------------------------------------------+
   * | null bitmap | count | max | min | sum |                       column data                       |
   * +-------------+-------+-----+-----+-----+---------------------------------------------------------+
   *
   * (1) null bitmap: entity_meta_->getBlockBitmapSize() bytes
   * (2) count: 2 bytes
   * (3) max/min/sum: column size
   * (4) column data: config_block_rows_ * column size
   *
   */
  inline size_t getDataBlockSize(uint16_t col_idx) const {
    assert(col_idx < col_block_size_.size());
    return col_block_size_[col_idx];
  }

  /**
   * @brief get block header address.
  */
  inline void* getBlockHeader(BLOCK_ID data_block_id, size_t c) const {
    assert(data_block_id >= segment_id_);
    return internalBlockHeader(data_block_id - segment_id_, c);
  }

  /**
   * @brief get bitmap address of certain column in block
  */
  inline void* columnNullBitmapAddr(BLOCK_ID block_id, size_t c) const {
    if (isColExist(c)) {
      return getBlockHeader(block_id, c);
    } else {
      return nullptr;
    }
  }

  /**
   * @brief get address of certain agg result in block.
   * @param[in] data_block_id   block id
   * @param[in] agg_type        agg type
   * @return void*  agg result address. nullptr if no this agg result
  */
  inline void* columnAggAddr(BLOCK_ID data_block_id, size_t c, kwdbts::Sumfunctype agg_type) const {
    // Calculate the offset of the address where the agg_type aggregation type is located
    size_t agg_offset = 0;
    switch (agg_type) {
      case kwdbts::Sumfunctype::MAX :
        agg_offset = BLOCK_AGG_COUNT_SIZE;
        break;
      case kwdbts::Sumfunctype::MIN :
        agg_offset = BLOCK_AGG_COUNT_SIZE + cols_info_include_dropped_[c].size;
        break;
      case kwdbts::Sumfunctype::SUM :
        agg_offset = BLOCK_AGG_COUNT_SIZE + cols_info_include_dropped_[c].size * 2;
        break;
      case kwdbts::Sumfunctype::COUNT :
        break;
      default:
        return nullptr;
    }
    // agg result address: block addr + null bitmap size + agg_type offset
    size_t offset = getBlockBitmapSize() + agg_offset;
    return reinterpret_cast<void*>((intptr_t) getBlockHeader(data_block_id, c) + offset);
  }

  inline void calculateAggAddr(BLOCK_ID data_block_id, size_t c, AggDataAddresses& addresses) {
    size_t offset = getBlockBitmapSize();
    addresses.count = reinterpret_cast<void*>((intptr_t) getBlockHeader(data_block_id, c) + offset);
    addresses.max = reinterpret_cast<void*>((intptr_t) addresses.count + BLOCK_AGG_COUNT_SIZE);
    addresses.min = reinterpret_cast<void*>((intptr_t) addresses.max + cols_info_include_dropped_[c].size);
    addresses.sum = reinterpret_cast<void*>((intptr_t) addresses.max + cols_info_include_dropped_[c].size * 2);
  }

  bool IsRowVaild(BlockItem* blk_item, k_uint32 cur_row) {
    bool is_deleted;
    if (blk_item->isDeleted(cur_row, &is_deleted) < 0 || is_deleted) {
      return false;
    }
    if (blk_item->alloc_row_count == blk_item->publish_row_count) {
      return true;
    }
    TimeStamp64LSN cur_ts(columnAddr({blk_item->block_id, cur_row}, 0));
    if (isTsWithLSNType((DATATYPE)(cols_info_include_dropped_[0].type))) {
      // lsn = 0, means this row space is not filled with data.
      if (cur_ts.lsn == 0) {
        return false;
      }
    } else {
      // ts = 0, means this row space is not filled with data.
      if (cur_ts.ts64 == 0) {
        return false;
      }
    }
    return true;
  }

  void setNullBitmap(MetricRowID row_id, size_t c) {
    if (!isColExist(c)) {
      return;
    }
    // 0 ~ config_block_rows_ - 1
    size_t row = row_id.offset_row - 1;
    size_t byte = row >> 3;
    size_t bit = 1 << (row & 7);
    char* bitmap = static_cast<char*>(columnNullBitmapAddr(row_id.block_id, c));
    bitmap[byte] |= bit;
  }

  bool isNullValue(MetricRowID row_id, size_t c) const {
    if (!isColExist(c)) {
      return true;
    }
    // 0 ~ config_block_rows_ - 1
    size_t row = row_id.offset_row - 1;
    size_t byte = row >> 3;
    size_t bit = 1 << (row & 7);
    char* bitmap = static_cast<char*>(columnNullBitmapAddr(row_id.block_id, c));
    return bitmap[byte] & bit;
  }

  // check if column exists, by file and column index.
  inline bool isColExist(size_t idx) const {
    if (idx >= col_files_.size()) {
      return false;
    }
    return col_files_[idx] != nullptr;
  }

  bool isBlockFirstRow(MetricRowID row_id) {
    return row_id.offset_row == 1;
  }

  /**
   * @brief update the result of Aggregate except ts column.
   * @param span  Range of data blocks where writing occurs.
   */
  void updateAggregateResult(const BlockSpan& span, bool include_k_timestamp);

  /**
   * @brief update the Aggregate result of the specified colum.
   * @param span             Range of data blocks where writing occurs.
   * @param start_row        Start row ID.
   * @param segment_col_idx  Column index where the segment data is written.
   * @param addresses        Store agg result address of certain block and certain column
   * @param row_num          The number of rows.
   */
  void columnAggCalculate(const BlockSpan& span, MetricRowID start_row, size_t segment_col_idx,
                          AggDataAddresses& addresses, size_t row_num, bool max_rows);


  virtual int create(EntityBlockMetaManager* meta_manager, const vector<AttributeInfo>& schema,
    const uint32_t& table_version, int encoding, ErrorInfo& err_info, uint32_t max_rows_per_block = 0,
    uint32_t max_blocks_per_segment = 0);

  virtual int open(EntityBlockMetaManager* meta_manager, BLOCK_ID segment_id, const string& file_path,
    const std::string& db_path, const string& tbl_sub_path, int flags, bool lazy_open, ErrorInfo& err_info);

  virtual int close(ErrorInfo& err_info);

  virtual void sync(int flags);

  virtual int remove();

  int try_umount();

//  virtual bool isTemporary() const;

  /*--------------------------------------------------------------------
   * data model functions
   *--------------------------------------------------------------------
   */
  virtual uint64_t dataLength() const;

  virtual const vector<AttributeInfo>& getSchemaInfo() const;

  virtual int reserveBase(size_t size);

  virtual int reserve(size_t size);

  virtual int truncate();

  virtual int addColumn(AttributeInfo& col_info, ErrorInfo& err_info);

  // Concurrent insertion optimization requires returning actual records stored in BO
  virtual size_t size() const {
    return actual_writed_count_.load();
  }

  virtual string path() const override ;

  virtual timestamp64& minTimestamp() { return meta_data_->min_ts; }

  virtual timestamp64& maxTimestamp() { return meta_data_->max_ts; }

  virtual uint64_t recordSize() const { return meta_data_->record_size; }

  uint32_t schemaVersion() const { return meta_data_->schema_version; }

  // num of column in this segment
  virtual int numColumn() const { return meta_data_->cols_num; };

  const string & tbl_sub_path() const { return tbl_sub_path_; }

  BLOCK_ID segment_id() const {
    return segment_id_;
  }

  inline void* columnAddr(MetricRowID row_id, size_t c) const {
    // return: block address + bitmap size + aggs size + count size(2 bytes) + row index in block * column value size
    size_t offset_size = col_block_header_size_[c] + cols_info_include_dropped_[c].size * (row_id.offset_row - 1);
    return reinterpret_cast<void*>((intptr_t) internalBlockHeader(row_id.block_id - segment_id_, c) + offset_size);
  }

  // get vartype column value, value is copied from stringfile.
  inline std::shared_ptr<void> varColumnAddr(MetricRowID row_id, size_t c) const {
    size_t offset = *reinterpret_cast<uint64_t*>(columnAddr(row_id, c));
    if (static_cast<SegmentStatus>(TsTableObject::status()) != SegmentStatus::ImmuSegment) {
      m_str_file_->rdLock();
      char* data = m_str_file_->getStringAddr(offset);
      uint16_t len = *(reinterpret_cast<uint16_t*>(data));
      void* var_data = std::malloc(len + MMapStringColumn::kStringLenLen);
      memcpy(var_data, data, len + MMapStringColumn::kStringLenLen);
      std::shared_ptr<void> ptr(var_data, free);
      m_str_file_->unLock();
      return ptr;
    } else {
      std::shared_ptr<void> ptr(m_str_file_->getStringAddr(offset), noFree);
      return ptr;
    }
  }

  // get vartype column agg result address.
  inline std::shared_ptr<void> varColumnAggAddr(MetricRowID row_id, size_t c, kwdbts::Sumfunctype agg_type) const {
    size_t offset = *reinterpret_cast<uint64_t*>(columnAggAddr(row_id.block_id, c, agg_type));
    if (static_cast<SegmentStatus>(TsTableObject::status()) != SegmentStatus::ImmuSegment) {
      m_str_file_->rdLock();
      char* data = m_str_file_->getStringAddr(offset);
      uint16_t len = *(reinterpret_cast<uint16_t*>(data));
      void* var_data = std::malloc(len + MMapStringColumn::kStringLenLen);
      memcpy(var_data, data, len + MMapStringColumn::kStringLenLen);
      std::shared_ptr<void> ptr(var_data, free);
      m_str_file_->unLock();
      return ptr;
    } else {
      std::shared_ptr<void> ptr(m_str_file_->getStringAddr(offset), noFree);
      return ptr;
    }
  }

  inline void* columnAddrByBlk(BLOCK_ID block_id, size_t r, size_t c) const {
    uint64_t offset_row = r;
    BLOCK_ID block_idx = block_id - segment_id_;
    size_t offset_size = col_block_header_size_[c] + cols_info_include_dropped_[c].size * offset_row;
    return reinterpret_cast<void*>((intptr_t) internalBlockHeader(block_idx, c) + offset_size);
  }

  // get vartype column value addrees
  inline std::shared_ptr<void> varColumnAddrByBlk(BLOCK_ID block_id, size_t r, size_t c) const {
    size_t offset = *reinterpret_cast<uint64_t*>(columnAddrByBlk(block_id, r, c));
    if (static_cast<SegmentStatus>(TsTableObject::status()) != SegmentStatus::ImmuSegment) {
      m_str_file_->rdLock();
      char* data = m_str_file_->getStringAddr(offset);
      uint16_t len = *(reinterpret_cast<uint16_t*>(data));
      void* var_data = std::malloc(len + MMapStringColumn::kStringLenLen);
      memcpy(var_data, data, len + MMapStringColumn::kStringLenLen);
      std::shared_ptr<void> ptr(var_data, free);
      m_str_file_->unLock();
      return ptr;
    } else {
      std::shared_ptr<void> ptr(m_str_file_->getStringAddr(offset), noFree);
      return ptr;
    }
  }

  inline AttributeInfo GetColInfo(size_t c) const {
    return cols_info_include_dropped_[c];
  }

  inline uint32_t GetColTypeWithoutHidden(size_t c) const {
    return cols_info_exclude_dropped_[c].type;
  }

  inline uint32_t GetColType(size_t c) const {
    return cols_info_include_dropped_[c].type;
  }

  inline std::vector<AttributeInfo> GetColsInfoWithoutHidden() const {
    return cols_info_exclude_dropped_;
  }

  // check if current segment can writing data
  inline bool canWrite() {
    return !is_compressed_ && getObjectStatus() == OBJ_READY && getSegmentStatus() < InActiveSegment;
  }

  // check if current segment is compressed
  inline bool sqfsIsExists() const {
    return is_compressed_;
  }

  inline void setSqfsIsExists() {
    is_compressed_ = true;
  }

  // check if column values are all null in block.
  inline bool isAllNullValue(BLOCK_ID block_id, size_t count, vector<kwdbts::k_uint32> c) const {
    size_t null_size = (count - 1) / 8 + 1;
    for (auto& col : c) {
      if (!isColExist(col)) {
        continue;
      }
      char* bitmap = static_cast<char*>(getBlockHeader(block_id, col));
      // todo(liangbo01) use at 2.1.0 version for full test.
      // if (!isAllDeleted(bitmap, 1, count)) {
      //   return false;
      // }
      for (int i = 0; i < null_size; ++i) {
        if (i == null_size - 1 && count % 8) {
          for (size_t j = 0 ; j < count % 8 ; ++j) {
            size_t bit = 1 << (j & 7);
            if (!(bitmap[i] & bit)) {
              return false;
            }
          }
        } else {
          if (*(reinterpret_cast<unsigned char*>((intptr_t) bitmap) + i) < 0xFF) {
            return false;
          }
        }
      }
    }
    return true;
  }

  bool TryUnmountSqfs(ErrorInfo& err_info) const {
    if(!is_compressed_) {
      return true;
    }
    return umount(db_path_, tbl_sub_path_, err_info);
  }

  // check if column has valid value in front rows of block.
  inline bool hasValue(MetricRowID start_row, size_t count, size_t c) const {
    if (!isColExist(c)) {
      return false;
    }
    assert(start_row.offset_row > 0);
    // 0 ~ config_block_rows_ - 1
    assert((start_row.offset_row - 1 + count) <= getBlockMaxRows());
    char* bitmap = static_cast<char*>(columnNullBitmapAddr(start_row.block_id, c));
    return !isAllNull(bitmap, start_row.offset_row, count);
  }

  string GetPath();
};

int convertStrToFixed(const std::string& str, DATATYPE new_type, char* data, int32_t old_len,
                      ErrorInfo& err_info);

std::shared_ptr<void> convertFixedToVar(DATATYPE old_type, DATATYPE new_type, char* data, ErrorInfo& err_info);
