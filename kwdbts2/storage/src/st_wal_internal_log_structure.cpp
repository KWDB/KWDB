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
#include "st_wal_internal_log_structure.h"

namespace kwdbts {

LogEntry::LogEntry(TS_LSN lsn, WALLogType type, uint64_t x_id)
    : lsn_(lsn), type_(type), x_id_(x_id), len_(0) {
}

WALLogType LogEntry::getType() {
  return type_;
}

char* LogEntry::encode() {
  return nullptr;
}

TS_LSN LogEntry::getLSN() const {
  return lsn_;
}

size_t LogEntry::getLen() {
  return len_;
}

uint64_t LogEntry::getXID() const {
  return x_id_;
}

InsertLogEntry::InsertLogEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type)
    : LogEntry(lsn, type, x_id), table_type_(table_type) {
}

size_t InsertLogEntry::getLen() {
  return LogEntry::getLen();
}

WALTableType InsertLogEntry::getTableType() {
  return table_type_;
}

InsertLogTagsEntry::InsertLogTagsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                       int64_t time_partition, uint64_t offset, uint64_t length, char* data)
    : InsertLogEntry(lsn, type, x_id, table_type), time_partition_(time_partition), offset_(offset), length_(length) {
  data_ = KNEW char[length_];
  memcpy(data_, data, length_);
}

InsertLogTagsEntry::~InsertLogTagsEntry() {
  delete[] data_;
}

size_t InsertLogTagsEntry::getLen() {
  if (len_ == 0) {
    len_ = sizeof(type_) +
           sizeof(x_id_) +
           sizeof(time_partition_) +
           sizeof(table_type_) +
           sizeof(offset_) +
           sizeof(length_) +
           length_;
  }
  return len_;
}

TSSlice InsertLogTagsEntry::getPayload() {
  return TSSlice{data_, length_};
}

InsertLogMetricsEntry::InsertLogMetricsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                             int64_t time_partition, uint64_t offset, uint64_t length,
                                             char* data, size_t p_tag_len, char* encoded_primary_tags)
    : InsertLogEntry(lsn, type, x_id, table_type), time_partition_(time_partition),
      offset_(offset), length_(length), p_tag_len_(p_tag_len) {
  encoded_primary_tags_ = KNEW char[p_tag_len_];
  memcpy(encoded_primary_tags_, encoded_primary_tags, p_tag_len_);

  data_ = KNEW char[length_];
  memcpy(data_, data, length_);
}

InsertLogMetricsEntry::InsertLogMetricsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                             int64_t time_partition, uint64_t offset, uint64_t length,
                                             size_t p_tag_len, char* data)
    : InsertLogEntry(lsn, type, x_id, table_type), time_partition_(time_partition),
      offset_(offset), length_(length), p_tag_len_(p_tag_len) {
  encoded_primary_tags_ = KNEW char[p_tag_len_];
  memcpy(encoded_primary_tags_, data, p_tag_len_);

  data_ = KNEW char[length_];
  memcpy(data_, data + p_tag_len_, length_);
}

InsertLogMetricsEntry::~InsertLogMetricsEntry() {
  delete[] data_;
  delete[] encoded_primary_tags_;
}

size_t InsertLogMetricsEntry::getLen() {
  if (len_ == 0) {
    len_ = sizeof(type_) +
           sizeof(x_id_) +
           sizeof(table_type_) +
           sizeof(time_partition_) +
           sizeof(offset_) +
           sizeof(length_) +
           length_ +
           sizeof(p_tag_len_) +
           p_tag_len_;
  }

  return len_;
}

TSSlice InsertLogMetricsEntry::getPayload() {
  return TSSlice{data_, length_};
}

UpdateLogEntry::UpdateLogEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type)
    : LogEntry(lsn, type, x_id), table_type_(table_type) {
}

size_t UpdateLogEntry::getLen() {
  return LogEntry::getLen();
}

WALTableType UpdateLogEntry::getTableType() {
  return table_type_;
}

UpdateLogTagsEntry::UpdateLogTagsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                       int64_t time_partition, uint64_t offset,
                                       uint64_t length, uint64_t old_len, char* data)
    : UpdateLogEntry(lsn, type, x_id, table_type), time_partition_(time_partition),
    offset_(offset), length_(length), old_len_(old_len) {
  data_ = KNEW char[length_];
  memcpy(data_, data, length_);
  old_data_ = KNEW char[old_len_];
  memcpy(old_data_, data + length_, old_len_);
}

UpdateLogTagsEntry::~UpdateLogTagsEntry() {
  delete[] data_;
  delete[] old_data_;
}

size_t UpdateLogTagsEntry::getLen() {
  if (len_ == 0) {
    len_ = sizeof(type_) +
           sizeof(x_id_) +
           sizeof(time_partition_) +
           sizeof(table_type_) +
           sizeof(offset_) +
           sizeof(length_) +
           sizeof(old_len_) +
           length_ +
           old_len_;
  }
  return len_;
}

TSSlice UpdateLogTagsEntry::getPayload() {
  return TSSlice{data_, length_};
}

TSSlice UpdateLogTagsEntry::getOldPayload() {
  return TSSlice{old_data_, old_len_};
}

DeleteLogEntry::DeleteLogEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type)
    : LogEntry(lsn, type, x_id), table_type_(table_type) {}

WALTableType DeleteLogEntry::getTableType() {
  return table_type_;
}

char* DeleteLogEntry::encode() {
  return nullptr;
}

DeleteLogMetricsEntry::DeleteLogMetricsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                             size_t p_tag_len, uint64_t range_size, char* data)
    : DeleteLogEntry(lsn, type, x_id, table_type), p_tag_len_(p_tag_len), range_size_(range_size) {
  start_ts_ = 0;
  end_ts_ = 0;

  encoded_primary_tags_ = KNEW char[p_tag_len_];
  memcpy(encoded_primary_tags_, data, p_tag_len_);

  row_spans_ = KNEW DelRowSpan[range_size];
  size_t partition_size = range_size * sizeof(DelRowSpan);
  memcpy(row_spans_, data + p_tag_len_, partition_size);
}

DeleteLogMetricsEntry::~DeleteLogMetricsEntry() {
  delete[] encoded_primary_tags_;
  delete[] row_spans_;
  row_spans_ = nullptr;
}

string DeleteLogMetricsEntry::getPrimaryTag() const {
  string p_tag = string(encoded_primary_tags_, p_tag_len_);
  return p_tag;
}

vector<DelRowSpan> DeleteLogMetricsEntry::getRowSpans() const {
  vector<DelRowSpan> partitions(row_spans_, row_spans_ + range_size_);

  return partitions;
}

size_t DeleteLogMetricsEntry::getLen() {
  if (len_ == 0) {
    len_ = sizeof(type_) +
           sizeof(x_id_) +
           sizeof(table_type_) +
           sizeof(p_tag_len_) +
           sizeof(start_ts_) +
           sizeof(end_ts_) +
           sizeof(range_size_);
    len_ += (range_size_) * sizeof(DelRowSpan);
    len_ += p_tag_len_;
  }

  return len_;
}

DeleteLogTagsEntry::DeleteLogTagsEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, WALTableType table_type,
                                       uint32_t group_id, uint32_t entity_id, size_t p_tag_len,
                                       size_t tag_len, char* encoded_data)
    : DeleteLogEntry(lsn, type, x_id, table_type), group_id_(group_id), entity_id_(entity_id),
      p_tag_len_(p_tag_len), tag_len_(tag_len) {
  encoded_primary_tags_ = KNEW char[p_tag_len_];
  memcpy(encoded_primary_tags_, encoded_data, p_tag_len_);
  encoded_tags_ = KNEW char[tag_len_];
  memcpy(encoded_tags_, encoded_data + p_tag_len_, tag_len_);
}

DeleteLogTagsEntry::~DeleteLogTagsEntry() {
  delete[] encoded_primary_tags_;
  delete[] encoded_tags_;
}

TSSlice DeleteLogTagsEntry::getPrimaryTag() const {
  return TSSlice{encoded_primary_tags_, p_tag_len_};
}

TSSlice DeleteLogTagsEntry::getTags() {
  return TSSlice{encoded_tags_, tag_len_};
}

size_t DeleteLogTagsEntry::getLen() {
  if (len_ == 0) {
    len_ = sizeof(type_) +
           sizeof(x_id_) +
           sizeof(table_type_) +
           sizeof(group_id_) +
           sizeof(entity_id_) +
           sizeof(p_tag_len_) +
           sizeof(tag_len_);
    len_ += p_tag_len_ + tag_len_;
  }

  return len_;
}

CheckpointEntry::CheckpointEntry(TS_LSN lsn, WALLogType type, char* data) : LogEntry(lsn, type, 0) {
  size_t read_offset = 0;
  memcpy(&x_id_, data, sizeof(x_id_));
  read_offset += sizeof(x_id_);
  memcpy(&checkpoint_no_, data + read_offset, sizeof(checkpoint_no_));
  read_offset += sizeof(checkpoint_no_);
  memcpy(&tag_offset_, data + read_offset, sizeof(tag_offset_));
  read_offset += sizeof(tag_offset_);
  memcpy(&partition_number_, data + read_offset, sizeof(partition_number_));
  partition_len_ = sizeof(CheckpointPartition) * partition_number_;
  data_ = KNEW CheckpointPartition[partition_len_];

  len_ = header_length + partition_len_;
}


CheckpointEntry::~CheckpointEntry() {
  delete[] data_;
}

size_t CheckpointEntry::getLen() {
  return LogEntry::getLen();
}

size_t CheckpointEntry::getPartitionLen() const {
  return partition_len_;
}

void CheckpointEntry::setCheckpointPartitions(char* data) {
  for (int i = 0; i < partition_number_; i++) {
    memcpy(&data_[i], data + i * (sizeof(CheckpointPartition)), sizeof(CheckpointPartition));
  }
}

MTREntry::MTREntry(TS_LSN lsn, WALLogType type, uint64_t x_id, const char* tsx_id)
    : LogEntry(lsn, type, x_id) {
  len_ = sizeof(type_) + sizeof(x_id_) + TS_TRANS_ID_LEN;
  memcpy(tsx_id_, tsx_id, TS_TRANS_ID_LEN);
}

TTREntry::TTREntry(TS_LSN lsn, WALLogType type, uint64_t x_id, char* tsx_id)
    : LogEntry(lsn, type, x_id) {
  len_ = sizeof(type_) + sizeof(x_id) + TS_TRANS_ID_LEN;

  memcpy(tsx_id_, tsx_id, TS_TRANS_ID_LEN);
}

DDLEntry::DDLEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, uint64_t object_id)
    : LogEntry(lsn, type, x_id), object_id_(object_id) {
}

uint64_t DDLEntry::getObjectID() const {
  return object_id_;
}

DDLCreateEntry::DDLCreateEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, uint64_t object_id, int meta_length,
                               uint64_t range_size, roachpb::CreateTsTable* meta, RangeGroup* ranges)
    : DDLEntry(lsn, type, x_id, object_id) {
  len_ = fixed_length + meta_length + range_size * sizeof(RangeGroup);
}

DDLCreateEntry::DDLCreateEntry(TS_LSN lsn, WALLogType type, char* data) : DDLEntry(lsn, type, 0, 0) {
  size_t read_offset = 0;
  memcpy(&x_id_, data, sizeof(x_id_));
  read_offset += sizeof(x_id_);
  memcpy(&object_id_, data + read_offset, sizeof(object_id_));
  read_offset += sizeof(object_id_);
  memcpy(&meta_length_, data + read_offset, sizeof(meta_length_));
  read_offset += sizeof(meta_length_);
  memcpy(&range_size_, data + read_offset, sizeof(range_size_));

  meta_ = KNEW roachpb::CreateTsTable();
  ranges_ = KNEW RangeGroup[range_size_];
  len_ = fixed_length + meta_length_ + range_size_ * sizeof(RangeGroup);
}

DDLCreateEntry::~DDLCreateEntry() {
  delete meta_;
  delete[] ranges_;
}

roachpb::CreateTsTable* DDLCreateEntry::getMeta() const {
  return meta_;
}

size_t DDLCreateEntry::getLen() {
  return LogEntry::getLen();
}

size_t DDLCreateEntry::getRangeGroupLen() const {
  return range_size_ * range_length;
}

void DDLCreateEntry::setRangeGroups(char* data) {
  for (int i = 0; i < range_size_; i++) {
    memcpy(&ranges_[i].range_group_id, data + i * range_length, sizeof(uint64_t));
    memcpy(&ranges_[i].typ, data + i * range_length + sizeof(uint64_t), sizeof(int8_t));
  }
}

int DDLCreateEntry::getMetaLength() const {
  return meta_length_;
}

DDLDropEntry::DDLDropEntry(TS_LSN lsn, WALLogType type, uint64_t x_id, uint64_t object_id)
    : DDLEntry(lsn, type, x_id, object_id) {
  len_ = LogEntry::LOG_TYPE_SIZE + sizeof(x_id_) + sizeof(object_id_);
}

size_t DDLDropEntry::getLen() {
  return LogEntry::getLen();
}

DDLAlterEntry::DDLAlterEntry(TS_LSN lsn, WALLogType type, char* data) : DDLEntry(lsn, type, 0, 0) {
  size_t read_offset = 0;
  memcpy(&x_id_, data, sizeof(x_id_));
  read_offset += sizeof(x_id_);
  memcpy(&object_id_, data + read_offset, sizeof(object_id_));
  read_offset += sizeof(object_id_);
  memcpy(&alter_type_, data + read_offset, sizeof(alter_type_));
  read_offset += sizeof(alter_type_);
  memcpy(&cur_version_, data + read_offset, sizeof(cur_version_));
  read_offset += sizeof(cur_version_);
  memcpy(&new_version_, data + read_offset, sizeof(new_version_));
  read_offset += sizeof(new_version_);
  memcpy(&length_, data + read_offset, sizeof(length_));

  data_ = KNEW char[length_];
  len_ = fixed_length + length_;
}

DDLAlterEntry::~DDLAlterEntry() {
  delete[] data_;
}

size_t DDLAlterEntry::getLen() {
  return LogEntry::getLen();
}

uint64_t DDLAlterEntry::getLength() const {
  return length_;
}

char* DDLAlterEntry::getData() const {
  return data_;
}

TSSlice DDLAlterEntry::getColumnMeta() const {
  return  TSSlice{data_, length_};
}

AlterType DDLAlterEntry::getAlterType() const {
  return alter_type_;
}

uint32_t DDLAlterEntry::getCurVersion() const {
  return cur_version_;
}

uint32_t DDLAlterEntry::getNewVersion() const {
  return new_version_;
}

CreateIndexEntry::CreateIndexEntry(kwdbts::TS_LSN lsn, kwdbts::WALLogType type, uint64_t x_id, uint64_t object_id,
                                   uint32_t index_id, uint32_t cur_ts_version, uint32_t new_ts_version,
                                   std::array<int32_t, 10> col_ids) :
                                   LogEntry(lsn, type, x_id), object_id_(object_id), index_id_(index_id),
                                   cur_ts_version_(cur_ts_version), new_ts_version_(new_ts_version),
                                   col_ids_(col_ids) {}

std::array<int32_t, 10> CreateIndexEntry::getColIDs() const {
  return col_ids_;
}

uint32_t CreateIndexEntry::getIndexID() const {
  return index_id_;
}

uint32_t CreateIndexEntry::getCurTsVersion() const {
  return cur_ts_version_;
}

uint32_t CreateIndexEntry::getNewTsVersion() const {
  return new_ts_version_;
}

uint64_t CreateIndexEntry::getObjectID() const {
  return object_id_;
}

DropIndexEntry::DropIndexEntry(kwdbts::TS_LSN lsn, kwdbts::WALLogType type, uint64_t x_id, uint64_t object_id,
                               uint32_t index_id, uint32_t cur_ts_version, uint32_t new_ts_version,
                               std::array<int32_t, 10> col_ids) :
                               LogEntry(lsn, type, x_id), object_id_(object_id), index_id_(index_id),
                               cur_ts_version_(cur_ts_version), new_ts_version_(new_ts_version), col_ids_(col_ids) {}

std::array<int32_t, 10> DropIndexEntry::getColIDs() const {
  return col_ids_;
}

uint32_t DropIndexEntry::getIndexID() const {
  return index_id_;
}

uint32_t DropIndexEntry::getCurTsVersion() const {
  return cur_ts_version_;
}

uint32_t DropIndexEntry::getNewTsVersion() const {
  return new_ts_version_;
}

uint64_t DropIndexEntry::getObjectID() const {
  return object_id_;
}

void InsertLogTagsEntry::prettyPrint() {
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "INSERT\t";
  std::cout << "x_id : " << x_id_;
  // EXPECT_EQ(table_type_, WALTableType::TAG);
  std::cout << "tbl_typ : TAG\t";
  std::cout << "time_partition : " << time_partition_ << "\t";
  std::cout << "offset : " << offset_ << "\t";
  std::cout << "length : " << length_ << "\t";
  std::cout << "data : ";
  for (int i = 0; i < length_ && i < 128; i++) {
    if ((data_[i] >= 'a' && data_[i] <= 'z') ||
        (data_[i] >= 'A' && data_[i] <= 'A') ||
        (data_[i] >= '0' && data_[i] <= '9')) {
      std::cout << data_[i];
    } else {
      std::cout << uint8_t(data_[i]);
    }
  }
}

void UpdateLogTagsEntry::prettyPrint() {
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "UPDATE\t";
  std::cout << "x_id : " << x_id_;
  // EXPECT_EQ(table_type_, WALTableType::TAG);
  std::cout << "tbl_typ : TAG\t";
  std::cout << "time_partition : " << time_partition_ << "\t";
  std::cout << "offset : " << offset_ << "\t";
  std::cout << "length : " << length_ << "\t";
  std::cout << "data : ";
  for (int i = 0; i < length_ && i < 128; i++) {
    if ((data_[i] >= 'a' && data_[i] <= 'z') ||
        (data_[i] >= 'A' && data_[i] <= 'A') ||
        (data_[i] >= '0' && data_[i] <= '9')) {
      std::cout << data_[i];
    } else {
      std::cout << uint8_t(data_[i]);
    }
  }
}

void InsertLogMetricsEntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "INSERT\t";
  std::cout << "x_id : " << x_id_ << "\t";
  // EXPECT_EQ(table_type_, WALTableType::TAG);
  std::cout << "tbl_typ : DATA\t";
  std::cout << "time_partition : " << time_partition_ << "\t";
  std::cout << "offset : " << offset_ << "\t";
  std::cout << "length : " << length_ << "\t";
  std::cout << "data : ";
  for (int i = 0; i < length_ && i < 128; i++) {
    if ((data_[i] >= 'a' && data_[i] <= 'z') ||
        (data_[i] >= 'A' && data_[i] <= 'A') ||
        (data_[i] >= '0' && data_[i] <= '9')) {
      std::cout << data_[i];
    } else {
      std::cout << uint8_t(data_[i]);
    }
  }
  std::cout << "encoded_primary_tags\t: ";
  for (int i = 0; i < p_tag_len_ && i < 128; i++) {
    if ((encoded_primary_tags_[i] >= 'a' && encoded_primary_tags_[i] <= 'z') ||
        (encoded_primary_tags_[i] >= 'A' && encoded_primary_tags_[i] <= 'A') ||
        (encoded_primary_tags_[i] >= '0' && encoded_primary_tags_[i] <= '9')) {
      std::cout << encoded_primary_tags_[i];
    } else {
      std::cout << uint8_t(encoded_primary_tags_[i]);
    }
  }
}

string InsertLogMetricsEntry::getPrimaryTag() const {
  string p_tag = string(encoded_primary_tags_, p_tag_len_);
  return p_tag;
}

void CheckpointEntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "CheckPoint\t";
  std::cout << "x_id : " << x_id_ << "\t";
  // EXPECT_EQ(table_type_, WALTableType::TAG);
  // std::cout << "tbl_typ : DATA\t";
  std::cout << "checkpoint_no_ : " << checkpoint_no_ << "\t";
  std::cout << "tag_offset : " << tag_offset_ << "\t";
  std::cout << "partition_number_ : " << partition_number_ << "\t";
  std::cout << "data : ";
  for (int i = 0; i < partition_number_ && i < 128; i++) {
    std::cout << "{ " << data_[i].time_partition << " " << data_[i].offset << " }";
  }
}

void SnapshotEntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "Snapshot\t";
  std::cout << "x_id : " << x_id_ << "\t";
  // EXPECT_EQ(table_type_, WALTableType::TAG);
  // std::cout << "tbl_typ : DATA\t";
  std::cout << "table id : " << table_id_ << "\t";
  std::cout << "hash [" << begin_hash_ << " - " << end_hash_ << "]\t";
  std::cout << "timestamp span ["<< start_ts_ << " - "<< end_ts_ << "]\n";
}

void TempDirectoryEntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "temp directory\t";
  std::cout << "x_id : " << x_id_ << "\t";
  std::cout << "path :" << abs_path_ << "\n";
}

void PartitionTierChangeEntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "partition tier change \t";
  std::cout << "x_id : " << x_id_ << "\t";
  std::cout << "link path :" << link_path_ << "\t";
  std::cout << "tier path :" << tier_path_ << "\n";
}

void MTREntry::prettyPrint() {
  std::cout << "start_lsn : " << lsn_ << "\t";
  std::cout << "typ : ";
  // EXPECT_EQ(type_, WALLogType::INSERT);
  std::cout << "MTR ENTRY\t";
  std::cout << "x_id : " << x_id_ << "\t";
}

void DDLDropEntry::prettyPrint() {
}


MTRBeginEntry::MTRBeginEntry(TS_LSN lsn, uint64_t x_id, const char* tsx_id, uint64_t range_id,
                             uint64_t index) : MTREntry(lsn, WALLogType::MTR_BEGIN, x_id, tsx_id),
                                               range_id_(range_id), index_(index) {
  len_ = LogEntry::LOG_TYPE_SIZE + header_length;
}

char* MTRBeginEntry::encode() {
  return construct(type_, x_id_, tsx_id_, range_id_, index_);
}

uint64_t MTRBeginEntry::getRangeID() const {
  return range_id_;
}

uint64_t MTRBeginEntry::getIndex() const {
  return index_;
}
}  // namespace kwdbts
