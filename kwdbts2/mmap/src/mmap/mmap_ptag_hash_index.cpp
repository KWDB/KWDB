#include <cmath>
#include <cstring>
#include <thread>
#include "mmap/mmap_hash_index.h"
#include "mmap/mmap_ptag_hash_index.h"
#include "ts_object_error.h"

MMapPTagHashIndex::MMapPTagHashIndex(int key_len, size_t bkt_instances, size_t per_bkt_count) : MMapHashIndex(key_len, bkt_instances, per_bkt_count) {
}

int MMapPTagHashIndex::insert(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) {
    HashCode hash_val = (*hash_func_)(s, len);
    mutexLock();
    std::pair<bool, size_t> do_rehash = is_need_rehash();
    if (do_rehash.first && rehash(do_rehash.second) < 0) {
        LOG_ERROR("rehash failed.");
        return -1;
    }
    // index remap need check here?
    size_t rownum = metaData().m_row_count + 1;  // change metaData
    metaData().m_row_count++;
    ++m_element_count_;
    mutexUnlock();

    size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ -1);
    buckets_[bkt_ins_idx]->Wlock();
    size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);

    dataRlock();
    // HashIndexData* rec = addrHash();
    // write to .ht file
    row(rownum)->hash_val = hash_val;
    row(rownum)->bt_row = tag_table_rowid;
    row(rownum)->tb_version = table_version;
    memcpy(keyvalue(rownum), s, len);

    row(rownum)->next_row = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = rownum;
    dataUnlock();

    buckets_[bkt_ins_idx]->Unlock();

    return 0;
}

std::pair<TableVersionID, TagPartitionTableRowID> MMapPTagHashIndex::read_first(const char *key, int len) {
    HashCode hash_val = (*hash_func_)(key, len);
    size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
    buckets_[bkt_ins_idx]->Rlock();
    size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
    size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);

    dataRlock();
    // HashIndexData* rec = addrHash();
    while (rownum) {
        // if (h_value->hash_val_ == rec[rownum].hash_val_) {
        if (this->compare(key, rownum)) {
            TableVersionID tmp_version = row(rownum)->tb_version;
            TagPartitionTableRowID tmp_part_rowid = row(rownum)->bt_row;
            dataUnlock();
            buckets_[bkt_ins_idx]->Unlock();
            return std::make_pair(tmp_version, tmp_part_rowid);
        }
        rownum = row(rownum)->next_row;
    }
    dataUnlock();
    buckets_[bkt_ins_idx]->Unlock();
    return std::make_pair(INVALID_TABLE_VERSION_ID, INVALID_TABLE_VERSION_ID);
}

std::pair<TableVersionID, TagPartitionTableRowID>  MMapPTagHashIndex::remove(const char *key, int len) {
    size_t hash_val_ = (*hash_func_)(key, len);
    size_t bkt_ins_idx = (hash_val_ >> 56) & (n_bkt_instances_ - 1);
    buckets_[bkt_ins_idx]->Wlock();
    size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val_);

    dataWlock();
    size_t delete_count = 0;
    // HashIndexData* rec = addrHash();
    size_t pre_rownum = 0;
    size_t tmp_rownum = 0;
    TagPartitionTableRowID ret_row = INVALID_TABLE_VERSION_ID;
    TableVersionID ret_tbl_version = INVALID_TABLE_VERSION_ID;
    size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    if (rownum && (hash_val_ == row(rownum)->hash_val &&
                   this->compare(key, rownum))) {
        // matched bucketValue
        pre_rownum = rownum;
        buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = row(rownum)->next_row;
        ret_tbl_version = row(rownum)->tb_version;
        ret_row = row(rownum)->bt_row;
        memset(row(rownum), 0x00, metaData().m_record_size);
        ++delete_count;
        goto end_success;
    }
    if (!rownum) {
        // list empty
        m_element_count_ -= delete_count;
        dataUnlock();
        buckets_[bkt_ins_idx]->Unlock();
        std::string key_str;
        // toHexString(key, len, key_str);
        LOG_WARN("failed to delete key: %s, hash: %lu from hash index %s, "
                 "not find the key",
                 key_str.c_str(), hash_val_, filePath().c_str());
        return std::make_pair(INVALID_TABLE_VERSION_ID, INVALID_TABLE_VERSION_ID);
    }
    // found list node
    pre_rownum = rownum;
    rownum = row(rownum)->next_row;
    while (rownum) {
        tmp_rownum = row(rownum)->next_row;
        if (hash_val_ == row(rownum)->hash_val &&
            this->compare(key, rownum) ) {
            // match node
            row(pre_rownum)->next_row = row(rownum)->next_row;
            ret_tbl_version = row(rownum)->tb_version;
            ret_row = row(rownum)->bt_row;
            ++delete_count;
            memset(row(rownum), 0x00, metaData().m_record_size);
            break;
        }
        pre_rownum = rownum;
        rownum = tmp_rownum;
    }
    end_success:
    m_element_count_ -= delete_count;
    dataUnlock();
    buckets_[bkt_ins_idx]->Unlock();
    return std::make_pair(ret_tbl_version, ret_row);
}

std::pair<TableVersionID, TagPartitionTableRowID> MMapPTagHashIndex::get(const char *s, int len) {
    return read_first(s, len);
}

std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> MMapPTagHashIndex::remove_all(const char *key, int len) {
    size_t hash_val_ = (*hash_func_)(key, len);
    size_t bkt_ins_idx = (hash_val_ >> 56) & (n_bkt_instances_ - 1);
    buckets_[bkt_ins_idx]->Wlock();
    size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val_);
    std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> result;

    dataWlock();
    size_t delete_count = 0;
    // HashIndexData* rec = addrHash();
    size_t pre_rownum = 0;
    size_t tmp_rownum = 0;
    TagPartitionTableRowID ret_row = INVALID_TABLE_VERSION_ID;
    TableVersionID ret_tbl_version = INVALID_TABLE_VERSION_ID;
    size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);
    if (rownum && (hash_val_ == row(rownum)->hash_val &&
                   this->compare(key, rownum))) {
        // matched bucketValue
        pre_rownum = rownum;
        buckets_[bkt_ins_idx]->bucketValue(bkt_idx) = row(rownum)->next_row;
        ret_tbl_version = row(rownum)->tb_version;
        ret_row = row(rownum)->bt_row;
        memset(row(rownum), 0x00, metaData().m_record_size);
        ++delete_count;
        result.emplace_back(std::make_pair(ret_tbl_version, ret_row));
        if (!row(rownum)->next_row) {
            goto end_success;
        }
    }
    if (!rownum) {
        // list empty
        m_element_count_ -= delete_count;
        dataUnlock();
        buckets_[bkt_ins_idx]->Unlock();
        std::string key_str;
        // toHexString(key, len, key_str);
        LOG_WARN("failed to delete key: %s, hash: %lu from hash index %s, "
                 "not find the key",
                 key_str.c_str(), hash_val_, filePath().c_str());
        return result;
    }
    // found list node
    pre_rownum = rownum;
    rownum = row(rownum)->next_row;
    while (rownum) {
        tmp_rownum = row(rownum)->next_row;
        if (hash_val_ == row(rownum)->hash_val &&
            this->compare(key, rownum) ) {
            // match node
            row(pre_rownum)->next_row = row(rownum)->next_row;
            ret_tbl_version = row(rownum)->tb_version;
            ret_row = row(rownum)->bt_row;
            ++delete_count;
            result.emplace_back(std::make_pair(ret_tbl_version, ret_row));
            memset(row(rownum), 0x00, metaData().m_record_size);
        }
        pre_rownum = rownum;
        rownum = tmp_rownum;
    }
    end_success:
    m_element_count_ -= delete_count;
    dataUnlock();
    buckets_[bkt_ins_idx]->Unlock();
    return result;
}

int MMapPTagHashIndex::get_all(const char *s, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) {
    return read_all(s, len, result);
}

int MMapPTagHashIndex::read_all(const char *key, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) {
    HashCode hash_val = (*hash_func_)(key, len);
    size_t bkt_ins_idx = (hash_val >> 56) & (n_bkt_instances_ - 1);
    buckets_[bkt_ins_idx]->Rlock();
    size_t bkt_idx = buckets_[bkt_ins_idx]->get_bucket_index(hash_val);
    size_t rownum = buckets_[bkt_ins_idx]->bucketValue(bkt_idx);

    dataRlock();
    // HashIndexData* rec = addrHash();
    while (rownum) {
        // if (h_value->hash_val_ == rec[rownum].hash_val_) {
        if (this->compare(key, rownum)) {
            TableVersionID tmp_version = row(rownum)->tb_version;
            TagPartitionTableRowID tmp_part_rowid = row(rownum)->bt_row;
            dataUnlock();
            buckets_[bkt_ins_idx]->Unlock();
            result.emplace_back(std::make_pair(tmp_version, tmp_part_rowid));
            rownum = row(rownum)->next_row;
        }
        rownum = row(rownum)->next_row;
    }
    dataUnlock();
    buckets_[bkt_ins_idx]->Unlock();
    return 0;
}

int MMapPTagHashIndex::open(const std::string &path, const std::string &db_path, const std::string &tbl_sub_path,
                        int flags, ErrorInfo &err_info) {
    if (flags & O_CREAT) {
        size_t new_file_size = (k_Hash_Default_Row_Count + 1) * m_record_size_ + kHashMetaDataSize;
        err_info.errcode = MMapFile::open(path, db_path + tbl_sub_path + path, flags, new_file_size, err_info);
    } else {
        err_info.errcode = MMapFile::open(path, db_path + tbl_sub_path + path, flags);
    }
    if (err_info.errcode < 0)
        return err_info.errcode;
    if (file_length_ < kHashMetaDataSize)
        err_info.errcode = mremap(kHashMetaDataSize);
    if (err_info.errcode < 0)
        return err_info.errcode;
    if (file_length_ >= kHashMetaDataSize) {
        mem_hash_ = addrHash();
        if (metaData().m_row_count) {
            resizeBucket(metaData().m_bucket_count);
            loadRecord(1, metaData().m_row_count);
            m_element_count_ = metaData().m_row_count;
        }
    }
    metaData().m_bucket_count = m_bucket_count_;
    metaData().m_record_size = m_record_size_;
    return err_info.errcode;
}