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

#ifndef MMAP_INCLUDE_MMAP_PTAG_HASH_INDEX_H
#define MMAP_INCLUDE_MMAP_PTAG_HASH_INDEX_H

#include "mmap_hash_index.h"

class MMapPTagHashIndex: public MMapHashIndex {
public:
    explicit MMapPTagHashIndex(int key_len, size_t bkt_instances = 1, size_t per_bkt_count = 1024);
    ~MMapPTagHashIndex() {};

    int insert(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) override;
    std::pair<TableVersionID, TagPartitionTableRowID> read_first(const char *key, int len) override;
    int read_all(const char *key, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override;
    std::pair<TableVersionID, TagPartitionTableRowID> remove(const char *key, int len) override;
    std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> remove_all(const char *key, int len) override;
    std::pair<TableVersionID, TagPartitionTableRowID> get(const char *s, int len) override;
    int get_all(const char *s, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override;

    int open(const std::string &path, const std::string &db_path, const std::string &tbl_sub_path,
                                int flags, ErrorInfo &err_info) override;
};
#endif //MMAP_INCLUDE_MMAP_PTAG_HASH_INDEX_H
