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

#include "mmap_file.h"

typedef uint32_t TagPartitionTableRowID;
typedef uint32_t TableVersionID;

using TagTableRowID = TagPartitionTableRowID;

class MMapIndex : public MMapFile {
protected:
    int    m_key_len_;

    virtual std::pair<TableVersionID, TagPartitionTableRowID> read_first(const char* key, int len) = 0;

    virtual int read_all(const char* key, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) = 0;

public:

    explicit MMapIndex(int key_len) : m_key_len_(key_len) {};

    MMapIndex() {};

    virtual ~MMapIndex() {};

    virtual int insert(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) = 0;

    virtual std::pair<TableVersionID, TagPartitionTableRowID> remove(const char *key, int len) = 0;

    virtual std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> remove_all(const char *key, int len) = 0;

    /**
     * @brief	find the value stored in hash table for a given key.
     *
     * @param 	key			key to be found.
     * @param 	len			length of the key.
     * @return	the stored value in the hash table if key is found; 0 otherwise.
     */
    virtual std::pair<TableVersionID, TagPartitionTableRowID> get(const char *s, int len) = 0;

    virtual int get_all(const char *s, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) = 0;

    virtual int size() const = 0;
};
