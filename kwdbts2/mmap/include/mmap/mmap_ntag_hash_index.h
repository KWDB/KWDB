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

#include <vector>
#include <algorithm>
#include "mmap_hash_index.h"
#include "lib/t1ha.h"

class MMapNTagHashIndex : public MMapHashIndex {
protected:
  uint32_t index_id_;
  std::array<int64_t , 10> tag_col_ids_;

  std::pair<TableVersionID, TagPartitionTableRowID> read_first(const char *key, int len) override;
  int read_all(const char *key, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override;

public:
  explicit MMapNTagHashIndex(int key_len, uint32_t index_id, std::vector<uint32_t> tag_col_ids, size_t bkt_instances = 1, size_t per_bkt_count = 1024);
  ~MMapNTagHashIndex() {};

  int open(const std::string &path, const std::string &db_path, const std::string &tbl_sub_path,
                              int flags, ErrorInfo &err_info);
  int updateKeyLen();
  inline uint32_t getIndexID() const { return index_id_; }
  inline std::vector<uint32_t> getColIDs() const {
    std::vector<uint32_t> ret;
    for (int64_t col_id : tag_col_ids_) {
      if (col_id > 0) {
        ret.emplace_back(col_id);
      }
      sort(ret.begin(), ret.end());
    }
    return ret;
  }
  inline std::array<int64_t , 10> getTagColIDs() const { return tag_col_ids_; }
  int insert(const char *s, int len, TableVersionID table_version, TagPartitionTableRowID tag_table_rowid) override;
  std::pair<TableVersionID, TagPartitionTableRowID> remove(TagPartitionTableRowID tbl_row, TableVersionID table_version,
                                                           const char *key, int len);
  int get_all(const char *s, int len, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result) override;
  // Subclasses only implement interfaces and are not used.
  std::pair<TableVersionID, TagPartitionTableRowID> get(const char *s, int len) override;
  std::pair<TableVersionID, TagPartitionTableRowID> remove(const char *key, int len) override;
  std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> remove_all(const char *key, int len) override;
};


