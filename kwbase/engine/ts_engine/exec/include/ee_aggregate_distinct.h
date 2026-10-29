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
#include <unordered_map>
#include <string>
#include <utility>

#include "ee_field_common.h"
#include "kwdb_type.h"

namespace kwdbts {

class FieldSum;
class DistinctData;
class AggregateRowBatch;
// base agg class
class Aggregator {
 public:
  Aggregator(FieldSum *field, AggregateRowBatch *batch);
  virtual ~Aggregator();

  virtual bool init() = 0;
  virtual bool fill(char *data_ptr, char *null_ptr, k_uint32 row_num) = 0;
  virtual bool update(char *data_ptr, char *null_ptr, k_uint32 row_num) = 0;
  virtual bool endup() = 0;

  DistinctData *new_distinctdata_with_null_ptr();
  DistinctData *new_distinctdata_with_data_ptr();
  DistinctData *new_distinctdata_with_all();

  void fill_group_concat_field(k_uint32 row_num, DistinctData *first);
  bool endup_group_concat();

 public:
  std::unordered_map<k_uint32, DistinctData *> slot_;  // key : row num
  std::unordered_map<k_uint32, char *> group_agg_data_;

 protected:
  FieldSum *field_{nullptr};
  AggregateRowBatch *batch_{nullptr};
  Field **inner_field_{nullptr};
  k_int32 inner_field_num_{0};
  k_uint32 data_len_{0};  // data len
  k_uint32 null_len_{0};  // null len
  bool is_group_concat_{false};
};

// it does not write to disk TODO:liguoliang
class AggregatorDistinct : public Aggregator {
 public:
  using Aggregator::Aggregator;

  bool init() override;
  bool fill(char *data_ptr, char *null_ptr, k_uint32 row_num) override;
  bool update(char *data_ptr, char *null_ptr, k_uint32 row_num) override;
  bool endup() override;

 private:
  DistinctData *create_distinctdata();
  bool equal_compare(DistinctData **data);

 public:
  Field **template_fields_{nullptr};
};

class AggregatorSimple : public Aggregator {
 public:
  using Aggregator::Aggregator;

  bool init() override;
  bool fill(char *data_ptr, char *null_ptr, k_uint32 row_num) override;
  bool update(char *data_ptr, char *null_ptr, k_uint32 row_num) override;
  bool endup() override;

 private:
  DistinctData *create_group_concat_data();
};
// distinct info
struct DistinctData {
  ~DistinctData();
  bool init(k_uint32 data_len, k_uint32 null_len);
  bool init_data_ptr(k_uint32 data_len);
  bool init_null_ptr(k_uint32 null_len);
  k_uint32 total_num_{0};  // only used in group_concat field
  k_uint64 total_len_{0};  // only used in group_concat field
  k_char *null_ptr_{nullptr};
  k_char *data_ptr_{nullptr};
  DistinctData *next_{nullptr};
};

}  // namespace kwdbts
