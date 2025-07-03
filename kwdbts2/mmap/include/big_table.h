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

#include <inttypes.h>
#include <memory>
#include <vector>
#include <iostream>
#include <string>
#include <map>
#include "ts_object.h"
#include "data_helper.h"
#include "kwdb_consts.h"

// the highest two bytes  0xXX------ is reserved
enum TableType {
  ROW_TABLE =             0x00000000,
  NO_DEFAULT_TABLE =      0x00000002,     // auxiliary table without default
  TAG_TABLE =             0x00000008,
  NULLBITMAP_TABLE =      0x00800000,
  ENTITY_TABLE     =      0x00200000,
  UNKNOWN_TABLE    =      0xff000000,
};

#define is_deletable(x)     ((x & DELETABLE_TABLE) != 0)


namespace kwdbts {
class KeyMaker;
class KeyPair;
};

using namespace kwdbts;

class BigTable : public TSObject {
 private:
  KRWLatch rw_latch_;

protected:

public:
  BigTable();

  BigTable(const string &path);

  virtual ~BigTable();

  int rdLock() override;
  int wrLock() override;
  int unLock() override;

  // name of table
  virtual string name() const override;

  virtual int getDeletableColumn() const;

  virtual int firstDataColumn() const;

  /**
   * @brief	Obtain the attribute schema (external view without '_cts' and '_d').
   *
   * @return	Attribute information for each column.
   */
  virtual const vector<AttributeInfo> & getSchema() const;

  // Actual schema (internal view)
  virtual const vector<AttributeInfo> & getSchemaInfo() const;

  virtual int structVersion() const;

  /*--------------------------------------------------------------------
   * big table functions
   *--------------------------------------------------------------------
   */

  /**
   * @brief	Create a table object.
   *
   * @param	schema		      Schema.
   * @param	key		          Key.
   * @param key_order_idx   Order column index for Last Index, -1 if not used.
   * @param	description	    Description string.
   * @param	alias		        Alias for the table object.
   * @param	tbl_sub_path		sub directory.
   * @param	source_path	    Data source PATH.
   * @param	dimension     	Dimension Information.
   * @param	ns_path		      Name service PATH.
   * @param encoding        Name encoding scheme & deletable table.
   * @return	0 if succeed; -1 otherwise.
   */
  virtual int create(const vector<AttributeInfo> &schema,
    const vector<string> &key, const string &key_order,
    const string &description = kwdbts::s_emptyString,
    const string &ns_path = defaultNameServicePath(),
    const string &tbl_sub_path = kwdbts::s_emptyString,
    const string &source_path = kwdbts::s_emptyString,
    int encoding = DICTIONARY,
    ErrorInfo &err_info = getDummyErrorInfo());

  virtual int reserveBase(size_t size);
  virtual int reserve(size_t size);

  virtual int resize(size_t size);

  // average string length of column col; used for VARSTIRNG
  virtual int avgColumnStrLen(int col);

  virtual vector<string> rank() const;

  virtual size_t size() const;

  // actual size of valid data (for default and sliding table)
  virtual size_t actualSize() const;

  virtual size_t correctActualSize();

  virtual size_t capacity() const;

  // return time bound or size bound
  virtual size_t bound() const;

  virtual size_t reservedSize() const;

  virtual size_t endRowid() const;

  virtual int numColumn() const;

  virtual DataHelper * getRecordHelper();

  // it doesn't need to write lock table for push_back
  // Insert data into table. It may expand table.
  // @return row_number: success; < 0: error
  virtual int64_t push_back(const void *rec);

  // no lock push_back;
  // when called directly, table should have reserved space for data
  // (mainly for INSERT INTO SELECT on the same table)
  // therefore, table extension should not happen
  // @return row_number: success; < 0: error
  virtual int64_t push_back_nolock(const void *rec);

  int push_back(const vector<string> &rec);

  // only apply on (default)deletable table.
  virtual bool isValidRow(size_t row);

  virtual int erase(size_t row);

  virtual void * record(size_t n) const;

  virtual void * header(size_t n) const;

  virtual uint64_t recordSize() const;

  virtual bool isNull(size_t row, size_t col);

  // return < 0 if NULL
  virtual int getColumnValue(size_t row, size_t column, void *data) const;

  virtual void setColumnValue(size_t row, size_t column, void *data);

  virtual void setColumnValue(size_t row, size_t column, const string &str);

  virtual void * getColumnAddr(size_t row, size_t column) const;

  virtual string columnToString(size_t row, size_t column) const;

  // @param   str     external string buffer
  // @return  length of string
  virtual int columnToString(size_t row, size_t column, char *str) const;

  // Returns the the first matched row for table with key/index;
  // 0 if not found.
  virtual size_t find(const vector<string> &key);

  // Returns matched row(s) in locs from table with key/index
  virtual size_t find(const vector<string> &key, vector<size_t> &locs);

  // Returns the matched row for table with numeric key
  // used in group by postAction for STD etc.
  virtual size_t find(void* key);

  // find row based on a key maker
  // used in table join and table itself
  virtual size_t find(const KeyMaker &km);

  virtual size_t find(const KeyPair &kp);

  virtual size_t count(const string &key_name);

  virtual int setColumnFlag(int col, int flag);

  virtual ostream & printRecord(std::ostream &os, size_t row);
};

