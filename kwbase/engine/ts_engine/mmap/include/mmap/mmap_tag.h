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

# ifndef KWDBTS2_MMAP_MMAP_TAG
# define KWDBTS2_MMAP_MMAP_TAG

#include <stdio.h>
#include "data_type.h"

enum TagType {
    UNKNOWN_TAG = -1,
    GENERAL_TAG = 1,
    PRIMARY_TAG,
};

enum OperateType {
  Invalid = 0,
  Insert = 1,
  Update = 2,
  Delete = 3,
  DeleteBySnapshot = 4,
  Ignore = 5,
};

struct TagInfo {
  uint32_t  m_id;        //  tag column id
  int32_t   m_data_type;  // data type
  uint32_t  m_length;   // data length
  uint32_t  m_offset;    // offset
  uint32_t  m_size;      // data size
  TagType   m_tag_type;  // tag type
  int32_t   m_flag;      // tag dropped flag
  bool      isEqual(const TagInfo& other) const { return (m_id == other.m_id) &&
                                                         (m_data_type == other.m_data_type) &&
                                                         (m_length == other.m_length);
                                                }
  bool      isDropped() const {return (m_flag & AINFO_DROPPED); }

  void      setFlag(int32_t flag) {m_flag |= flag;}

  bool      isPrimaryTag() const { return m_tag_type == PRIMARY_TAG;}
};

# endif