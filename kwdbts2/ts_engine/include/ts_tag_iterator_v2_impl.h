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

#include "tag_iterator.h"

namespace kwdbts {

class TagIteratorV2Impl : public BaseEntityIterator {
  public:
   explicit TagIterator(std::vector<EntityGroupTagIterator*>& tag_grp_iters) : entitygrp_iters_(tag_grp_iters) {}
   ~TagIterator() override;
 
   KStatus Init() override;
   KStatus Next(std::vector<EntityResultIndex>* entity_id_list, ResultSet* res, k_uint32* count) override;
   KStatus Close() override;
 
  private:
   std::vector<EntityGroupTagIterator*> entitygrp_iters_;
   EntityGroupTagIterator* cur_iterator_ = nullptr;
   uint32_t cur_idx_{0};
  };

}  //  namespace kwdbts
