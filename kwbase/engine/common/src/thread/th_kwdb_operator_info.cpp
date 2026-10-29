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

#include "th_kwdb_operator_info.h"
#include <string>

namespace kwdbts {

void ProxyThreadInfo::Reset() {
  parent_thread_id = 0;
  child_thread_id = 0;
  remote_conn_id = 0;
}

KWDBOperatorInfo::KWDBOperatorInfo()
    : operator_name_(""), operator_owner_(""), operator_status_(""),
      operator_start_time_(0), proxy_thread_info_() {}

void KWDBOperatorInfo::SetOperatorName(const KString &oper_name) {
  if ("" == oper_name) {
    return;
  }
  operator_name_ = oper_name;
}

KString KWDBOperatorInfo::GetOperatorName() { return operator_name_; }

void KWDBOperatorInfo::SetOperatorOwner(const KString &oper_owner) {
  if ("" == oper_owner) {
    return;
  }
  operator_owner_ = oper_owner;
}

KString KWDBOperatorInfo::GetOperatorOwner() { return operator_owner_; }

void KWDBOperatorInfo::SetOperatorStartTime(k_uint64 oper_start_time) {
  operator_start_time_ = oper_start_time;
}

k_uint64 KWDBOperatorInfo::GetOperatorStartTime() {
  return operator_start_time_;
}

void KWDBOperatorInfo::SetProxyThreadInfo(struct ProxyThreadInfo pro_th_info) {
  proxy_thread_info_.remote_conn_id = pro_th_info.remote_conn_id;
  proxy_thread_info_.child_thread_id = pro_th_info.child_thread_id;
  proxy_thread_info_.parent_thread_id = pro_th_info.parent_thread_id;
}

ProxyThreadInfo KWDBOperatorInfo::GetProxyThreadInfo() {
  return proxy_thread_info_;
}

void KWDBOperatorInfo::SetOperatorStatus(const KString &oper_status) {
  if ("" == oper_status) {
    return;
  }
  operator_status_ = oper_status;
}

KString KWDBOperatorInfo::GetOperatorStatus() { return operator_status_; }

void KWDBOperatorInfo::Reset() {
  SetOperatorName("");
  SetOperatorOwner("");
  SetOperatorStatus("");
  SetOperatorStartTime(0);
  proxy_thread_info_.Reset();
}

KWDBOperatorInfo::~KWDBOperatorInfo() { Reset(); }

}  // namespace kwdbts
