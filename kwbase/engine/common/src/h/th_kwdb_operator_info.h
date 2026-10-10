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

#ifndef COMMON_SRC_H_TH_KWDB_OPERATOR_INFO_H_
#define COMMON_SRC_H_TH_KWDB_OPERATOR_INFO_H_

#include <string>
#include "kwdb_type.h"
#include "cm_kwdb_context.h"

namespace kwdbts {

struct ProxyThreadInfo {
  ProxyThreadInfo()
      : parent_thread_id(0), child_thread_id(0), remote_conn_id(0) {}

  ProxyThreadInfo(k_uint64 parent, k_uint64 child,
                  k_uint64 remote)
      : parent_thread_id(parent), child_thread_id(child),
        remote_conn_id(remote) {}

  void Reset();
  // Thread id from TaskServer_t; either goRouteId or tsThreadId.
  k_uint64 parent_thread_id;
  // Helper thread Id; cloud-edge collaboration thread.
  k_uint64 child_thread_id;
  // connection used to connect with a remote KWDB.
  k_uint64 remote_conn_id;
  // Other client Info that we stored in
  // SDK when establishing a connection with KWDB.
};

class KWDBOperatorInfo {
 public:
  KWDBOperatorInfo();
  // Releases all operation info of thread.
  virtual ~KWDBOperatorInfo();

  void SetOperatorName(const KString &oper_name);

  KString GetOperatorName();

  void SetOperatorOwner(const KString &oper_owner);

  KString GetOperatorOwner();

  void SetOperatorStatus(const KString &oper_status);

  KString GetOperatorStatus();

  void SetOperatorStartTime(k_uint64 oper_start_time);

  k_uint64 GetOperatorStartTime();

  void SetProxyThreadInfo(ProxyThreadInfo pro_th_info);

  ProxyThreadInfo GetProxyThreadInfo();

  void Reset();

  void Print();

 private:
  KString operator_name_;
  KString operator_owner_;
  KString operator_status_;
  k_uint64 operator_start_time_;
  struct ProxyThreadInfo proxy_thread_info_;
};

}  // namespace kwdbts

#endif  // COMMON_SRC_H_TH_KWDB_OPERATOR_INFO_H_
