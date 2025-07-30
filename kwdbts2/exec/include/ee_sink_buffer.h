// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

#include <algorithm>
#include <atomic>
#include <future>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>


#include "kwdb_type.h"
#include "br_stub_cache.h"
#include "br_disposable_closure.h"
#include "br_internal_service_recoverable_stub.h"
#include "ee_global.h"

namespace kwdbts {

using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;
using PDialDataRecvrPtr = std::shared_ptr<PDialDataRecvr>;
using PSendExecStatusPtr = std::shared_ptr<PSendExecStatus>;

struct ClosureContext {
  k_int64 instance_id;
  k_int64 sequence;
  k_int64 send_timestamp;
};

struct TransmitChunkInfo {
  // For BUCKET_SHUFFLE_HASH_PARTITIONED, multiple channels may be related to
  // a same exchange source fragment instance, so we should use
  // fragment_instance_id of the destination as the key of destination instead
  // of channel_id.
  k_int32 target_node_id;
  std::shared_ptr<PInternalServiceRecoverableStub> brpc_stub;
  PTransmitChunkParamsPtr params;
  butil::IOBuf attachment;
  k_int64 attachment_physical_bytes;
  const TNetworkAddress brpc_addr;
};

struct TransmitSimpleInfo {
  k_int32 target_node_id;
  k_int32 error_code;
  std::string error_msg;
  std::shared_ptr<PInternalServiceRecoverableStub> brpc_stub;
  PDialDataRecvrPtr params;
  PSendExecStatusPtr exec_status;
  const TNetworkAddress brpc_addr;
};

struct FragmentDestination {
  k_int64 query_id;
  k_int32 target_node_id;
  TNetworkAddress brpc_addr;
};

class SinkBuffer {
 public:
  SinkBuffer(const std::vector<FragmentDestination>& destinations, bool is_dest_merge);
  ~SinkBuffer();

  KStatus AddRequest(TransmitChunkInfo& request);
  KStatus SendSimpleMsg(TransmitSimpleInfo& request);
  KStatus SendErrMsg(TransmitSimpleInfo& request);
  k_bool IsFull() const;

  void SetFinishing();
  k_bool IsFinished() const;
  k_bool IsFinishedExtra() const;
  void CancelOneSinker();
  void IncrSinker();
  k_int32 GetUnfinihedRpc() {
    return total_in_flight_rpc_;
  }

  void SetReceiveNotify(ReceiveNotify notify) {
    notify_rpc_callback_ = notify;
  }
  void SetReceiveNotifyEx(ReceiveNotifyEx notify) {
    notify_callback_ = notify;
  }

 private:
  using Mutex = bthread::Mutex;
  void ProcessSendWindow(const k_int64& instance_id, const k_int64 sequence);
  KStatus TrySendRpc(const k_int64& instance_id, const std::function<void()>& pre_works);

  // send by rpc
  KStatus SendRpc(DisposableClosure<PTransmitChunkResult, ClosureContext>* closure, const TransmitChunkInfo& req);
  const k_bool is_dest_merge_;
  struct SinkContext {
    k_int64 num_sinker;
    k_int64 request_seq;
    k_int64 max_continuous_acked_seqs;
    std::unordered_set<k_int64> discontinuous_acked_seqs;
    k_int64 query_id;
    std::queue<TransmitChunkInfo, std::list<TransmitChunkInfo>> buffer;
    Mutex request_mutex;

    std::atomic_size_t num_finished_rpcs;
    std::atomic_size_t num_in_flight_rpcs;

    Mutex mutex;

    TNetworkAddress dest_addrs;
  };
  std::map<k_int64, std::unique_ptr<SinkContext>> sink_ctxs_;
  SinkContext& SinkCtx(k_int64 instance_id) {
    return *sink_ctxs_[instance_id];
  }

  std::atomic<k_int32> total_in_flight_rpc_ = 0;
  std::atomic<k_int32> num_uncancelled_sinkers_ = 0;
  std::atomic<k_int32> num_remaining_eos_ = 0;
  std::atomic<k_bool> is_finishing_ = false;
  std::atomic<k_int32> num_sending_rpc_ = 0;

  std::atomic<k_int64> request_sequence_ = 0;
  k_int64 sent_audit_stats_frequency_ = 1;
  k_int64 sent_audit_stats_frequency_upper_limit_ = 64;
  ReceiveNotify notify_rpc_callback_{nullptr};
  ReceiveNotifyEx notify_callback_{nullptr};
  k_bool ignore_ = false;
};

}  // namespace kwdbts
