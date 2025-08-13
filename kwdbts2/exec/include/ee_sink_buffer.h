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

// PDialDataRecvrPtr is used to store the dial data recvr.
using PDialDataRecvrPtr = std::shared_ptr<PDialDataRecvr>;
// PTransmitChunkParamsPtr is used to store the transmit chunk params.
using PTransmitChunkParamsPtr = std::shared_ptr<PTransmitChunkParams>;
// PSendExecStatusPtr is used to store the send exec status.
using PSendExecStatusPtr = std::shared_ptr<PSendExecStatus>;

// TransmitSimpleInfo is used to store the transmit simple info.
struct TransmitSimpleInfo {
  k_int32 target_node_id;
  k_int32 error_code;
  std::string error_msg;
  std::shared_ptr<BoxServiceRetryableClosureStub> brpc_stub;
  PDialDataRecvrPtr params;
  PSendExecStatusPtr exec_status;
  const TNetworkAddress brpc_addr;
};

struct ChunkTransmitContext {
  k_int32 target_node_id;
  std::shared_ptr<BoxServiceRetryableClosureStub> brpc_service_stub;
  PTransmitChunkParamsPtr transmit_params;
  butil::IOBuf attachment_data;
  k_int64 actual_memory_bytes;
  const TNetworkAddress brpc_addr;
};

// FragmentDestination is used to store the fragment destination.
struct FragmentDestination {
  k_int64 query_id;
  k_int32 target_node_id;
  TNetworkAddress brpc_addr;
};

struct ClosureContext {
  k_int64 instance_id;
  k_int64 seq;
  // for send time stamp
  k_int64 timestamp;
};

class OutboundBuffer {
 public:
  OutboundBuffer(const std::vector<FragmentDestination>& destinations, bool is_dest_merge);
  ~OutboundBuffer();

  KStatus AddSendRequest(ChunkTransmitContext& request);
  // SendSimpleMsg is used to send the simple msg.
  KStatus SendSimpleMsg(TransmitSimpleInfo& request);
  // SendErrMsg is used to send the err msg.
  KStatus SendErrMsg(TransmitSimpleInfo& request);
  k_bool IsFull() const;
  // IsFinished is used to check if the buffer is finished.
  k_bool IsFinished() const;
  // IsFinishedExtra is used to check if the buffer is finished extra.
  k_bool IsFinishedExtra() const;
  void CancelOneSinker();
  void IncrSinker();
  // GetUnfinihedRpc is used to get the unfinished rpc.
  k_int32 GetUnfinihedRpc() {
    return pending_rpc_total_;
  }
  // SetReceiveNotify is used to set the receive notify.
  void SetReceiveNotify(ReceiveNotify notify) {
    notify_rpc_callback_ = notify;
  }
  // SetReceiveNotifyEx is used to set the receive notify ex.
  void SetReceiveNotifyEx(ReceiveNotifyEx notify) {
    notify_callback_ = notify;
  }

 private:
  // send by rpc
  KStatus SendDataViaRpc(DisposableClosure<PTransmitChunkResult, ClosureContext>* closure,
                         const ChunkTransmitContext& req);
  using Mutex = bthread::Mutex;
  struct OutboundContext {
    k_int64 count_outbounder;
    k_int64 request_seq;
    k_int64 max_in_contiguous_ack_seq;
    std::unordered_set<k_int64> non_contiguous_acked_seqs;
    k_int64 query_id;
    std::queue<ChunkTransmitContext, std::list<ChunkTransmitContext>> buffer;
    Mutex request_mutex;

    std::atomic_size_t num_finished_rpcs;
    std::atomic_size_t num_in_flight_rpcs;

    Mutex mutex;

    TNetworkAddress brpc_dest_addrs;
  };
  OutboundContext& OutboundCtx(k_int64 instance_id) {
    return *outbound_ctxs_[instance_id];
  }
  // AttemptSendRpc is used to attempt send rpc.
  KStatus AttemptSendRpc(const k_int64& instance_id, const std::function<void()>& pre_works);

  void UpdateSendWindow(const k_int64& instance_id, const k_int64 sequence);

 private:
  const k_bool is_dest_merge_;
  std::map<k_int64, std::unique_ptr<OutboundContext>> outbound_ctxs_;
  k_int64 audit_stats_send_frequency_ = 1;
  std::atomic<k_int32> sending_rpc_count_ = 0;
  std::atomic<k_int32> pending_rpc_total_ = 0;
  ReceiveNotify notify_rpc_callback_{nullptr};
  std::atomic<k_int32> uncancelled_sinker_count_ = 0;
  ReceiveNotifyEx notify_callback_{nullptr};
  std::atomic<k_int32> unprocessed_eos_num_ = 0;
  std::atomic<k_bool> is_completing_ = false;
  std::atomic<k_int64> request_sequence_id_ = 0;
  k_int64 audit_stats_send_frequency_max_ = 64;
};

}  // namespace kwdbts
