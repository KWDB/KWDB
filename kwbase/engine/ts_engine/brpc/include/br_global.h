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

#include <algorithm>
#include <limits>
#include <string>
#include <utility>

#include "br_internal_service.pb.h"
#include "kwdb_type.h"
#include "lg_api.h"

namespace kwdbts {

enum BRStatusCode {
  OK = 0,
  ERR,
  SERVICE_UNAVAILABLE,
  INTERNAL_ERROR,
  NOT_READY,
  NOT_FOUND_RECV,
  UNKNOWN,
};

static const char g_moved_from_state[3] = {'\x00', '\x00', '\x00'};

class BRStatus {
 public:
  BRStatus() = default;

  ~BRStatus() noexcept {
    if (state_ != nullptr && !IsMovedFrom(state_)) {
      delete[] state_;
    }
  }

  BRStatus(BRStatusCode code, std::string_view msg) : state_(CreateState(code, msg)) {}

  // Copy c'tor makes copy of error detail so Status can be returned by value
  BRStatus(const BRStatus& s) : state_(s.state_ == nullptr ? nullptr : CopyState(s.state_)) {}

  // Move c'tor
  BRStatus(BRStatus&& s) noexcept : state_(s.state_) { s.state_ = g_moved_from_state; }

  // Same as copy c'tor
  BRStatus& operator=(const BRStatus& s) {
    if (this != &s) {
      BRStatus tmp(s);
      std::swap(this->state_, tmp.state_);
    }
    return *this;
  }

  // Move assign.
  BRStatus& operator=(BRStatus&& s) noexcept {
    if (this != &s) {
      BRStatus tmp(std::move(s));
      std::swap(this->state_, tmp.state_);
    }
    return *this;
  }

  explicit BRStatus(const StatusPB& pstatus) {
    auto code = static_cast<BRStatusCode>(pstatus.status_code());
    if (code != BRStatusCode::OK) {
      if (pstatus.error_msgs_size() == 0) {
        state_ = CreateState(static_cast<BRStatusCode>(code), {});
      } else {
        state_ = CreateState(static_cast<BRStatusCode>(code), pstatus.error_msgs(0));
      }
    }
  }

  static BRStatus OK() { return BRStatus(); }

  static BRStatus Unknown(std::string_view msg) { return BRStatus(BRStatusCode::UNKNOWN, msg); }
  static BRStatus ServiceUnavailable(std::string_view msg) {
    return BRStatus(BRStatusCode::SERVICE_UNAVAILABLE, msg);
  }

  static BRStatus InternalError(std::string_view msg) {
    return BRStatus(BRStatusCode::INTERNAL_ERROR, msg);
  }
  static BRStatus NotReady(std::string_view msg) { return BRStatus(BRStatusCode::NOT_READY, msg); }

  static BRStatus NotFoundRecv(std::string_view msg) {
    return BRStatus(BRStatusCode::NOT_FOUND_RECV, msg);
  }

  void toProtobuf(StatusPB* status) const {
    status->clear_error_msgs();
    if (state_ == nullptr) {
      status->set_status_code(static_cast<int>(BRStatusCode::OK));
    } else {
      status->set_status_code(code());
      auto msg = message();
      status->add_error_msgs(msg.data(), msg.size());
    }
  }

  std::string_view message() const {
    if (state_ == nullptr) {
      return {};
    }

    k_uint16 len;
    memcpy(&len, state_, sizeof(len));
    return {state_ + 3, len};
  }

  BRStatusCode code() const {
    return state_ == nullptr ? BRStatusCode::OK : static_cast<BRStatusCode>(state_[2]);
  }

 private:
  k_char* CreateState(BRStatusCode code, std::string_view msg) {
    if (code == BRStatusCode::OK) {
      return nullptr;
    }

    auto msg_size = std::min<k_size_t>(msg.size(), std::numeric_limits<k_uint16>::max());
    const auto len = static_cast<k_uint16>(msg_size);
    const k_uint32 size = static_cast<k_uint32>(len);

    k_char* state = new k_char[size + 3];
    if (state == nullptr) {
      LOG_ERROR("CreateState failed: malloc error\n");
      return nullptr;
    }

    memcpy(state, &len, sizeof(len));
    state[2] = static_cast<k_char>(code);
    memcpy(state + 3, msg.data(), len);

    return state;
  }

  const char* CopyState(const char* state) {
    if (state == nullptr) {
      return nullptr;
    }

    k_uint16 len;
    memcpy(&len, state, sizeof(len));

    k_uint32 total_size = static_cast<k_uint32>(len) + 3;

    k_char* result = new k_char[total_size];
    memcpy(result, state, total_size);

    return result;
  }

  static k_bool IsMovedFrom(const char* state) { return state == g_moved_from_state; }
  // OK status has a nullptr state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..1]                        == len1: length of message
  //    state_[2]                           == code
  //    state_[3.. 3 + len1]                == message
  const char* state_ = nullptr;
};

}  // namespace kwdbts
