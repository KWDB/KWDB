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

#include <cstdio>
#include <ctime>
#include <functional>

#include "brpc/authenticator.h"
#include "kwdb_type.h"
#include "lg_api.h"

namespace kwdbts {

constexpr k_uint32 BOX_AUTH_TIMESTAMP_EXPIRE_SECONDS = 300;

class BoxAuthenticator : public brpc::Authenticator {
 public:
  explicit BoxAuthenticator(const KString& token) : token_(token) {
    token_hash_ = ComputeHash(token_);
  }

  k_int32 GenerateCredential(KString* auth_str) const override {
    k_uint32 timestamp = static_cast<k_uint32>(time(nullptr));
    k_uint32 nonce = GenerateNonce();
    k_uint64 signature = ComputeSignature(timestamp, nonce);

    k_char buffer[64];
    snprintf(buffer, sizeof(buffer), "%u,%08x,%016lx", timestamp, nonce,
             static_cast<k_uint64>(signature));
    auth_str->assign(buffer);

    return 0;
  }

  k_int32 VerifyCredential(const KString& auth_str, const butil::EndPoint& client_addr,
                           brpc::AuthContext* out_ctx) const override {
    k_uint32 timestamp, nonce;
    k_uint64 provided_signature;

    if (sscanf(auth_str.c_str(), "%u,%x,%lx", &timestamp, &nonce,
               reinterpret_cast<k_uint64*>(&provided_signature)) != 3) {
      LOG_ERROR("VerifyCredential failed: Invalid format\n");
      return -1;
    }

    k_uint32 current_time = static_cast<k_uint32>(time(nullptr));
    if (KAbs(static_cast<k_int32>(current_time - timestamp)) > BOX_AUTH_TIMESTAMP_EXPIRE_SECONDS) {
      LOG_ERROR("VerifyCredential failed: Timestamp expired\n");
      return -1;
    }

    k_uint64 expected_signature = ComputeSignature(timestamp, nonce);
    if (SecureCompare64(provided_signature, expected_signature)) {
      LOG_INFO("VerifyCredential success\n");
      return 0;
    }

    LOG_ERROR("VerifyCredential failed: Signature mismatch\n");
    return -1;
  }

 private:
  const KString token_;
  k_uint64 token_hash_;
  mutable k_uint32 nonce_counter_ = 1;

  k_uint64 ComputeHash(const KString& input) const {
    std::hash<KString> hasher;
    return hasher(input);
  }

  k_uint64 ComputeSignature(k_uint32 timestamp, k_uint32 nonce) const {
    k_uint64 combined = (static_cast<k_uint64>(timestamp) << 32) | nonce;
    k_uint64 result = combined ^ token_hash_;

    result ^= (result >> 17);
    result *= 0x85ebca6b;
    result ^= (result >> 13);
    result *= 0xc2b2ae35;
    result ^= (result >> 16);

    return result;
  }

  k_uint32 GenerateNonce() const {
    k_uint32 base = static_cast<k_uint32>(time(nullptr));

    return base ^ (++nonce_counter_);
  }

  k_bool SecureCompare64(k_uint64 a, k_uint64 b) const {
    volatile k_uint64 diff = a ^ b;
    volatile k_uint32 result = 0;

    for (k_int32 i = 0; i < 8; ++i) {
      result |= static_cast<k_uint32>((diff >> (i * 8)) & 0xFF);
    }

    return result == 0 ? KTRUE : KFALSE;
  }
};

}  // namespace kwdbts
