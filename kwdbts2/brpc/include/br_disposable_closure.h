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

#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "lg_api.h"

namespace kwdbts {

// Disposable call back, it must be created on the heap.
// It will destroy itself after call back
template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
 public:
  using FailedFunc = std::function<void(const C&, std::string_view)>;
  using SuccessFunc = std::function<void(const C&, const T&)>;

  explicit DisposableClosure(const C& ctx) : ctx_(ctx) {}
  ~DisposableClosure() override = default;
  // Disallow copy and assignment.
  DisposableClosure(const DisposableClosure& other) = delete;
  DisposableClosure& operator=(const DisposableClosure& other) = delete;

  void AddFailedHandler(FailedFunc fn) { failed_handler_ = std::move(fn); }
  void AddSuccessHandler(SuccessFunc fn) { success_handler_ = fn; }

  void Run() noexcept override {
    std::unique_ptr<DisposableClosure> self_guard(this);

    try {
      if (cntl.Failed()) {
        std::string rpc_err_str = "brpc failed, error={" + std::string(berror(cntl.ErrorCode())) +
                                  "}, error_text={" + cntl.ErrorText() + "}";
        failed_handler_(ctx_, std::string_view(rpc_err_str));
      } else {
        success_handler_(ctx_, result);
      }
    } catch (const std::exception& exp) {
      LOG_ERROR("Callback error: {%s}", exp.what());
    } catch (...) {
      LOG_ERROR("Callback error: Unknown");
    }
  }

 public:
  brpc::Controller cntl;
  T result;

 private:
  const C ctx_;
  FailedFunc failed_handler_;
  SuccessFunc success_handler_;
};

}  // namespace kwdbts
