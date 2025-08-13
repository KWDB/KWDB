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

template <typename T, typename C = void>
class DisposableClosure : public google::protobuf::Closure {
 public:
  explicit DisposableClosure(const C& ctx) : ctx_(ctx) {}

  DisposableClosure(const DisposableClosure&) = delete;
  DisposableClosure& operator=(const DisposableClosure&) = delete;

  ~DisposableClosure() override = default;

  using SuccessFunc = std::function<void(const C&, const T&)>;
  using FailedFunc = std::function<void(const C&, std::string_view)>;

  void AddSuccessHandler(SuccessFunc fn) { success_handler_ = fn; }
  void AddFailedHandler(FailedFunc fn) { failed_handler_ = std::move(fn); }

  T result;
  brpc::Controller cntl;

  void Run() noexcept override {
    std::unique_ptr<DisposableClosure> self_guard(this);

    try {
      if (!cntl.Failed()) {
        success_handler_(ctx_, result);

      } else {
        std::string rpc_err_str = "brpc failed, error={" + std::string(berror(cntl.ErrorCode())) +
                                  "}, error_text={" + cntl.ErrorText() + "}";
        failed_handler_(ctx_, std::string_view(rpc_err_str));
      }
    } catch (const std::exception& exp) {
      LOG_ERROR("Callback error: {%s}", exp.what());
    } catch (...) {
      LOG_ERROR("Callback error: Unknown");
    }
  }

 private:
  SuccessFunc success_handler_;
  FailedFunc failed_handler_;
  const C ctx_;
};

}  // namespace kwdbts
