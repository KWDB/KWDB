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

#include <memory>
#include <string>
#include <utility>

#include "libkwdbts2.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "ts_io.h"

class TsWritableFile : public rocksdb::WritableFile {
 private:
  std::unique_ptr<kwdbts::TsMMapFile> file_;

 public:
  explicit TsWritableFile(std::unique_ptr<kwdbts::TsMMapFile> file) : file_(std::move(file)) {}
  rocksdb::Status Append(const rocksdb::Slice& data) override {
    return file_->Append(TSSlice{const_cast<char*>(data.data()), data.size()});
  }
  rocksdb::Status Close() override { return file_->Close(); }
  rocksdb::Status Flush() override { return file_->Flush(); }
  rocksdb::Status Sync() override { return file_->Sync(); };
};

class TsEnv : public rocksdb::EnvWrapper {
 public:
  TsEnv() : rocksdb::EnvWrapper(rocksdb::Env::Default()) {}
  rocksdb::Status NewWritableFile(const std::string& f, std::unique_ptr<rocksdb::WritableFile>* r,
                                  const rocksdb::EnvOptions& options) override {
    auto file = std::make_unique<kwdbts::TsMMapFile>(f, false);
    *r = std::make_unique<TsWritableFile>(std::move(file));
    return rocksdb::Status::OK();
  }
};
