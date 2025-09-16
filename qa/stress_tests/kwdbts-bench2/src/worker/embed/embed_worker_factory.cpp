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
/*
 * @author jiadx
 * @date 2023/1/17
 * @version 1.0
 */
#include "SampleWorker.h"
#include "st_order_worker.h"
#include "st_worker.h"

namespace kwdbts {
BenchParams global_params_;

// Register embed workers
void RegisterEmbedWorker(const BenchParams& params) {
  global_params_ = params;

  GenericWorkerFactory<StWriteWorker>::RegisterInstance("savedata");
  GenericWorkerFactory<StScanWorker, Worker::BenchType::READ>::RegisterInstance("scan");
  GenericWorkerFactory<StRetentionsWorker, Worker::BenchType::RETENTIONS>::RegisterInstance("retentions");
  GenericWorkerFactory<StCompressWorker, Worker::BenchType::COMPRESS>::RegisterInstance("compress");
  GenericWorkerFactory<StSnapshotWorker, Worker::BenchType::SNAPSHOT>::RegisterInstance("snapshot");
  GenericWorkerFactory<StSnapshotByBlockWorker, Worker::BenchType::SNAPSHOT>::RegisterInstance("snapshot_blk");
//  GenericWorkerFactory<SampleWorker, Worker::BenchType::READ>::RegisterInstance("sample");
//  GenericWorkerFactory<StOrderWorker, Worker::BenchType::WRITE>::RegisterInstance("ordersave");
//  GenericWorkerFactory<StOrderRange, Worker::BenchType::READ>::RegisterInstance("orderscan");
}
}  // namespace kwdbts
