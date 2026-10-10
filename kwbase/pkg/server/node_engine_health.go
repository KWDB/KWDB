// Copyright 2018 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package server

import (
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// maxSyncDuration is the threshold above which an observed engine sync duration
// triggers either a warning or a fatal error.
var maxSyncDuration = envutil.EnvOrDefaultDuration("KWBASE_ENGINE_MAX_SYNC_DURATION", 10*time.Second)

// maxSyncDurationFatalOnExceeded defaults to false due to issues such as
// https://gitee.com/kwbasedb/kwbase/issues/34860#issuecomment-469262019.
// Similar problems have been known to occur during index backfill and, possibly,
// IMPORT/RESTORE.
var maxSyncDurationFatalOnExceeded = envutil.EnvOrDefaultBool("KWBASE_ENGINE_MAX_SYNC_DURATION_FATAL", false)

// startAssertEngineHealth starts a goroutine that periodically verifies that
// syncing the engines is possible within maxSyncDuration. If not,
// the process is terminated (with an attempt at a descriptive message).
func (n *Node) startAssertEngineHealth(ctx context.Context, engines []relational.Engine) {
	n.stopper.RunWorker(ctx, func(ctx context.Context) {
		t := timeutil.NewTimer()
		t.Reset(0)

		for {
			select {
			case <-t.C:
				t.Read = true
				t.Reset(10 * time.Second)
				n.assertEngineHealth(ctx, engines, maxSyncDuration)
			case <-n.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

func guaranteedExitFatal(ctx context.Context, msg string, args ...interface{}) {
	// NB: log.Shout sets up a timer that guarantees process termination.
	log.Shout(ctx, log.Severity_FATAL, fmt.Sprintf(msg, args...))
}

func (n *Node) assertEngineHealth(
	ctx context.Context, engines []relational.Engine, maxDuration time.Duration,
) {
	logSync := log.LogSyncEnable.Get(&n.storeCfg.Settings.SV)
	for _, eng := range engines {
		func() {
			t := time.AfterFunc(maxDuration, func() {
				n.metrics.DiskStalls.Inc(1)
				stats := "\n" + eng.GetCompactionStats()
				logger := log.Warningf
				if maxSyncDurationFatalOnExceeded && logSync {
					logger = guaranteedExitFatal
				}
				// NB: the disk-stall-detected roachtest matches on this message.
				logger(ctx, "disk stall detected: unable to write to %s within %s %s",
					eng, maxSyncDuration, stats,
				)
			})
			defer t.Stop()
			if err := relational.WriteSyncNoop(ctx, eng); err != nil {
				if logSync {
					log.Fatal(ctx, err)
				} else {
					log.Warning(ctx, err)
				}
			}
		}()
	}
}
