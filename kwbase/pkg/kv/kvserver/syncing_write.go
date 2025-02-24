// Copyright 2017 The Cockroach Authors.
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

package kvserver

import (
	"context"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"golang.org/x/time/rate"
)

// bulkIOWriteBurst is the burst for the BulkIOWriteLimiter.
const bulkIOWriteBurst = 2 * 1024 * 1024 // 2MB

const bulkIOWriteLimiterLongWait = 500 * time.Millisecond

func limitBulkIOWrite(ctx context.Context, limiter *rate.Limiter, cost int) {
	// The limiter disallows anything greater than its burst (set to
	// BulkIOWriteLimiterBurst), so cap the batch size if it would overflow.
	//
	// TODO(dan): This obviously means the limiter is no longer accounting for the
	// full cost. I've tried calling WaitN in a loop to fully cover the cost, but
	// that didn't seem to be as smooth in practice (NB [dt]: that was when this
	// limit was done before writing the whole file, rather than on individual
	// chunks).
	if cost > bulkIOWriteBurst {
		cost = bulkIOWriteBurst
	}

	begin := timeutil.Now()
	if err := limiter.WaitN(ctx, cost); err != nil {
		log.Errorf(ctx, "error rate limiting bulk io write: %+v", err)
	}

	if d := timeutil.Since(begin); d > bulkIOWriteLimiterLongWait {
		log.Warningf(ctx, "bulk io write limiter took %s (>%s):\n%s",
			d, bulkIOWriteLimiterLongWait, debug.Stack())
	}
}

// sstWriteSyncRate wraps "kv.bulk_sst.sync_size". 0 disables syncing.
var sstWriteSyncRate = settings.RegisterByteSizeSetting(
	"kv.bulk_sst.sync_size",
	"threshold after which non-Rocks SST writes must fsync (0 disables)",
	bulkIOWriteBurst,
)

// writeFileSyncing is essentially ioutil.WriteFile -- writes data to a file
// named by filename -- but with rate limiting and periodic fsyncing controlled
// by settings and the passed limiter (should be the store's limiter). Periodic
// fsync provides smooths out disk IO, as mentioned in #20352 and #20279, and
// provides back-pressure, along with the explicit rate limiting. If the file
// does not exist, WriteFile creates it with permissions perm; otherwise
// WriteFile truncates it before writing.
func writeFileSyncing(
	ctx context.Context,
	filename string,
	data []byte,
	eng storage.Engine,
	perm os.FileMode,
	settings *cluster.Settings,
	limiter *rate.Limiter,
) error {
	chunkSize := sstWriteSyncRate.Get(&settings.SV)
	sync := true
	if chunkSize == 0 {
		chunkSize = bulkIOWriteBurst
		sync = false
	}

	f, err := eng.CreateFile(filename)
	if err != nil {
		if strings.Contains(err.Error(), "No such file or directory") {
			return os.ErrNotExist
		}
		return err
	}

	for i := int64(0); i < int64(len(data)); i += chunkSize {
		end := i + chunkSize
		if l := int64(len(data)); end > l {
			end = l
		}
		chunk := data[i:end]

		// rate limit
		limitBulkIOWrite(ctx, limiter, len(chunk))
		_, err = f.Write(chunk)
		if err == nil && sync {
			err = f.Sync()
		}
		if err != nil {
			break
		}
	}

	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
