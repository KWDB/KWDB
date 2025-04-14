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

package server

import (
	"context"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

// tsTableGCTTL is the duration for collection the garbage of ts table meta in storages.
var tsTableGCTTL = settings.RegisterPublicDurationSetting(
	"server.ts_table_gc.ttl",
	"the duration of clear ts table garbage",
	25*time.Hour,
)

// startTSTableGC starts a worker which periodically GCs ts table in storages.
func (s *Server) startTSTableGC(ctx context.Context) {
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		period := tsTableGCTTL.Get(&s.cfg.Settings.SV)

		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(period)

		for {
			select {
			case <-timer.C:
				err := s.tsEngine.DropLeftTsTableGarbage()
				if err != nil {
					log.Error(ctx, err)
				}
				timer.Read = true
				period = tsTableGCTTL.Get(&s.cfg.Settings.SV)
				timer.Reset(period)
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}
