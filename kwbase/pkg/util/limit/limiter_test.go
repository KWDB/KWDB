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

package limit

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"golang.org/x/sync/errgroup"
)

func TestConcurrentRequestLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l := MakeConcurrentRequestLimiter("test", 1)
	var wg errgroup.Group

	const threads = 20
	const runs = 1000000
	var count int64

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	for thread := 0; thread < threads; thread++ {
		wg.Go(func() error {
			runtime.Gosched()
			req := 0
			for {
				//t.Logf("waiting to make request %d... (%d / %d)", req+1, l.sem.GetCount(), l.sem.GetLimit())
				if err := l.Begin(ctx); err != nil {
					if err == ctx.Err() {
						break
					} else {
						return err
					}
				}
				if x := atomic.AddInt64(&count, 1); x >= runs {
					t.Logf("canceling ctx after %d runs", x)
					cancel()
				}
				req++
				l.Finish()
			}
			t.Logf("thread done after handling %d requests", req)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}
