// Copyright 2020 The Cockroach Authors.
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

package gcjobnotifier

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

type testingProvider struct {
	syncutil.Mutex
	cfg *config.SystemConfig
	ch  chan struct{}
}

func (t *testingProvider) GetSystemConfig() *config.SystemConfig {
	t.Lock()
	defer t.Unlock()
	return t.cfg
}

func (t *testingProvider) setSystemConfig(cfg *config.SystemConfig) {
	t.Lock()
	defer t.Unlock()
	t.cfg = cfg
}

func (t *testingProvider) RegisterSystemConfigChannel() <-chan struct{} {
	return t.ch
}

var _ systemConfigProvider = (*testingProvider)(nil)

func TestNotifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettings()
	t.Run("start with stopped stopper leads to nil being returned", func(t *testing.T) {
		stopper := stop.NewStopper()
		stopper.Stop(ctx)
		n := New(settings, &testingProvider{}, stopper)
		n.Start(ctx)
		ch, _ := n.AddNotifyee(ctx)
		require.Nil(t, ch)
	})
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	t.Run("panic on double start", func(t *testing.T) {
		n := New(settings, &testingProvider{ch: make(chan struct{})}, stopper)
		n.Start(ctx)
		require.Panics(t, func() {
			n.Start(ctx)
		})
	})
	t.Run("panic on AddNotifyee before start", func(t *testing.T) {
		n := New(settings, &testingProvider{ch: make(chan struct{})}, stopper)
		require.Panics(t, func() {
			n.AddNotifyee(ctx)
		})
	})
	t.Run("notifies on changed delta and cleanup", func(t *testing.T) {
		cfg := config.NewSystemConfig(zonepb.DefaultSystemZoneConfigRef())
		cfg.Values = []roachpb.KeyValue{
			mkZoneConfigKV(1, 1, "1"),
		}
		ch := make(chan struct{}, 1)
		p := &testingProvider{
			cfg: mkSystemConfig(mkZoneConfigKV(1, 1, "1")),
			ch:  ch,
		}
		n := New(settings, p, stopper)
		n.Start(ctx)
		n1Ch, cleanup1 := n.AddNotifyee(ctx)

		t.Run("don't receive on new notifyee", func(t *testing.T) {
			expectNoSend(t, n1Ch)
		})
		t.Run("don't receive with no change", func(t *testing.T) {
			ch <- struct{}{}
			expectNoSend(t, n1Ch)
		})
		n2Ch, _ := n.AddNotifyee(ctx)
		t.Run("receive from all notifyees when data does change", func(t *testing.T) {
			p.setSystemConfig(mkSystemConfig(mkZoneConfigKV(1, 2, "2")))
			ch <- struct{}{}
			expectSend(t, n1Ch)
			expectSend(t, n2Ch)
		})
		t.Run("don't receive after cleanup", func(t *testing.T) {
			cleanup1()
			p.setSystemConfig(mkSystemConfig(mkZoneConfigKV(1, 3, "3")))
			ch <- struct{}{}
			expectSend(t, n2Ch)
			expectNoSend(t, n1Ch)
		})
	})
}

func TestNotifierInitialSystemConfigUnavailable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	p := &testingProvider{ch: make(chan struct{}, 1)}
	n := New(cluster.MakeTestingClusterSettings(), p, stopper)
	n.Start(ctx)

	notifyCh, cleanup := n.AddNotifyee(ctx)
	defer cleanup()
	require.NotNil(t, notifyCh)
	n.maybeNotify()
	expectNoSend(t, notifyCh)

	p.setSystemConfig(mkSystemConfig(mkZoneConfigKV(1, 1, "1")))
	p.ch <- struct{}{}
	expectSend(t, notifyCh)
}

const (
	// used for timeouts of things which should be fast
	longTime = time.Second
	// used for sanity check of channel sends which shouldn't happen
	shortTime = 10 * time.Millisecond
)

func expectNoSend(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("did not expect to receive")
	case <-time.After(shortTime):
	}
}

func expectSend(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(longTime):
		t.Fatal("expected to receive")
	}
}

func mkZoneConfigKV(id uint32, ts int64, value string) roachpb.KeyValue {
	kv := roachpb.KeyValue{
		Key: config.MakeZoneKey(id),
		Value: roachpb.Value{
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
	}
	kv.Value.SetString(value)
	return kv
}

func mkSystemConfig(kvs ...roachpb.KeyValue) *config.SystemConfig {
	cfg := config.NewSystemConfig(zonepb.DefaultSystemZoneConfigRef())
	cfg.Values = kvs
	return cfg
}
