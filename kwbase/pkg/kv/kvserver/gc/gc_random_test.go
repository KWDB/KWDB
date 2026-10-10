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

package gc

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// randomRunGCTestSpec specifies a distribution for to create random data for
// testing Run
type randomRunGCTestSpec struct {
	ds  distSpec
	now hlc.Timestamp
	ttl int32 // seconds
}

var (
	fewVersionsTinyRows = uniformDistSpec{
		tsFrom: 0, tsTo: 100,
		keySuffixMin: 2, keySuffixMax: 3,
		valueLenMin: 1, valueLenMax: 1,
		deleteFrac:      0,
		keysPerValueMin: 1, keysPerValueMax: 2,
		intentFrac: .1,
	}
	someVersionsMidSizeRows = uniformDistSpec{
		tsFrom: 0, tsTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1, keysPerValueMax: 100,
		intentFrac: .1,
	}
	lotsOfVersionsMidSizeRows = uniformDistSpec{
		tsFrom: 0, tsTo: 100,
		keySuffixMin: 8, keySuffixMax: 8,
		valueLenMin: 8, valueLenMax: 16,
		deleteFrac:      .1,
		keysPerValueMin: 1000, keysPerValueMax: 1000000,
		intentFrac: .1,
	}
)

const intentAgeThreshold = 2 * time.Hour

// TestRunNewVsOld exercises the behavior of Run relative to the old
// implementation. It runs both the new and old implementation and ensures
// that they produce exactly the same results on the same set of keys.
func TestRunNewVsOld(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	const N = 100000

	someVersionsMidSizeRowsLotsOfIntents := someVersionsMidSizeRows
	someVersionsMidSizeRowsLotsOfIntents.intentFrac = 1
	for _, tc := range []randomRunGCTestSpec{
		{
			ds: someVersionsMidSizeRowsLotsOfIntents,
			// Current time in the future enough for intents to get resolved
			now: hlc.Timestamp{
				WallTime: (intentAgeThreshold + 100*time.Second).Nanoseconds(),
			},
			// GC everything beyond intent resolution threshold
			ttl: int32(intentAgeThreshold.Seconds()),
		},
		{
			ds: someVersionsMidSizeRows,
			now: hlc.Timestamp{
				WallTime: 100 * time.Second.Nanoseconds(),
			},
			ttl: 1,
		},
	} {
		t.Run(fmt.Sprintf("%v@%v,ttl=%v", tc.ds, tc.now, tc.ttl), func(t *testing.T) {
			eng := relational.NewDefaultInMem()
			defer eng.Close()

			tc.ds.dist(N, rng).setupTest(t, eng, *tc.ds.desc())
			snap := eng.NewSnapshot()

			oldGCer := makeFakeGCer()
			policy := zonepb.GCPolicy{TTLSeconds: tc.ttl}
			newThreshold := CalculateThreshold(tc.now, policy)
			gcInfoOld, err := runGCOld(ctx, tc.ds.desc(), snap, tc.now,
				newThreshold, intentAgeThreshold, policy,
				&oldGCer,
				oldGCer.resolveIntents,
				oldGCer.resolveIntentsAsync)
			require.NoError(t, err)

			newGCer := makeFakeGCer()
			gcInfoNew, err := Run(ctx, tc.ds.desc(), snap, tc.now,
				newThreshold, intentAgeThreshold, policy,
				&newGCer,
				newGCer.resolveIntents,
				newGCer.resolveIntentsAsync)
			require.NoError(t, err)

			oldGCer.normalize()
			newGCer.normalize()
			require.EqualValues(t, gcInfoOld, gcInfoNew)
			require.EqualValues(t, oldGCer, newGCer)
		})
	}
}

// BenchmarkRun benchmarks the old and implementations of Run with different
// data distributions.
func BenchmarkRun(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	ctx := context.Background()
	runGC := func(eng relational.Engine, old bool, spec randomRunGCTestSpec) (Info, error) {
		runGCFunc := Run
		if old {
			runGCFunc = runGCOld
		}
		snap := eng.NewSnapshot()
		policy := zonepb.GCPolicy{TTLSeconds: spec.ttl}
		return runGCFunc(ctx, spec.ds.desc(), snap, spec.now,
			CalculateThreshold(spec.now, policy), intentAgeThreshold,
			policy,
			NoopGCer{},
			func(ctx context.Context, intents []roachpb.Intent) error {
				return nil
			},
			func(ctx context.Context, txn *roachpb.Transaction, intents []roachpb.LockUpdate) error {
				return nil
			})
	}
	makeTest := func(old bool, spec randomRunGCTestSpec) func(b *testing.B) {
		return func(b *testing.B) {
			eng := relational.NewDefaultInMem()
			defer eng.Close()
			ms := spec.ds.dist(b.N, rng).setupTest(b, eng, *spec.ds.desc())
			b.SetBytes(int64(float64(ms.Total()) / float64(b.N)))
			b.ResetTimer()
			_, err := runGC(eng, old, spec)
			b.StopTimer()
			require.NoError(b, err)
		}
	}
	specsWithTTLs := func(
		ds distSpec, now hlc.Timestamp, ttls []int32,
	) (specs []randomRunGCTestSpec) {
		for _, ttl := range ttls {
			specs = append(specs, randomRunGCTestSpec{
				ds:  ds,
				now: now,
				ttl: ttl,
			})
		}
		return specs
	}
	ts100 := hlc.Timestamp{WallTime: (100 * time.Second).Nanoseconds()}
	ttls := []int32{0, 25, 50, 75, 100}
	specs := specsWithTTLs(fewVersionsTinyRows, ts100, ttls)
	specs = append(specs, specsWithTTLs(someVersionsMidSizeRows, ts100, ttls)...)
	specs = append(specs, specsWithTTLs(lotsOfVersionsMidSizeRows, ts100, ttls)...)
	for _, old := range []bool{true, false} {
		b.Run(fmt.Sprintf("old=%v", old), func(b *testing.B) {
			for _, spec := range specs {
				b.Run(fmt.Sprint(spec.ds), makeTest(old, spec))
			}
		})
	}
}

type fakeGCer struct {
	gcKeys     map[string]roachpb.GCRequest_GCKey
	threshold  Threshold
	intents    []roachpb.Intent
	txnIntents []txnIntents
}

func makeFakeGCer() fakeGCer {
	return fakeGCer{
		gcKeys: make(map[string]roachpb.GCRequest_GCKey),
	}
}

var _ GCer = (*fakeGCer)(nil)

func (f *fakeGCer) SetGCThreshold(ctx context.Context, t Threshold) error {
	f.threshold = t
	return nil
}

func (f *fakeGCer) GC(ctx context.Context, keys []roachpb.GCRequest_GCKey) error {
	for _, k := range keys {
		f.gcKeys[k.Key.String()] = k
	}
	return nil
}

func (f *fakeGCer) resolveIntentsAsync(
	_ context.Context, txn *roachpb.Transaction, intents []roachpb.LockUpdate,
) error {
	f.txnIntents = append(f.txnIntents, txnIntents{txn: txn, intents: intents})
	return nil
}

func (f *fakeGCer) resolveIntents(_ context.Context, intents []roachpb.Intent) error {
	f.intents = append(f.intents, intents...)
	return nil
}

func (f *fakeGCer) normalize() {
	sortIntents := func(i, j int) bool {
		return intentLess(&f.intents[i], &f.intents[j])
	}
	sort.Slice(f.intents, sortIntents)
	for i := range f.txnIntents {
		sort.Slice(f.txnIntents[i].intents, sortIntents)
	}
	sort.Slice(f.txnIntents, func(i, j int) bool {
		return f.txnIntents[i].txn.ID.String() < f.txnIntents[j].txn.ID.String()
	})
}

func intentLess(a, b *roachpb.Intent) bool {
	cmp := a.Key.Compare(b.Key)
	switch {
	case cmp < 0:
		return true
	case cmp > 0:
		return false
	default:
		return a.Txn.ID.String() < b.Txn.ID.String()
	}
}

type txnIntents struct {
	txn     *roachpb.Transaction
	intents []roachpb.LockUpdate
}
