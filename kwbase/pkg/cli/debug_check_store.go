// Copyright 2019 The Cockroach Authors.
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

package cli

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/stateloader"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/sync/errgroup"
)

var debugCheckStoreCmd = &cobra.Command{
	Use:   "check-store <directory>",
	Short: "consistency check for a single store",
	Long: `
Perform local consistency checks of a single store.

Capable of detecting the following errors:
* Raft logs that are inconsistent with their metadata
* MVCC stats that are inconsistent with the data within the range
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugCheckStoreCmd),
}

var errCheckFoundProblem = errors.New("check-store found problems")

func runDebugCheckStoreCmd(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	dir := args[0]
	foundProblem := false
	// At time of writing, this takes around ~110s for 71GB (1k warehouse TPCC
	// fully compacted) on local SSD. This is quite fast, well north of 600MB/s.
	err := checkStoreRangeStats(ctx, dir, func(args ...interface{}) {
		fmt.Println(args...)
	})
	foundProblem = foundProblem || err != nil
	if err != nil && !errors.Is(err, errCheckFoundProblem) {
		_, _ = fmt.Println(err)
	}
	// This is not optimized at all, but for the same data set as above, it
	// returns instantly, so we won't need to optimize it for quite some time.
	err = checkStoreRaftState(ctx, dir, func(format string, args ...interface{}) {
		_, _ = fmt.Printf(format, args...)
	})
	foundProblem = foundProblem || err != nil
	if err != nil && !errors.Is(err, errCheckFoundProblem) {
		fmt.Println(err)
	}
	if foundProblem {
		return errCheckFoundProblem
	}
	return nil
}

type replicaCheckInfo struct {
	truncatedIndex uint64
	appliedIndex   uint64
	firstIndex     uint64
	lastIndex      uint64
	committedIndex uint64
}

type checkInput struct {
	eng  storage.Engine
	desc *roachpb.RangeDescriptor
	sl   stateloader.StateLoader
}

type checkResult struct {
	desc           *roachpb.RangeDescriptor
	err            error
	claimMS, actMS enginepb.MVCCStats
}

func (cr *checkResult) Error() error {
	var errs []string
	if cr.err != nil {
		errs = append(errs, cr.err.Error())
	}
	if !cr.actMS.Equal(enginepb.MVCCStats{}) && !cr.actMS.Equal(cr.claimMS) && cr.claimMS.ContainsEstimates <= 0 {
		err := fmt.Sprintf("stats inconsistency:\n- stored:\n%+v\n- recomputed:\n%+v\n- diff:\n%s",
			cr.claimMS, cr.actMS, strings.Join(pretty.Diff(cr.claimMS, cr.actMS), ","),
		)
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		if cr.desc != nil {
			prefix := cr.desc.String() + ": "
			for i := range errs {
				errs[i] = prefix + errs[i]
			}
		}
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

func worker(ctx context.Context, in checkInput) checkResult {
	desc, eng := in.desc, in.eng

	res := checkResult{desc: desc}
	claimedMS, err := in.sl.LoadMVCCStats(ctx, eng)
	if err != nil {
		res.err = err
		return res
	}
	ms, err := rditer.ComputeStatsForRange(desc, eng, claimedMS.LastUpdateNanos)
	if err != nil {
		res.err = err
		return res
	}
	res.claimMS = claimedMS
	res.actMS = ms
	return res
}

func checkStoreRangeStats(
	ctx context.Context,
	dir string, // the store directory
	println func(...interface{}), // fmt.Println outside of tests
) error {
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	eng, err := OpenExistingStore(dir, stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	inCh := make(chan checkInput)
	outCh := make(chan checkResult, 1000)

	n := runtime.NumCPU()
	var g errgroup.Group
	for i := 0; i < n; i++ {
		g.Go(func() error {
			for in := range inCh {
				outCh <- worker(ctx, in)
			}
			return nil
		})
	}

	go func() {
		if err := kvserver.IterateRangeDescriptors(ctx, eng,
			func(desc roachpb.RangeDescriptor) (bool, error) {
				inCh <- checkInput{eng: eng, desc: &desc, sl: stateloader.Make(desc.RangeID)}
				return false, nil
			}); err != nil {
			outCh <- checkResult{err: err}
		}
		close(inCh) // we were the only writer
		if err := g.Wait(); err != nil {
			outCh <- checkResult{err: err}
		}
		close(outCh) // all writers done due to Wait()
	}()

	foundProblem := false
	var total enginepb.MVCCStats
	var cR, cE int
	for res := range outCh {
		cR++
		if err := res.Error(); err != nil {
			foundProblem = true
			errS := err.Error()
			println(errS)
		} else {
			if res.claimMS.ContainsEstimates > 0 {
				cE++
			}
			total.Add(res.actMS)
		}
	}

	println(fmt.Sprintf("scanned %d ranges (%d with estimates), total stats %s", cR, cE, &total))

	if foundProblem {
		// The details were already emitted.
		return errCheckFoundProblem
	}
	return nil
}

func checkStoreRaftState(
	ctx context.Context,
	dir string, // the store directory
	printf func(string, ...interface{}), // fmt.Printf outside of tests
) error {
	foundProblem := false
	goldenPrintf := printf
	printf = func(format string, args ...interface{}) {
		foundProblem = true
		goldenPrintf(format, args...)
	}
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	db, err := OpenExistingStore(dir, stopper, true /* readOnly */)
	if err != nil {
		return err
	}

	// Iterate over the entire range-id-local space.
	start := roachpb.Key(keys.LocalRangeIDPrefix)
	end := start.PrefixEnd()

	replicaInfo := map[roachpb.RangeID]*replicaCheckInfo{}
	getReplicaInfo := func(rangeID roachpb.RangeID) *replicaCheckInfo {
		if info, ok := replicaInfo[rangeID]; ok {
			return info
		}
		replicaInfo[rangeID] = &replicaCheckInfo{}
		return replicaInfo[rangeID]
	}

	if _, err := storage.MVCCIterate(ctx, db, start, end, hlc.MaxTimestamp,
		storage.MVCCScanOptions{Inconsistent: true}, func(kv roachpb.KeyValue) (bool, error) {
			rangeID, _, suffix, detail, err := keys.DecodeRangeIDKey(kv.Key)
			if err != nil {
				return false, err
			}

			switch {
			case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
				var hs raftpb.HardState
				if err := kv.Value.GetProto(&hs); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).committedIndex = hs.Commit
			case bytes.Equal(suffix, keys.LocalRaftTruncatedStateLegacySuffix):
				var trunc roachpb.RaftTruncatedState
				if err := kv.Value.GetProto(&trunc); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).truncatedIndex = trunc.Index
			case bytes.Equal(suffix, keys.LocalRangeAppliedStateSuffix):
				var state enginepb.RangeAppliedState
				if err := kv.Value.GetProto(&state); err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = state.RaftAppliedIndex
			case bytes.Equal(suffix, keys.LocalRaftAppliedIndexLegacySuffix):
				idx, err := kv.Value.GetInt()
				if err != nil {
					return false, err
				}
				getReplicaInfo(rangeID).appliedIndex = uint64(idx)
			case bytes.Equal(suffix, keys.LocalRaftLogSuffix):
				_, index, err := encoding.DecodeUint64Ascending(detail)
				if err != nil {
					return false, err
				}
				ri := getReplicaInfo(rangeID)
				if ri.firstIndex == 0 {
					ri.firstIndex = index
					ri.lastIndex = index
				} else {
					if index != ri.lastIndex+1 {
						printf("range %s: log index anomaly: %v followed by %v\n",
							rangeID, ri.lastIndex, index)
					}
					ri.lastIndex = index
				}
			}

			return false, nil
		}); err != nil {
		return err
	}

	for rangeID, info := range replicaInfo {
		if info.truncatedIndex != 0 && info.truncatedIndex != info.firstIndex-1 {
			printf("range %s: truncated index %v should equal first index %v - 1\n",
				rangeID, info.truncatedIndex, info.firstIndex)
		}
		if info.firstIndex > info.lastIndex {
			printf("range %s: [first index, last index] is [%d, %d]\n",
				rangeID, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex < info.firstIndex || info.appliedIndex > info.lastIndex {
			printf("range %s: applied index %v should be between first index %v and last index %v\n",
				rangeID, info.appliedIndex, info.firstIndex, info.lastIndex)
		}
		if info.appliedIndex > info.committedIndex {
			printf("range %s: committed index %d must not trail applied index %d\n",
				rangeID, info.committedIndex, info.appliedIndex)
		}
		if info.committedIndex > info.lastIndex {
			printf("range %s: committed index %d ahead of last index  %d\n",
				rangeID, info.committedIndex, info.lastIndex)
		}
	}
	if foundProblem {
		return errCheckFoundProblem
	}

	return nil
}
