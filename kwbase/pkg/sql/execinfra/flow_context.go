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

// This file lives here instead of sql/flowinfra to avoid an import cycle.

package execinfra

import (
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/tse"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/stop"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

// FlowCtx encompasses the configuration parameters needed for various flow
// components.
type FlowCtx struct {
	log.AmbientContext

	Cfg *ServerConfig

	// ID is a unique identifier for a remote flow. It is mainly used as a key
	// into the flowRegistry. Since local flows do not need to exist in the flow
	// registry (no inbound stream connections need to be performed), they are not
	// assigned ids. This is done for performance reasons, as local flows are
	// more likely to be dominated by setup time.
	ID execinfrapb.FlowID

	// EvalCtx is used by all the processors in the flow to evaluate expressions.
	// Processors that intend to evaluate expressions with this EvalCtx should
	// get a copy with NewEvalCtx instead of storing a pointer to this one
	// directly (since some processor mutate the EvalContext they use).
	//
	// TODO(andrei): Get rid of this field and pass a non-shared EvalContext to
	// cores of the processors that need it.
	EvalCtx *tree.EvalContext

	// The transaction in which kv operations performed by processors in the flow
	// must be performed. Processors in the Flow will use this txn concurrently.
	// This field is generally not nil, except for flows that don't run in a
	// higher-level txn (like backfills).
	Txn *kv.Txn

	// nodeID is the ID of the node on which the processors using this FlowCtx
	// run.
	NodeID roachpb.NodeID

	// TraceKV is true if KV tracing was requested by the session.
	TraceKV bool

	// Local is true if this flow is being run as part of a local-only query.
	Local bool

	// Identify whether the timing query is a single node
	// Single node is 1
	// GateWay nodes with multiple nodes are greater than 1, while other nodes are 0
	NoopInputNums uint32

	// Distributed temporal query
	TsTableReaders []Processor

	// batchlookup join input
	// only pass the data chunk formed by batchlookupjoiner to tse for multiple model processing
	// when the switch is on and the server starts with single node mode.
	TsBatchLookupInput *tse.DataChunkGo

	// TsHandleMap define a tsreader id vs. tshandle map
	// only pass the tshandle to push data chunk to tse for multiple model processing
	// when the switch is on and the server starts with single node mode.
	TsHandleMap map[int32]unsafe.Pointer

	// Identify whether the tse connection is still valid
	// used only to jump out of the waiting loop in BLJ operator
	// when tse returns an error.
	TsHandleBreak bool

	// PushingBLJCount define the number of BLJ operators pussing rel data into ts engine
	PushingBLJCount map[int32]int

	// Mu is the Mutex to protect TsHandleMap
	// only used to get tshandle when the switch is on and the server starts
	// with single node mode.
	Mu syncutil.Mutex
}

// NewEvalCtx returns a modifiable copy of the FlowCtx's EvalContext.
// Processors should use this method any time they need to store a pointer to
// the EvalContext, since processors may mutate the EvalContext. Specifically,
// every processor that runs ProcOutputHelper.Init must pass in a modifiable
// EvalContext, since it stores that EvalContext in its exprHelpers and mutates
// them at runtime to ensure expressions are evaluated with the correct indexed
// var context.
func (ctx *FlowCtx) NewEvalCtx() *tree.EvalContext {
	return ctx.EvalCtx.Copy()
}

// TestingKnobs returns the distsql testing knobs for this flow context.
func (ctx *FlowCtx) TestingKnobs() TestingKnobs {
	return ctx.Cfg.TestingKnobs
}

// Stopper returns the stopper for this flowCtx.
func (ctx *FlowCtx) Stopper() *stop.Stopper {
	return ctx.Cfg.Stopper
}
