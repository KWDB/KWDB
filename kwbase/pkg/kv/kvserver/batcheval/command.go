// Copyright 2014 The Cockroach Authors.
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

package batcheval

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/batcheval/result"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/spanset"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// declareKeysFunc adds all key spans that a command touches to the latchSpans
// set. It then adds all key spans within which that the command expects to have
// isolation from conflicting transactions to the lockSpans set.
type declareKeysFunc func(
	_ *roachpb.RangeDescriptor, _ roachpb.Header, _ roachpb.Request, latchSpans, lockSpans *spanset.SpanSet,
)

// A Command is the implementation of a single request within a BatchRequest.
type Command struct {
	// DeclareKeys adds all keys this command touches, and when (if applicable),
	// to the given SpanSet.
	//
	// TODO(nvanbenschoten): rationalize this RangeDescriptor. Can it change
	// between key declaration and cmd evaluation? Really, do it.
	DeclareKeys declareKeysFunc

	// Eval{RW,RO} evaluates a read-{write,only} command respectively on the
	// given engine.{ReadWriter,Reader}. This is typically derived from
	// engine.NewBatch or engine.NewReadOnly (which is more performant than
	// engine.Batch for read-only commands).
	// It should populate the supplied response (always a non-nil pointer to the
	// correct type) and return special side effects (if any) in the Result. If
	// it writes to the engine it should also update *CommandArgs.Stats. It
	// should treat the provided request as immutable.
	//
	// Only one of these is ever set at a time.
	EvalRW func(context.Context, relational.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error)
	EvalRO func(context.Context, relational.Reader, CommandArgs, roachpb.Response) (result.Result, error)
}

var cmds = make(map[roachpb.Method]Command)

// RegisterReadWriteCommand makes a read-write command available for execution.
// It must only be called before any evaluation takes place.
func RegisterReadWriteCommand(
	method roachpb.Method,
	declare declareKeysFunc,
	impl func(context.Context, relational.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error),
) {
	register(method, Command{
		DeclareKeys: declare,
		EvalRW:      impl,
	})
}

// RegisterReadOnlyCommand makes a read-only command available for execution. It
// must only be called before any evaluation takes place.
func RegisterReadOnlyCommand(
	method roachpb.Method,
	declare declareKeysFunc,
	impl func(context.Context, relational.Reader, CommandArgs, roachpb.Response) (result.Result, error),
) {
	register(method, Command{
		DeclareKeys: declare,
		EvalRO:      impl,
	})
}

func register(method roachpb.Method, command Command) {
	if _, ok := cmds[method]; ok {
		log.Fatalf(context.TODO(), "cannot overwrite previously registered method %v", method)
	}
	cmds[method] = command
}

// UnregisterCommand is provided for testing and allows removing a command.
// It is a no-op if the command is not registered.
func UnregisterCommand(method roachpb.Method) {
	delete(cmds, method)
}

// LookupCommand returns the command for the given method, with the boolean
// indicating success or failure.
func LookupCommand(method roachpb.Method) (Command, bool) {
	cmd, ok := cmds[method]
	return cmd, ok
}
