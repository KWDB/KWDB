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

package fsm_test

import (
	"bytes"
	"context"
	"fmt"
	"os"

	. "gitee.com/kwbasedb/kwbase/pkg/util/fsm"
)

/// States.

type stateNoTxn struct{}
type stateOpen struct {
	RetryIntent Bool
}
type stateAborted struct {
	RetryIntent Bool
}
type stateRestartWait struct{}

func (stateNoTxn) State()       {}
func (stateOpen) State()        {}
func (stateAborted) State()     {}
func (stateRestartWait) State() {}

func (stateNoTxn) GetImplicitTxn() bool       { return false }
func (stateOpen) GetImplicitTxn() bool        { return false }
func (stateAborted) GetImplicitTxn() bool     { return false }
func (stateRestartWait) GetImplicitTxn() bool { return false }

/// Events.

type eventNoTopLevelTransition struct {
	RetryIntent Bool
}
type eventTxnStart struct{}
type eventTxnFinish struct{}
type eventTxnRestart struct{}
type eventNonRetriableErr struct {
	IsCommit Bool
}
type eventRetriableErr struct {
	CanAutoRetry Bool
	IsCommit     Bool
}

func (eventNoTopLevelTransition) Event() {}
func (eventTxnStart) Event()             {}
func (eventTxnFinish) Event()            {}
func (eventTxnRestart) Event()           {}
func (eventNonRetriableErr) Event()      {}
func (eventRetriableErr) Event()         {}

/// Transitions.

var txnStateTransitions = Compile(Pattern{
	stateNoTxn{}: {
		eventNoTopLevelTransition{False}: {
			Description: "my test transition",
			Next:        stateNoTxn{},
			Action:      writeAction("Identity"),
		},
		eventTxnStart{}: {
			Next:   stateOpen{False},
			Action: writeAction("Open..."),
		},
	},
	stateOpen{Var("x")}: {
		eventNoTopLevelTransition{False}: {
			Next:   stateOpen{Var("x")},
			Action: writeAction("Identity"),
		},
		eventTxnFinish{}: {
			Next:   stateNoTxn{},
			Action: writeAction("Finish..."),
		},
		eventNonRetriableErr{True}: {
			Next:   stateNoTxn{},
			Action: writeAction("Error"),
		},
		eventRetriableErr{True, Any}: {
			Next:   stateOpen{Var("x")},
			Action: writeAction("Transition 6"),
		},
		eventNonRetriableErr{False}: {
			Next:   stateAborted{Var("x")},
			Action: writeAction("Abort"),
		},
	},
	stateOpen{False}: {
		eventNoTopLevelTransition{True}: {
			Next:   stateOpen{True},
			Action: writeAction("Make Open"),
		},
		eventRetriableErr{False, Any}: {
			Next:   stateAborted{False},
			Action: writeAction("Abort"),
		},
	},
	stateOpen{True}: {
		eventRetriableErr{False, False}: {
			Next:   stateRestartWait{},
			Action: writeAction("Wait for restart"),
		},
		eventRetriableErr{False, True}: {
			Next:   stateNoTxn{},
			Action: writeAction("No more"),
		},
	},
	stateAborted{Var("x")}: {
		eventNoTopLevelTransition{False}: {
			Next:   stateAborted{Var("x")},
			Action: writeAction("Identity"),
		},
		eventTxnFinish{}: {
			Next:   stateNoTxn{},
			Action: writeAction("Abort finished"),
		},
		eventTxnStart{}: {
			Next:   stateOpen{Var("x")},
			Action: writeAction("Open from abort"),
		},
		eventNonRetriableErr{Any}: {
			Next:   stateAborted{Var("x")},
			Action: writeAction("Abort"),
		},
	},
	stateRestartWait{}: {
		eventNoTopLevelTransition{False}: {
			Next:   stateRestartWait{},
			Action: writeAction("Identity"),
		},
		eventTxnFinish{}: {
			Next:   stateNoTxn{},
			Action: writeAction("No more"),
		},
		eventTxnRestart{}: {
			Next:   stateOpen{True},
			Action: writeAction("Restarting"),
		},
		eventNonRetriableErr{Any}: {
			Next:   stateAborted{True},
			Action: writeAction("Abort"),
		},
	},
})

func writeAction(s string) func(Args) error {
	return func(a Args) error {
		a.Extended.(*executor).write(s)
		return nil
	}
}

type executor struct {
	m   Machine
	log bytes.Buffer
}

func (e *executor) write(s string) {
	e.log.WriteString(s)
	e.log.WriteString("\n")
}

func ExampleMachine() {
	ctx := context.Background()

	var e executor
	e.m = MakeMachine(txnStateTransitions, stateNoTxn{}, &e)
	_ = e.m.Apply(ctx, eventTxnStart{})
	_ = e.m.Apply(ctx, eventNoTopLevelTransition{True})
	_ = e.m.Apply(ctx, eventRetriableErr{False, False})
	_ = e.m.Apply(ctx, eventTxnRestart{})
	_ = e.m.Apply(ctx, eventTxnFinish{})
	fmt.Print(e.log.String())

	// Output:
	// Open...
	// Make Open
	// Wait for restart
	// Restarting
	// Finish...
}

func ExampleTransitions_WriteReport() {
	txnStateTransitions.WriteReport(os.Stdout)

	// Output:
	// Aborted{RetryIntent:false}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		TxnFinish{}
	// 		TxnStart{}
	// 	missing events:
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnRestart{}
	// Aborted{RetryIntent:true}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		TxnFinish{}
	// 		TxnStart{}
	// 	missing events:
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnRestart{}
	// NoTxn{}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		TxnStart{}
	// 	missing events:
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnFinish{}
	// 		TxnRestart{}
	// Open{RetryIntent:false}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnFinish{}
	// 	missing events:
	// 		TxnRestart{}
	// 		TxnStart{}
	// Open{RetryIntent:true}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnFinish{}
	// 	missing events:
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		TxnRestart{}
	// 		TxnStart{}
	// RestartWait{}
	// 	handled events:
	// 		NoTopLevelTransition{RetryIntent:false}
	// 		NonRetriableErr{IsCommit:false}
	// 		NonRetriableErr{IsCommit:true}
	// 		TxnFinish{}
	// 		TxnRestart{}
	// 	missing events:
	// 		NoTopLevelTransition{RetryIntent:true}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:false, IsCommit:true}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:false}
	// 		RetriableErr{CanAutoRetry:true, IsCommit:true}
	// 		TxnStart{}
}

func ExampleTransitions_WriteDotGraph() {
	txnStateTransitions.WriteDotGraph(os.Stdout, stateNoTxn{})

	// Output:
	// digraph finite_state_machine {
	// 	rankdir=LR;
	//
	// 	node [shape = doublecircle]; "NoTxn{}";
	// 	node [shape = point ]; qi
	// 	qi -> "NoTxn{}";
	//
	// 	node [shape = circle];
	// 	"Aborted{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "NoTopLevelTransition{RetryIntent:false}"]
	// 	"Aborted{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "NonRetriableErr{IsCommit:false}"]
	// 	"Aborted{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "NonRetriableErr{IsCommit:true}"]
	// 	"Aborted{RetryIntent:false}" -> "NoTxn{}" [label = "TxnFinish{}"]
	// 	"Aborted{RetryIntent:false}" -> "Open{RetryIntent:false}" [label = "TxnStart{}"]
	// 	"Aborted{RetryIntent:true}" -> "Aborted{RetryIntent:true}" [label = "NoTopLevelTransition{RetryIntent:false}"]
	// 	"Aborted{RetryIntent:true}" -> "Aborted{RetryIntent:true}" [label = "NonRetriableErr{IsCommit:false}"]
	// 	"Aborted{RetryIntent:true}" -> "Aborted{RetryIntent:true}" [label = "NonRetriableErr{IsCommit:true}"]
	// 	"Aborted{RetryIntent:true}" -> "NoTxn{}" [label = "TxnFinish{}"]
	// 	"Aborted{RetryIntent:true}" -> "Open{RetryIntent:true}" [label = "TxnStart{}"]
	// 	"NoTxn{}" -> "NoTxn{}" [label = <NoTopLevelTransition{RetryIntent:false}<BR/><I>my test transition</I>>]
	// 	"NoTxn{}" -> "Open{RetryIntent:false}" [label = "TxnStart{}"]
	// 	"Open{RetryIntent:false}" -> "Open{RetryIntent:false}" [label = "NoTopLevelTransition{RetryIntent:false}"]
	// 	"Open{RetryIntent:false}" -> "Open{RetryIntent:true}" [label = "NoTopLevelTransition{RetryIntent:true}"]
	// 	"Open{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "NonRetriableErr{IsCommit:false}"]
	// 	"Open{RetryIntent:false}" -> "NoTxn{}" [label = "NonRetriableErr{IsCommit:true}"]
	// 	"Open{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "RetriableErr{CanAutoRetry:false, IsCommit:false}"]
	// 	"Open{RetryIntent:false}" -> "Aborted{RetryIntent:false}" [label = "RetriableErr{CanAutoRetry:false, IsCommit:true}"]
	// 	"Open{RetryIntent:false}" -> "Open{RetryIntent:false}" [label = "RetriableErr{CanAutoRetry:true, IsCommit:false}"]
	// 	"Open{RetryIntent:false}" -> "Open{RetryIntent:false}" [label = "RetriableErr{CanAutoRetry:true, IsCommit:true}"]
	// 	"Open{RetryIntent:false}" -> "NoTxn{}" [label = "TxnFinish{}"]
	// 	"Open{RetryIntent:true}" -> "Open{RetryIntent:true}" [label = "NoTopLevelTransition{RetryIntent:false}"]
	// 	"Open{RetryIntent:true}" -> "Aborted{RetryIntent:true}" [label = "NonRetriableErr{IsCommit:false}"]
	// 	"Open{RetryIntent:true}" -> "NoTxn{}" [label = "NonRetriableErr{IsCommit:true}"]
	// 	"Open{RetryIntent:true}" -> "RestartWait{}" [label = "RetriableErr{CanAutoRetry:false, IsCommit:false}"]
	// 	"Open{RetryIntent:true}" -> "NoTxn{}" [label = "RetriableErr{CanAutoRetry:false, IsCommit:true}"]
	// 	"Open{RetryIntent:true}" -> "Open{RetryIntent:true}" [label = "RetriableErr{CanAutoRetry:true, IsCommit:false}"]
	// 	"Open{RetryIntent:true}" -> "Open{RetryIntent:true}" [label = "RetriableErr{CanAutoRetry:true, IsCommit:true}"]
	// 	"Open{RetryIntent:true}" -> "NoTxn{}" [label = "TxnFinish{}"]
	// 	"RestartWait{}" -> "RestartWait{}" [label = "NoTopLevelTransition{RetryIntent:false}"]
	// 	"RestartWait{}" -> "Aborted{RetryIntent:true}" [label = "NonRetriableErr{IsCommit:false}"]
	// 	"RestartWait{}" -> "Aborted{RetryIntent:true}" [label = "NonRetriableErr{IsCommit:true}"]
	// 	"RestartWait{}" -> "NoTxn{}" [label = "TxnFinish{}"]
	// 	"RestartWait{}" -> "Open{RetryIntent:true}" [label = "TxnRestart{}"]
	// }
}
