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

package fsm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type state1 struct{}
type state2 struct{}
type state3 struct {
	Field Bool
}
type state4 struct {
	Field1 Bool
	Field2 Bool
}

func (state1) State() {}
func (state2) State() {}
func (state3) State() {}
func (state4) State() {}

func (state1) GetImplicitTxn() bool { return false }
func (state2) GetImplicitTxn() bool { return false }
func (state3) GetImplicitTxn() bool { return false }
func (state4) GetImplicitTxn() bool { return false }

type event1 struct{}
type event2 struct{}
type event3 struct {
	Field Bool
}
type event4 struct {
	Field1 Bool
	Field2 Bool
}

func (event1) Event() {}
func (event2) Event() {}
func (event3) Event() {}
func (event4) Event() {}

var noAction func(Args) error

func noErr(f func(Args)) func(Args) error {
	return func(a Args) error { f(a); return nil }
}

func (tr Transitions) applyWithoutErr(t *testing.T, a Args) State {
	s, err := tr.apply(a)
	require.Nil(t, err)
	return s
}
func (tr Transitions) applyWithErr(t *testing.T, a Args) error {
	_, err := tr.apply(a)
	require.NotNil(t, err)
	return err
}

func TestBasicTransitions(t *testing.T) {
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, noAction, ""},
			event2{}: {state1{}, noAction, ""},
		},
		state2{}: {
			event1{}: {state1{}, noAction, ""},
			event2{}: {state2{}, noAction, ""},
		},
	})

	// Valid transitions.
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state1{}, Event: event1{}}), state2{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state1{}, Event: event2{}}), state1{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state2{}, Event: event1{}}), state1{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state2{}, Event: event2{}}), state2{})

	// Invalid transitions.
	notFoundErr := TransitionNotFoundError{}
	require.IsType(t, trans.applyWithErr(t, Args{Prev: state3{}, Event: event1{}}), notFoundErr)
	require.IsType(t, trans.applyWithErr(t, Args{Prev: state1{}, Event: event3{}}), notFoundErr)
}

func TestTransitionActions(t *testing.T) {
	var extendedState int
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, noErr(func(a Args) { *a.Extended.(*int) = 1 }), ""},
			event2{}: {state1{}, noErr(func(a Args) { *a.Extended.(*int) = 2 }), ""},
		},
		state2{}: {
			event1{}: {state1{}, noErr(func(a Args) { *a.Extended.(*int) = 3 }), ""},
			event2{}: {state2{}, noErr(func(a Args) { *a.Extended.(*int) = 4 }), ""},
		},
	})

	trans.applyWithoutErr(t, Args{Prev: state1{}, Event: event1{}, Extended: &extendedState})
	require.Equal(t, extendedState, 1)

	trans.applyWithoutErr(t, Args{Prev: state1{}, Event: event2{}, Extended: &extendedState})
	require.Equal(t, extendedState, 2)

	trans.applyWithoutErr(t, Args{Prev: state2{}, Event: event1{}, Extended: &extendedState})
	require.Equal(t, extendedState, 3)

	trans.applyWithoutErr(t, Args{Prev: state2{}, Event: event2{}, Extended: &extendedState})
	require.Equal(t, extendedState, 4)
}

func TestTransitionsWithWildcards(t *testing.T) {
	trans := Compile(Pattern{
		state3{Any}: {
			event3{Any}: {state1{}, noAction, ""},
		},
	})

	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{True}, Event: event3{True}}), state1{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{True}, Event: event3{False}}), state1{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{False}, Event: event3{True}}), state1{})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{False}, Event: event3{False}}), state1{})
}

func TestTransitionsWithVarBindings(t *testing.T) {
	trans := Compile(Pattern{
		state3{Var("a")}: {
			event3{Var("b")}: {state4{Var("b"), Var("a")}, noAction, ""},
		},
	})

	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{True}, Event: event3{True}}), state4{True, True})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{True}, Event: event3{False}}), state4{False, True})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{False}, Event: event3{True}}), state4{True, False})
	require.Equal(t, trans.applyWithoutErr(t, Args{Prev: state3{False}, Event: event3{False}}), state4{False, False})
}

func TestCurState(t *testing.T) {
	ctx := context.Background()
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, func(a Args) error { return nil }, ""},
		},
	})
	m := MakeMachine(trans, state1{}, nil /* es */)

	e := Event(event1{})
	if err := m.Apply(ctx, e); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, m.CurState(), state2{})
}

func BenchmarkPatternCompilation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Compile(Pattern{
			state1{}: {
				event4{True, Any}:  {state2{}, noAction, ""},
				event4{False, Any}: {state1{}, noAction, ""},
			},
			state2{}: {
				event4{Any, Any}: {state2{}, noAction, ""},
			},
			state3{True}: {
				event1{}: {state1{}, noAction, ""},
			},
			state3{False}: {
				event3{True}:  {state2{}, noAction, ""},
				event3{False}: {state1{}, noAction, ""},
			},
			state4{Var("x"), Var("y")}: {
				event4{True, True}:   {state1{}, noAction, ""},
				event4{True, False}:  {state2{}, noAction, ""},
				event4{False, True}:  {state3{Var("x")}, noAction, ""},
				event4{False, False}: {state4{Var("y"), Var("x")}, noAction, ""},
			},
		})
	}
}

func BenchmarkStateTransition(b *testing.B) {
	var extendedState int
	ctx := context.Background()
	trans := Compile(Pattern{
		state1{}: {
			event1{}: {state2{}, noErr(func(a Args) { *a.Extended.(*int) = 1 }), ""},
			event2{}: {state1{}, noErr(func(a Args) { *a.Extended.(*int) = 2 }), ""},
		},
		state2{}: {
			event1{}: {state1{}, noErr(func(a Args) { *a.Extended.(*int) = 3 }), ""},
			event2{}: {state2{}, noErr(func(a Args) { *a.Extended.(*int) = 4 }), ""},
		},
		// Unused, but complicates transition graph. Demonstrates that a more
		// complicated graph does not hurt runtime performance.
		state3{True}: {
			event1{}: {state1{}, noAction, ""},
		},
		state3{False}: {
			event3{True}:  {state2{}, noAction, ""},
			event3{False}: {state1{}, noAction, ""},
		},
		state4{Var("x"), Var("y")}: {
			event4{True, True}:   {state1{}, noAction, ""},
			event4{True, False}:  {state2{}, noAction, ""},
			event4{False, True}:  {state3{Var("x")}, noAction, ""},
			event4{False, False}: {state4{Var("y"), Var("x")}, noAction, ""},
		},
	})
	m := MakeMachine(trans, state1{}, &extendedState)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := Event(event1{})
		if i%2 == 1 {
			e = event2{}
		}
		_ = m.ApplyWithPayload(ctx, e, 12)
	}
}
