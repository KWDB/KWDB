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

package mysqlwire

import (
	"bytes"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/lib/pq/oid"
)

func newTestRegistry() *stmtRegistry {
	reg := &stmtRegistry{}
	reg.init()
	return reg
}

// TestStmtRegistryLifecycle covers the ordinary path: a statement is reserved
// by the reader, filled in by the Describe, and only then becomes reachable by
// its handle.
func TestStmtRegistryLifecycle(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reg := newTestRegistry()
	ps := reg.begin("SELECT $1")

	// Before the Describe completes the handle must not resolve, or a client
	// could execute a statement whose parameter types are not yet known.
	if _, ok := reg.get(ps.id); ok {
		t.Fatal("handle resolved before the describe completed")
	}

	reg.setMetadata(ps, []oid.Oid{oid.T_int8}, sqlbase.ResultColumns{{Name: "x"}})
	reg.commit(ps)

	got, ok := reg.get(ps.id)
	if !ok {
		t.Fatal("handle did not resolve after commit")
	}
	if got != ps {
		t.Fatal("handle resolved to a different statement")
	}

	paramTypes, _, longData := reg.snapshot(ps)
	if len(paramTypes) != 1 || paramTypes[0] != oid.T_int8 {
		t.Errorf("param types = %v, want [int8]", paramTypes)
	}
	// commit sizes the long-data slots from the parameter count.
	if len(longData) != 1 {
		t.Errorf("long data slots = %d, want 1", len(longData))
	}

	if _, ok := reg.remove(ps.id); !ok {
		t.Error("remove reported the statement as absent")
	}
	if _, ok := reg.get(ps.id); ok {
		t.Error("handle still resolves after remove")
	}
}

// TestStmtRegistryPendingIsFIFO checks that pending statements are handed out
// oldest first. Nothing in the protocol stops a client from sending a second
// COM_STMT_PREPARE before reading the first response, and with a single
// pending slot the second statement's metadata would land under the first
// statement's handle — a wrong answer rather than an error.
func TestStmtRegistryPendingIsFIFO(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reg := newTestRegistry()
	first := reg.begin("SELECT 1")
	second := reg.begin("SELECT 2")

	if got := reg.peekPending(); got != first {
		t.Fatal("peekPending did not return the oldest statement")
	}
	if got := reg.takePending(); got != first {
		t.Fatal("takePending did not return the oldest statement")
	}
	if got := reg.takePending(); got != second {
		t.Fatal("takePending did not return the second statement")
	}
	if got := reg.takePending(); got != nil {
		t.Fatal("takePending returned a statement from an empty queue")
	}
}

// TestStmtRegistryFailedPrepareReleasesSlot covers the case that made a queue
// necessary in the first place. When preparation fails the SQL layer skips the
// rest of the batch, including the Describe that would have consumed the
// reservation. If the slot were not released, the next prepare's Describe
// would claim it and the client would receive a handle pointing at the wrong
// statement.
func TestStmtRegistryFailedPrepareReleasesSlot(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reg := newTestRegistry()
	failed := reg.begin("THIS IS NOT SQL")

	// The Prepare result releases the reservation on error; the Describe never
	// runs.
	if got := reg.takePending(); got != failed {
		t.Fatal("the failed statement was not the one released")
	}

	// The next prepare must now see its own statement, not the failed one.
	next := reg.begin("SELECT 1")
	if got := reg.takePending(); got != next {
		t.Fatal("a later prepare inherited the failed statement's slot")
	}
}

// TestStmtRegistryLongData covers COM_STMT_SEND_LONG_DATA accumulation and the
// fact that streamed arguments apply to exactly one execution.
func TestStmtRegistryLongData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reg := newTestRegistry()
	ps := reg.begin("INSERT INTO t VALUES ($1, $2)")
	reg.setMetadata(ps, []oid.Oid{oid.T_bytea, oid.T_text}, nil)
	reg.commit(ps)

	// Chunks accumulate in order.
	if !reg.appendLongData(ps, 0, []byte("abc")) {
		t.Fatal("appendLongData rejected a valid parameter")
	}
	if !reg.appendLongData(ps, 0, []byte("def")) {
		t.Fatal("appendLongData rejected a second chunk")
	}
	_, _, longData := reg.snapshot(ps)
	if !bytes.Equal(longData[0], []byte("abcdef")) {
		t.Errorf("accumulated %q, want abcdef", longData[0])
	}
	if longData[1] != nil {
		t.Errorf("parameter 1 was not streamed but holds %q", longData[1])
	}

	// An out-of-range parameter is refused rather than growing the slice: the
	// command carries no reply, so the caller logs and moves on.
	if reg.appendLongData(ps, 5, []byte("x")) {
		t.Error("appendLongData accepted an out-of-range parameter")
	}
	if reg.appendLongData(ps, -1, []byte("x")) {
		t.Error("appendLongData accepted a negative parameter index")
	}

	// Executing clears the buffers; a client that streams again next time
	// expects to start from empty.
	reg.setBoundTypes(ps, []mysqlwirebase.ParamType{{Type: mysqlwirebase.TypeBlob}})
	_, _, longData = reg.snapshot(ps)
	if longData[0] != nil {
		t.Errorf("streamed data survived an execute: %q", longData[0])
	}

	// COM_STMT_RESET discards streamed data without disturbing bound types.
	if !reg.appendLongData(ps, 0, []byte("zz")) {
		t.Fatal("appendLongData failed after reset of bound types")
	}
	reg.clearLongData(ps)
	_, boundTypes, longData := reg.snapshot(ps)
	if longData[0] != nil {
		t.Errorf("clearLongData left %q behind", longData[0])
	}
	if len(boundTypes) != 1 {
		t.Errorf("clearLongData disturbed the bound types: %v", boundTypes)
	}
}

// TestRenderParam checks how a decoded binary argument is handed to the SQL
// layer. Arguments are passed as text and decoded against the placeholder's
// inferred type, so a byte string bound to a bytea placeholder has to be
// spelled in the hex form KWDB's text decoding accepts; raw bytes would be
// read as an escape sequence.
func TestRenderParam(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		name  string
		param mysqlwirebase.Param
		typ   oid.Oid
		want  []byte
	}{
		{
			name:  "NULL is nil",
			param: mysqlwirebase.Param{Null: true},
			typ:   oid.T_int8,
			want:  nil,
		},
		{
			name:  "integer passes through",
			param: mysqlwirebase.Param{Value: []byte("42")},
			typ:   oid.T_int8,
			want:  []byte("42"),
		},
		{
			name:  "text passes through",
			param: mysqlwirebase.Param{Value: []byte("hello")},
			typ:   oid.T_text,
			want:  []byte("hello"),
		},
		{
			name:  "bytea is hex encoded",
			param: mysqlwirebase.Param{Value: []byte{0x00, 0xff, 0x10}, Binary: true},
			typ:   oid.T_bytea,
			want:  []byte(`\x00ff10`),
		},
		{
			name:  "empty bytea is still hex",
			param: mysqlwirebase.Param{Value: []byte{}, Binary: true},
			typ:   oid.T_bytea,
			want:  []byte(`\x`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := renderParam(tc.param, tc.typ)
			if tc.want == nil {
				if got != nil {
					t.Errorf("got %q, want nil", got)
				}
				return
			}
			if !bytes.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// TestParamColumnDefinition checks that a placeholder is reported with the type
// the SQL layer inferred. Clients use it to decide how to encode the argument
// they send, so an integer placeholder reported as a string makes a driver send
// text where the server expects an integer.
func TestParamColumnDefinition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	def := paramColumnDefinition(oid.T_int8)
	if def.Name != "?" {
		t.Errorf("parameter name = %q, want ?", def.Name)
	}
	if def.Type != mysqlwirebase.TypeLongLong {
		t.Errorf("int8 reported as type 0x%02x, want 0x%02x",
			def.Type, mysqlwirebase.TypeLongLong)
	}

	// An OID with no KWDB type behind it must still produce a usable
	// definition rather than a zero value, which a client would read as
	// MYSQL_TYPE_DECIMAL.
	unknown := paramColumnDefinition(oid.Oid(0))
	if unknown.Type != mysqlwirebase.TypeVarString {
		t.Errorf("unknown OID reported as type 0x%02x, want VarString 0x%02x",
			unknown.Type, mysqlwirebase.TypeVarString)
	}
}
