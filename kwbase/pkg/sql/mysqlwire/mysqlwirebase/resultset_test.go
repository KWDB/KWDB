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

package mysqlwirebase

import (
	"bytes"
	"testing"
)

// TestTextRowRoundTrip checks that values survive encoding, and in particular
// that SQL NULL stays distinct from the empty string. The two differ by a
// single byte on the wire (0xfb versus 0x00), and conflating them silently
// corrupts query results rather than failing.
func TestTextRowRoundTrip(t *testing.T) {
	values := [][]byte{
		[]byte("1"),
		nil, // SQL NULL
		[]byte(""),
		[]byte("hello"),
		[]byte("2026-08-11 12:34:56.789"),
		bytes.Repeat([]byte("x"), 300), // forces the 2-byte length prefix
	}
	got, err := DecodeTextRow(EncodeTextRow(values), len(values))
	if err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if values[i] == nil {
			if got[i] != nil {
				t.Errorf("value %d: NULL decoded as %q", i, got[i])
			}
			continue
		}
		if got[i] == nil {
			t.Errorf("value %d: %q decoded as NULL", i, values[i])
			continue
		}
		if !bytes.Equal(got[i], values[i]) {
			t.Errorf("value %d: round-tripped %q as %q", i, values[i], got[i])
		}
	}
}

// TestColumnDefinitionRoundTrip checks the field order of ColumnDefinition41.
// Clients read the type and flags to decide how to interpret every row that
// follows, so a misplaced field mis-types the whole result set.
func TestColumnDefinitionRoundTrip(t *testing.T) {
	want := &ColumnDefinition{
		Schema:       "defaultdb",
		Table:        "t",
		OrgTable:     "t",
		Name:         "ts",
		OrgName:      "ts",
		CharacterSet: CollationBinary,
		ColumnLength: 26,
		Type:         TypeTimestamp,
		Flags:        ColumnFlagNotNull | ColumnFlagPrimaryKey,
		Decimals:     6,
	}
	got, err := DecodeColumnDefinition(want.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if *got != *want {
		t.Errorf("round-tripped %+v as %+v", *want, *got)
	}
}

// TestEncodeOKEOFSubstitute checks the one difference between an OK packet and
// the OK packet that stands in for EOF once CLIENT_DEPRECATE_EOF is
// negotiated. A client that negotiated the flag treats a 0x00 header as the
// start of another row.
func TestEncodeOKEOFSubstitute(t *testing.T) {
	ok := &OK{AffectedRows: 3, StatusFlags: ServerStatusAutocommit}

	plain := EncodeOK(ok, ClientProtocol41, false)
	if plain[0] != OKPacketHeader {
		t.Errorf("OK header = 0x%02x, want 0x%02x", plain[0], OKPacketHeader)
	}

	substitute := EncodeOK(ok, ClientProtocol41, true)
	if substitute[0] != EOFPacketHeader {
		t.Errorf("EOF-substitute header = 0x%02x, want 0x%02x", substitute[0], EOFPacketHeader)
	}
	if !bytes.Equal(plain[1:], substitute[1:]) {
		t.Error("the two encodings differ beyond the header byte")
	}
}

// TestEncodeOKFields checks that affected-rows and last-insert-id survive as
// length-encoded integers, including values past the 1-byte form. Clients
// surface these directly as the return value of an INSERT.
func TestEncodeOKFields(t *testing.T) {
	ok := &OK{
		AffectedRows: 300,
		LastInsertID: 1 << 40,
		StatusFlags:  ServerStatusAutocommit,
		Warnings:     2,
	}
	r := NewReader(EncodeOK(ok, ClientProtocol41, false))
	if h, err := r.Byte(); err != nil || h != OKPacketHeader {
		t.Fatalf("header = 0x%02x, err = %v", h, err)
	}
	affected, _, err := r.LenEncInt()
	if err != nil {
		t.Fatal(err)
	}
	if affected != ok.AffectedRows {
		t.Errorf("affected rows = %d, want %d", affected, ok.AffectedRows)
	}
	lastID, _, err := r.LenEncInt()
	if err != nil {
		t.Fatal(err)
	}
	if lastID != ok.LastInsertID {
		t.Errorf("last insert id = %d, want %d", lastID, ok.LastInsertID)
	}
	status, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if status != ok.StatusFlags {
		t.Errorf("status flags = 0x%04x, want 0x%04x", status, ok.StatusFlags)
	}
	warnings, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if warnings != ok.Warnings {
		t.Errorf("warnings = %d, want %d", warnings, ok.Warnings)
	}
}

// TestEncodeErrLayout checks the ERR packet, including the '#' marker that
// separates the SQLSTATE from the message. Frameworks branch on the code and
// SQLSTATE, so both have to land where clients look for them.
func TestEncodeErrLayout(t *testing.T) {
	got := EncodeErr(ErrAccessDenied, SQLStateAccessDenied, "Access denied", ClientProtocol41)

	r := NewReader(got)
	header, err := r.Byte()
	if err != nil {
		t.Fatal(err)
	}
	if header != ErrPacketHeader {
		t.Errorf("header = 0x%02x, want 0x%02x", header, ErrPacketHeader)
	}
	code, err := r.Uint16()
	if err != nil {
		t.Fatal(err)
	}
	if code != ErrAccessDenied {
		t.Errorf("error code = %d, want %d", code, ErrAccessDenied)
	}
	marker, err := r.Byte()
	if err != nil {
		t.Fatal(err)
	}
	if marker != '#' {
		t.Errorf("SQLSTATE marker = %q, want '#'", marker)
	}
	state, err := r.Bytes(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(state) != SQLStateAccessDenied {
		t.Errorf("SQLSTATE = %q, want %q", state, SQLStateAccessDenied)
	}
	if msg := r.EOFString(); msg != "Access denied" {
		t.Errorf("message = %q, want %q", msg, "Access denied")
	}
}

// TestEncodeErrRejectsMalformedSQLState checks that a SQLSTATE of the wrong
// length is replaced rather than emitted. Clients read the 5 bytes after '#'
// positionally, so a short value would be padded out of the error message and
// leave the client reporting a corrupted state.
func TestEncodeErrRejectsMalformedSQLState(t *testing.T) {
	got := EncodeErr(ErrUnknownError, "XX", "boom", ClientProtocol41)
	r := NewReader(got)
	if err := r.Skip(4); err != nil { // header + code + '#'
		t.Fatal(err)
	}
	state, err := r.Bytes(5)
	if err != nil {
		t.Fatal(err)
	}
	if string(state) != SQLStateGeneralError {
		t.Errorf("SQLSTATE = %q, want the general-error fallback %q", state, SQLStateGeneralError)
	}
	if msg := r.EOFString(); msg != "boom" {
		t.Errorf("message = %q, want %q", msg, "boom")
	}
}

// TestAccessDeniedMessageShape checks the wording clients and their users
// expect, and pins the property that the message does not distinguish an
// unknown user from a wrong password.
func TestAccessDeniedMessageShape(t *testing.T) {
	withPassword := NewAccessDeniedError("alice", "10.0.0.1", true)
	if want := "Access denied for user 'alice'@'10.0.0.1' (using password: YES)"; withPassword.Message != want {
		t.Errorf("message = %q, want %q", withPassword.Message, want)
	}
	withoutPassword := NewAccessDeniedError("alice", "10.0.0.1", false)
	if want := "Access denied for user 'alice'@'10.0.0.1' (using password: NO)"; withoutPassword.Message != want {
		t.Errorf("message = %q, want %q", withoutPassword.Message, want)
	}

	// An account that does not exist and one whose password is wrong must be
	// indistinguishable, or an unauthenticated caller can enumerate users.
	unknownUser := NewAccessDeniedError("nosuchuser", "10.0.0.1", true)
	knownUser := NewAccessDeniedError("nosuchuser", "10.0.0.1", true)
	if unknownUser.Code != knownUser.Code || unknownUser.SQLState != knownUser.SQLState {
		t.Error("the error differs depending on whether the account exists")
	}
}

// TestEncodeColumnCount checks the length-encoded count that opens a result
// set, including counts past the 1-byte form.
func TestEncodeColumnCount(t *testing.T) {
	for _, n := range []int{1, 5, 250, 251, 300, 70000} {
		got, _, err := NewReader(EncodeColumnCount(n)).LenEncInt()
		if err != nil {
			t.Fatal(err)
		}
		if int(got) != n {
			t.Errorf("column count %d round-tripped as %d", n, got)
		}
	}
}
