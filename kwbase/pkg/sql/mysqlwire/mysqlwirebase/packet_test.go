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
	"bufio"
	"bytes"
	"strings"
	"testing"
)

// newLoopback returns a reader and writer over the same buffer, so a test can
// write packets as one side and read them back as the other.
func newLoopback() (*PacketReader, *PacketWriter, *bytes.Buffer) {
	var buf bytes.Buffer
	w := NewPacketWriter(bufio.NewWriter(&buf))
	r := NewPacketReader(bufio.NewReader(&buf), 64<<20)
	return r, w, &buf
}

// TestPacketRoundTrip covers ordinary payloads, including the empty one, which
// is a real packet on the wire (a COM_PING reply body, for instance) and not a
// no-op.
func TestPacketRoundTrip(t *testing.T) {
	r, w, _ := newLoopback()

	payloads := [][]byte{
		{},
		[]byte("a"),
		[]byte(strings.Repeat("x", 1000)),
		{0x00, 0xff, 0xfe, 0xfb},
	}
	for _, p := range payloads {
		if err := w.WritePacket(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i, want := range payloads {
		got, err := r.ReadPacket()
		if err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("packet %d: got %d bytes, want %d", i, len(got), len(want))
		}
	}
}

// TestPacketSplitAtBoundary is the case most implementations get wrong: a
// payload of exactly MaxPayloadLen must be followed by an empty continuation
// packet, or the peer waits forever for the rest of a message that already
// arrived in full.
func TestPacketSplitAtBoundary(t *testing.T) {
	for _, size := range []int{
		MaxPayloadLen - 1,
		MaxPayloadLen,
		MaxPayloadLen + 1,
		2 * MaxPayloadLen,
	} {
		t.Run(itoa(size), func(t *testing.T) {
			r, w, buf := newLoopback()
			payload := bytes.Repeat([]byte{0xab}, size)
			if err := w.WritePacket(payload); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}

			// Every physical packet carries a 4-byte header, and a payload
			// that is an exact multiple of the limit needs one extra empty
			// packet to terminate it.
			wantPackets := size/MaxPayloadLen + 1
			if wantBytes := size + 4*wantPackets; buf.Len() != wantBytes {
				t.Errorf("wrote %d bytes, want %d (%d physical packets)",
					buf.Len(), wantBytes, wantPackets)
			}

			got, err := r.ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Errorf("reassembled %d bytes, want %d", len(got), size)
			}
		})
	}
}

// TestPacketSequenceMismatch checks that a desynchronized stream is reported
// rather than silently accepted. Sequence numbers are the protocol's only
// framing check, so ignoring them turns a desync into misparsed data.
func TestPacketSequenceMismatch(t *testing.T) {
	var buf bytes.Buffer
	w := NewPacketWriter(bufio.NewWriter(&buf))
	w.SetSequence(7)
	if err := w.WritePacket([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := NewPacketReader(bufio.NewReader(&buf), 64<<20)
	// The reader expects sequence 0; the packet carries 7.
	if _, err := r.ReadPacket(); err == nil {
		t.Fatal("expected a sequence mismatch error")
	} else if !strings.Contains(err.Error(), "desynchronized") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestPacketPayloadLimit checks the reassembly cap. Without it a client could
// stream 16 MiB chunks indefinitely and make the server buffer all of them.
func TestPacketPayloadLimit(t *testing.T) {
	var buf bytes.Buffer
	w := NewPacketWriter(bufio.NewWriter(&buf))
	if err := w.WritePacket(bytes.Repeat([]byte{1}, 5000)); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	r := NewPacketReader(bufio.NewReader(&buf), 4096)
	if _, err := r.ReadPacket(); err == nil {
		t.Fatal("expected the payload limit to be enforced")
	} else if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestLenEncInt covers every width of the length-encoded integer encoding,
// including the boundaries where the prefix byte changes.
func TestLenEncInt(t *testing.T) {
	values := []uint64{
		0, 1, 0xfa, // 1-byte form ends at 0xfa
		0xfb, 0xff, 0xffff, // 0xfc prefix, 2-byte form
		0x10000, 0xffffff, // 0xfd prefix, 3-byte form
		0x1000000, 1 << 40, 1<<64 - 1, // 0xfe prefix, 8-byte form
	}
	for _, v := range values {
		t.Run(itoa(int(v&0x7fffffff)), func(t *testing.T) {
			w := NewWriter(16)
			w.LenEncInt(v)
			got, null, err := NewReader(w.Bytes()).LenEncInt()
			if err != nil {
				t.Fatal(err)
			}
			if null {
				t.Fatal("value decoded as NULL")
			}
			if got != v {
				t.Errorf("round-tripped %d as %d", v, got)
			}
		})
	}
}

// TestLenEncIntNull checks that the 0xfb sentinel is reported as NULL rather
// than as the integer 251, which is how a text-protocol row signals SQL NULL.
func TestLenEncIntNull(t *testing.T) {
	_, null, err := NewReader([]byte{0xfb}).LenEncInt()
	if err != nil {
		t.Fatal(err)
	}
	if !null {
		t.Error("0xfb was not reported as NULL")
	}
}

// TestReaderRejectsTruncatedInput checks that every accessor reports a
// truncated payload instead of reading past the end. Payload contents are
// fully client-controlled, so this is the boundary between a malformed packet
// and a panic in the connection goroutine.
func TestReaderRejectsTruncatedInput(t *testing.T) {
	for name, tc := range map[string]struct {
		// input is deliberately one byte short of what fn needs.
		input []byte
		fn    func(*Reader) error
	}{
		"Byte":   {nil, func(r *Reader) error { _, err := r.Byte(); return err }},
		"Uint16": {[]byte{1}, func(r *Reader) error { _, err := r.Uint16(); return err }},
		"Uint32": {[]byte{1, 2, 3}, func(r *Reader) error { _, err := r.Uint32(); return err }},
		"Bytes":  {[]byte{1, 2, 3}, func(r *Reader) error { _, err := r.Bytes(4); return err }},
		"Skip":   {[]byte{1, 2, 3}, func(r *Reader) error { return r.Skip(4) }},
		// An unterminated string: no NUL anywhere in the payload.
		"NulString": {[]byte("abc"), func(r *Reader) error { _, err := r.NulString(); return err }},
		// A declared length of 9 with nothing following it.
		"LenEncString": {[]byte{0x09}, func(r *Reader) error { _, err := r.LenEncString(); return err }},
	} {
		t.Run(name, func(t *testing.T) {
			if err := tc.fn(NewReader(tc.input)); err == nil {
				t.Error("expected an error on truncated input")
			}
		})
	}
}

// TestLenEncStringRoundTrip covers the empty string, which must encode as a
// zero length rather than as NULL.
func TestLenEncStringRoundTrip(t *testing.T) {
	for _, s := range []string{"", "a", "hello world", strings.Repeat("x", 300)} {
		w := NewWriter(16)
		w.LenEncString(s)
		got, err := NewReader(w.Bytes()).LenEncString()
		if err != nil {
			t.Fatal(err)
		}
		if got == nil && s != "" {
			t.Errorf("%q round-tripped as NULL", s)
		}
		if string(got) != s {
			t.Errorf("round-tripped %q as %q", s, string(got))
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [24]byte
	i := len(b)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
