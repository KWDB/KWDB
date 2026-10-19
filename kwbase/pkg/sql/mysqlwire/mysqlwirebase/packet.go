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
	"encoding/binary"
	"fmt"
	"io"
)

// A MySQL packet is a 4-byte header followed by a payload: 3 bytes of
// little-endian payload length, then a 1-byte sequence number.
const packetHeaderLen = 4

// PacketReader reads MySQL packets from a connection, reassembling logical
// packets that the sender had to split across several physical ones.
//
// The sequence number is protocol state shared with PacketWriter: the client
// starts each command at 0 and both sides increment it for every packet, so a
// mismatch means the two sides have desynchronized and the connection can no
// longer be trusted.
type PacketReader struct {
	rd  *bufio.Reader
	seq uint8

	// maxPayload caps the size of a reassembled logical packet, bounding how
	// much memory a single client can make the server allocate.
	maxPayload int
}

// NewPacketReader returns a PacketReader over rd. maxPayload is the largest
// reassembled payload accepted; a client exceeding it gets an error rather
// than an unbounded allocation.
func NewPacketReader(rd *bufio.Reader, maxPayload int) *PacketReader {
	return &PacketReader{rd: rd, maxPayload: maxPayload}
}

// Sequence returns the sequence number the next packet is expected to carry.
func (r *PacketReader) Sequence() uint8 { return r.seq }

// SetSequence forces the next expected sequence number. The TLS upgrade needs
// this: the SSLRequest packet and the HandshakeResponse41 that follows it are
// numbered consecutively even though a TLS handshake happens in between.
func (r *PacketReader) SetSequence(seq uint8) { r.seq = seq }

// ResetSequence restarts numbering, as happens at the start of every command
// in the command phase.
func (r *PacketReader) ResetSequence() { r.seq = 0 }

// ReadPacket returns the payload of the next logical packet.
//
// The returned slice is freshly allocated and owned by the caller; the reader
// keeps no reference to it.
func (r *PacketReader) ReadPacket() ([]byte, error) {
	var payload []byte
	for {
		var header [packetHeaderLen]byte
		if _, err := io.ReadFull(r.rd, header[:]); err != nil {
			return nil, err
		}
		length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
		if got := header[3]; got != r.seq {
			return nil, fmt.Errorf(
				"mysql protocol desynchronized: expected packet sequence %d, got %d", r.seq, got)
		}
		r.seq++

		if len(payload)+length > r.maxPayload {
			return nil, fmt.Errorf(
				"mysql packet payload of %d bytes exceeds the %d byte limit",
				len(payload)+length, r.maxPayload)
		}

		if length > 0 {
			chunk := make([]byte, length)
			if _, err := io.ReadFull(r.rd, chunk); err != nil {
				return nil, err
			}
			payload = append(payload, chunk...)
		}

		// A chunk of exactly MaxPayloadLen means the logical packet continues.
		// The sender must then emit a final short (possibly empty) chunk, so a
		// payload whose length is an exact multiple of MaxPayloadLen is still
		// terminated correctly.
		if length < MaxPayloadLen {
			return payload, nil
		}
	}
}

// PacketWriter buffers packets and writes them to a connection, splitting
// payloads that exceed the single-packet limit.
type PacketWriter struct {
	wr  *bufio.Writer
	seq uint8
}

// NewPacketWriter returns a PacketWriter over wr.
func NewPacketWriter(wr *bufio.Writer) *PacketWriter {
	return &PacketWriter{wr: wr}
}

// Sequence returns the sequence number the next written packet will carry.
func (w *PacketWriter) Sequence() uint8 { return w.seq }

// SetSequence forces the next sequence number.
func (w *PacketWriter) SetSequence(seq uint8) { w.seq = seq }

// ResetSequence restarts numbering, as happens at the start of every command.
func (w *PacketWriter) ResetSequence() { w.seq = 0 }

// WritePacket frames payload and buffers it. Call Flush to put it on the wire.
func (w *PacketWriter) WritePacket(payload []byte) error {
	for {
		chunk := payload
		if len(chunk) > MaxPayloadLen {
			chunk = chunk[:MaxPayloadLen]
		}

		var header [packetHeaderLen]byte
		header[0] = byte(len(chunk))
		header[1] = byte(len(chunk) >> 8)
		header[2] = byte(len(chunk) >> 16)
		header[3] = w.seq
		w.seq++

		if _, err := w.wr.Write(header[:]); err != nil {
			return err
		}
		if _, err := w.wr.Write(chunk); err != nil {
			return err
		}

		payload = payload[len(chunk):]
		// A full-length chunk obliges us to send a continuation, even if the
		// remaining payload is empty.
		if len(chunk) < MaxPayloadLen {
			return nil
		}
	}
}

// Flush writes any buffered packets to the underlying connection.
func (w *PacketWriter) Flush() error { return w.wr.Flush() }

// Reader is a cursor over a packet payload. Its methods report an error rather
// than panicking on a truncated packet, since payload contents are entirely
// client-controlled.
type Reader struct {
	buf []byte
	pos int
}

// NewReader returns a Reader over payload.
func NewReader(payload []byte) *Reader { return &Reader{buf: payload} }

// Remaining returns the not-yet-consumed bytes without consuming them.
func (r *Reader) Remaining() []byte { return r.buf[r.pos:] }

// Len returns how many bytes remain unconsumed.
func (r *Reader) Len() int { return len(r.buf) - r.pos }

var errTruncated = fmt.Errorf("mysql packet truncated")

// Byte consumes one byte.
func (r *Reader) Byte() (byte, error) {
	if r.Len() < 1 {
		return 0, errTruncated
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

// Uint16 consumes a 2-byte little-endian integer.
func (r *Reader) Uint16() (uint16, error) {
	if r.Len() < 2 {
		return 0, errTruncated
	}
	v := binary.LittleEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

// Uint32 consumes a 4-byte little-endian integer.
func (r *Reader) Uint32() (uint32, error) {
	if r.Len() < 4 {
		return 0, errTruncated
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// Uint64 consumes an 8-byte little-endian integer.
func (r *Reader) Uint64() (uint64, error) {
	if r.Len() < 8 {
		return 0, errTruncated
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

// Skip advances past n bytes.
func (r *Reader) Skip(n int) error {
	if r.Len() < n {
		return errTruncated
	}
	r.pos += n
	return nil
}

// Bytes consumes exactly n bytes and returns a copy.
func (r *Reader) Bytes(n int) ([]byte, error) {
	if r.Len() < n {
		return nil, errTruncated
	}
	out := make([]byte, n)
	copy(out, r.buf[r.pos:r.pos+n])
	r.pos += n
	return out, nil
}

// LenEncInt consumes a length-encoded integer. See "Protocol::LengthEncodedInteger".
//
// The second return value reports whether the value was the NULL sentinel
// (0xfb), which appears in text-protocol rows.
func (r *Reader) LenEncInt() (val uint64, null bool, err error) {
	first, err := r.Byte()
	if err != nil {
		return 0, false, err
	}
	switch {
	case first < 0xfb:
		return uint64(first), false, nil
	case first == 0xfb:
		return 0, true, nil
	case first == 0xfc:
		v, err := r.Uint16()
		return uint64(v), false, err
	case first == 0xfd:
		b, err := r.Bytes(3)
		if err != nil {
			return 0, false, err
		}
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16, false, nil
	case first == 0xfe:
		b, err := r.Bytes(8)
		if err != nil {
			return 0, false, err
		}
		return binary.LittleEndian.Uint64(b), false, nil
	default:
		// 0xff never introduces a length-encoded integer; in a payload where
		// one is expected it means the packet is malformed.
		return 0, false, fmt.Errorf("invalid length-encoded integer prefix 0x%02x", first)
	}
}

// LenEncString consumes a length-encoded string.
func (r *Reader) LenEncString() ([]byte, error) {
	n, null, err := r.LenEncInt()
	if err != nil {
		return nil, err
	}
	if null {
		return nil, nil
	}
	return r.Bytes(int(n))
}

// NulString consumes a NUL-terminated string, returning it without the
// terminator.
func (r *Reader) NulString() (string, error) {
	for i := r.pos; i < len(r.buf); i++ {
		if r.buf[i] == 0 {
			s := string(r.buf[r.pos:i])
			r.pos = i + 1
			return s, nil
		}
	}
	return "", errTruncated
}

// EOFString consumes the rest of the payload as a string.
func (r *Reader) EOFString() string {
	s := string(r.buf[r.pos:])
	r.pos = len(r.buf)
	return s
}

// Writer builds a packet payload.
type Writer struct {
	buf []byte
}

// NewWriter returns a Writer with capacity reserved for n bytes.
func NewWriter(n int) *Writer { return &Writer{buf: make([]byte, 0, n)} }

// Bytes returns the payload built so far.
func (w *Writer) Bytes() []byte { return w.buf }

// Reset empties the payload, retaining the allocated capacity.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

// Byte appends one byte.
func (w *Writer) Byte(b byte) { w.buf = append(w.buf, b) }

// Uint16 appends a 2-byte little-endian integer.
func (w *Writer) Uint16(v uint16) { w.buf = append(w.buf, byte(v), byte(v>>8)) }

// Uint32 appends a 4-byte little-endian integer.
func (w *Writer) Uint32(v uint32) {
	w.buf = append(w.buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// Uint64 appends an 8-byte little-endian integer.
func (w *Writer) Uint64(v uint64) {
	w.buf = append(w.buf,
		byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

// Raw appends bytes verbatim.
func (w *Writer) Raw(b []byte) { w.buf = append(w.buf, b...) }

// String appends a string verbatim.
func (w *Writer) String(s string) { w.buf = append(w.buf, s...) }

// Zeros appends n zero bytes.
func (w *Writer) Zeros(n int) {
	for i := 0; i < n; i++ {
		w.buf = append(w.buf, 0)
	}
}

// NulString appends a string followed by a NUL terminator.
func (w *Writer) NulString(s string) {
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}

// LenEncInt appends a length-encoded integer.
func (w *Writer) LenEncInt(v uint64) {
	switch {
	case v < 0xfb:
		w.buf = append(w.buf, byte(v))
	case v <= 0xffff:
		w.buf = append(w.buf, 0xfc, byte(v), byte(v>>8))
	case v <= 0xffffff:
		w.buf = append(w.buf, 0xfd, byte(v), byte(v>>8), byte(v>>16))
	default:
		w.buf = append(w.buf, 0xfe)
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		w.buf = append(w.buf, b[:]...)
	}
}

// LenEncBytes appends a length-encoded string.
func (w *Writer) LenEncBytes(b []byte) {
	w.LenEncInt(uint64(len(b)))
	w.buf = append(w.buf, b...)
}

// LenEncString appends a length-encoded string.
func (w *Writer) LenEncString(s string) {
	w.LenEncInt(uint64(len(s)))
	w.buf = append(w.buf, s...)
}
