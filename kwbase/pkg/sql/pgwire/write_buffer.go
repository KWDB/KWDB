// Copyright 2015 The Cockroach Authors.
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

package pgwire

import (
	"bytes"
	"encoding/binary"
	"io"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/metric"
)

// writeBuffer is a wrapper around bytes.Buffer that provides a convenient interface
// for writing PGWire results. The buffer preserves any errors it encounters when writing,
// and will turn all subsequent write attempts into no-ops until finishMsg is called.
type writeBuffer struct {
	_ util.NoCopy

	wrapped bytes.Buffer
	err     error

	// Buffer used for temporary storage.
	putbuf [64]byte

	textFormatter   *tree.FmtCtx
	simpleFormatter *tree.FmtCtx

	// bytecount counts the number of bytes written across all pgwire connections, not just this
	// buffer. This is passed in so that finishMsg can track all messages we've sent to a network
	// socket, reducing the onus on the many callers of finishMsg.
	bytecount *metric.Counter
}

func newWriteBuffer(bytecount *metric.Counter) *writeBuffer {
	b := new(writeBuffer)
	b.init(bytecount)
	return b
}

// init exists to avoid the allocation imposed by newWriteBuffer.
func (b *writeBuffer) init(bytecount *metric.Counter) {
	b.bytecount = bytecount
	b.textFormatter = tree.NewFmtCtx(tree.FmtPgwireText)
	b.simpleFormatter = tree.NewFmtCtx(tree.FmtSimple)
}

// Write implements the io.Write interface.
func (b *writeBuffer) Write(p []byte) (int, error) {
	b.write(p)
	return len(p), b.err
}

func (b *writeBuffer) writeByte(c byte) {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(c)
	}
}

func (b *writeBuffer) write(p []byte) {
	if b.err == nil {
		_, b.err = b.wrapped.Write(p)
	}
}

func (b *writeBuffer) writeString(s string) {
	if b.err == nil {
		_, b.err = b.wrapped.WriteString(s)
	}
}

func (b *writeBuffer) nullTerminate() {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(0)
	}
}

// WriteFromFmtCtx writes the current contents of
// the given formatter with a length prefix.
// The function resets the contents of the formatter.
func (b *writeBuffer) writeFromFmtCtx(fmtCtx *tree.FmtCtx) {
	if b.err == nil {
		b.putInt32(int32(fmtCtx.Buffer.Len()))

		// bytes.Buffer.WriteTo resets the Buffer.
		_, b.err = fmtCtx.Buffer.WriteTo(&b.wrapped)
	}
}

// writeLengthPrefixedBuffer writes the contents of a bytes.Buffer with a
// length prefix.
func (b *writeBuffer) writeLengthPrefixedBuffer(buf *bytes.Buffer) {
	if b.err == nil {
		b.putInt32(int32(buf.Len()))

		// bytes.Buffer.WriteTo resets the Buffer.
		_, b.err = buf.WriteTo(&b.wrapped)
	}
}

// writeLengthPrefixedString writes a length-prefixed string. The
// length is encoded as an int32.
func (b *writeBuffer) writeLengthPrefixedString(s string) {
	b.putInt32(int32(len(s)))
	b.writeString(s)
}

// writeLengthPrefixedDatum writes a length-prefixed Datum in its
// string representation. The length is encoded as an int32.
func (b *writeBuffer) writeLengthPrefixedDatum(d tree.Datum) {
	b.simpleFormatter.FormatNode(d)
	b.writeFromFmtCtx(b.simpleFormatter)
}

// writeTerminatedString writes a null-terminated string.
func (b *writeBuffer) writeTerminatedString(s string) {
	b.writeString(s)
	b.nullTerminate()
}

func (b *writeBuffer) putInt16(v int16) {
	if b.err == nil {
		binary.BigEndian.PutUint16(b.putbuf[:], uint16(v))
		_, b.err = b.wrapped.Write(b.putbuf[:2])
	}
}

func (b *writeBuffer) putInt32(v int32) {
	if b.err == nil {
		binary.BigEndian.PutUint32(b.putbuf[:], uint32(v))
		_, b.err = b.wrapped.Write(b.putbuf[:4])
	}
}

func (b *writeBuffer) putInt64(v int64) {
	if b.err == nil {
		binary.BigEndian.PutUint64(b.putbuf[:], uint64(v))
		_, b.err = b.wrapped.Write(b.putbuf[:8])
	}
}

func (b *writeBuffer) putErrFieldMsg(field pgwirebase.ServerErrFieldType) {
	if b.err == nil {
		b.err = b.wrapped.WriteByte(byte(field))
	}
}

func (b *writeBuffer) reset() {
	b.wrapped.Reset()
	b.err = nil
}

// initMsg begins writing a message into the writeBuffer with the provided type.
func (b *writeBuffer) initMsg(typ pgwirebase.ServerMessageType) {
	b.reset()
	b.putbuf[0] = byte(typ)
	_, b.err = b.wrapped.Write(b.putbuf[:5]) // message type + message length
}

// finishMsg attempts to write the data it has accumulated to the provided io.Writer.
// If the writeBuffer previously encountered an error since the last call to initMsg,
// or if it encounters an error while writing to w, it will return an error.
func (b *writeBuffer) finishMsg(w io.Writer) error {
	defer b.reset()
	if b.err != nil {
		return b.err
	}
	bytes := b.wrapped.Bytes()
	binary.BigEndian.PutUint32(bytes[1:5], uint32(b.wrapped.Len()-1))
	n, err := w.Write(bytes)
	b.bytecount.Inc(int64(n))
	return err
}

// setError sets the writeBuffer's error, if it does not already have one.
func (b *writeBuffer) setError(err error) {
	if b.err == nil {
		b.err = err
	}
}
