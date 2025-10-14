// Copyright 2016 The Cockroach Authors.
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

package flowinfra

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/pkg/errors"
)

// PreferredEncoding is the encoding used for EncDatums that don't already have
// an encoding available.
const PreferredEncoding = sqlbase.DatumEncoding_ASCENDING_KEY

// StreamEncoder converts EncDatum rows into a sequence of ProducerMessage.
//
// Sample usage:
//
//	se := StreamEncoder{}
//
//	for {
//	    for ... {
//	       err := se.AddRow(...)
//	       ...
//	    }
//	    msg := se.FormMessage(nil)
//	    // Send out message.
//	    ...
//	}
type StreamEncoder struct {
	// infos is fully initialized when the first row is received.
	infos            []execinfrapb.DatumInfo
	infosInitialized bool

	rowBuf       []byte
	numEmptyRows int
	metadata     []execinfrapb.RemoteProducerMetadata

	// headerSent is set after the first message (which contains the header) has
	// been sent.
	headerSent bool
	// typingSent is set after the first message that contains any rows has been
	// sent.
	typingSent bool
	alloc      sqlbase.DatumAlloc

	// Preallocated structures to avoid allocations.
	msg    execinfrapb.ProducerMessage
	msgHdr execinfrapb.ProducerHeader
}

// HasHeaderBeenSent returns whether the header has been sent.
func (se *StreamEncoder) HasHeaderBeenSent() bool {
	return se.headerSent
}

// SetHeaderFields sets the header fields.
func (se *StreamEncoder) SetHeaderFields(flowID execinfrapb.FlowID, streamID execinfrapb.StreamID) {
	se.msgHdr.FlowID = flowID
	se.msgHdr.StreamID = streamID
}

// Init initializes the encoder.
func (se *StreamEncoder) Init(types []types.T) {
	se.infos = make([]execinfrapb.DatumInfo, len(types))
	for i := range types {
		se.infos[i].Type = types[i]
	}
}

// AddMetadata encodes a metadata message. Unlike AddRow(), it cannot fail. This
// is important for the caller because a failure to encode a piece of metadata
// (particularly one that contains an error) would not be recoverable.
//
// Metadata records lose their ordering wrt the data rows. The convention is
// that the StreamDecoder will return them first, before the data rows, thus
// ensuring that rows produced _after_ an error are not received _before_ the
// error.
func (se *StreamEncoder) AddMetadata(ctx context.Context, meta execinfrapb.ProducerMetadata) {
	se.metadata = append(se.metadata, execinfrapb.LocalMetaToRemoteProducerMeta(ctx, meta))
}

// AddRow encodes a message.
func (se *StreamEncoder) AddRow(row sqlbase.EncDatumRow) error {
	if se.infos == nil {
		panic("Init not called")
	}
	if len(se.infos) != len(row) {
		return errors.Errorf("inconsistent row length: expected %d, got %d", len(se.infos), len(row))
	}
	if !se.infosInitialized {
		// First row. Initialize encodings.
		for i := range row {
			enc, ok := row[i].Encoding()
			if !ok {
				enc = PreferredEncoding
			}
			sType := se.infos[i].Type.Family()
			if enc != sqlbase.DatumEncoding_VALUE &&
				(sqlbase.HasCompositeKeyEncoding(sType) || sqlbase.MustBeValueEncoded(sType)) {
				// Force VALUE encoding for composite types (key encodings may lose data).
				enc = sqlbase.DatumEncoding_VALUE
			}
			se.infos[i].Encoding = enc
		}
		se.infosInitialized = true
	}
	if len(row) == 0 {
		se.numEmptyRows++
		return nil
	}
	for i := range row {
		var err error
		se.rowBuf, err = row[i].Encode(&se.infos[i].Type, &se.alloc, se.infos[i].Encoding, se.rowBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

// FormMessage populates a message containing the rows added since the last call
// to FormMessage. The returned ProducerMessage should be treated as immutable.
func (se *StreamEncoder) FormMessage(ctx context.Context) *execinfrapb.ProducerMessage {
	msg := &se.msg
	msg.Header = nil
	msg.Data.RawBytes = se.rowBuf
	msg.Data.NumEmptyRows = int32(se.numEmptyRows)
	msg.Data.Metadata = make([]execinfrapb.RemoteProducerMetadata, len(se.metadata))
	copy(msg.Data.Metadata, se.metadata)
	se.metadata = se.metadata[:0]

	if !se.headerSent {
		msg.Header = &se.msgHdr
		se.headerSent = true
	}
	if !se.typingSent {
		if se.infosInitialized {
			msg.Typing = se.infos
			se.typingSent = true
		}
	} else {
		msg.Typing = nil
	}

	se.rowBuf = se.rowBuf[:0]
	se.numEmptyRows = 0
	return msg
}
