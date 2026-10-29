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

import "fmt"

// OK describes a successful command with no result set. See
// "Protocol::OK_Packet".
type OK struct {
	AffectedRows uint64
	LastInsertID uint64
	StatusFlags  uint16
	Warnings     uint16
	Info         string
}

// EncodeOK serializes an OK packet.
//
// When CLIENT_DEPRECATE_EOF has been negotiated, an OK packet also stands in
// for the EOF that used to terminate a result set; eofSubstitute selects that
// encoding, which differs only in the leading byte.
func EncodeOK(ok *OK, capabilities uint32, eofSubstitute bool) []byte {
	w := NewWriter(32 + len(ok.Info))
	if eofSubstitute {
		w.Byte(EOFPacketHeader)
	} else {
		w.Byte(OKPacketHeader)
	}
	w.LenEncInt(ok.AffectedRows)
	w.LenEncInt(ok.LastInsertID)

	if capabilities&ClientProtocol41 != 0 {
		w.Uint16(ok.StatusFlags)
		w.Uint16(ok.Warnings)
	} else if capabilities&ClientTransactions != 0 {
		w.Uint16(ok.StatusFlags)
	}

	// The human-readable info string is the rest of the packet. It is only
	// length-encoded when session tracking is on, in which case it is followed
	// by state-change blocks we never emit.
	if capabilities&ClientSessionTrack != 0 {
		w.LenEncString(ok.Info)
	} else {
		w.String(ok.Info)
	}
	return w.Bytes()
}

// EncodeEOF serializes a classic EOF packet, used when the client has not
// negotiated CLIENT_DEPRECATE_EOF. See "Protocol::EOF_Packet".
func EncodeEOF(warnings, statusFlags uint16, capabilities uint32) []byte {
	w := NewWriter(5)
	w.Byte(EOFPacketHeader)
	if capabilities&ClientProtocol41 != 0 {
		w.Uint16(warnings)
		w.Uint16(statusFlags)
	}
	return w.Bytes()
}

// EncodeErr serializes an ERR packet. See "Protocol::ERR_Packet".
//
// sqlState must be exactly 5 characters; MySQL clients parse it positionally
// and a shorter value corrupts the message that follows.
func EncodeErr(code uint16, sqlState, message string, capabilities uint32) []byte {
	w := NewWriter(16 + len(message))
	w.Byte(ErrPacketHeader)
	w.Uint16(code)
	if capabilities&ClientProtocol41 != 0 {
		if len(sqlState) != 5 {
			sqlState = SQLStateGeneralError
		}
		w.Byte('#')
		w.String(sqlState)
	}
	w.String(message)
	return w.Bytes()
}

// ColumnDefinition describes one column of a result set. See
// "Protocol::ColumnDefinition41".
type ColumnDefinition struct {
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	CharacterSet uint16
	ColumnLength uint32
	Type         byte
	Flags        uint16
	Decimals     byte
}

// Encode serializes a column definition.
func (c *ColumnDefinition) Encode() []byte {
	w := NewWriter(64 + len(c.Name))
	w.LenEncString("def") // catalog; always this constant in MySQL
	w.LenEncString(c.Schema)
	w.LenEncString(c.Table)
	w.LenEncString(c.OrgTable)
	w.LenEncString(c.Name)
	w.LenEncString(c.OrgName)
	w.LenEncInt(0x0c) // length of the fixed-length block that follows
	w.Uint16(c.CharacterSet)
	w.Uint32(c.ColumnLength)
	w.Byte(c.Type)
	w.Uint16(c.Flags)
	w.Byte(c.Decimals)
	w.Zeros(2) // filler
	return w.Bytes()
}

// DecodeColumnDefinition parses a column definition. Only the test client needs
// this.
func DecodeColumnDefinition(payload []byte) (*ColumnDefinition, error) {
	r := NewReader(payload)
	readStr := func(what string) (string, error) {
		b, err := r.LenEncString()
		if err != nil {
			return "", fmt.Errorf("reading %s: %w", what, err)
		}
		return string(b), nil
	}
	if _, err := readStr("catalog"); err != nil {
		return nil, err
	}
	c := &ColumnDefinition{}
	var err error
	if c.Schema, err = readStr("schema"); err != nil {
		return nil, err
	}
	if c.Table, err = readStr("table"); err != nil {
		return nil, err
	}
	if c.OrgTable, err = readStr("org_table"); err != nil {
		return nil, err
	}
	if c.Name, err = readStr("name"); err != nil {
		return nil, err
	}
	if c.OrgName, err = readStr("org_name"); err != nil {
		return nil, err
	}
	if _, _, err = r.LenEncInt(); err != nil {
		return nil, fmt.Errorf("reading fixed-length block size: %w", err)
	}
	if c.CharacterSet, err = r.Uint16(); err != nil {
		return nil, fmt.Errorf("reading character set: %w", err)
	}
	if c.ColumnLength, err = r.Uint32(); err != nil {
		return nil, fmt.Errorf("reading column length: %w", err)
	}
	if c.Type, err = r.Byte(); err != nil {
		return nil, fmt.Errorf("reading column type: %w", err)
	}
	if c.Flags, err = r.Uint16(); err != nil {
		return nil, fmt.Errorf("reading column flags: %w", err)
	}
	if c.Decimals, err = r.Byte(); err != nil {
		return nil, fmt.Errorf("reading decimals: %w", err)
	}
	return c, nil
}

// EncodeTextRow serializes one row of a text-protocol result set. A nil entry
// in values encodes SQL NULL.
//
// Every value is a length-encoded string regardless of the column's declared
// type: in the text protocol the client parses according to the column
// definition it was already sent.
func EncodeTextRow(values [][]byte) []byte {
	n := 0
	for _, v := range values {
		n += len(v) + 9
	}
	w := NewWriter(n)
	for _, v := range values {
		if v == nil {
			w.Byte(NullColumnHeader)
			continue
		}
		w.LenEncBytes(v)
	}
	return w.Bytes()
}

// DecodeTextRow parses a text-protocol row into count values, with nil for
// SQL NULL. Only the test client needs this.
func DecodeTextRow(payload []byte, count int) ([][]byte, error) {
	r := NewReader(payload)
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		n, null, err := r.LenEncInt()
		if err != nil {
			return nil, fmt.Errorf("reading value %d: %w", i, err)
		}
		if null {
			continue
		}
		if out[i], err = r.Bytes(int(n)); err != nil {
			return nil, fmt.Errorf("reading value %d: %w", i, err)
		}
	}
	return out, nil
}

// EncodeColumnCount serializes the length-encoded column count that opens a
// result set.
func EncodeColumnCount(n int) []byte {
	w := NewWriter(9)
	w.LenEncInt(uint64(n))
	return w.Bytes()
}
