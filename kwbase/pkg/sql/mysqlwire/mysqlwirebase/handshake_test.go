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

// TestHandshakeEncodeLayout pins the byte layout of the server greeting.
// Clients parse it positionally, so a field that moves by one byte breaks
// every client without any error message that points at the cause.
func TestHandshakeEncodeLayout(t *testing.T) {
	scramble := []byte("0123456789abcdefghij") // ScrambleLen bytes
	h := &Handshake{
		ServerVersion: "8.0.11-KWDB",
		ConnectionID:  0x01020304,
		Scramble:      scramble,
		Capabilities:  ServerCapabilities,
		Charset:       byte(CollationUTF8MB4GeneralCI),
		StatusFlags:   ServerStatusAutocommit,
		AuthPlugin:    AuthCachingSHA2Password,
	}
	got := h.Encode()

	pos := 0
	expectByte := func(what string, want byte) {
		t.Helper()
		if got[pos] != want {
			t.Errorf("%s at offset %d = 0x%02x, want 0x%02x", what, pos, got[pos], want)
		}
		pos++
	}

	expectByte("protocol version", ProtocolVersion)

	if v := string(got[pos : pos+len(h.ServerVersion)]); v != h.ServerVersion {
		t.Errorf("server version = %q, want %q", v, h.ServerVersion)
	}
	pos += len(h.ServerVersion)
	expectByte("server version terminator", 0)

	// Connection id, little-endian.
	for _, want := range []byte{0x04, 0x03, 0x02, 0x01} {
		expectByte("connection id", want)
	}

	if !bytes.Equal(got[pos:pos+8], scramble[:8]) {
		t.Errorf("auth-plugin-data-part-1 = %q, want %q", got[pos:pos+8], scramble[:8])
	}
	pos += 8
	expectByte("filler", 0)

	// Capability flags, lower then (after charset and status) upper half.
	// The masks keep these as conversions of in-range constants; Go rejects a
	// constant conversion whose value does not fit the target type.
	expectByte("capabilities low byte 0", byte(ServerCapabilities&0xff))
	expectByte("capabilities low byte 1", byte(ServerCapabilities>>8&0xff))
	expectByte("charset", byte(CollationUTF8MB4GeneralCI))
	expectByte("status low", byte(ServerStatusAutocommit&0xff))
	expectByte("status high", byte(ServerStatusAutocommit>>8&0xff))
	expectByte("capabilities high byte 0", byte(ServerCapabilities>>16&0xff))
	expectByte("capabilities high byte 1", byte(ServerCapabilities>>24&0xff))

	// Clients read this as the length of the whole auth-plugin-data including
	// the NUL that terminates part 2.
	expectByte("auth-plugin-data length", byte(ScrambleLen+1))

	for i := 0; i < 10; i++ {
		expectByte("reserved", 0)
	}

	if !bytes.Equal(got[pos:pos+12], scramble[8:]) {
		t.Errorf("auth-plugin-data-part-2 = %q, want %q", got[pos:pos+12], scramble[8:])
	}
	pos += 12
	expectByte("auth-plugin-data terminator", 0)

	if name := string(got[pos : pos+len(AuthCachingSHA2Password)]); name != AuthCachingSHA2Password {
		t.Errorf("auth plugin = %q, want %q", name, AuthCachingSHA2Password)
	}
	pos += len(AuthCachingSHA2Password)
	expectByte("auth plugin terminator", 0)

	if pos != len(got) {
		t.Errorf("handshake has %d trailing bytes", len(got)-pos)
	}
}

// TestHandshakeResponseRoundTrip checks that a response encoded the way a
// client encodes it decodes back to the same values, across the combinations
// of optional fields that change the layout.
func TestHandshakeResponseRoundTrip(t *testing.T) {
	base := ClientProtocol41 | ClientSecureConnection | ClientPluginAuth

	for name, caps := range map[string]uint32{
		"minimal":     base,
		"with db":     base | ClientConnectWithDB,
		"lenenc auth": base | ClientPluginAuthLenEncClientData,
		"with attrs":  base | ClientConnectAttrs,
		"everything":  base | ClientConnectWithDB | ClientPluginAuthLenEncClientData | ClientConnectAttrs,
	} {
		t.Run(name, func(t *testing.T) {
			want := &HandshakeResponse{
				Capabilities:  caps,
				MaxPacketSize: 16 << 20,
				Charset:       byte(CollationUTF8MB4GeneralCI),
				User:          "kwdb_user",
				AuthResponse:  bytes.Repeat([]byte{0xab}, 20),
				AuthPlugin:    AuthNativePassword,
			}
			if caps&ClientConnectWithDB != 0 {
				want.Database = "defaultdb"
			}
			if caps&ClientConnectAttrs != 0 {
				want.Attrs = map[string]string{"_client_name": "libmysql"}
			}

			got, err := DecodeHandshakeResponse(want.Encode(), caps)
			if err != nil {
				t.Fatal(err)
			}
			if got.User != want.User {
				t.Errorf("user = %q, want %q", got.User, want.User)
			}
			if got.Database != want.Database {
				t.Errorf("database = %q, want %q", got.Database, want.Database)
			}
			if got.AuthPlugin != want.AuthPlugin {
				t.Errorf("auth plugin = %q, want %q", got.AuthPlugin, want.AuthPlugin)
			}
			if !bytes.Equal(got.AuthResponse, want.AuthResponse) {
				t.Errorf("auth response = %x, want %x", got.AuthResponse, want.AuthResponse)
			}
			if got.MaxPacketSize != want.MaxPacketSize {
				t.Errorf("max packet size = %d, want %d", got.MaxPacketSize, want.MaxPacketSize)
			}
			for k, v := range want.Attrs {
				if got.Attrs[k] != v {
					t.Errorf("attr %q = %q, want %q", k, got.Attrs[k], v)
				}
			}
		})
	}
}

// TestDecodeHandshakeResponseMasksCapabilities checks that a client cannot
// switch on a capability the server never advertised. The negotiated set
// governs how later packets are encoded, so an unmasked flag would make the
// server emit a format the rest of its own code does not expect.
func TestDecodeHandshakeResponseMasksCapabilities(t *testing.T) {
	clientCaps := ClientProtocol41 | ClientSecureConnection | ClientPluginAuth |
		ClientCompress | ClientLocalFiles

	resp := &HandshakeResponse{
		Capabilities: clientCaps,
		User:         "kwdb_user",
		AuthResponse: []byte{},
	}
	got, err := DecodeHandshakeResponse(resp.Encode(), ServerCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	if got.Capabilities&ClientCompress != 0 {
		t.Error("CLIENT_COMPRESS survived negotiation despite not being advertised")
	}
	if got.Capabilities&ClientLocalFiles != 0 {
		t.Error("CLIENT_LOCAL_FILES survived negotiation despite not being advertised")
	}
	if got.Capabilities&ClientProtocol41 == 0 {
		t.Error("CLIENT_PROTOCOL_41 was masked off despite being advertised by both sides")
	}
}

// TestDecodeHandshakeResponsePreservesSSLRequest checks that the client's TLS
// intent survives masking. CLIENT_SSL describes what the client wants rather
// than a feature the server grants, and the server's advertised set does not
// carry it unless TLS is configured, so a naive mask would drop it.
func TestDecodeHandshakeResponsePreservesSSLRequest(t *testing.T) {
	resp := &HandshakeResponse{
		Capabilities: ClientProtocol41 | ClientSecureConnection | ClientSSL,
		User:         "kwdb_user",
		AuthResponse: []byte{},
	}
	got, err := DecodeHandshakeResponse(resp.Encode(), ServerCapabilities)
	if err != nil {
		t.Fatal(err)
	}
	if got.Capabilities&ClientSSL == 0 {
		t.Error("the client's TLS request was masked off")
	}
}

// TestIsSSLRequest checks the length-based discrimination between an
// SSLRequest packet and a full handshake response. Getting this wrong means
// either dropping TLS silently or trying to parse a username out of padding.
func TestIsSSLRequest(t *testing.T) {
	ssl := EncodeSSLRequest(ClientProtocol41|ClientSecureConnection, 16<<20, 45)
	if len(ssl) != sslRequestLen {
		t.Fatalf("SSLRequest is %d bytes, want %d", len(ssl), sslRequestLen)
	}
	caps, err := PeekCapabilities(ssl)
	if err != nil {
		t.Fatal(err)
	}
	if !IsSSLRequest(ssl, caps) {
		t.Error("an SSLRequest packet was not recognized")
	}

	full := (&HandshakeResponse{
		Capabilities: ClientProtocol41 | ClientSecureConnection | ClientSSL,
		User:         "kwdb_user",
		AuthResponse: []byte{},
	}).Encode()
	fullCaps, err := PeekCapabilities(full)
	if err != nil {
		t.Fatal(err)
	}
	if IsSSLRequest(full, fullCaps) {
		t.Error("a full handshake response was mistaken for an SSLRequest")
	}
}

// TestDecodeHandshakeResponseRejectsTruncated checks that a response cut short
// at each field boundary is reported rather than yielding a half-populated
// struct that later code would treat as authenticated input.
func TestDecodeHandshakeResponseRejectsTruncated(t *testing.T) {
	full := (&HandshakeResponse{
		Capabilities: ClientProtocol41 | ClientSecureConnection | ClientPluginAuth | ClientConnectWithDB,
		User:         "kwdb_user",
		AuthResponse: bytes.Repeat([]byte{0xab}, 20),
		Database:     "defaultdb",
		AuthPlugin:   AuthNativePassword,
	}).Encode()

	// Offset 32 is where the variable-length part starts; truncating anywhere
	// inside it must fail rather than silently produce empty fields.
	for n := 32; n < len(full); n++ {
		if _, err := DecodeHandshakeResponse(full[:n], ServerCapabilities); err == nil {
			t.Errorf("a response truncated to %d bytes was accepted", n)
		}
	}
}

// TestAuthSwitchRequestLayout pins the packet that lets one server offer all
// three plugins: the client is told which plugin to use and given the nonce to
// use with it.
func TestAuthSwitchRequestLayout(t *testing.T) {
	scramble := []byte("0123456789abcdefghij")
	got := EncodeAuthSwitchRequest(AuthNativePassword, scramble)

	if got[0] != AuthSwitchRequestHeader {
		t.Errorf("header = 0x%02x, want 0x%02x", got[0], AuthSwitchRequestHeader)
	}
	r := NewReader(got[1:])
	plugin, err := r.NulString()
	if err != nil {
		t.Fatal(err)
	}
	if plugin != AuthNativePassword {
		t.Errorf("plugin = %q, want %q", plugin, AuthNativePassword)
	}
	rest := r.Remaining()
	if !bytes.Equal(rest, append(append([]byte{}, scramble...), 0)) {
		t.Errorf("scramble = %q, want %q followed by NUL", rest, scramble)
	}
}
