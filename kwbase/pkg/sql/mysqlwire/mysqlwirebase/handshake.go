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

// Handshake is the server's greeting, the first thing written on a new
// connection. See "Protocol::HandshakeV10".
//
// Unlike the PostgreSQL wire protocol, where the client speaks first, the
// MySQL server opens the conversation. That asymmetry is why the two protocols
// cannot share a listening port: neither side would speak until the other did.
type Handshake struct {
	ServerVersion string
	ConnectionID  uint32
	// Scramble is the ScrambleLen-byte nonce, split across two fields on the
	// wire for backwards compatibility with pre-4.1 servers.
	Scramble     []byte
	Capabilities uint32
	Charset      byte
	StatusFlags  uint16
	AuthPlugin   string
}

// Encode serializes the handshake into a packet payload.
func (h *Handshake) Encode() []byte {
	w := NewWriter(128)
	w.Byte(ProtocolVersion)
	w.NulString(h.ServerVersion)
	w.Uint32(h.ConnectionID)

	// auth-plugin-data-part-1: the first 8 scramble bytes, then a filler.
	w.Raw(h.Scramble[:8])
	w.Byte(0)

	w.Uint16(uint16(h.Capabilities))
	w.Byte(h.Charset)
	w.Uint16(h.StatusFlags)
	w.Uint16(uint16(h.Capabilities >> 16))

	if h.Capabilities&ClientPluginAuth != 0 {
		// Length of the whole auth-plugin-data including the NUL that
		// terminates part 2, which is what clients expect here.
		w.Byte(byte(len(h.Scramble) + 1))
	} else {
		w.Byte(0)
	}
	w.Zeros(10) // reserved

	// auth-plugin-data-part-2: the remaining scramble bytes, NUL-terminated.
	// The field is specified as MAX(13, len(auth-plugin-data) - 8) bytes, which
	// for a 20-byte scramble is exactly 12 bytes plus the terminator.
	w.Raw(h.Scramble[8:])
	w.Byte(0)

	if h.Capabilities&ClientPluginAuth != 0 {
		w.NulString(h.AuthPlugin)
	}
	return w.Bytes()
}

// HandshakeResponse is the client's reply to the greeting. See
// "Protocol::HandshakeResponse41".
type HandshakeResponse struct {
	Capabilities  uint32
	MaxPacketSize uint32
	Charset       byte
	User          string
	AuthResponse  []byte
	Database      string
	AuthPlugin    string
	Attrs         map[string]string
}

// sslRequestLen is the size of the truncated handshake response a client sends
// to request a TLS upgrade: 4 bytes of capabilities, 4 of max packet size, 1
// of charset, and 23 reserved.
const sslRequestLen = 32

// IsSSLRequest reports whether payload is an SSLRequest packet rather than a
// full handshake response.
//
// The two are distinguished only by length: a client that wants TLS sends the
// fixed-size prefix and then restarts the handshake inside the TLS session. A
// payload can be exactly sslRequestLen bytes long only in the SSLRequest case,
// since a real response also carries at least a NUL-terminated username.
func IsSSLRequest(payload []byte, capabilities uint32) bool {
	return capabilities&ClientSSL != 0 && len(payload) == sslRequestLen
}

// PeekCapabilities extracts the client capability flags from the first 4 bytes
// of a handshake response, so the caller can decide whether a TLS upgrade is
// being requested before parsing the rest.
func PeekCapabilities(payload []byte) (uint32, error) {
	r := NewReader(payload)
	return r.Uint32()
}

// DecodeHandshakeResponse parses a HandshakeResponse41 payload.
//
// serverCapabilities is intersected with the client's, so that a client cannot
// switch on a feature the server never advertised.
func DecodeHandshakeResponse(
	payload []byte, serverCapabilities uint32,
) (*HandshakeResponse, error) {
	r := NewReader(payload)

	clientCaps, err := r.Uint32()
	if err != nil {
		return nil, fmt.Errorf("reading capability flags: %w", err)
	}
	if clientCaps&ClientProtocol41 == 0 {
		// Everything below assumes the 4.1 layout, and no client from the last
		// two decades omits this flag.
		return nil, fmt.Errorf("client does not support the 4.1 protocol")
	}
	resp := &HandshakeResponse{Capabilities: clientCaps & serverCapabilities}
	// CLIENT_SSL describes what the client wants, not a feature the server
	// grants, and the server's own capability word does not carry it unless
	// TLS is configured; preserve the client's bit so the caller can act on it.
	resp.Capabilities |= clientCaps & ClientSSL

	if resp.MaxPacketSize, err = r.Uint32(); err != nil {
		return nil, fmt.Errorf("reading max packet size: %w", err)
	}
	if resp.Charset, err = r.Byte(); err != nil {
		return nil, fmt.Errorf("reading charset: %w", err)
	}
	if err := r.Skip(23); err != nil {
		return nil, fmt.Errorf("reading reserved bytes: %w", err)
	}

	// An SSLRequest packet ends here; the caller detects that by length before
	// calling this function, so reaching the end now means a truncated packet.
	if resp.User, err = r.NulString(); err != nil {
		return nil, fmt.Errorf("reading username: %w", err)
	}

	switch {
	case clientCaps&ClientPluginAuthLenEncClientData != 0:
		if resp.AuthResponse, err = r.LenEncString(); err != nil {
			return nil, fmt.Errorf("reading auth response: %w", err)
		}
	case clientCaps&ClientSecureConnection != 0:
		n, err := r.Byte()
		if err != nil {
			return nil, fmt.Errorf("reading auth response length: %w", err)
		}
		if resp.AuthResponse, err = r.Bytes(int(n)); err != nil {
			return nil, fmt.Errorf("reading auth response: %w", err)
		}
	default:
		s, err := r.NulString()
		if err != nil {
			return nil, fmt.Errorf("reading auth response: %w", err)
		}
		resp.AuthResponse = []byte(s)
	}

	if clientCaps&ClientConnectWithDB != 0 {
		if resp.Database, err = r.NulString(); err != nil {
			return nil, fmt.Errorf("reading database name: %w", err)
		}
	}
	if clientCaps&ClientPluginAuth != 0 {
		if resp.AuthPlugin, err = r.NulString(); err != nil {
			return nil, fmt.Errorf("reading auth plugin name: %w", err)
		}
	}
	if clientCaps&ClientConnectAttrs != 0 {
		// Connection attributes are advisory metadata (client name, version,
		// program name). A malformed block should not sink the connection, but
		// a truncated one means we can no longer trust the packet, so parse
		// errors are reported rather than ignored.
		total, _, err := r.LenEncInt()
		if err != nil {
			return nil, fmt.Errorf("reading connect attrs length: %w", err)
		}
		if int(total) > r.Len() {
			return nil, fmt.Errorf("connect attrs length %d exceeds remaining payload", total)
		}
		attrs := make(map[string]string)
		end := r.Len() - int(total)
		for r.Len() > end {
			k, err := r.LenEncString()
			if err != nil {
				return nil, fmt.Errorf("reading connect attr key: %w", err)
			}
			v, err := r.LenEncString()
			if err != nil {
				return nil, fmt.Errorf("reading connect attr value: %w", err)
			}
			attrs[string(k)] = string(v)
		}
		resp.Attrs = attrs
	}
	return resp, nil
}

// Encode serializes a handshake response. Only the test client needs this; the
// server never sends one.
func (h *HandshakeResponse) Encode() []byte {
	w := NewWriter(128)
	w.Uint32(h.Capabilities)
	w.Uint32(h.MaxPacketSize)
	w.Byte(h.Charset)
	w.Zeros(23)
	w.NulString(h.User)

	switch {
	case h.Capabilities&ClientPluginAuthLenEncClientData != 0:
		w.LenEncBytes(h.AuthResponse)
	case h.Capabilities&ClientSecureConnection != 0:
		w.Byte(byte(len(h.AuthResponse)))
		w.Raw(h.AuthResponse)
	default:
		w.Raw(h.AuthResponse)
		w.Byte(0)
	}

	if h.Capabilities&ClientConnectWithDB != 0 {
		w.NulString(h.Database)
	}
	if h.Capabilities&ClientPluginAuth != 0 {
		w.NulString(h.AuthPlugin)
	}
	if h.Capabilities&ClientConnectAttrs != 0 {
		attrs := NewWriter(64)
		for k, v := range h.Attrs {
			attrs.LenEncString(k)
			attrs.LenEncString(v)
		}
		w.LenEncBytes(attrs.Bytes())
	}
	return w.Bytes()
}

// EncodeSSLRequest builds the truncated response a client sends to ask for a
// TLS upgrade.
func EncodeSSLRequest(capabilities uint32, maxPacketSize uint32, charset byte) []byte {
	w := NewWriter(sslRequestLen)
	w.Uint32(capabilities | ClientSSL)
	w.Uint32(maxPacketSize)
	w.Byte(charset)
	w.Zeros(23)
	return w.Bytes()
}

// EncodeAuthSwitchRequest builds the packet that asks the client to restart
// authentication with a different plugin. See "Protocol::AuthSwitchRequest".
//
// The server sends this when the plugin the client guessed is not the one the
// account is configured for. Clients are required to support the switch, which
// is what lets a single server offer all three plugins.
func EncodeAuthSwitchRequest(plugin string, scramble []byte) []byte {
	w := NewWriter(len(plugin) + len(scramble) + 3)
	w.Byte(AuthSwitchRequestHeader)
	w.NulString(plugin)
	w.Raw(scramble)
	w.Byte(0)
	return w.Bytes()
}

// EncodeAuthMoreData builds an AuthMoreData packet, the generic carrier for
// plugin-specific payloads mid-authentication: the caching_sha2_password
// status bytes, and the server's RSA public key.
func EncodeAuthMoreData(data []byte) []byte {
	w := NewWriter(len(data) + 1)
	w.Byte(AuthMoreDataHeader)
	w.Raw(data)
	return w.Bytes()
}
