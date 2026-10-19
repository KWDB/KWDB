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
	"crypto/rand"
	"crypto/rsa"
	"io"
	"net"
	"testing"
)

// The tests below drive both halves of a plugin's conversation over a real
// socket pair. Unit tests of the digest functions establish that the maths is
// right; these establish that the packets carrying it are exchanged in the
// right order, with the right sequence numbers, and that each side reads what
// the other wrote. Sequencing bugs do not show up in the maths.

// pipeConn returns two connected endpoints along with framed readers and
// writers for each, so a test can play server on one side and client on the
// other.
type pipeConn struct {
	serverPR *PacketReader
	serverPW *PacketWriter
	clientPR *PacketReader
	clientPW *PacketWriter
	close    func()
}

func newPipeConn(t *testing.T) *pipeConn {
	t.Helper()
	srv, cli := net.Pipe()
	return &pipeConn{
		serverPR: NewPacketReader(bufio.NewReader(srv), 1<<20),
		serverPW: NewPacketWriter(bufio.NewWriter(srv)),
		clientPR: NewPacketReader(bufio.NewReader(cli), 1<<20),
		clientPW: NewPacketWriter(bufio.NewWriter(cli)),
		close:    func() { _ = srv.Close(); _ = cli.Close() },
	}
}

// TestNativePasswordExchange plays a full mysql_native_password handshake over
// a socket: greeting, response, verification.
func TestNativePasswordExchange(t *testing.T) {
	const password = "hunter2"

	for _, tc := range []struct {
		name      string
		clientPwd string
		wantOK    bool
	}{
		{"correct password", password, true},
		{"wrong password", "wrong", false},
		{"empty password", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPipeConn(t)
			defer p.close()

			scramble, err := GenerateScramble()
			if err != nil {
				t.Fatal(err)
			}
			stored := NativePasswordDigest(password)

			// Server: send the greeting.
			greetingDone := make(chan error, 1)
			go func() {
				h := &Handshake{
					ServerVersion: "8.0.11-KWDB",
					ConnectionID:  1,
					Scramble:      scramble,
					Capabilities:  ServerCapabilities,
					Charset:       byte(CollationUTF8MB4GeneralCI),
					StatusFlags:   ServerStatusAutocommit,
					AuthPlugin:    AuthNativePassword,
				}
				if err := p.serverPW.WritePacket(h.Encode()); err != nil {
					greetingDone <- err
					return
				}
				greetingDone <- p.serverPW.Flush()
			}()

			// Client: read the greeting and reply.
			payload, err := p.clientPR.ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			if err := <-greetingDone; err != nil {
				t.Fatal(err)
			}
			if payload[0] != ProtocolVersion {
				t.Fatalf("greeting starts with 0x%02x, want 0x%02x", payload[0], ProtocolVersion)
			}

			resp := &HandshakeResponse{
				Capabilities: ClientProtocol41 | ClientSecureConnection | ClientPluginAuth,
				User:         "kwdb_user",
				AuthResponse: NativePasswordResponse(tc.clientPwd, scramble),
				AuthPlugin:   AuthNativePassword,
			}
			respDone := make(chan error, 1)
			go func() {
				if err := p.clientPW.WritePacket(resp.Encode()); err != nil {
					respDone <- err
					return
				}
				respDone <- p.clientPW.Flush()
			}()

			// Server: read the response and verify it.
			got, err := p.serverPR.ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			if err := <-respDone; err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeHandshakeResponse(got, ServerCapabilities)
			if err != nil {
				t.Fatal(err)
			}
			if decoded.User != "kwdb_user" {
				t.Errorf("user = %q, want %q", decoded.User, "kwdb_user")
			}
			if ok := VerifyNativePassword(decoded.AuthResponse, scramble, stored); ok != tc.wantOK {
				t.Errorf("verification = %t, want %t", ok, tc.wantOK)
			}
		})
	}
}

// TestCachingSHA2FullAuthRSAExchange plays the caching_sha2_password
// full-authentication path over a socket without TLS: the server asks for the
// cleartext password, the client requests the public key, and the password
// comes back RSA-encrypted.
//
// This is the exchange that lets an account with no recorded SHA2 digest
// authenticate, so it is the path most existing KWDB accounts will take.
func TestCachingSHA2FullAuthRSAExchange(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	const password = "hunter2"

	p := newPipeConn(t)
	defer p.close()

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- func() error {
			// The server tells us the fast path did not apply.
			more, err := p.clientPR.ReadPacket()
			if err != nil {
				return err
			}
			if more[0] != AuthMoreDataHeader || more[1] != CachingSHA2PerformFullAuth {
				t.Errorf("expected perform-full-auth, got % x", more)
			}

			// Ask for the public key.
			if err := p.clientPW.WritePacket([]byte{CachingSHA2RequestPublicKey}); err != nil {
				return err
			}
			if err := p.clientPW.Flush(); err != nil {
				return err
			}

			keyPacket, err := p.clientPR.ReadPacket()
			if err != nil {
				return err
			}
			if keyPacket[0] != AuthMoreDataHeader {
				t.Errorf("public key packet starts with 0x%02x, want 0x%02x",
					keyPacket[0], AuthMoreDataHeader)
			}
			pub, err := DecodePublicKeyPEM(keyPacket[1:])
			if err != nil {
				return err
			}
			if pub.N.Cmp(key.PublicKey.N) != 0 {
				t.Error("the client received a different public key than the server holds")
			}

			ciphertext, err := EncryptPasswordRSA(password, scramble, pub)
			if err != nil {
				return err
			}
			if err := p.clientPW.WritePacket(ciphertext); err != nil {
				return err
			}
			return p.clientPW.Flush()
		}()
	}()

	// Server side of the same conversation.
	if err := p.serverPW.WritePacket(
		EncodeAuthMoreData([]byte{CachingSHA2PerformFullAuth})); err != nil {
		t.Fatal(err)
	}
	if err := p.serverPW.Flush(); err != nil {
		t.Fatal(err)
	}

	req, err := p.serverPR.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if len(req) != 1 || req[0] != CachingSHA2RequestPublicKey {
		t.Fatalf("expected a public key request, got % x", req)
	}

	pubPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.serverPW.WritePacket(EncodeAuthMoreData(pubPEM)); err != nil {
		t.Fatal(err)
	}
	if err := p.serverPW.Flush(); err != nil {
		t.Fatal(err)
	}

	ciphertext, err := p.serverPR.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-clientDone; err != nil {
		t.Fatal(err)
	}

	recovered, err := DecryptPasswordRSA(ciphertext, scramble, key)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != password {
		t.Errorf("server recovered %q, want %q", recovered, password)
	}
}

// TestAuthSwitchExchange covers the plugin renegotiation a server performs
// when the client guessed a plugin the account is not configured for. Clients
// are required to honor it, and it is what lets one server offer all three
// plugins on one port.
func TestAuthSwitchExchange(t *testing.T) {
	p := newPipeConn(t)
	defer p.close()

	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	const password = "hunter2"

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- func() error {
			pkt, err := p.clientPR.ReadPacket()
			if err != nil {
				return err
			}
			if pkt[0] != AuthSwitchRequestHeader {
				t.Errorf("switch packet starts with 0x%02x, want 0x%02x",
					pkt[0], AuthSwitchRequestHeader)
			}
			r := NewReader(pkt[1:])
			plugin, err := r.NulString()
			if err != nil {
				return err
			}
			if plugin != AuthNativePassword {
				t.Errorf("switched to %q, want %q", plugin, AuthNativePassword)
			}
			newScramble := r.Remaining()
			// The trailing NUL is not part of the nonce.
			newScramble = newScramble[:len(newScramble)-1]

			if err := p.clientPW.WritePacket(
				NativePasswordResponse(password, newScramble)); err != nil {
				return err
			}
			return p.clientPW.Flush()
		}()
	}()

	if err := p.serverPW.WritePacket(
		EncodeAuthSwitchRequest(AuthNativePassword, scramble)); err != nil {
		t.Fatal(err)
	}
	if err := p.serverPW.Flush(); err != nil {
		t.Fatal(err)
	}

	resp, err := p.serverPR.ReadPacket()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-clientDone; err != nil {
		t.Fatal(err)
	}
	if !VerifyNativePassword(resp, scramble, NativePasswordDigest(password)) {
		t.Error("the response sent after the plugin switch did not verify")
	}
}

// TestResultSetExchange writes a complete text-protocol result set and reads
// it back the way a client does: column count, column definitions, rows, then
// the terminator. It covers both EOF conventions, since which one applies is
// negotiated per connection and getting it wrong desynchronizes the stream
// rather than producing a visible error.
func TestResultSetExchange(t *testing.T) {
	for _, tc := range []struct {
		name string
		caps uint32
	}{
		{"classic EOF", ClientProtocol41},
		{"deprecated EOF", ClientProtocol41 | ClientDeprecateEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			pw := NewPacketWriter(bufio.NewWriter(&buf))

			cols := []*ColumnDefinition{
				{Schema: "defaultdb", Table: "t", Name: "id",
					Type: TypeLongLong, ColumnLength: 20, Flags: ColumnFlagNum,
					CharacterSet: CollationUTF8MB4GeneralCI},
				{Schema: "defaultdb", Table: "t", Name: "label",
					Type: TypeVarString, ColumnLength: 65535,
					CharacterSet: CollationUTF8MB4GeneralCI},
			}
			rows := [][][]byte{
				{[]byte("1"), []byte("first")},
				{[]byte("2"), nil}, // SQL NULL
			}

			write := func(p []byte) {
				t.Helper()
				if err := pw.WritePacket(p); err != nil {
					t.Fatal(err)
				}
			}
			write(EncodeColumnCount(len(cols)))
			for _, c := range cols {
				write(c.Encode())
			}
			if tc.caps&ClientDeprecateEOF == 0 {
				write(EncodeEOF(0, ServerStatusAutocommit, tc.caps))
			}
			for _, r := range rows {
				write(EncodeTextRow(r))
			}
			if tc.caps&ClientDeprecateEOF != 0 {
				write(EncodeOK(&OK{StatusFlags: ServerStatusAutocommit}, tc.caps, true))
			} else {
				write(EncodeEOF(0, ServerStatusAutocommit, tc.caps))
			}
			if err := pw.Flush(); err != nil {
				t.Fatal(err)
			}

			// Read it back as a client would.
			pr := NewPacketReader(bufio.NewReader(&buf), 1<<20)

			countPkt, err := pr.ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			n, _, err := NewReader(countPkt).LenEncInt()
			if err != nil {
				t.Fatal(err)
			}
			if int(n) != len(cols) {
				t.Fatalf("column count = %d, want %d", n, len(cols))
			}

			for i := 0; i < int(n); i++ {
				pkt, err := pr.ReadPacket()
				if err != nil {
					t.Fatal(err)
				}
				def, err := DecodeColumnDefinition(pkt)
				if err != nil {
					t.Fatal(err)
				}
				if def.Name != cols[i].Name {
					t.Errorf("column %d named %q, want %q", i, def.Name, cols[i].Name)
				}
				if def.Type != cols[i].Type {
					t.Errorf("column %d type 0x%02x, want 0x%02x", i, def.Type, cols[i].Type)
				}
			}

			if tc.caps&ClientDeprecateEOF == 0 {
				pkt, err := pr.ReadPacket()
				if err != nil {
					t.Fatal(err)
				}
				if pkt[0] != EOFPacketHeader {
					t.Errorf("expected an EOF after the metadata, got 0x%02x", pkt[0])
				}
			}

			for i := range rows {
				pkt, err := pr.ReadPacket()
				if err != nil {
					t.Fatal(err)
				}
				// A terminator here would mean the row count was wrong.
				if pkt[0] == EOFPacketHeader && len(pkt) < 9 {
					t.Fatalf("result set ended after %d rows, want %d", i, len(rows))
				}
				values, err := DecodeTextRow(pkt, len(cols))
				if err != nil {
					t.Fatal(err)
				}
				for j := range values {
					if (values[j] == nil) != (rows[i][j] == nil) {
						t.Errorf("row %d value %d: NULL-ness did not survive", i, j)
						continue
					}
					if !bytes.Equal(values[j], rows[i][j]) {
						t.Errorf("row %d value %d = %q, want %q", i, j, values[j], rows[i][j])
					}
				}
			}

			last, err := pr.ReadPacket()
			if err != nil {
				t.Fatal(err)
			}
			if last[0] != EOFPacketHeader {
				t.Errorf("terminator starts with 0x%02x, want 0x%02x", last[0], EOFPacketHeader)
			}

			if _, err := pr.ReadPacket(); err != io.EOF {
				t.Errorf("expected the stream to be exhausted, got %v", err)
			}
		})
	}
}
