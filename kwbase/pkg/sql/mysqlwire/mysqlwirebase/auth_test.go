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
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"testing"
)

// TestNativePasswordKnownVectors pins mysql_native_password's stored digest so
// that a refactor cannot silently redefine the algorithm into something no
// client would accept.
//
// The expected values were computed independently of this package, with
// `printf %s <pw> | openssl dgst -sha1 -binary | openssl dgst -sha1`. The same
// digests appear, uppercased and prefixed with '*', in the
// authentication_string column of mysql.user on a real MySQL server, which is
// the cross-check that matters for interoperability.
func TestNativePasswordKnownVectors(t *testing.T) {
	for _, tc := range []struct {
		password string
		digest   string
	}{
		{"", "be1bdec0aa74b4dcb079943e70528096cca985f8"},
		{"password", "2470c0c06dee42fd1618bb99005adca2ec9d1e19"},
		{"root", "81f5e21e35407d884a6cd4a731aebfb6af209e1b"},
		{"kwdb", "f93bcadb80d7e4bca1629afcde6f897c4b6cc31e"},
	} {
		t.Run(tc.password, func(t *testing.T) {
			got := hex.EncodeToString(NativePasswordDigest(tc.password))
			if got != tc.digest {
				t.Errorf("sha1(sha1(%q)) = %s, want %s", tc.password, got, tc.digest)
			}
		})
	}
}

// TestNativePasswordRoundTrip checks that a response built the way a client
// builds it verifies against the digest a server would store, and that a wrong
// password does not.
func TestNativePasswordRoundTrip(t *testing.T) {
	for _, password := range []string{"", "password", "root", "hunter2", "长密码-with-unicode"} {
		t.Run(password, func(t *testing.T) {
			scramble, err := GenerateScramble()
			if err != nil {
				t.Fatal(err)
			}
			stored := NativePasswordDigest(password)

			resp := NativePasswordResponse(password, scramble)
			if !VerifyNativePassword(resp, scramble, stored) {
				t.Error("correct password rejected")
			}

			wrong := NativePasswordResponse(password+"x", scramble)
			if VerifyNativePassword(wrong, scramble, stored) {
				t.Error("wrong password accepted")
			}
		})
	}
}

// TestNativePasswordRejectsReplay checks the scramble actually binds a
// response to one handshake: a response captured from an earlier connection
// must not verify against a fresh nonce.
func TestNativePasswordRejectsReplay(t *testing.T) {
	const password = "hunter2"
	stored := NativePasswordDigest(password)

	first, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	captured := NativePasswordResponse(password, first)
	if VerifyNativePassword(captured, second, stored) {
		t.Error("a response replayed against a different scramble was accepted")
	}
}

// TestCachingSHA2RoundTrip is the caching_sha2_password equivalent of
// TestNativePasswordRoundTrip. The operand order inside the inner hash differs
// from mysql_native_password, which is exactly the kind of detail this pins.
func TestCachingSHA2RoundTrip(t *testing.T) {
	for _, password := range []string{"", "password", "root", "hunter2", "长密码-with-unicode"} {
		t.Run(password, func(t *testing.T) {
			scramble, err := GenerateScramble()
			if err != nil {
				t.Fatal(err)
			}
			stored := CachingSHA2Digest(password)

			resp := CachingSHA2Response(password, scramble)
			if !VerifyCachingSHA2(resp, scramble, stored) {
				t.Error("correct password rejected")
			}

			wrong := CachingSHA2Response(password+"x", scramble)
			if VerifyCachingSHA2(wrong, scramble, stored) {
				t.Error("wrong password accepted")
			}
		})
	}
}

// TestCachingSHA2RejectsReplay mirrors TestNativePasswordRejectsReplay.
func TestCachingSHA2RejectsReplay(t *testing.T) {
	const password = "hunter2"
	stored := CachingSHA2Digest(password)

	first, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	captured := CachingSHA2Response(password, first)
	if VerifyCachingSHA2(captured, second, stored) {
		t.Error("a response replayed against a different scramble was accepted")
	}
}

// TestVerifyRejectsMalformedInput checks that responses of the wrong length,
// and digests of the wrong length, are refused rather than mishandled. These
// are entirely client-controlled, so the verifiers must not assume any shape.
func TestVerifyRejectsMalformedInput(t *testing.T) {
	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("native", func(t *testing.T) {
		stored := NativePasswordDigest("password")
		for _, n := range []int{1, 19, 21, 32, 64} {
			if VerifyNativePassword(make([]byte, n), scramble, stored) {
				t.Errorf("accepted a %d-byte response", n)
			}
		}
		if VerifyNativePassword(make([]byte, 20), scramble, make([]byte, 32)) {
			t.Error("accepted a SHA256-sized digest for a SHA1-based plugin")
		}
	})

	t.Run("caching_sha2", func(t *testing.T) {
		stored := CachingSHA2Digest("password")
		for _, n := range []int{1, 20, 31, 33, 64} {
			if VerifyCachingSHA2(make([]byte, n), scramble, stored) {
				t.Errorf("accepted a %d-byte response", n)
			}
		}
		if VerifyCachingSHA2(make([]byte, 32), scramble, make([]byte, 20)) {
			t.Error("accepted a SHA1-sized digest for a SHA256-based plugin")
		}
	})
}

// TestEmptyPasswordIsVerifiedNotAssumed guards a subtle failure mode: a client
// signals an empty password with an empty response, and it would be easy to
// treat "empty response" as "no password required" and let anyone in. The
// empty response must instead be checked against the stored digest.
func TestEmptyPasswordIsVerifiedNotAssumed(t *testing.T) {
	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}

	nonEmptyNative := NativePasswordDigest("password")
	if VerifyNativePassword(nil, scramble, nonEmptyNative) {
		t.Error("an empty native_password response was accepted for an account that has a password")
	}
	if !VerifyNativePassword(nil, scramble, NativePasswordDigest("")) {
		t.Error("an empty native_password response was rejected for a passwordless account")
	}

	nonEmptySHA2 := CachingSHA2Digest("password")
	if VerifyCachingSHA2(nil, scramble, nonEmptySHA2) {
		t.Error("an empty caching_sha2 response was accepted for an account that has a password")
	}
	if !VerifyCachingSHA2(nil, scramble, CachingSHA2Digest("")) {
		t.Error("an empty caching_sha2 response was rejected for a passwordless account")
	}
}

// TestScrambleIsNulFree checks the property GenerateScramble exists to
// guarantee. Clients treat the scramble as a C string, so an embedded NUL
// truncates the challenge and breaks authentication for that connection only,
// which is the sort of bug that shows up as a rare flake in production.
func TestScrambleIsNulFree(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 512; i++ {
		s, err := GenerateScramble()
		if err != nil {
			t.Fatal(err)
		}
		if len(s) != ScrambleLen {
			t.Fatalf("scramble length = %d, want %d", len(s), ScrambleLen)
		}
		if bytes.IndexByte(s, 0) >= 0 {
			t.Fatalf("scramble contains a NUL byte: %x", s)
		}
		if seen[string(s)] {
			t.Fatalf("scramble repeated within %d draws: %x", i, s)
		}
		seen[string(s)] = true
	}
}

// TestRSAPasswordExchange covers the transport used by sha256_password and by
// caching_sha2_password's full-authentication fallback when the connection is
// not already protected by TLS.
func TestRSAPasswordExchange(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}

	for _, password := range []string{"", "password", "hunter2", "长密码-with-unicode"} {
		t.Run(password, func(t *testing.T) {
			ciphertext, err := EncryptPasswordRSA(password, scramble, &key.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecryptPasswordRSA(ciphertext, scramble, key)
			if err != nil {
				t.Fatal(err)
			}
			if got != password {
				t.Errorf("recovered %q, want %q", got, password)
			}
		})
	}
}

// TestRSAExchangeIsBoundToScramble checks that the XOR obfuscation does what
// it is there for: a ciphertext captured from one handshake must not decrypt
// to the right password under a different nonce.
func TestRSAExchangeIsBoundToScramble(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	first, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}

	const password = "hunter2"
	ciphertext, err := EncryptPasswordRSA(password, first, &key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecryptPasswordRSA(ciphertext, second, key)
	if err == nil && got == password {
		t.Error("a captured RSA blob replayed under a different scramble yielded the password")
	}
}

// TestObfuscatePasswordRoundTrip exercises the XOR layer directly, including
// the case where the password is longer than the scramble and the nonce has to
// be cycled.
func TestObfuscatePasswordRoundTrip(t *testing.T) {
	scramble, err := GenerateScramble()
	if err != nil {
		t.Fatal(err)
	}
	for _, password := range []string{
		"",
		"a",
		"exactly-twenty-chars",
		"a password comfortably longer than the twenty byte scramble, forcing the nonce to cycle",
	} {
		t.Run(password, func(t *testing.T) {
			got, err := DeobfuscatePassword(ObfuscatePassword(password, scramble), scramble)
			if err != nil {
				t.Fatal(err)
			}
			if got != password {
				t.Errorf("recovered %q, want %q", got, password)
			}
		})
	}
}
