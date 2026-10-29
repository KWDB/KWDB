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
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// This file implements the three password-based authentication plugins the
// MySQL protocol defines. They differ in which digest the server must hold:
//
//	plugin                 server-side secret            transports cleartext?
//	mysql_native_password  SHA1(SHA1(password))          never
//	caching_sha2_password  SHA256(SHA256(password))      only on the full-auth path
//	sha256_password        (none; needs the cleartext)   always
//
// The first two are challenge-response: the client proves knowledge of the
// password against a server nonce without ever sending a password-equivalent,
// but that only works if the server stores a digest in exactly the shape the
// plugin prescribes. The third, and the full-auth fallback of the second,
// instead move the cleartext password over a confidential channel (TLS, or
// RSA encryption when TLS is not in use) and let the server verify it against
// whatever it happens to store.
//
// KWDB stores bcrypt hashes, which are deliberately not reconstructible into
// either of the two MySQL digests. That is why NativePassword and the
// caching_sha2 fast path need digests recorded alongside the bcrypt hash,
// while the cleartext paths work against the existing bcrypt hash unchanged.

// GenerateScramble returns a fresh ScrambleLen-byte nonce for the
// authentication exchange.
//
// MySQL clients historically treat the scramble as a NUL-terminated string, so
// a zero byte anywhere inside it truncates the challenge and makes
// authentication fail nondeterministically. Bytes are therefore drawn from a
// NUL-free range rather than being uniform over [0, 256).
func GenerateScramble() ([]byte, error) {
	buf := make([]byte, ScrambleLen)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	for i, b := range buf {
		// Map into the printable ASCII range [0x21, 0x7e], which excludes NUL
		// and, incidentally, whitespace that some clients mishandle.
		buf[i] = 0x21 + b%(0x7e-0x21+1)
	}
	return buf, nil
}

// xorBytes writes dst[i] = a[i] ^ b[i] for the length of a, cycling b if it is
// shorter. Used by both the challenge-response plugins and by the RSA path,
// which obfuscates the cleartext password with the scramble before encrypting.
func xorBytes(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i%len(b)]
	}
	return out
}

// NativePasswordDigest returns SHA1(SHA1(password)), the secret a server must
// hold to authenticate a mysql_native_password client.
func NativePasswordDigest(password string) []byte {
	first := sha1.Sum([]byte(password))
	second := sha1.Sum(first[:])
	return second[:]
}

// CachingSHA2Digest returns SHA256(SHA256(password)), the secret a server must
// hold to serve the caching_sha2_password fast path.
func CachingSHA2Digest(password string) []byte {
	first := sha256.Sum256([]byte(password))
	second := sha256.Sum256(first[:])
	return second[:]
}

// NativePasswordResponse computes the client's mysql_native_password reply:
//
//	SHA1(password) XOR SHA1(scramble || SHA1(SHA1(password)))
//
// It is exported because the test client needs it, and because expressing the
// verification below in terms of the same helper keeps the two halves of the
// protocol honest.
func NativePasswordResponse(password string, scramble []byte) []byte {
	if password == "" {
		// An empty password is signaled by an empty response, not by the
		// scramble of the empty string.
		return nil
	}
	stage1 := sha1.Sum([]byte(password))
	stage2 := sha1.Sum(stage1[:])

	h := sha1.New()
	h.Write(scramble)
	h.Write(stage2[:])
	return xorBytes(stage1[:], h.Sum(nil))
}

// VerifyNativePassword checks a mysql_native_password response against the
// stored SHA1(SHA1(password)) digest.
//
// The server recovers SHA1(password) by XORing the response with
// SHA1(scramble || storedDigest), then checks that hashing it once more
// reproduces storedDigest. A client that does not know the password cannot
// produce a response that survives that round trip.
func VerifyNativePassword(response, scramble, storedDigest []byte) bool {
	if len(storedDigest) != sha1.Size {
		return false
	}
	if len(response) == 0 {
		// The client claims an empty password. Verify that against the stored
		// digest instead of accepting it outright.
		return subtle.ConstantTimeCompare(storedDigest, NativePasswordDigest("")) == 1
	}
	if len(response) != sha1.Size {
		return false
	}

	h := sha1.New()
	h.Write(scramble)
	h.Write(storedDigest)
	stage1 := xorBytes(response, h.Sum(nil))

	candidate := sha1.Sum(stage1)
	return subtle.ConstantTimeCompare(candidate[:], storedDigest) == 1
}

// CachingSHA2Response computes the client's caching_sha2_password fast-path
// reply:
//
//	SHA256(password) XOR SHA256(SHA256(SHA256(password)) || scramble)
//
// Note the operand order inside the inner hash differs from
// mysql_native_password, where the scramble comes first.
func CachingSHA2Response(password string, scramble []byte) []byte {
	if password == "" {
		return nil
	}
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])

	h := sha256.New()
	h.Write(stage2[:])
	h.Write(scramble)
	return xorBytes(stage1[:], h.Sum(nil))
}

// VerifyCachingSHA2 checks a caching_sha2_password fast-path response against
// the stored SHA256(SHA256(password)) digest, mirroring VerifyNativePassword.
func VerifyCachingSHA2(response, scramble, storedDigest []byte) bool {
	if len(storedDigest) != sha256.Size {
		return false
	}
	if len(response) == 0 {
		return subtle.ConstantTimeCompare(storedDigest, CachingSHA2Digest("")) == 1
	}
	if len(response) != sha256.Size {
		return false
	}

	h := sha256.New()
	h.Write(storedDigest)
	h.Write(scramble)
	stage1 := xorBytes(response, h.Sum(nil))

	candidate := sha256.Sum256(stage1)
	return subtle.ConstantTimeCompare(candidate[:], storedDigest) == 1
}

// ObfuscatePassword XORs a NUL-terminated password with the scramble, which is
// what both cleartext-over-RSA plugins encrypt.
//
// The XOR is not a security measure on its own — the scramble travels in the
// clear — but it binds the ciphertext to this particular handshake, so a
// captured RSA blob cannot be replayed against a later connection.
func ObfuscatePassword(password string, scramble []byte) []byte {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	// plain[len(password)] is already the NUL terminator.
	return xorBytes(plain, scramble)
}

// DeobfuscatePassword reverses ObfuscatePassword, returning the cleartext
// password with its NUL terminator stripped.
func DeobfuscatePassword(obfuscated, scramble []byte) (string, error) {
	if len(obfuscated) == 0 {
		return "", fmt.Errorf("empty password payload")
	}
	plain := xorBytes(obfuscated, scramble)
	// Trailing NUL is expected but some clients omit it for empty passwords.
	if plain[len(plain)-1] == 0 {
		plain = plain[:len(plain)-1]
	}
	return string(plain), nil
}

// EncryptPasswordRSA performs the client half of the RSA path: it obfuscates
// the password with the scramble and encrypts it under the server's public
// key.
//
// MySQL uses RSA-OAEP with SHA-1, which crypto/rsa expresses as
// EncryptOAEP(sha1.New(), ...).
func EncryptPasswordRSA(password string, scramble []byte, pub *rsa.PublicKey) ([]byte, error) {
	return rsa.EncryptOAEP(sha1.New(), rand.Reader, pub, ObfuscatePassword(password, scramble), nil)
}

// EncodePublicKeyPEM renders an RSA public key in the form MySQL clients
// expect from an AuthMoreData packet: a PEM block of the PKIX
// SubjectPublicKeyInfo encoding, labeled "PUBLIC KEY".
//
// The older "RSA PUBLIC KEY" label wrapping a PKCS#1 body is not
// interchangeable here; clients feed the block straight to OpenSSL's
// PEM_read_bio_PUBKEY, which only accepts the PKIX form.
func EncodePublicKeyPEM(pub *rsa.PublicKey) ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("encoding RSA public key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

// DecodePublicKeyPEM parses what EncodePublicKeyPEM produced. Only the test
// client needs this.
func DecodePublicKeyPEM(data []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in public key payload")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing RSA public key: %w", err)
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is %T, want an RSA key", pub)
	}
	return rsaPub, nil
}

// DecryptPasswordRSA performs the server half: it decrypts the blob and undoes
// the scramble obfuscation, yielding the cleartext password.
func DecryptPasswordRSA(ciphertext []byte, scramble []byte, priv *rsa.PrivateKey) (string, error) {
	plain, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, priv, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypting password: %w", err)
	}
	return DeobfuscatePassword(plain, scramble)
}
