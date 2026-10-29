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

package mysqlwire

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"net"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mysqlwire/mysqlwirebase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// authenticator runs the MySQL connection phase for one connection.
//
// The three plugins split into two families, and which family applies decides
// what the server needs to hold:
//
//   - The challenge-response family (mysql_native_password, and the
//     caching_sha2_password fast path) never transmits anything
//     password-equivalent, but requires the server to hold a plugin-specific
//     digest. KWDB stores bcrypt, from which those digests cannot be derived,
//     so they are read from the columns userDigests describes.
//   - The cleartext family (sha256_password, and caching_sha2_password's full
//     authentication) transports the password itself over a confidential
//     channel, and verifies it against the existing bcrypt hash. These work
//     for any account, including ones created before the digest columns
//     existed.
//
// The practical consequence is that an account whose password was set before
// the digest columns were populated can still authenticate, but only through
// the cleartext family. That is why the server falls back to full
// authentication rather than rejecting such an account outright.
type authenticator struct {
	conn     net.Conn
	pr       *mysqlwirebase.PacketReader
	pw       *mysqlwirebase.PacketWriter
	scramble []byte

	// tlsConfig is nil when the server has no certificates, in which case the
	// cleartext family must fall back to RSA.
	tlsConfig *tls.Config
	// rsaKey carries the cleartext password when TLS is not in use.
	rsaKey *rsa.PrivateKey

	// tlsActive records whether the connection was upgraded, which decides
	// whether the cleartext family may send the password directly.
	tlsActive bool

	// greetingPlugin is the plugin named in the server's greeting. A client
	// that wants a different one sends an empty response and waits to be
	// switched, so this is what decides whether a switch is owed.
	greetingPlugin string

	// insecure mirrors the node's --insecure setting. pgwire treats insecure
	// connections as "trust" regardless of any other configuration, and this
	// listener has to agree: a node where one protocol demands a password and
	// the other does not would be confusing to operate and would make the
	// MySQL port unusable on the insecure single-node setups used for
	// development.
	insecure bool
}

// userDigests holds what KWDB knows about an account's password, in the three
// shapes the plugins need.
type userDigests struct {
	// exists distinguishes a missing account from one with no password. Both
	// must produce the same client-visible outcome, but only the latter can
	// authenticate.
	exists bool
	// bcryptHash is KWDB's own password hash, used by the cleartext family.
	bcryptHash []byte
	// nativeDigest is SHA1(SHA1(password)); empty when it was never recorded.
	nativeDigest []byte
	// sha2Digest is SHA256(SHA256(password)); empty when never recorded.
	sha2Digest []byte
}

// authResult reports how a connection authenticated, for logging and for the
// caller to know whether the channel was confidential.
type authResult struct {
	plugin    string
	tlsActive bool
}

// authenticate runs the connection phase and returns once the client is either
// authenticated or rejected.
//
// lookup resolves an account to its digests. It is a parameter rather than a
// method so that the exchange can be tested without a running SQL layer: the
// protocol conversation is intricate, and its failure modes are the ones an
// attacker probes.
func (a *authenticator) authenticate(
	ctx context.Context,
	user string,
	requestedPlugin string,
	initialResponse []byte,
	lookup func(context.Context, string) (userDigests, error),
) (authResult, error) {
	if a.insecure {
		// Trust: accept the client without exchanging any credentials. Sending
		// an auth-switch or a full-auth request here would make the client wait
		// for an exchange we have no intention of completing.
		return authResult{plugin: "trust", tlsActive: a.tlsActive}, nil
	}

	digests, lookupErr := lookup(ctx, user)
	if lookupErr != nil {
		return authResult{}, lookupErr
	}

	// The plugin is chosen from what the client asked for, not from what the
	// account has recorded. Steering a client toward the plugin whose digest
	// happens to be present would tell an unauthenticated caller which
	// accounts exist and how their passwords were set.
	plugin := requestedPlugin
	switch plugin {
	case mysqlwirebase.AuthNativePassword,
		mysqlwirebase.AuthCachingSHA2Password,
		mysqlwirebase.AuthSHA256Password:
	default:
		// An unrecognized or absent plugin gets the modern default, which is
		// what MySQL 8.0 does.
		plugin = mysqlwirebase.AuthCachingSHA2Password
	}

	if plugin != a.greetingPlugin {
		// The client asked for a plugin the greeting did not nominate. Such a
		// client sends an empty auth response and waits to be switched, because
		// it has no scramble it knows belongs to the plugin it wants. Verifying
		// the empty response instead of switching rejects every correct
		// password, and for the cleartext plugins it leaves both sides waiting
		// on each other until the connection dies.
		var err error
		if initialResponse, err = a.switchPlugin(plugin); err != nil {
			return authResult{}, err
		}
	}

	ok, err := a.runPlugin(ctx, plugin, initialResponse, digests)
	if err != nil {
		return authResult{}, err
	}
	if !ok {
		return authResult{}, mysqlwirebase.NewAccessDeniedError(
			user, remoteHost(a.conn), len(initialResponse) > 0)
	}
	return authResult{plugin: plugin, tlsActive: a.tlsActive}, nil
}

// runPlugin performs one plugin's exchange and reports whether it succeeded.
func (a *authenticator) runPlugin(
	ctx context.Context, plugin string, response []byte, digests userDigests,
) (bool, error) {
	switch plugin {
	case mysqlwirebase.AuthNativePassword:
		if len(digests.nativeDigest) == 0 {
			// The account has no mysql_native_password digest recorded, so the
			// challenge-response cannot be verified. The plugin has no
			// cleartext fallback, so this account is unreachable through it
			// until its password is set again.
			log.VEventf(ctx, 1,
				"mysqlwire: account has no %s digest; the password must be reset to use this plugin",
				plugin)
			return false, nil
		}
		return mysqlwirebase.VerifyNativePassword(response, a.scramble, digests.nativeDigest), nil

	case mysqlwirebase.AuthCachingSHA2Password:
		if len(digests.sha2Digest) > 0 &&
			mysqlwirebase.VerifyCachingSHA2(response, a.scramble, digests.sha2Digest) {
			// Fast path: tell the client its scramble matched. The OK packet
			// that completes the handshake follows separately.
			if err := a.writePacket(
				mysqlwirebase.EncodeAuthMoreData([]byte{mysqlwirebase.CachingSHA2FastAuthSuccess}),
			); err != nil {
				return false, err
			}
			return true, nil
		}
		if isEmptyAuthResponse(response) {
			// The client already said in its handshake response that it has no
			// password, and it will not take part in a full-authentication
			// exchange after saying so. Requesting one anyway leaves it reading
			// an AuthMoreData packet it does not expect, which it reports as
			// "ERROR 2027 (HY000): Malformed packet" -- an account with no
			// password recorded has no sha2 digest either, so it can never take
			// the fast path above and this was the only exchange it could have.
			// Verifying the empty password here rejects it through the same
			// path as any other wrong one, which is what mysql_native_password
			// already did for the same account.
			return verifyBcrypt(digests, ""), nil
		}
		// Either no digest is recorded or the scramble did not match. Both
		// lead to full authentication, which is what makes an account with no
		// recorded digest usable, and which keeps a wrong password
		// indistinguishable from a stale one.
		password, err := a.fullAuthentication(mysqlwirebase.CachingSHA2RequestPublicKey)
		if err != nil {
			return false, err
		}
		return verifyBcrypt(digests, password), nil

	case mysqlwirebase.AuthSHA256Password:
		password, err := a.sha256Authentication(response)
		if err != nil {
			return false, err
		}
		return verifyBcrypt(digests, password), nil

	default:
		return false, mysqlwirebase.NewError(
			mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError,
			"unsupported authentication plugin "+plugin)
	}
}

// isEmptyAuthResponse reports whether a handshake response carries no password.
//
// A client with no password sends either nothing or a single NUL, depending on
// the plugin and version; both mean the same thing and neither can be followed
// by a further exchange.
func isEmptyAuthResponse(response []byte) bool {
	return len(response) == 0 || (len(response) == 1 && response[0] == 0)
}

// verifyBcrypt checks a cleartext password against KWDB's stored hash.
//
// A missing account and one with an empty hash are both rejected here rather
// than earlier, so that the work done is the same either way and the outcome
// does not depend on whether the account exists.
func verifyBcrypt(digests userDigests, password string) bool {
	if !digests.exists || len(digests.bcryptHash) == 0 {
		return false
	}
	return security.CompareHashAndPassword(digests.bcryptHash, password) == nil
}

// fullAuthentication asks the client for the cleartext password.
//
// Over TLS the password is sent directly. Otherwise the client is given the
// server's RSA public key and sends the password encrypted under it, which is
// what makes these plugins usable without certificates.
func (a *authenticator) fullAuthentication(requestKeyByte byte) (string, error) {
	if err := a.writePacket(
		mysqlwirebase.EncodeAuthMoreData([]byte{mysqlwirebase.CachingSHA2PerformFullAuth}),
	); err != nil {
		return "", err
	}
	return a.readCleartextPassword(requestKeyByte)
}

// sha256Authentication runs the sha256_password exchange, which has no fast
// path: the client always sends the cleartext password.
//
// The client may already have sent it in the handshake response when TLS was
// active, in which case there is nothing more to exchange.
func (a *authenticator) sha256Authentication(response []byte) (string, error) {
	if a.tlsActive {
		if len(response) == 0 {
			// An empty response over TLS means an empty password, which still
			// has to be checked against the stored hash rather than assumed.
			return "", nil
		}
		return trimNul(response), nil
	}
	if len(response) == 0 {
		// Nothing to work from yet; the client is still to speak.
		return a.readCleartextPassword(mysqlwirebase.SHA256RequestPublicKey)
	}
	return a.processCleartextPayload(response, mysqlwirebase.SHA256RequestPublicKey)
}

// readCleartextPassword reads the next packet and completes a cleartext
// exchange with it.
func (a *authenticator) readCleartextPassword(requestKeyByte byte) (string, error) {
	payload, err := a.readPacket()
	if err != nil {
		return "", err
	}
	return a.processCleartextPayload(payload, requestKeyByte)
}

// processCleartextPayload completes a cleartext exchange whose first packet has
// already been received.
//
// Over TLS the payload is the password. Otherwise it is either a request for
// the server's public key, or the RSA-encrypted password from a client that
// already had the key.
func (a *authenticator) processCleartextPayload(
	payload []byte, requestKeyByte byte,
) (string, error) {
	if a.tlsActive {
		// The channel is already confidential; the password arrives as-is.
		return trimNul(payload), nil
	}

	// Without TLS the client first asks for the public key. Some clients that
	// were configured with the key out of band skip the request and send the
	// ciphertext straight away, which is why this is a check rather than an
	// assumption.
	if len(payload) == 1 && payload[0] == requestKeyByte {
		if a.rsaKey == nil {
			return "", mysqlwirebase.NewError(
				mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError,
				"the server has no RSA key for password exchange; connect with TLS instead")
		}
		pubPEM, err := mysqlwirebase.EncodePublicKeyPEM(&a.rsaKey.PublicKey)
		if err != nil {
			return "", err
		}
		if err := a.writePacket(mysqlwirebase.EncodeAuthMoreData(pubPEM)); err != nil {
			return "", err
		}
		if payload, err = a.readPacket(); err != nil {
			return "", err
		}
	}

	if a.rsaKey == nil {
		return "", mysqlwirebase.NewError(
			mysqlwirebase.ErrHandshake, mysqlwirebase.SQLStateGeneralError,
			"the server has no RSA key for password exchange; connect with TLS instead")
	}
	return mysqlwirebase.DecryptPasswordRSA(payload, a.scramble, a.rsaKey)
}

// switchPlugin asks the client to restart authentication with a different
// plugin and returns its new response.
func (a *authenticator) switchPlugin(plugin string) ([]byte, error) {
	if err := a.writePacket(
		mysqlwirebase.EncodeAuthSwitchRequest(plugin, a.scramble),
	); err != nil {
		return nil, err
	}
	return a.readPacket()
}

// writePacket sends one packet of the connection phase.
//
// The connection phase numbers packets with a single counter that both sides
// advance, unlike the command phase where the client restarts at zero for
// every command. The reader and writer keep separate counters, so each send
// has to hand its position to the reader and each receive has to hand it back;
// otherwise the next packet looks out of order and the connection is dropped.
func (a *authenticator) writePacket(payload []byte) error {
	if err := a.pw.WritePacket(payload); err != nil {
		return err
	}
	if err := a.pw.Flush(); err != nil {
		return err
	}
	a.pr.SetSequence(a.pw.Sequence())
	return nil
}

// readPacket receives one packet of the connection phase, keeping the writer's
// counter in step. See writePacket.
func (a *authenticator) readPacket() ([]byte, error) {
	payload, err := a.pr.ReadPacket()
	if err != nil {
		return nil, err
	}
	a.pw.SetSequence(a.pr.Sequence())
	return payload, nil
}

// trimNul drops the terminator MySQL puts after a cleartext password.
func trimNul(b []byte) string {
	if n := len(b); n > 0 && b[n-1] == 0 {
		return string(b[:n-1])
	}
	return string(b)
}

// remoteHost renders the client address the way MySQL's access-denied message
// spells it.
func remoteHost(c net.Conn) string {
	addr := c.RemoteAddr()
	if addr == nil {
		return "localhost"
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}
