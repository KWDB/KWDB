// Copyright 2019 The Cockroach Authors.
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
	"context"
	"crypto/tls"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/hba"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// This file contains the methods that are accepted to perform
// authentication of users during the pgwire connection handshake.
//
// Which method are accepted for which user is selected using
// the HBA config loaded into the cluster setting
// server.host_based_authentication.configuration.
//
// Other methods can be added using RegisterAuthMethod(). This is done
// e.g. in the CCL modules to add support for GSS authentication using
// Kerberos.

func loadDefaultMethods() {
	// The "password" method requires a clear text password.
	//
	// Care should be taken by administrators to only accept this auth
	// method over secure connections, e.g. those encrypted using SSL.
	RegisterAuthMethod("password", authPassword, clusterversion.VersionMinSupported, hba.ConnAny, nil)

	// The "cert" method requires a valid client certificate for the
	// user attempting to connect.
	//
	// This method is only usable over SSL connections.
	RegisterAuthMethod("cert", authCert, clusterversion.VersionMinSupported, hba.ConnHostSSL, nil)

	// The "cert-password" method requires either a valid client
	// certificate for the connecting user, or, if no cert is provided,
	// a cleartext password.
	RegisterAuthMethod("cert-password", authCertPassword, clusterversion.VersionMinSupported, hba.ConnAny, nil)

	// The "reject" method rejects any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("reject", authReject, clusterversion.VersionMinSupported, hba.ConnAny, nil)

	// The "trust" method accepts any connection attempt that matches
	// the current rule.
	RegisterAuthMethod("trust", authTrust, clusterversion.VersionMinSupported, hba.ConnAny, nil)

}

// AuthMethod defines a method for authentication of a connection.
type AuthMethod func(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error)

// PasswordRetrievalFn defines a method to retrieve the hashed
// password for the user logging in.
type PasswordRetrievalFn = func(context.Context) ([]byte, error)

// PasswordValidUntilFn defines a method to retrieve the expiration time
// of the user's password.
type PasswordValidUntilFn = func(context.Context) (*tree.DTimestamp, error)

func authPassword(
	ctx context.Context,
	c AuthConn,
	_ tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	if err := c.SendAuthRequest(authCleartextPassword, nil /* data */); err != nil {
		return nil, err
	}
	pwdData, err := c.GetPwdData()
	if err != nil {
		return nil, err
	}
	password, err := passwordString(pwdData)
	if err != nil {
		return nil, err
	}
	hashedPassword, err := pwRetrieveFn(ctx)
	if err != nil {
		return nil, err
	}
	if len(hashedPassword) == 0 {
		c.Logf(ctx, "user has no password defined")
	}

	validUntil, err := pwValidUntilFn(ctx)
	if err != nil {
		return nil, err
	}
	if validUntil != nil {
		if validUntil.Sub(timeutil.Now()) < 0 {
			c.Logf(ctx, "password is expired")
			return nil, errors.New("password is expired")
		}
	}

	return security.UserAuthPasswordHook(
		false /*insecure*/, password, hashedPassword,
	), nil
}

func passwordString(pwdData []byte) (string, error) {
	// Make a string out of the byte array.
	if bytes.IndexByte(pwdData, 0) != len(pwdData)-1 {
		return "", fmt.Errorf("expected 0-terminated byte array")
	}
	return string(pwdData[:len(pwdData)-1]), nil
}

func authCert(
	_ context.Context,
	_ AuthConn,
	tlsState tls.ConnectionState,
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	if len(tlsState.PeerCertificates) == 0 {
		return nil, errors.New("no TLS peer certificates, but required for auth")
	}
	// Normalize the username contained in the certificate.
	tlsState.PeerCertificates[0].Subject.CommonName = tree.Name(
		tlsState.PeerCertificates[0].Subject.CommonName,
	).Normalize()
	return security.UserAuthCertHook(false /*insecure*/, &tlsState)
}

func authCertPassword(
	ctx context.Context,
	c AuthConn,
	tlsState tls.ConnectionState,
	pwRetrieveFn PasswordRetrievalFn,
	pwValidUntilFn PasswordValidUntilFn,
	execCfg *sql.ExecutorConfig,
	entry *hba.Entry,
) (security.UserAuthHook, error) {
	var fn AuthMethod
	if len(tlsState.PeerCertificates) == 0 {
		c.Logf(ctx, "no client certificate, proceeding with password authentication")
		fn = authPassword
	} else {
		c.Logf(ctx, "client presented certificate, proceeding with certificate validation")
		fn = authCert
	}
	return fn(ctx, c, tlsState, pwRetrieveFn, pwValidUntilFn, execCfg, entry)
}

func authTrust(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ string, _ bool) (func(), error) { return nil, nil }, nil
}

func authReject(
	_ context.Context,
	_ AuthConn,
	_ tls.ConnectionState,
	_ PasswordRetrievalFn,
	_ PasswordValidUntilFn,
	_ *sql.ExecutorConfig,
	_ *hba.Entry,
) (security.UserAuthHook, error) {
	return func(_ string, _ bool) (func(), error) {
		return nil, errors.New("authentication rejected by configuration")
	}, nil
}
