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

package sqlutils

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/util/fileutil"
)

// PGUrl returns a postgres connection url which connects to this server with the given user, and a
// cleanup function which must be called after all connections created using the connection url have
// been closed.
//
// In order to connect securely using postgres, this method will create temporary on-disk copies of
// certain embedded security certificates. The certificates will be created in a new temporary
// directory. The returned cleanup function will delete this temporary directory.
// Note that two calls to this function for the same `user` will generate different
// copies of the certificates, so the cleanup function must always be called.
//
// Args:
//
//	prefix: A prefix to be prepended to the temp file names generated, for debugging.
func PGUrl(t testing.TB, servingAddr, prefix string, user *url.Userinfo) (url.URL, func()) {
	return PGUrlWithOptionalClientCerts(t, servingAddr, prefix, user, true /* withCerts */)
}

// PGUrlWithNoneCer returns a postgres connection url which connects to this server with the given user, and a
// cleanup function which must be called after all connections created using the connection url have
// been closed.
//
// In order to connect securely using postgres, this method will create temporary on-disk copies of
// certain embedded security certificates. The certificates will be created in a new temporary
// directory. The returned cleanup function will delete this temporary directory.
// Note that two calls to this function for the same `user` will generate different
// copies of the certificates, so the cleanup function must always be called.
//
// Args:
//
//	prefix: A prefix to be prepended to the temp file names generated, for debugging.
func PGUrlWithNoneCer(
	t testing.TB, servingAddr, prefix string, user *url.Userinfo,
) (url.URL, func()) {
	return PGUrlWithOptionalClientCerts(t, servingAddr, prefix, user, false /* withCerts */)
}

// PGUrlWithOptionalClientCerts is like PGUrl but the caller can
// customize whether the client certificates are loaded on-disk and in the URL.
func PGUrlWithOptionalClientCerts(
	t testing.TB, servingAddr, prefix string, user *url.Userinfo, withClientCerts bool,
) (url.URL, func()) {
	host, port, err := net.SplitHostPort(servingAddr)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(benesch): Audit usage of prefix and replace the following line with
	// `testutils.TempDir(t)` if prefix can always be `t.Name()`.
	tempDir, err := ioutil.TempDir("", fileutil.EscapeFilename(prefix))
	if err != nil {
		t.Fatal(err)
	}

	caPath := filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert)
	tempCAPath := securitytest.RestrictedCopy(t, caPath, tempDir, "ca")
	options := url.Values{}
	options.Add("sslrootcert", tempCAPath)

	if withClientCerts {
		certPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.crt", user.Username()))
		keyPath := filepath.Join(security.EmbeddedCertsDir, fmt.Sprintf("client.%s.key", user.Username()))

		// Copy these assets to disk from embedded strings, so this test can
		// run from a standalone binary.
		tempCertPath := securitytest.RestrictedCopy(t, certPath, tempDir, "cert")
		tempKeyPath := securitytest.RestrictedCopy(t, keyPath, tempDir, "key")
		options.Add("sslcert", tempCertPath)
		options.Add("sslkey", tempKeyPath)
		options.Add("sslmode", "verify-full")
	} else {
		options.Add("sslmode", "verify-ca")
	}

	return url.URL{
			Scheme:   "postgres",
			User:     user,
			Host:     net.JoinHostPort(host, port),
			RawQuery: options.Encode(),
		}, func() {
			if err := os.RemoveAll(tempDir); err != nil {
				// Not Fatal() because we might already be panicking.
				t.Error(err)
			}
		}
}
