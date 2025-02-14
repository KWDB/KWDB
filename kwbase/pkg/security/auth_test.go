// Copyright 2015 The Cockroach Authors.
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

package security_test

import (
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// Construct a fake tls.ConnectionState object. The spec is a semicolon
// separated list if peer certificate specifications. Each peer certificate
// specification is a comma separated list of names where the first name is the
// CommonName and the remaining names are SubjectAlternateNames. For example,
// "foo" creates a single peer certificate with the CommonName "foo". The spec
// "foo,bar" creates a single peer certificate with the CommonName "foo" and a
// single SubjectAlternateName "bar". Contrast that with "foo;bar" which
// creates two peer certificates with the CommonNames "foo" and "bar"
// respectively.
func makeFakeTLSState(spec string) *tls.ConnectionState {
	tls := &tls.ConnectionState{}
	if spec != "" {
		for _, peerSpec := range strings.Split(spec, ";") {
			names := strings.Split(peerSpec, ",")
			if len(names) == 0 {
				continue
			}
			peerCert := &x509.Certificate{}
			peerCert.Subject = pkix.Name{CommonName: names[0]}
			peerCert.DNSNames = names[1:]
			tls.PeerCertificates = append(tls.PeerCertificates, peerCert)
		}
	}
	return tls
}

func TestGetCertificateUsers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Nil TLS state.
	if _, err := security.GetCertificateUsers(nil); err == nil {
		t.Error("unexpected success")
	}

	// No certificates.
	if _, err := security.GetCertificateUsers(makeFakeTLSState("")); err == nil {
		t.Error("unexpected success")
	}

	// Good request: single certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Request with multiple certs, but only one chain (eg: origin certs are client and CA).
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo;CA")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Always use the first certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo;bar")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo"})
	}

	// Extract all of the principals from the first certificate.
	if names, err := security.GetCertificateUsers(makeFakeTLSState("foo,bar,blah;CA")); err != nil {
		t.Error(err)
	} else {
		require.EqualValues(t, names, []string{"foo", "bar", "blah"})
	}
}

func TestSetCertPrincipalMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	testCases := []struct {
		vals     []string
		expected string
	}{
		{[]string{}, ""},
		{[]string{"foo"}, "invalid <cert-principal>:<db-principal> mapping:"},
		{[]string{"foo:bar"}, ""},
		{[]string{"foo:bar", "blah:blah"}, ""},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			err := security.SetCertPrincipalMap(c.vals)
			if !testutils.IsError(err, c.expected) {
				t.Fatalf("expected %q, but found %v", c.expected, err)
			}
		})
	}
}

func TestGetCertificateUsersMapped(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	testCases := []struct {
		spec     string
		val      string
		expected string
	}{
		// No mapping present.
		{"foo", "", "foo"},
		// The basic mapping case.
		{"foo", "foo:bar", "bar"},
		// Identity mapping.
		{"foo", "foo:foo", "foo"},
		// Mapping does not apply to cert principals.
		{"foo", "bar:bar", "foo"},
		// The last mapping for a principal takes precedence.
		{"foo", "foo:bar,foo:blah", "blah"},
		// First principal mapped, second principal unmapped.
		{"foo,bar", "foo:blah", "blah,bar"},
		// First principal unmapped, second principal mapped.
		{"bar,foo", "foo:blah", "bar,blah"},
		// Both principals mapped.
		{"foo,bar", "foo:bar,bar:foo", "bar,foo"},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			vals := strings.Split(c.val, ",")
			if err := security.SetCertPrincipalMap(vals); err != nil {
				t.Fatal(err)
			}
			names, err := security.GetCertificateUsers(makeFakeTLSState(c.spec))
			if err != nil {
				t.Fatal(err)
			}
			require.EqualValues(t, strings.Join(names, ","), c.expected)
		})
	}
}

func TestAuthenticationHook(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer func() { _ = security.SetCertPrincipalMap(nil) }()

	testCases := []struct {
		insecure           bool
		tlsSpec            string
		username           string
		principalMap       string
		buildHookSuccess   bool
		publicHookSuccess  bool
		privateHookSuccess bool
	}{
		// Insecure mode, empty username.
		{true, "", "", "", true, false, false},
		// Insecure mode, non-empty username.
		{true, "", "foo", "", true, true, false},
		// Secure mode, no TLS state.
		{false, "", "", "", false, false, false},
		// Secure mode, bad user.
		{false, "foo", "node", "", true, false, false},
		// Secure mode, node user.
		{false, security.NodeUser, "node", "", true, true, true},
		// Secure mode, root user.
		{false, security.RootUser, "node", "", true, false, false},
		// Secure mode, multiple cert principals.
		{false, "foo,bar", "foo", "", true, true, false},
		{false, "foo,bar", "bar", "", true, true, false},
		// Secure mode, principal map.
		{false, "foo,bar", "blah", "foo:blah", true, true, false},
		{false, "foo,bar", "blah", "bar:blah", true, true, false},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			err := security.SetCertPrincipalMap(strings.Split(tc.principalMap, ","))
			if err != nil {
				t.Fatal(err)
			}
			hook, err := security.UserAuthCertHook(tc.insecure, makeFakeTLSState(tc.tlsSpec))
			if (err == nil) != tc.buildHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.buildHookSuccess, err)
			}
			if err != nil {
				return
			}
			_, err = hook(tc.username, true /* clientConnection */)
			if (err == nil) != tc.publicHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.publicHookSuccess, err)
			}
			_, err = hook(tc.username, false /* clientConnection */)
			if (err == nil) != tc.privateHookSuccess {
				t.Fatalf("expected success=%t, got err=%v", tc.privateHookSuccess, err)
			}
		})
	}
}
