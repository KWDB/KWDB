// Copyright 2017 The Cockroach Authors.
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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
)

const wiggle = time.Minute * 5

// Returns true if "|a-b| <= wiggle".
func timesFuzzyEqual(a, b time.Time) bool {
	diff := a.Sub(b)
	if diff < 0 {
		diff = -diff
	}
	return diff <= wiggle
}

func TestGenerateCertLifetime(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testKey, err := rsa.GenerateKey(rand.Reader, 512)
	if err != nil {
		t.Fatal(err)
	}

	// Create a CA that expires in 2 days.
	caDuration := time.Hour * 48
	now := timeutil.Now()
	caBytes, err := security.GenerateCA(testKey, caDuration)
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := x509.ParseCertificate(caBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := caCert.NotAfter, now.Add(caDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("CA expiration differs from requested: %s vs %s", a, e)
	}

	// Create a Node certificate expiring in 4 days. Fails on shorter CA lifetime.
	nodeDuration := time.Hour * 96
	_, err = security.GenerateServerCert(caCert, testKey,
		testKey.Public(), nodeDuration, security.NodeUser, []string{"localhost"})
	if !testutils.IsError(err, "CA lifetime is .*, shorter than the requested .*") {
		t.Fatal(err)
	}

	// Try again, but expiring before the CA cert.
	nodeDuration = time.Hour * 24
	nodeBytes, err := security.GenerateServerCert(caCert, testKey,
		testKey.Public(), nodeDuration, security.NodeUser, []string{"localhost"})
	if err != nil {
		t.Fatal(err)
	}

	nodeCert, err := x509.ParseCertificate(nodeBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := nodeCert.NotAfter, now.Add(nodeDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("node expiration differs from requested: %s vs %s", a, e)
	}

	// Create a Client certificate expiring in 4 days. Should get reduced to the CA lifetime.
	clientDuration := time.Hour * 96
	_, err = security.GenerateClientCert(caCert, testKey, testKey.Public(), clientDuration, "testuser")
	if !testutils.IsError(err, "CA lifetime is .*, shorter than the requested .*") {
		t.Fatal(err)
	}

	// Try again, but expiring before the CA cert.
	clientDuration = time.Hour * 24
	clientBytes, err := security.GenerateClientCert(caCert, testKey, testKey.Public(), clientDuration, "testuser")
	if err != nil {
		t.Fatal(err)
	}

	clientCert, err := x509.ParseCertificate(clientBytes)
	if err != nil {
		t.Fatal(err)
	}

	if a, e := clientCert.NotAfter, now.Add(clientDuration); !timesFuzzyEqual(a, e) {
		t.Fatalf("client expiration differs from requested: %s vs %s", a, e)
	}

}
