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

import "testing"

// TestIsEmptyAuthResponse pins how a handshake response that carries no
// password is recognized.
//
// A client with no password puts a single NUL in its handshake response --
// observed from libmysqlclient 8.0.46 as the length-encoded pair 01 00 -- and
// will not take part in any further exchange after saying so. An account
// created without a password has no sha2 digest either, so
// caching_sha2_password cannot take its fast path; before this was
// recognized, the server answered with a full-authentication request the
// client did not expect and the client reported "ERROR 2027 (HY000):
// Malformed packet" instead of an access-denied error. mysql_native_password
// answered 1045 for the same account, so the two plugins disagreed.
//
// The boundary matters in both directions: a one-byte response that is not
// NUL is a real (if short) credential and must still go to full
// authentication, or a wrong password would be silently treated as an absent
// one.
func TestIsEmptyAuthResponse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response []byte
		want     bool
	}{
		{name: "nil", response: nil, want: true},
		{name: "zero length", response: []byte{}, want: true},
		{name: "single NUL, as libmysqlclient sends", response: []byte{0}, want: true},

		{name: "single non-NUL byte is a credential", response: []byte{1}, want: false},
		{name: "two NULs are not an empty response", response: []byte{0, 0}, want: false},
		{name: "scramble", response: make([]byte, 20), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isEmptyAuthResponse(tc.response); got != tc.want {
				t.Fatalf("isEmptyAuthResponse(%v) = %v, want %v", tc.response, got, tc.want)
			}
		})
	}
}
