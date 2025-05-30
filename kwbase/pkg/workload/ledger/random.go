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

package ledger

import (
	"math/rand"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
)

const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const aChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

// randInt returns a number within [min, max] inclusive.
func randInt(rng *rand.Rand, min, max int) int {
	return rng.Intn(max-min+1) + min
}

func randStringFromAlphabet(rng *rand.Rand, minLen, maxLen int, alphabet string) string {
	size := maxLen
	if maxLen-minLen != 0 {
		size = randInt(rng, minLen, maxLen)
	}
	if size == 0 {
		return ""
	}

	b := make([]byte, size)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

// randAString generates a random alphanumeric string of length between min and
// max inclusive.
func randAString(rng *rand.Rand, min, max int) string {
	return randStringFromAlphabet(rng, min, max, aChars)
}

// randPaymentID produces a random payment id string.
func randPaymentID(rng *rand.Rand) string {
	uuidStr := uuid.MakeV4().String()
	return paymentIDPrefix + uuidStr
}

// randContext produces a random context string.
func randContext(rng *rand.Rand) string {
	return randAString(rng, 56, 56)
}

// randUsername produces a random username string.
func randUsername(rng *rand.Rand) string {
	return randAString(rng, 18, 20)
}

// randResponse produces a random transaction response string.
func randResponse(rng *rand.Rand) string {
	return randAString(rng, 400, 400)
}

// randCurrencyCode produces a random currency code string.
func randCurrencyCode(rng *rand.Rand) string {
	return randStringFromAlphabet(rng, 3, 3, letters)
}

// randTimestamp produces a random timestamp.
func randTimestamp(rng *rand.Rand) time.Time {
	return timeutil.Unix(rng.Int63n(1600000000), rng.Int63())
}

// randSessionID produces a random session ID string.
func randSessionID(rng *rand.Rand) string {
	return randAString(rng, 60, 62)
}

// randSessionData produces a random session data string.
func randSessionData(rng *rand.Rand) string {
	return randAString(rng, 160, 160)
}

// randAmount produces a random amount string.
func randAmount(rng *rand.Rand) float64 {
	return float64(randInt(rng, 100, 100000)) / 100
}

// randCustomer returns a random customer ID.
func (w ledger) randCustomer(rng *rand.Rand) int {
	return randInt(rng, 0, w.customers-1)
}
