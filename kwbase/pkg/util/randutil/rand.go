// Copyright 2014 The Cockroach Authors.
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

package randutil

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log" // Don't bring kwbase/util/log into this low-level package.
	"math/rand"

	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
)

// NewPseudoSeed generates a seed from crypto/rand.
func NewPseudoSeed() int64 {
	var seed int64
	err := binary.Read(crypto_rand.Reader, binary.LittleEndian, &seed)
	if err != nil {
		panic(fmt.Sprintf("could not read from crypto/rand: %s", err))
	}
	return seed
}

// NewPseudoRand returns an instance of math/rand.Rand seeded from the
// environment variable KWBASE_RANDOM_SEED.  If that variable is not set,
// crypto/rand is used to generate a seed. The seed is also returned so we can
// easily and cheaply generate unique streams of numbers. The created object is
// not safe for concurrent access.
func NewPseudoRand() (*rand.Rand, int64) {
	seed := envutil.EnvOrDefaultInt64("KWBASE_RANDOM_SEED", NewPseudoSeed())
	return rand.New(rand.NewSource(seed)), seed
}

// RandIntInRange returns a value in [min, max)
func RandIntInRange(r *rand.Rand, min, max int) int {
	return min + r.Intn(max-min)
}

var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandBytes returns a byte slice of the given length with random
// data.
func RandBytes(r *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}

	arr := make([]byte, size)
	for i := 0; i < len(arr); i++ {
		arr[i] = randLetters[r.Intn(len(randLetters))]
	}
	return arr
}

// ReadTestdataBytes reads random bytes, but then nudges them into printable
// ASCII, *reducing their randomness* to make them a little friendlier for
// humans using them as testdata.
func ReadTestdataBytes(r *rand.Rand, arr []byte) {
	_, _ = r.Read(arr)
	for i := range arr {
		arr[i] = arr[i] & 0x7F // mask out non-ascii
		if arr[i] < ' ' {      // Nudge the control chars up, into the letters.
			arr[i] += 'A'
		}
	}
}

// SeedForTests seeds the random number generator and prints the seed
// value used. This value can be specified via an environment variable
// KWBASE_RANDOM_SEED=x to reuse the same value later. This function should
// be called from TestMain; individual tests should not touch the seed
// of the global random number generator.
func SeedForTests() {
	seed := envutil.EnvOrDefaultInt64("KWBASE_RANDOM_SEED", NewPseudoSeed())
	rand.Seed(seed)
	log.Printf("Random seed: %v", seed)
}
