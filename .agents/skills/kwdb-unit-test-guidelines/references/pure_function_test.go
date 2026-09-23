// Package keys demonstrates a pure function unit test — no server dependency.
// This is the reference example for Template A.
//
// Patterns shown:
//   - Table-driven tests with give/want fields
//   - t.Run subtests
//   - leaktest goroutine detection
//   - testutils.IsError for error regex matching
//   - Direct t.Error / t.Fatal (no testify needed for simple cases)
//   - Benchmark with sink variable
package keys

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestKeyAddress demonstrates a table-driven test with give/want fields.
// Each case runs as a subtest via t.Run.
func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		give roachpb.Key
		want roachpb.RKey
	}{
		{give: roachpb.Key{}, want: roachpb.RKeyMin},
		{give: roachpb.Key("123"), want: roachpb.RKey("123")},
		{give: roachpb.Key("foo"), want: roachpb.RKey("foo")},
		{give: nil, want: nil},
	}

	for i, tc := range testCases {
		t.Run(tc.give.String(), func(t *testing.T) {
			got, err := Addr(tc.give)
			if err != nil {
				t.Errorf("%d: unexpected error: %v", i, err)
			}
			if !got.Equal(tc.want) {
				t.Errorf("%d: Addr(%q) = %q, want %q", i, tc.give, got, tc.want)
			}
		})
	}
}

// TestKeyAddressError demonstrates error matching with testutils.IsError.
// The regex pattern must be specific enough to identify the error cause
// but flexible enough to survive message rewording.
func TestKeyAddressError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		give    roachpb.Key
		wantErr string // regex pattern
	}{
		{give: StoreIdentKey(), wantErr: "store-local key .* is not addressable"},
		{give: makeKey(localPrefix, roachpb.Key("bad")), wantErr: "local key .* malformed"},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			_, err := Addr(tc.give)
			if !testutils.IsError(err, tc.wantErr) {
				t.Errorf("expected error matching %q, got %v", tc.wantErr, err)
			}
		})
	}
}

// Benchmark example: use a package-level sink to prevent the compiler
// from optimizing away the function call.
var sink roachpb.RKey

func BenchmarkAddr(b *testing.B) {
	key := roachpb.Key("benchmark-key")
	for i := 0; i < b.N; i++ {
		sink, _ = Addr(key)
	}
}
