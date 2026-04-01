package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDiscardFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *Discard
		expected string
	}{
		{
			name:     "discard all",
			node:     &Discard{Mode: DiscardModeAll},
			expected: `DISCARD ALL`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
			require.Equal(t, tc.expected, tc.node.String())
		})
	}
}
