package xform

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/norm"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/testutils/testcat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestOrderedTSScanBuilder(t *testing.T) {
	defer leaktest.AfterTest(t)()

	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	catalog := testcat.New()

	var f norm.Factory
	f.Init(&evalCtx, catalog)

	mem := f.Memo()

	var e explorer
	e.evalCtx = &evalCtx
	e.f = &f
	e.mem = mem
	e.funcs.Init(&e)

	// Create dummy grp
	expr := f.ConstructZeroValues()
	grp := mem.AddValuesToGroup(&memo.ValuesExpr{}, expr)

	// Test case 1: opt.OrderedScan -> opt.SortAfterScan
	private1 := memo.TSScanPrivate{}
	private1.Flags.ExploreOrderedScan = true
	private1.Flags.OrderedScanType = opt.OrderedScan

	var b1 orderedTSScanBuilder
	b1.init(&e.funcs, private1)
	require.Equal(t, &e.funcs, b1.c)
	require.Equal(t, &f, b1.f)
	require.Equal(t, mem, b1.mem)

	b1.build(grp)

	// In build, it adds TSScanExpr to the group and mutates its own copy of flags
	// But it does not mutate the passed private1, rather it mutates b1.scanPrivate
	require.False(t, b1.scanPrivate.Flags.ExploreOrderedScan)
	require.Equal(t, opt.SortAfterScan, b1.scanPrivate.Flags.OrderedScanType)

	// Test case 2: opt.SortAfterScan -> opt.OrderedScan
	private2 := memo.TSScanPrivate{}
	private2.Flags.ExploreOrderedScan = true
	private2.Flags.OrderedScanType = opt.SortAfterScan

	var b2 orderedTSScanBuilder
	b2.init(&e.funcs, private2)
	b2.build(grp)

	require.False(t, b2.scanPrivate.Flags.ExploreOrderedScan)
	require.Equal(t, opt.OrderedScan, b2.scanPrivate.Flags.OrderedScanType)
}
