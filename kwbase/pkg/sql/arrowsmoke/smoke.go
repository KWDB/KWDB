package arrowsmoke

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/compute"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// Smoke validates that arrow-go/v17 (arrow/array, arrow/memory, arrow/compute)
// compiles under this GOPATH+vendor build and the packages we plan to use are
// reachable. It is a throwaway probe and will be removed.
func Smoke() {
	pool := memory.NewGoAllocator()
	_ = pool
	_ = arrow.PrimitiveTypes.Int64

	b := array.NewInt64Builder(pool)
	b.Append(1)
	b.Append(2)
	arr := b.NewArray()
	_ = arr

	// Reference compute so the whole package (kernels/registry) is compiled.
	_ = compute.CallFunction
}
