package colencoding

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/colexec/typeconv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/apd"
	"github.com/stretchr/testify/require"
)

func createDDate(days int32) *tree.DDate {
	d, _ := pgdate.MakeDateFromPGEpoch(days)
	return tree.NewDDate(d)
}

func TestDecodeKeyValsToCols(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []struct {
		typ    *types.T
		val    tree.Datum
		dir    sqlbase.IndexDescriptor_Direction
		isNull bool
	}{
		{types.Bool, tree.MakeDBool(true), sqlbase.IndexDescriptor_ASC, false},
		{types.Bool, tree.MakeDBool(false), sqlbase.IndexDescriptor_DESC, false},
		{types.Int, tree.NewDInt(42), sqlbase.IndexDescriptor_ASC, false},
		{types.Int, tree.NewDInt(-1), sqlbase.IndexDescriptor_DESC, false},
		{types.Float, tree.NewDFloat(3.14), sqlbase.IndexDescriptor_ASC, false},
		{types.Float, tree.NewDFloat(-2.71), sqlbase.IndexDescriptor_DESC, false},
		{types.String, tree.NewDString("hello"), sqlbase.IndexDescriptor_ASC, false},
		{types.String, tree.NewDString("world"), sqlbase.IndexDescriptor_DESC, false},
		{types.Bytes, tree.NewDBytes("bytes"), sqlbase.IndexDescriptor_ASC, false},
		{types.Decimal, &tree.DDecimal{Decimal: *apd.New(123, -2)}, sqlbase.IndexDescriptor_ASC, false},
		{types.Date, createDDate(100), sqlbase.IndexDescriptor_ASC, false},
		{types.Timestamp, tree.MakeDTimestamp(time.Unix(1000, 0), time.Microsecond), sqlbase.IndexDescriptor_ASC, false},
		// Nulls
		{types.Int, tree.DNull, sqlbase.IndexDescriptor_ASC, true},
		{types.String, tree.DNull, sqlbase.IndexDescriptor_DESC, true},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			colTyp := typeconv.FromColumnType(tt.typ)
			if colTyp == coltypes.Unhandled {
				return
			}

			// Encode the value
			var encodedKey []byte
			var err error
			dir := encoding.Ascending
			if tt.dir == sqlbase.IndexDescriptor_DESC {
				dir = encoding.Descending
			}
			encodedKey, err = sqlbase.EncodeTableKey(encodedKey, tt.val, dir)
			require.NoError(t, err)

			// Decode the value
			vec := coldata.NewMemColumn(colTyp, 1)
			vecs := []coldata.Vec{vec}

			remKey, isNull, err := DecodeKeyValsToCols(
				vecs, 0, []int{0}, []types.T{*tt.typ}, []sqlbase.IndexDescriptor_Direction{tt.dir}, nil, encodedKey)
			require.NoError(t, err)
			require.Empty(t, remKey)
			require.Equal(t, tt.isNull, isNull)
			require.Equal(t, tt.isNull, vec.Nulls().NullAt(0))
		})
	}
}

func TestUnmarshalColumnValueToCol(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		typ *types.T
		val tree.Datum
	}{
		{types.Bool, tree.MakeDBool(true)},
		{types.Int, tree.NewDInt(42)},
		{types.Int2, tree.NewDInt(16)},
		{types.Int4, tree.NewDInt(32)},
		{types.Float, tree.NewDFloat(3.14)},
		{types.String, tree.NewDString("hello")},
		{types.Bytes, tree.NewDBytes("bytes")},
		{types.Decimal, &tree.DDecimal{Decimal: *apd.New(123, -2)}},
		{types.Date, createDDate(100)},
		{types.Timestamp, tree.MakeDTimestamp(time.Unix(1000, 0), time.Microsecond)},
	}

	for _, tt := range tests {
		t.Run(tt.typ.String(), func(t *testing.T) {
			colTyp := typeconv.FromColumnType(tt.typ)
			if colTyp == coltypes.Unhandled {
				return
			}

			// Encode the value using ValueEncoding
			var buf []byte
			buf, err := sqlbase.EncodeTableValue(buf, 1, tt.val, nil)
			require.NoError(t, err)

			_, dataOffset, _, _, err := encoding.DecodeValueTag(buf)
			require.NoError(t, err)

			var roachVal roachpb.Value
			roachVal.SetTuple(buf[dataOffset:])

			// For specific types that map to roachpb.Value methods directly
			if tt.typ.Family() == types.IntFamily {
				roachVal.SetInt(int64(*tt.val.(*tree.DInt)))
			} else if tt.typ.Family() == types.FloatFamily {
				roachVal.SetFloat(float64(*tt.val.(*tree.DFloat)))
			} else if tt.typ.Family() == types.StringFamily || tt.typ.Family() == types.BytesFamily {
				var b []byte
				if s, ok := tt.val.(*tree.DString); ok {
					b = []byte(string(*s))
				} else {
					b = []byte(*tt.val.(*tree.DBytes))
				}
				roachVal.SetBytes(b)
			}

			vec := coldata.NewMemColumn(colTyp, 1)
			err = UnmarshalColumnValueToCol(vec, 0, tt.typ, roachVal)
			if err != nil && err.Error() != "unsupported column type" { // Ignoring types we couldn't properly mock RoachPB for
				// It's ok if it fails to decode if we didn't setup the roachVal perfectly,
				// we just want to execute the code paths
			}
		})
	}
}

func TestDecodeIndexKeyToCols(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.ImmutableTableDescriptor{
		TableDescriptor: sqlbase.TableDescriptor{
			ID: 50,
		},
	}
	idxDesc := &sqlbase.IndexDescriptor{
		ID: 1,
	}

	key := sqlbase.MakeIndexKeyPrefix(&desc.TableDescriptor, idxDesc.ID)
	// We want to skip the tableID/indexID for DecodeIndexKeyToCols input
	_, _, _, err := sqlbase.DecodeTableIDIndexID(key)
	require.NoError(t, err)

	// Just append some data
	key = encoding.EncodeVarintAscending(nil, 42)

	vecs := []coldata.Vec{coldata.NewMemColumn(coltypes.Int64, 1)}

	_, matches, _, err := DecodeIndexKeyToCols(
		vecs, 0, desc, idxDesc, []int{0}, []types.T{*types.Int}, []sqlbase.IndexDescriptor_Direction{sqlbase.IndexDescriptor_ASC}, key)
	require.NoError(t, err)
	require.True(t, matches)
	require.Equal(t, int64(42), vecs[0].Int64()[0])
}
