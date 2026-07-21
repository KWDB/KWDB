// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sqlutil

import (
	"bytes"
	"context"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ShowCreatePartitioning returns a PARTITION BY clause for the specified
// index, if applicable.
func ShowCreatePartitioning(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	buf *bytes.Buffer,
	indent int,
	colOffset int,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	indentStr := strings.Repeat("\t", indent)
	buf.WriteString(` PARTITION BY `)
	if len(partDesc.List) > 0 {
		if partDesc.IsHash {
			buf.WriteString(`HASH`)
		} else {
			buf.WriteString(`LIST`)
		}
	} else if len(partDesc.Range) > 0 {
		buf.WriteString(`RANGE`)
	} else {
		return errors.Errorf(`invalid partition descriptor: %v`, partDesc)
	}
	buf.WriteString(` (`)
	for i := 0; i < int(partDesc.NumColumns); i++ {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(idxDesc.ColumnNames[colOffset+i])
	}
	buf.WriteString(`) (`)
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i := range partDesc.List {
		part := &partDesc.List[i]
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		fmtCtx.FormatNameP(&part.Name)
		_, _ = fmtCtx.Buffer.WriteTo(buf)
		buf.WriteString(` VALUES IN (`)
		for j, values := range part.Values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := sqlbase.DecodePartitionTuple(
				a, tableDesc, idxDesc, partDesc, values, fakePrefixDatums)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}
		buf.WriteString(`)`)
		if err := ShowCreatePartitioning(
			a, tableDesc, idxDesc, &part.Subpartitioning, buf, indent+1,
			colOffset+int(partDesc.NumColumns),
		); err != nil {
			return err
		}
	}
	for i, part := range partDesc.Range {
		if i != 0 {
			buf.WriteString(`, `)
		}
		buf.WriteString("\n")
		buf.WriteString(indentStr)
		buf.WriteString("\tPARTITION ")
		buf.WriteString(part.Name)
		buf.WriteString(" VALUES FROM ")
		fromTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.FromInclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			a, tableDesc, idxDesc, partDesc, part.ToExclusive, fakePrefixDatums)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
	}
	buf.WriteString("\n")
	buf.WriteString(indentStr)
	buf.WriteString(")")
	return nil
}

// ShowCreateView returns a valid SQL representation of the CREATE VIEW
// statement used to create the given view. It is used in the implementation of
// the kwdb_internal.create_statements virtual table.
func ShowCreateView(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	if desc.IsMaterializedView {
		f.WriteString("MATERIALIZED ")
	}
	f.WriteString("VIEW ")
	f.FormatNode(tn)
	f.WriteString(" (")
	for i := range desc.Columns {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&desc.Columns[i].Name)
	}
	f.WriteString(") AS ")
	f.WriteString(desc.ViewQuery)
	return f.CloseAndGetString(), nil
}

// ShowCreateSequence returns a valid SQL representation of the
// CREATE SEQUENCE statement used to create the given sequence.
func ShowCreateSequence(
	ctx context.Context, tn *tree.Name, desc *sqlbase.TableDescriptor,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	if desc.Temporary {
		f.WriteString("TEMP ")
	}
	f.WriteString("SEQUENCE ")
	f.FormatNode(tn)
	opts := desc.SequenceOpts
	f.Printf(" MINVALUE %d", opts.MinValue)
	f.Printf(" MAXVALUE %d", opts.MaxValue)
	f.Printf(" INCREMENT %d", opts.Increment)
	f.Printf(" START %d", opts.Start)
	if opts.Virtual {
		f.Printf(" VIRTUAL")
	}
	return f.CloseAndGetString(), nil
}

// ShowCreateInstanceTable returns a valid SQL representation of the CREATE
// INSTANCE TABLE statement used to create the given table.
func ShowCreateInstanceTable(
	sTable tree.Name, cTable string, tagName []string, tagValue []string, typ []types.T,
) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	f.WriteString("TABLE ")
	f.WriteString(cTable)
	f.WriteString(" USING ")
	f.FormatNode(&sTable)
	f.WriteString(" (")
	for i := range tagName {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")
		f.WriteString(tagName[i])
	}
	f.WriteString(" )")
	f.WriteString(" TAGS")
	f.WriteString(" (")
	for i := range tagValue {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")

		if tagValue[i] != sqlbase.EmptyTagValue && typ[i].InternalType.Family == types.BytesFamily && sqlbase.NeedConvert(tagValue[i]) {
			f.WriteString(tree.NewDBytes(tree.DBytes(strings.Trim(tagValue[i], "'"))).String())
		} else {
			f.WriteString(tagValue[i])
		}
	}
	f.WriteString(" )")
	return f.CloseAndGetString()
}

// FormatQuoteNames quotes and adds commas between names.
func FormatQuoteNames(buf *bytes.Buffer, names ...string) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	for i := range names {
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&names[i])
	}
	buf.WriteString(f.CloseAndGetString())
}

// ScanMetaKVs returns the meta KVs for the ranges that touch the given span.
func ScanMetaKVs(ctx context.Context, txn *kv.Txn, span roachpb.Span) ([]kv.KeyValue, error) {
	metaStart := keys.RangeMetaKey(keys.MustAddr(span.Key).Next())
	metaEnd := keys.RangeMetaKey(keys.MustAddr(span.EndKey))

	kvs, err := txn.Scan(ctx, metaStart, metaEnd, 0)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 || !kvs[len(kvs)-1].Key.Equal(metaEnd.AsRawKey()) {
		// Normally we need to scan one more KV because the ranges are addressed by
		// the end key.
		extraKV, err := txn.Scan(ctx, metaEnd, keys.Meta2Prefix.PrefixEnd(), 1 /* one result */)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, extraKV[0])
	}
	return kvs, nil
}

// GetTimeFromTimeInput convert time in different unit to second
func GetTimeFromTimeInput(input tree.TimeInput) int64 {

	switch input.Unit {
	case "s", "second":
		return input.Value
	case "m", "minute":
		return input.Value * 60
	case "h", "hour":
		return input.Value * 60 * 60
	case "d", "day":
		return input.Value * 24 * 60 * 60
	case "w", "week":
		return input.Value * 24 * 60 * 60 * 7
	case "mon", "month":
		return input.Value * 24 * 60 * 60 * 30
	case "y", "year":
		return input.Value * 24 * 60 * 60 * 365
	default:
		return -1
	}
}

// minimumTypeUsageVersions defines the minimum version needed for a new
// data type.
var minimumTypeUsageVersions = map[types.Family]clusterversion.VersionKey{
	types.TimeTZFamily: clusterversion.VersionTimeTZType,
}

// IsTypeSupportedInVersion returns whether a given type is supported in the given version.
func IsTypeSupportedInVersion(v clusterversion.ClusterVersion, t *types.T) (bool, error) {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == types.ArrayFamily {
		t = t.ArrayContents()
	}

	switch t.Family() {
	case types.TimeFamily, types.TimestampFamily, types.TimestampTZFamily, types.TimeTZFamily:
		if t.Precision() != 6 && !v.IsActive(clusterversion.VersionTimePrecision) {
			return false, nil
		}
	case types.IntervalFamily:
		itm, err := t.IntervalTypeMetadata()
		if err != nil {
			return false, err
		}
		if (t.Precision() != 6 || itm.DurationField != types.IntervalDurationField{}) &&
			!v.IsActive(clusterversion.VersionTimePrecision) {
			return false, nil
		}
	}
	minVersion, ok := minimumTypeUsageVersions[t.Family()]
	if !ok {
		return true, nil
	}
	return v.IsActive(minVersion), nil
}

// TimeInputToString converts a time input value to its string representation
func TimeInputToString(input tree.TimeInput) string {
	return strconv.Itoa(int(input.Value)) + input.Unit
}
