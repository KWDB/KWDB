// Copyright 2019 The Cockroach Authors.
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

package colserde

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"unsafe"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/cockroachdb/errors"
)

// ArrowBatchConverter converts batches to arrow column data
// ([]*array.Data) and back again.
type ArrowBatchConverter struct {
	typs []coltypes.T

	// builders are the set of builders that need to be kept around in order to
	// construct arrow representations of certain types when they cannot be simply
	// cast from our current data representation.
	builders struct {
		// boolBuilder builds arrow bool columns as a bitmap from a bool slice.
		boolBuilder *array.BooleanBuilder
		// binaryBuilder builds arrow []byte columns as one []byte slice with
		// accompanying offsets from a [][]byte slice.
		binaryBuilder *array.BinaryBuilder
	}

	scratch struct {
		// arrowData is used as scratch space returned as the corresponding
		// conversion result.
		arrowData []*array.Data
		// buffers is scratch space for exactly two buffers per element in
		// arrowData.
		buffers [][]*memory.Buffer
	}
}

// NewArrowBatchConverter converts coldata.Batches to []*array.Data and back
// again according to the schema specified by typs. Converting data that does
// not conform to typs results in undefined behavior.
func NewArrowBatchConverter(typs []coltypes.T) (*ArrowBatchConverter, error) {
	for _, t := range typs {
		if _, supported := supportedTypes[t]; !supported {
			return nil, errors.Errorf("arrowbatchconverter unsupported type %v", t.String())
		}
	}
	c := &ArrowBatchConverter{typs: typs}
	c.builders.boolBuilder = array.NewBooleanBuilder(memory.DefaultAllocator)
	c.builders.binaryBuilder = array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	c.scratch.arrowData = make([]*array.Data, len(typs))
	c.scratch.buffers = make([][]*memory.Buffer, len(typs))
	for i := range c.scratch.buffers {
		// Most types need only two buffers: one for the nulls, and one for the
		// values, but some types (i.e. Bytes) need an extra buffer for the
		// offsets.
		c.scratch.buffers[i] = make([]*memory.Buffer, 0, 3)
	}
	return c, nil
}

const (
	sizeOfInt16   = int(unsafe.Sizeof(int16(0)))
	sizeOfInt32   = int(unsafe.Sizeof(int32(0)))
	sizeOfInt64   = int(unsafe.Sizeof(int64(0)))
	sizeOfFloat64 = int(unsafe.Sizeof(float64(0)))
)

var supportedTypes = func() map[coltypes.T]struct{} {
	typs := make(map[coltypes.T]struct{})
	for _, t := range []coltypes.T{
		coltypes.Bool,
		coltypes.Bytes,
		coltypes.Decimal,
		coltypes.Float64,
		coltypes.Int16,
		coltypes.Int32,
		coltypes.Int64,
		coltypes.Timestamp,
		coltypes.Interval,
	} {
		typs[t] = struct{}{}
	}
	return typs
}()

// BatchToArrow converts the first batch.Length elements of the batch into an
// arrow []*array.Data. It is assumed that the batch is not larger than
// coldata.BatchSize(). The returned []*array.Data may only be used until the
// next call to BatchToArrow.
func (c *ArrowBatchConverter) BatchToArrow(batch coldata.Batch) ([]*array.Data, error) {
	if batch.Width() != len(c.typs) {
		return nil, errors.AssertionFailedf("mismatched batch width and schema length: %d != %d", batch.Width(), len(c.typs))
	}
	n := batch.Length()
	for i, typ := range c.typs {
		vec := batch.ColVec(i)

		var arrowBitmap []byte
		if vec.MaybeHasNulls() {
			n := vec.Nulls()
			// To conform to the Arrow spec, zero out all trailing null values.
			n.Truncate(batch.Length())
			arrowBitmap = n.NullBitmap()
		}

		if typ == coltypes.Bool || typ == coltypes.Decimal || typ == coltypes.Timestamp || typ == coltypes.Interval {
			var data *array.Data
			switch typ {
			case coltypes.Bool:
				c.builders.boolBuilder.AppendValues(vec.Bool()[:n], nil /* valid */)
				data = c.builders.boolBuilder.NewBooleanArray().Data()
			case coltypes.Decimal:
				decimals := vec.Decimal()[:n]
				for _, d := range decimals {
					marshaled, err := d.MarshalText()
					if err != nil {
						return nil, err
					}
					c.builders.binaryBuilder.Append(marshaled)
				}
				data = c.builders.binaryBuilder.NewBinaryArray().Data()
			case coltypes.Timestamp:
				timestamps := vec.Timestamp()[:n]
				for _, ts := range timestamps {
					marshaled, err := ts.MarshalBinary()
					if err != nil {
						return nil, err
					}
					c.builders.binaryBuilder.Append(marshaled)
				}
				data = c.builders.binaryBuilder.NewBinaryArray().Data()
			case coltypes.Interval:
				intervals := vec.Interval()[:n]
				// Appending to the binary builder will copy the bytes, so it's safe to
				// reuse a scratch bytes to encode the interval into.
				scratchIntervalBytes := make([]byte, sizeOfInt64*3)
				for _, interval := range intervals {
					nanos, months, days, err := interval.Encode()
					if err != nil {
						return nil, err
					}
					binary.LittleEndian.PutUint64(scratchIntervalBytes[0:sizeOfInt64], uint64(nanos))
					binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64:sizeOfInt64*2], uint64(months))
					binary.LittleEndian.PutUint64(scratchIntervalBytes[sizeOfInt64*2:sizeOfInt64*3], uint64(days))
					c.builders.binaryBuilder.Append(scratchIntervalBytes)
				}
				data = c.builders.binaryBuilder.NewBinaryArray().Data()
			default:
				panic(fmt.Sprintf("unexpected type %s", typ))
			}
			if arrowBitmap != nil {
				// Overwrite empty null bitmap with the true bitmap.
				data.Buffers()[0] = memory.NewBufferBytes(arrowBitmap)
			}
			c.scratch.arrowData[i] = data
			continue
		}

		var (
			values  []byte
			offsets []byte

			// dataHeader is the reflect.SliceHeader of the coldata.Vec's underlying
			// data slice that we are casting to bytes.
			dataHeader *reflect.SliceHeader
			// datumSize is the size of one datum that we are casting to a byte slice.
			datumSize int
		)

		switch typ {
		case coltypes.Bytes:
			var int32Offsets []int32
			values, int32Offsets = vec.Bytes().ToArrowSerializationFormat(n)
			// Cast int32Offsets to []byte.
			int32Header := (*reflect.SliceHeader)(unsafe.Pointer(&int32Offsets))
			offsetsHeader := (*reflect.SliceHeader)(unsafe.Pointer(&offsets))
			offsetsHeader.Data = int32Header.Data
			offsetsHeader.Len = int32Header.Len * sizeOfInt32
			offsetsHeader.Cap = int32Header.Cap * sizeOfInt32
		case coltypes.Int16:
			ints := vec.Int16()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt16
		case coltypes.Int32:
			ints := vec.Int32()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt32
		case coltypes.Int64:
			ints := vec.Int64()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&ints))
			datumSize = sizeOfInt64
		case coltypes.Float64:
			floats := vec.Float64()[:n]
			dataHeader = (*reflect.SliceHeader)(unsafe.Pointer(&floats))
			datumSize = sizeOfFloat64
		default:
			panic(fmt.Sprintf("unsupported type for conversion to arrow data %s", typ))
		}

		// Cast values if not set (mostly for non-byte types).
		if values == nil {
			valuesHeader := (*reflect.SliceHeader)(unsafe.Pointer(&values))
			valuesHeader.Data = dataHeader.Data
			valuesHeader.Len = dataHeader.Len * datumSize
			valuesHeader.Cap = dataHeader.Cap * datumSize
		}

		// Construct the underlying arrow buffers.
		// WARNING: The ordering of construction is critical.
		c.scratch.buffers[i] = c.scratch.buffers[i][:0]
		c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(arrowBitmap))
		if offsets != nil {
			c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(offsets))
		}
		c.scratch.buffers[i] = append(c.scratch.buffers[i], memory.NewBufferBytes(values))

		// Create the data from the buffers. It might be surprising that we don't
		// set a type or a null count, but these fields are not used in the way that
		// we're using array.Data. The part we care about are the buffers. Type
		// information is inferred from the ArrowBatchConverter schema, null count
		// is an optimization we can use when working with nulls, and childData is
		// only used for nested types like Lists, Structs, or Unions.
		c.scratch.arrowData[i] = array.NewData(
			nil /* dtype */, n, c.scratch.buffers[i], nil /* childData */, 0 /* nulls */, 0, /* offset */
		)
	}
	return c.scratch.arrowData, nil
}

// ArrowToBatch converts []*array.Data to a coldata.Batch. There must not be
// more than coldata.BatchSize() elements in data. It's safe to call ArrowToBatch
// concurrently.
//
// The passed in batch is overwritten, but after this method returns it stays
// valid as long as `data` stays valid. Callers can use this to control the
// lifetimes of the batches, saving allocations when they can be reused (i.e.
// reused by passing them back into this function).
//
// The passed in data is also mutated (we store nulls differently than arrow and
// the adjustment is done in place).
func (c *ArrowBatchConverter) ArrowToBatch(data []*array.Data, b coldata.Batch) error {
	if len(data) != len(c.typs) {
		return errors.Errorf("mismatched data and schema length: %d != %d", len(data), len(c.typs))
	}
	// Assume > 0 length data.
	n := data[0].Len()
	// ResetNoTruncation reuses the passed-in Batch when possible, saving
	// allocations but overwriting it. If the passed-in Batch is not suitable
	// for use, a new one is allocated.
	coldata.ResetNoTruncation(b.(*coldata.MemBatch), c.typs, n)
	b.SetLength(n)
	// Reset the batch, this resets the selection vector as well.
	b.ResetInternalBatch()

	for i, typ := range c.typs {
		vec := b.ColVec(i)
		d := data[i]

		var arr array.Interface
		switch typ {
		case coltypes.Bool:
			boolArr := array.NewBooleanData(d)
			vecArr := vec.Bool()
			for i := 0; i < boolArr.Len(); i++ {
				vecArr[i] = boolArr.Value(i)
			}
			arr = boolArr
		case coltypes.Bytes:
			bytesArr := array.NewBinaryData(d)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			coldata.BytesFromArrowSerializationFormat(vec.Bytes(), bytes, bytesArr.ValueOffsets())
			arr = bytesArr
		case coltypes.Decimal:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Decimal()
			for i := 0; i < len(offsets)-1; i++ {
				if err := vecArr[i].UnmarshalText(bytes[offsets[i]:offsets[i+1]]); err != nil {
					return err
				}
			}
			arr = bytesArr
		case coltypes.Timestamp:
			// TODO(yuzefovich): this serialization is quite inefficient - improve
			// it.
			bytesArr := array.NewBinaryData(d)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Timestamp()
			for i := 0; i < len(offsets)-1; i++ {
				if err := vecArr[i].UnmarshalBinary(bytes[offsets[i]:offsets[i+1]]); err != nil {
					return err
				}
			}
			arr = bytesArr
		case coltypes.Interval:
			// TODO(asubiotto): this serialization is quite inefficient compared to
			//  the direct casts below. Improve it.
			bytesArr := array.NewBinaryData(d)
			bytes := bytesArr.ValueBytes()
			if bytes == nil {
				// All bytes values are empty, so the representation is solely with the
				// offsets slice, so create an empty slice so that the conversion
				// corresponds.
				bytes = make([]byte, 0)
			}
			offsets := bytesArr.ValueOffsets()
			vecArr := vec.Interval()
			for i := 0; i < len(offsets)-1; i++ {
				intervalBytes := bytes[offsets[i]:offsets[i+1]]
				var err error
				vecArr[i], err = duration.Decode(
					int64(binary.LittleEndian.Uint64(intervalBytes[0:sizeOfInt64])),
					int64(binary.LittleEndian.Uint64(intervalBytes[sizeOfInt64:sizeOfInt64*2])),
					int64(binary.LittleEndian.Uint64(intervalBytes[sizeOfInt64*2:sizeOfInt64*3])),
				)
				if err != nil {
					return err
				}
			}
			arr = bytesArr
		default:
			var col interface{}
			switch typ {
			case coltypes.Int16:
				intArr := array.NewInt16Data(d)
				col = intArr.Int16Values()
				arr = intArr
			case coltypes.Int32:
				intArr := array.NewInt32Data(d)
				col = intArr.Int32Values()
				arr = intArr
			case coltypes.Int64:
				intArr := array.NewInt64Data(d)
				col = intArr.Int64Values()
				arr = intArr
			case coltypes.Float64:
				floatArr := array.NewFloat64Data(d)
				col = floatArr.Float64Values()
				arr = floatArr
			default:
				panic(
					fmt.Sprintf("unsupported type for conversion to column batch %s", d.DataType().Name()),
				)
			}
			vec.SetCol(col)
		}
		arrowBitmap := arr.NullBitmapBytes()
		if len(arrowBitmap) != 0 {
			vec.Nulls().SetNullBitmap(arrowBitmap, n)
		}
	}
	return nil
}
