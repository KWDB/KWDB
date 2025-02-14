// Copyright 2020 The Cockroach Authors.
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

package tpch

import (
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
	"gitee.com/kwbasedb/kwbase/pkg/util/bufalloc"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil/pgdate"
	"golang.org/x/exp/rand"
)

var regionNames = [...]string{`AFRICA`, `AMERICA`, `ASIA`, `EUROPE`, `MIDDLE EAST`}
var nations = [...]struct {
	name      string
	regionKey int
}{
	{name: `ALGERIA`, regionKey: 0},
	{name: `ARGENTINA`, regionKey: 1},
	{name: `BRAZIL`, regionKey: 1},
	{name: `CANADA`, regionKey: 1},
	{name: `EGYPT`, regionKey: 4},
	{name: `ETHIOPIA`, regionKey: 0},
	{name: `FRANCE`, regionKey: 3},
	{name: `GERMANY`, regionKey: 3},
	{name: `INDIA`, regionKey: 2},
	{name: `INDONESIA`, regionKey: 2},
	{name: `IRAN`, regionKey: 4},
	{name: `IRAQ`, regionKey: 4},
	{name: `JAPAN`, regionKey: 2},
	{name: `JORDAN`, regionKey: 4},
	{name: `KENYA`, regionKey: 0},
	{name: `MOROCCO`, regionKey: 0},
	{name: `MOZAMBIQUE`, regionKey: 0},
	{name: `PERU`, regionKey: 1},
	{name: `CHINA`, regionKey: 2},
	{name: `ROMANIA`, regionKey: 3},
	{name: `SAUDI ARABIA`, regionKey: 4},
	{name: `VIETNAM`, regionKey: 2},
	{name: `RUSSIA`, regionKey: 3},
	{name: `UNITED KINGDOM`, regionKey: 3},
	{name: `UNITED STATES`, regionKey: 1},
}

var regionColTypes = []coltypes.T{
	coltypes.Int16,
	coltypes.Bytes,
	coltypes.Bytes,
}

func (w *tpch) tpchRegionInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	regionKey := batchIdx
	cb.Reset(regionColTypes, 1)
	cb.ColVec(0).Int16()[0] = int16(regionKey)                       // r_regionkey
	cb.ColVec(1).Bytes().Set(0, []byte(regionNames[regionKey]))      // r_name
	cb.ColVec(2).Bytes().Set(0, w.textPool.randString(rng, 31, 115)) // r_comment
}

var nationColTypes = []coltypes.T{
	coltypes.Int16,
	coltypes.Bytes,
	coltypes.Int16,
	coltypes.Bytes,
}

func (w *tpch) tpchNationInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	nationKey := batchIdx
	nation := nations[nationKey]
	cb.Reset(nationColTypes, 1)
	cb.ColVec(0).Int16()[0] = int16(nationKey)                       // n_nationkey
	cb.ColVec(1).Bytes().Set(0, []byte(nation.name))                 // n_name
	cb.ColVec(2).Int16()[0] = int16(nation.regionKey)                // n_regionkey
	cb.ColVec(3).Bytes().Set(0, w.textPool.randString(rng, 31, 115)) // r_comment
}

var supplierColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int16,
	coltypes.Bytes,
	coltypes.Float64,
	coltypes.Bytes,
}

func (w *tpch) tpchSupplierInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	suppKey := int64(batchIdx) + 1
	nationKey := int16(randInt(rng, 0, 24))
	cb.Reset(supplierColTypes, 1)
	cb.ColVec(0).Int64()[0] = suppKey                                        // s_suppkey
	cb.ColVec(1).Bytes().Set(0, supplierName(a, suppKey))                    // s_name
	cb.ColVec(2).Bytes().Set(0, randVString(rng, a, 10, 40))                 // s_address
	cb.ColVec(3).Int16()[0] = nationKey                                      // s_nationkey
	cb.ColVec(4).Bytes().Set(0, randPhone(rng, a, nationKey))                // s_phone
	cb.ColVec(5).Float64()[0] = float64(randFloat(rng, -99999, 999999, 100)) // s_acctbal
	// TODO(jordan): this needs to sometimes have Customer Complaints or Customer Recommends. see 4.2.3.
	cb.ColVec(6).Bytes().Set(0, w.textPool.randString(rng, 25, 100)) // s_comment
}

var partColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int16,
	coltypes.Bytes,
	coltypes.Float64,
	coltypes.Bytes,
}

func makeRetailPriceFromPartKey(partKey int) float32 {
	return float32(90000+((partKey/10)%20001)+100*(partKey%1000)) / 100
}

func (w *tpch) tpchPartInitialRowBatch(batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	partKey := batchIdx + 1
	cb.Reset(partColTypes, 1)

	// P_PARTKEY unique within [SF * 200,000].
	cb.ColVec(0).Int64()[0] = int64(partKey)
	// P_NAME generated by concatenating five unique randomly selected part name strings.
	cb.ColVec(1).Bytes().Set(0, randPartName(rng, l.namePerm, a))
	m, mfgr := randMfgr(rng, a)
	// P_MFGR text appended with digit ["Manufacturer#",M], where M = random value [1,5].
	cb.ColVec(2).Bytes().Set(0, mfgr) //
	// P_BRAND text appended with digits ["Brand#",MN], where N = random value [1,5] and M is defined
	// while generating P_MFGR.
	cb.ColVec(3).Bytes().Set(0, randBrand(rng, a, m))
	// P_TYPE random string [Types].
	cb.ColVec(4).Bytes().Set(0, randType(rng, a))
	// P_SIZE random value [1 .. 50].
	cb.ColVec(5).Int16()[0] = int16(randInt(rng, 1, 50))
	// P_CONTAINER random string [Containers].
	cb.ColVec(6).Bytes().Set(0, randContainer(rng, a))
	// P_RETAILPRICE = (90000 + ((P_PARTKEY/10) modulo 20001 ) + 100 * (P_PARTKEY modulo 1000))/100.
	cb.ColVec(7).Float64()[0] = float64(makeRetailPriceFromPartKey(partKey))
	// P_COMMENT text string [5,22].
	cb.ColVec(8).Bytes().Set(0, w.textPool.randString(rng, 5, 22))
}

var partSuppColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Int16,
	coltypes.Float64,
	coltypes.Bytes,
}

func (w *tpch) tpchPartSuppInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	partKey := batchIdx + 1
	cb.Reset(partSuppColTypes, numPartSuppPerPart)

	// P_PARTKEY unique within [SF * 200,000].
	partKeyCol := cb.ColVec(0).Int64()
	suppKeyCol := cb.ColVec(1).Int64()
	availQtyCol := cb.ColVec(2).Int16()
	supplyCostCol := cb.ColVec(3).Float64()
	commentCol := cb.ColVec(4).Bytes()

	// For each row in the PART table, four rows in PartSupp table.
	for i := 0; i < numPartSuppPerPart; i++ {
		// PS_PARTKEY = P_PARTKEY.
		partKeyCol[i] = int64(partKey)
		// PS_SUPPKEY = (ps_partkey + (i * (( S/4 ) + (int)(ps_partkey-1 )/S))))
		// modulo S + 1 where i is the ith supplier within [0 .. 3] and S = SF *
		// 10,000.
		s := w.scaleFactor * 10000
		suppKeyCol[i] = int64((partKey+(i*((s/numPartSuppPerPart)+(partKey-1)/s)))%s + 1)
		// PS_AVAILQTY random value [1 .. 9,999].
		availQtyCol[i] = int16(randInt(rng, 1, 9999))
		// PS_SUPPLYCOST random value [1.00 .. 1,000.00].
		supplyCostCol[i] = float64(randFloat(rng, 1, 1000, 100))
		// PS_COMMENT text string [49,198].
		commentCol.Set(i, w.textPool.randString(rng, 49, 198))
	}
}

var customerColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int16,
	coltypes.Bytes,
	coltypes.Float64,
	coltypes.Bytes,
	coltypes.Bytes,
}

func (w *tpch) tpchCustomerInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng
	rng.Seed(w.seed + uint64(batchIdx))

	custKey := int64(batchIdx) + 1
	cb.Reset(customerColTypes, 1)

	// C_CUSTKEY unique within [SF * 150,000].
	cb.ColVec(0).Int64()[0] = custKey
	// C_NAME text appended with minimum 9 digits with leading zeros ["Customer#", C_CUSTKEY].
	cb.ColVec(1).Bytes().Set(0, customerName(a, custKey))
	// C_ADDRESS random v-string [10,40].
	cb.ColVec(2).Bytes().Set(0, randVString(rng, a, 10, 40))
	nationKey := int16(randInt(rng, 0, 24))
	// C_NATIONKEY random value [0 .. 24].
	cb.ColVec(3).Int16()[0] = nationKey
	// C_PHONE generated according to Clause 4.2.2.9.
	cb.ColVec(4).Bytes().Set(0, randPhone(rng, a, nationKey))
	// C_ACCTBAL random value [-999.99 .. 9,999.99].
	cb.ColVec(5).Float64()[0] = float64(randFloat(rng, -99999, 999999, 100))
	// C_MKTSEGMENT random string [Segments].
	cb.ColVec(6).Bytes().Set(0, randSegment(rng))
	// C_COMMENT text string [29,116].
	cb.ColVec(7).Bytes().Set(0, w.textPool.randString(rng, 29, 116))
}

const sparseBits = 2
const sparseKeep = 3

var (
	startDate   = time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
	currentDate = time.Date(1995, 6, 17, 0, 0, 0, 0, time.UTC)
	endDate     = time.Date(1998, 12, 31, 0, 0, 0, 0, time.UTC)

	startDateDays   int64
	currentDateDays int64
	endDateDays     int64
)

func init() {
	var d pgdate.Date
	var err error
	d, err = pgdate.MakeDateFromTime(startDate)
	if err != nil {
		panic(err)
	}
	startDateDays = d.UnixEpochDaysWithOrig()
	d, err = pgdate.MakeDateFromTime(currentDate)
	if err != nil {
		panic(err)
	}
	currentDateDays = d.UnixEpochDaysWithOrig()
	d, err = pgdate.MakeDateFromTime(endDate)
	if err != nil {
		panic(err)
	}
	endDateDays = d.UnixEpochDaysWithOrig()
}

func getOrderKey(orderIdx int) int {
	// This method is taken from dbgen's build.c: mk_sparse. Given an input i, it
	// returns the ith order id in the tpch database, by using only the first 8
	// integers from every 32 integers.

	// Our input is 0-indexed; convert to 1-index.
	i := orderIdx + 1

	lowBits := i & ((1 << sparseKeep) - 1)
	i = i >> sparseKeep
	i = i << sparseBits
	i = i << sparseKeep
	i += lowBits
	return i
}

type orderSharedRandomData struct {
	nOrders    int
	orderDate  int
	partKeys   []int
	shipDates  []int64
	quantities []float32
	discount   []float32
	tax        []float32

	allO bool
	allF bool
}

var ordersColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Float64,
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int16,
	coltypes.Bytes,
}

func populateSharedData(rng *rand.Rand, seed uint64, sf int, data *orderSharedRandomData) {
	// Seed the rng here to force orders and lineitems to get the same results.
	rng.Seed(seed)

	data.nOrders = randInt(rng, 1, 7)
	data.orderDate = randInt(rng, int(startDateDays), int(endDateDays-151))
	data.partKeys = data.partKeys[:data.nOrders]
	data.shipDates = data.shipDates[:data.nOrders]
	data.quantities = data.quantities[:data.nOrders]
	data.discount = data.discount[:data.nOrders]
	data.tax = data.tax[:data.nOrders]
	// These will be invalidated in the loop.
	data.allF = true
	data.allO = true

	for i := 0; i < data.nOrders; i++ {
		shipDate := int64(data.orderDate + randInt(rng, 1, 121))
		data.shipDates[i] = shipDate
		if shipDate > currentDateDays {
			data.allF = false
		} else {
			data.allO = false
		}
		data.partKeys[i] = randInt(rng, 1, sf*numPartPerSF)
		data.quantities[i] = float32(randInt(rng, 1, 50))
		data.discount[i] = randFloat(rng, 0, 10, 100)
		data.tax[i] = randFloat(rng, 0, 8, 100)
	}
}

func (w *tpch) tpchOrdersInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng

	cb.Reset(ordersColTypes, numOrderPerCustomer)

	orderKeyCol := cb.ColVec(0).Int64()
	custKeyCol := cb.ColVec(1).Int64()
	orderStatusCol := cb.ColVec(2).Bytes()
	totalPriceCol := cb.ColVec(3).Float64()
	orderDateCol := cb.ColVec(4).Int64()
	orderPriorityCol := cb.ColVec(5).Bytes()
	clerkCol := cb.ColVec(6).Bytes()
	shipPriorityCol := cb.ColVec(7).Int16()
	commentCol := cb.ColVec(8).Bytes()

	orderStartIdx := numOrderPerCustomer * batchIdx
	for i := 0; i < numOrderPerCustomer; i++ {
		populateSharedData(rng, w.seed+uint64(orderStartIdx+i), w.scaleFactor, l.orderData)

		orderKeyCol[i] = int64(getOrderKey(orderStartIdx + i))
		// O_CUSTKEY = random value c [1 .. (SF * 150,000)], s.t. c % 3 != 0.
		numCust := w.scaleFactor * numCustomerPerSF
		custKeyCol[i] = int64((randInt(rng, 1, w.scaleFactor*(numCustomerPerSF/3))*3 + rng.Intn(2) + 1) % numCust)
		// O_ORDERSTATUS = F if all lineitems.LINESTATUS = F; O if all O; P
		// otherwise.
		if l.orderData.allF {
			orderStatusCol.Set(i, []byte("F"))
		} else if l.orderData.allO {
			orderStatusCol.Set(i, []byte("O"))
		} else {
			orderStatusCol.Set(i, []byte("P"))
		}
		totalPrice := float32(0)
		for j := 0; j < l.orderData.nOrders; j++ {
			ep := l.orderData.quantities[j] * makeRetailPriceFromPartKey(l.orderData.partKeys[j])
			// Use an extra float32 conversion to disable "fused multiply and add" (FMA) to force
			// identical behavior on all platforms. See https://golang.org/ref/spec#Floating_point_operators
			totalPrice += float32(ep * (1 + l.orderData.tax[j]) * (1 - l.orderData.discount[j])) // nolint:unconvert
		}
		// O_TOTALPRICE computed as:
		// sum (L_EXTENDEDPRICE * (1+L_TAX) * (1-L_DISCOUNT)) for all LINEITEM of
		// this order.
		totalPriceCol[i] = float64(totalPrice)
		// O_ORDERDATE uniformly distributed between STARTDATE and
		// (ENDDATE - 151 days).
		orderDateCol[i] = int64(l.orderData.orderDate)
		// O_ORDERPRIORITY random string [Priorities].
		orderPriorityCol.Set(i, randPriority(rng))
		// O_CLERK text appended with minimum 9 digits with leading zeros
		// ["Clerk#", C] where C = random value [000000001 .. (SF * 1000)].
		clerkCol.Set(i, randClerk(rng, a, w.scaleFactor))
		// O_SHIPPRIORITY set to 0.
		shipPriorityCol[i] = 0
		// O_COMMENT text string [19,78].
		commentCol.Set(i, w.textPool.randString(rng, 19, 78))
	}
}

var lineItemColTypes = []coltypes.T{
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Int16,
	coltypes.Float64,
	coltypes.Float64,
	coltypes.Float64,
	coltypes.Float64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Int64,
	coltypes.Bytes,
	coltypes.Bytes,
	coltypes.Bytes,
}

func (w *tpch) tpchLineItemInitialRowBatch(
	batchIdx int, cb coldata.Batch, a *bufalloc.ByteAllocator,
) {
	l := w.localsPool.Get().(*generateLocals)
	defer w.localsPool.Put(l)
	rng := l.rng

	cb.Reset(lineItemColTypes, numOrderPerCustomer*7)

	orderKeyCol := cb.ColVec(0).Int64()
	partKeyCol := cb.ColVec(1).Int64()
	suppKeyCol := cb.ColVec(2).Int64()
	lineNumberCol := cb.ColVec(3).Int16()
	quantityCol := cb.ColVec(4).Float64()
	extendedPriceCol := cb.ColVec(5).Float64()
	discountCol := cb.ColVec(6).Float64()
	taxCol := cb.ColVec(7).Float64()
	returnFlagCol := cb.ColVec(8).Bytes()
	lineStatusCol := cb.ColVec(9).Bytes()
	shipDateCol := cb.ColVec(10).Int64()
	commitDateCol := cb.ColVec(11).Int64()
	receiptDateCol := cb.ColVec(12).Int64()
	shipInstructCol := cb.ColVec(13).Bytes()
	shipModeCol := cb.ColVec(14).Bytes()
	commentCol := cb.ColVec(15).Bytes()

	orderStartIdx := numOrderPerCustomer * batchIdx
	s := w.scaleFactor * 10000
	offset := 0
	for i := 0; i < numOrderPerCustomer; i++ {
		populateSharedData(rng, w.seed+uint64(orderStartIdx+i), w.scaleFactor, l.orderData)

		orderKey := int64(getOrderKey(orderStartIdx + i))
		for j := 0; j < l.orderData.nOrders; j++ {
			idx := offset + j
			// L_ORDERKEY = O_ORDERKEY.
			orderKeyCol[idx] = orderKey
			partKey := l.orderData.partKeys[j]
			// L_PARTKEY random value [1 .. (SF * 200,000)].
			partKeyCol[idx] = int64(partKey)
			// L_SUPPKEY = (L_PARTKEY + (i * (( S/4 ) + (int)(L_partkey-1 )/S))))
			// modulo S + 1 where i is the corresponding supplier within [0 .. 3] and
			// S = SF * 10,000.
			suppKey := (partKey+(randInt(rng, 0, 3)*((s/4)+(partKey-1)/s)))%s + 1
			suppKeyCol[idx] = int64(suppKey)
			// L_LINENUMBER unique within [7].
			lineNumberCol[idx] = int16(j)
			// L_QUANTITY random value [1 .. 50].
			quantityCol[idx] = float64(l.orderData.quantities[j])
			// L_EXTENDEDPRICE = L_QUANTITY * P_RETAILPRICE where P_RETAILPRICE is
			// from the part with P_PARTKEY = L_PARTKEY.
			extendedPriceCol[idx] = float64(l.orderData.quantities[j] * makeRetailPriceFromPartKey(partKey))
			// L_DISCOUNT random value [0.00 .. 0.10].
			discountCol[idx] = float64(l.orderData.discount[j])
			// L_TAX random value [0.00 .. 0.08].
			taxCol[idx] = float64(l.orderData.tax[j])
			// L_SHIPDATE = O_ORDERDATE + random value [1 .. 121].
			shipDate := l.orderData.shipDates[j]
			shipDateCol[idx] = shipDate
			// L_RECEIPTDATE = L_SHIPDATE + random value [1 .. 30].
			receiptDate := shipDate + int64(randInt(rng, 1, 30))
			receiptDateCol[idx] = receiptDate
			// L_COMMITDATE = O_ORDERDATE + random value [30 .. 90].
			commitDateCol[idx] = int64(l.orderData.orderDate + randInt(rng, 30, 90))
			// L_RETURNFLAG set to a value selected as follows:
			// If L_RECEIPTDATE <= CURRENTDATE
			// then either "R" or "A" is selected at random
			// else "N" is selected.
			if receiptDate < currentDateDays {
				if rng.Intn(2) == 0 {
					returnFlagCol.Set(idx, []byte("R"))
				} else {
					returnFlagCol.Set(idx, []byte("A"))
				}
			} else {
				returnFlagCol.Set(idx, []byte("N"))
			}
			// L_LINESTATUS set the following value:
			// "O" if L_SHIPDATE > CURRENTDATE
			// "F" otherwise.
			if shipDate > currentDateDays {
				lineStatusCol.Set(idx, []byte("O"))
			} else {
				lineStatusCol.Set(idx, []byte("F"))
			}
			// L_SHIPINSTRUCT random string [Instructions].
			shipInstructCol.Set(idx, randInstruction(rng))
			// L_SHIPMODE random string [Modes].
			shipModeCol.Set(idx, randMode(rng))
			// L_COMMENT text string [10,43].
			commentCol.Set(idx, w.textPool.randString(rng, 10, 43))
		}
		offset += l.orderData.nOrders
	}
	cb.SetLength(offset)
}
