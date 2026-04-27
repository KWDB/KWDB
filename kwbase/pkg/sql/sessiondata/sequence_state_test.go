// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sessiondata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewSequenceState(t *testing.T) {
	ss := NewSequenceState()
	assert.NotNil(t, ss)

	// Test initial state
	latestVals, lastSeq := ss.Export()
	assert.Empty(t, latestVals)
	assert.Equal(t, uint32(0), lastSeq)
}

func TestSequenceState_RecordValue(t *testing.T) {
	ss := NewSequenceState()

	// Record the first value
	ss.RecordValue(1, 100)

	latestVals, lastSeq := ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100}, latestVals)
	assert.Equal(t, uint32(1), lastSeq)

	// Record the second value
	ss.RecordValue(2, 200)

	latestVals, lastSeq = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100, 2: 200}, latestVals)
	assert.Equal(t, uint32(2), lastSeq)

	// Update existing sequence value
	ss.RecordValue(1, 101)

	latestVals, lastSeq = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 101, 2: 200}, latestVals)
	assert.Equal(t, uint32(1), lastSeq)
}

func TestSequenceState_SetLastSequenceIncremented(t *testing.T) {
	ss := NewSequenceState()

	ss.RecordValue(1, 100)
	ss.SetLastSequenceIncremented(2)

	_, lastSeq := ss.Export()
	assert.Equal(t, uint32(2), lastSeq)
}

func TestSequenceState_GetLastValue(t *testing.T) {
	ss := NewSequenceState()

	// Should return error when no value is recorded
	val, err := ss.GetLastValue()
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)

	// Should return correct value after recording
	ss.RecordValue(1, 100)
	val, err = ss.GetLastValue()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), val)

	// Should return the latest value after recording new one
	ss.RecordValue(2, 200)
	val, err = ss.GetLastValue()
	assert.NoError(t, err)
	assert.Equal(t, int64(200), val)

	// After updating lastSequenceIncremented, return corresponding sequence value
	ss.SetLastSequenceIncremented(1)
	val, err = ss.GetLastValue()
	assert.NoError(t, err)
	assert.Equal(t, int64(100), val)

	// Setting a non-existent sequence ID should return error
	ss.SetLastSequenceIncremented(999)
	val, err = ss.GetLastValue()
	assert.Error(t, err)
	assert.Equal(t, int64(0), val)
}

func TestSequenceState_GetLastValueByID(t *testing.T) {
	ss := NewSequenceState()

	// Query non-existent sequence
	val, ok := ss.GetLastValueByID(1)
	assert.False(t, ok)
	assert.Equal(t, int64(0), val)

	// Query after recording value
	ss.RecordValue(1, 100)
	val, ok = ss.GetLastValueByID(1)
	assert.True(t, ok)
	assert.Equal(t, int64(100), val)

	// Query another non-existent sequence
	val, ok = ss.GetLastValueByID(2)
	assert.False(t, ok)
	assert.Equal(t, int64(0), val)

	// Record multiple values and query
	ss.RecordValue(2, 200)
	ss.RecordValue(3, 300)

	val, ok = ss.GetLastValueByID(2)
	assert.True(t, ok)
	assert.Equal(t, int64(200), val)

	val, ok = ss.GetLastValueByID(3)
	assert.True(t, ok)
	assert.Equal(t, int64(300), val)
}

func TestSequenceState_Export(t *testing.T) {
	ss := NewSequenceState()

	// Initial state
	latestVals, lastSeq := ss.Export()
	assert.Empty(t, latestVals)
	assert.Equal(t, uint32(0), lastSeq)

	// Record some values
	ss.RecordValue(1, 100)
	ss.RecordValue(2, 200)
	ss.RecordValue(3, 300)

	latestVals, lastSeq = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100, 2: 200, 3: 300}, latestVals)
	assert.Equal(t, uint32(3), lastSeq)

	// Ensure returned map is a copy; modifying it should not affect original data
	latestVals[4] = 400
	latestVals, lastSeq = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100, 2: 200, 3: 300}, latestVals)
}

func TestSequenceState_DeleteLastValue(t *testing.T) {
	ss := NewSequenceState()

	// Record some values
	ss.RecordValue(1, 100)
	ss.RecordValue(2, 200)
	ss.RecordValue(3, 300)

	// Delete a non-existent sequence
	ss.DeleteLastValue(999)
	latestVals, _ := ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100, 2: 200, 3: 300}, latestVals)

	// Delete an existing sequence
	ss.DeleteLastValue(2)
	latestVals, _ = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100, 3: 300}, latestVals)

	// Delete the last sequence
	ss.DeleteLastValue(3)
	latestVals, _ = ss.Export()
	assert.Equal(t, map[uint32]int64{1: 100}, latestVals)

	// Delete all sequences
	ss.DeleteLastValue(1)
	latestVals, _ = ss.Export()
	assert.Empty(t, latestVals)
}

func TestSequenceState_Comprehensive(t *testing.T) {
	ss := NewSequenceState()

	// Test complete workflow
	ss.RecordValue(100, 1000)
	ss.RecordValue(200, 2000)

	// Get last value
	val, err := ss.GetLastValue()
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), val)

	// Get value by ID
	val, ok := ss.GetLastValueByID(100)
	assert.True(t, ok)
	assert.Equal(t, int64(1000), val)

	// Export state
	latestVals, lastSeq := ss.Export()
	assert.Equal(t, map[uint32]int64{100: 1000, 200: 2000}, latestVals)
	assert.Equal(t, uint32(200), lastSeq)

	// Delete a sequence
	ss.DeleteLastValue(100)
	latestVals, _ = ss.Export()
	assert.Equal(t, map[uint32]int64{200: 2000}, latestVals)

	// Set last sequence
	ss.SetLastSequenceIncremented(200)
	val, err = ss.GetLastValue()
	assert.NoError(t, err)
	assert.Equal(t, int64(2000), val)
}
