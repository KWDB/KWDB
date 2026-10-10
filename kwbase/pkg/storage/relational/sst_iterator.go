// Copyright 2017 The Cockroach Authors.
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

package relational

import (
	"bytes"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pkg/errors"
)

type sstIterator struct {
	sst  *sstable.Reader
	iter sstable.Iterator

	mvccKey   MVCCKey
	value     []byte
	iterValid bool
	err       error

	// For allocation avoidance in NextKey.
	nextKeyStart []byte

	// roachpb.Verify k/v pairs on each call to Next()
	verify bool
}

// NewSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewSSTIterator(path string) (SimpleIterator, error) {
	file, err := vfs.Default.Open(path)
	if err != nil {
		return nil, err
	}
	sst, err := sstable.NewReader(file, sstable.ReaderOptions{
		Comparer: MVCCComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst}, nil
}

// NewMemSSTIterator returns a `SimpleIterator` for an in-memory sstable.
// It's compatible with sstables written by `RocksDBSstFileWriter` and
// Pebble's `sstable.Writer`, and assumes the keys use Cockroach's MVCC
// format.
func NewMemSSTIterator(data []byte, verify bool) (SimpleIterator, error) {
	sst, err := sstable.NewReader(vfs.NewMemFile(data), sstable.ReaderOptions{
		Comparer: MVCCComparer,
	})
	if err != nil {
		return nil, err
	}
	return &sstIterator{sst: sst, verify: verify}, nil
}

// Close implements the SimpleIterator interface.
func (r *sstIterator) Close() {
	if r.iter != nil {
		r.err = errors.Wrap(r.iter.Close(), "closing sstable iterator")
	}
	if err := r.sst.Close(); err != nil && r.err == nil {
		r.err = errors.Wrap(err, "closing sstable")
	}
}

// SeekGE implements the SimpleIterator interface.
func (r *sstIterator) SeekGE(key MVCCKey) {
	if r.err != nil {
		return
	}
	if r.iter == nil {
		// Iterator creation happens on the first Seek as it involves I/O.
		r.iter, r.err = r.sst.NewIter(nil /* lower */, nil /* upper */)
		if r.err != nil {
			return
		}
	}
	var iKey *sstable.InternalKey
	iKey, r.value = r.iter.SeekGE(EncodeKey(key))
	if iKey != nil {
		r.iterValid = true
		r.mvccKey, r.err = DecodeMVCCKey(iKey.UserKey)
	} else {
		r.iterValid = false
		r.err = r.iter.Error()
	}
	if r.iterValid && r.err == nil && r.verify {
		r.err = roachpb.Value{RawBytes: r.value}.Verify(r.mvccKey.Key)
	}
}

// Valid implements the SimpleIterator interface.
func (r *sstIterator) Valid() (bool, error) {
	return r.iterValid && r.err == nil, r.err
}

// Next implements the SimpleIterator interface.
func (r *sstIterator) Next() {
	if !r.iterValid || r.err != nil {
		return
	}
	var iKey *sstable.InternalKey
	iKey, r.value = r.iter.Next()
	if iKey != nil {
		r.mvccKey, r.err = DecodeMVCCKey(iKey.UserKey)
	} else {
		r.iterValid = false
		r.err = r.iter.Error()
	}
	if r.iterValid && r.err == nil && r.verify {
		r.err = roachpb.Value{RawBytes: r.value}.Verify(r.mvccKey.Key)
	}
}

// NextKey implements the SimpleIterator interface.
func (r *sstIterator) NextKey() {
	if !r.iterValid || r.err != nil {
		return
	}
	r.nextKeyStart = append(r.nextKeyStart[:0], r.mvccKey.Key...)
	for r.Next(); r.iterValid && r.err == nil && bytes.Equal(r.nextKeyStart, r.mvccKey.Key); r.Next() {
	}
}

// UnsafeKey implements the SimpleIterator interface.
func (r *sstIterator) UnsafeKey() MVCCKey {
	return r.mvccKey
}

// UnsafeValue implements the SimpleIterator interface.
func (r *sstIterator) UnsafeValue() []byte {
	return r.value
}
