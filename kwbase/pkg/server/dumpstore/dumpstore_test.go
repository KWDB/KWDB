// Copyright 2020 The Cockroach Authors.
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

package dumpstore

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveOldAndTooBig(t *testing.T) {
	now := time.Date(2020, 6, 15, 13, 19, 19, 543000000, time.UTC)
	testData := []struct {
		startFiles []string
		sizes      []int64
		maxS       int64
		preserved  map[int]bool
		cleaned    []string
	}{
		// Simple case: no files.
		{[]string{}, []int64{}, 10, nil, []string{}},

		// Some files spanning a few days.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-13T13_19_19.002.3",
				"memprof.2020-06-14T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
			},
			// The actual sizes for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // June 13
				10, // June 14
				10, // June 15
			},
			// The max size to keep.
			35,
			// The preserved files (no file preserved).
			nil,
			// Expected files to clean up.
			[]string{
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-11T13_19_19.000.1",
			},
		},

		// Some files spanning a few days with some unknown files
		// interleaved.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"unknown",
			},
			// The actual sizes for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // unknown
			},
			// The max size to keep.
			25,
			// The preserved files (no file preserved).
			nil,
			// Expected files to clean up: none.
			[]string{},
		},

		// Some files spanning a few days with some unknown files
		// interleaved.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"unknown",
			},
			// The actual sizes for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // unknown
			},
			// The max size to keep.
			25,
			// The preserved files (no file preserved).
			nil,
			// Expected files to clean up: none.
			[]string{},
		},

		// Ditto, with some files preserved.
		{
			[]string{
				"memprof.2020-06-11T13_19_19.000.1",
				"memprof.2020-06-12T13_19_19.001.2",
				"memprof.2020-06-13T13_19_19.002.3",
				"memprof.2020-06-14T13_19_19.003.4",
				"memprof.2020-06-15T13_19_19.004.5",
			},
			// The actual time.Times for the 5 names above.
			[]int64{
				10, // June 11
				10, // June 12
				10, // June 13
				10, // June 14
				10, // June 15
			},
			// The max size to keep.
			25,
			// The preserved files. This takes priority over size-based
			// deletion.
			map[int]bool{
				0: true, // June 11
				2: true, // June 13
				3: true, // June 14
			},
			// Expected files to clean up. The other files
			// are preserved.
			[]string{
				"memprof.2020-06-12T13_19_19.001.2",
			},
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			path, err := ioutil.TempDir("", "remove")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(path) }()

			files := populate(t, path, tc.startFiles, tc.sizes)

			cleaned := []string{}
			cleanupFn := func(s string) error {
				cleaned = append(cleaned, s)
				return nil
			}

			removeOldAndTooBigExcept(context.Background(), myDumper{}, files, now, tc.maxS, tc.preserved, cleanupFn)
			assert.EqualValues(t, tc.cleaned, cleaned)
		})
	}
}

type myDumper struct{}

func (myDumper) PreFilter(
	_ context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, err error) {
	panic("unimplemented")
}

func (myDumper) CheckOwnsFile(_ context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), "memprof")
}

func populate(t *testing.T, dirName string, fileNames []string, sizes []int64) []os.FileInfo {
	if len(sizes) > 0 {
		require.Equal(t, len(fileNames), len(sizes))
	}

	for i, fn := range fileNames {
		f, err := os.Create(filepath.Join(dirName, fn))
		if err != nil {
			t.Fatal(err)
		}

		if len(sizes) > 0 {
			// Populate a size if requested.
			fmt.Fprintf(f, "%*s", sizes[i], " ")
		}

		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// Retrieve the file list for the remainder of the test.
	files, err := ioutil.ReadDir(dirName)
	if err != nil {
		t.Fatal(err)
	}
	return files
}
