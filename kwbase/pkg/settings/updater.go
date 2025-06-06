// Copyright 2017 The Cockroach Authors.
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

package settings

// #cgo CPPFLAGS: -I../../../kwdbts2/include
// #cgo LDFLAGS: -lkwdbts2 -lrocksdb -lcommon -lsnappy -lm  -lstdc++
// #cgo LDFLAGS: -lprotobuf
// #cgo linux LDFLAGS: -lrt -lpthread
//
// #include <stdlib.h>
// #include <libkwdbts2.h>
import "C"
import (
	"strconv"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// EncodeDuration encodes a duration in the format parseRaw expects.
func EncodeDuration(d time.Duration) string {
	return d.String()
}

// EncodeBool encodes a bool in the format parseRaw expects.
func EncodeBool(b bool) string {
	return strconv.FormatBool(b)
}

// EncodeInt encodes an int in the format parseRaw expects.
func EncodeInt(i int64) string {
	return strconv.FormatInt(i, 10)
}

// EncodeFloat encodes a bool in the format parseRaw expects.
func EncodeFloat(f float64) string {
	return strconv.FormatFloat(f, 'G', -1, 64)
}

type updater struct {
	sv *Values
	m  map[string]struct{}
}

// Updater is a helper for updating the in-memory settings.
//
// RefreshSettings passes the serialized representations of all individual
// settings -- e.g. the rows read from the system.settings table. We update the
// wrapped atomic settings values as we go and note which settings were updated,
// then set the rest to default in ResetRemaining().
type Updater interface {
	Set(k, rawValue, valType string) error
	ResetRemaining()
}

// A NoopUpdater ignores all updates.
type NoopUpdater struct{}

// Set implements Updater. It is a no-op.
func (u NoopUpdater) Set(_, _, _ string) error { return nil }

// ResetRemaining implements Updater. It is a no-op.
func (u NoopUpdater) ResetRemaining() {}

// NewUpdater makes an Updater.
func NewUpdater(sv *Values) Updater {
	return updater{
		m:  make(map[string]struct{}, len(registry)),
		sv: sv,
	}
}

func goToTSSlice(b []byte) C.TSSlice {
	if len(b) == 0 {
		return C.TSSlice{data: nil, len: 0}
	}
	return C.TSSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

// SendToAeList is the list of settings that need to be sent to AE.
var SendToAeList = []string{"ts.trace.on_off_list", "ts.dedup.rule", "ts.mount.max_limit",
	"ts.wal.files_in_group", "ts.entities_per_subgroup.max_limit", "ts.blocks_per_segment.max_limit",
	"ts.rows_per_block.max_limit", "ts.compression.type", "ts.compression.level",
	"ts.cached_partitions_per_subgroup.max_limit", "immediate_compression.threads", "ts.count.use_statistics.enabled",
	"ts.disk_free_space.alert_threshold", "ts.table_cache.capacity", "ts.auto_vacuum.sleep"}

func needSendToAE(key string) bool {
	for _, value := range SendToAeList {
		if value == key {
			return true
		}
	}
	return false
}

// Set attempts to parse and update a setting and notes that it was updated.
func (u updater) Set(key, rawValue string, vt string) error {
	d, ok := registry[key]
	if !ok {
		if _, ok := retiredSettings[key]; ok {
			return nil
		}
		// Likely a new setting this old node doesn't know about.
		return errors.Errorf("unknown setting '%s'", key)
	}

	u.m[key] = struct{}{}

	if expected := d.Typ(); vt != expected {
		return errors.Errorf("setting '%s' defined as type %s, not %s", key, expected, vt)
	}

	if needSendToAE(key) {
		C.TSSetClusterSetting(goToTSSlice([]byte(key)), goToTSSlice([]byte(rawValue)))
	}

	switch setting := d.(type) {
	case *StringSetting:
		return setting.set(u.sv, rawValue)
	case *BoolSetting:
		b, err := strconv.ParseBool(rawValue)
		if err != nil {
			return err
		}
		setting.set(u.sv, b)
		return nil
	case numericSetting: // includes *EnumSetting
		i, err := strconv.Atoi(rawValue)
		if err != nil {
			return err
		}
		if key == "ts.wal.file_size" || key == "ts.wal.buffer_size" {
			fileSize := strconv.Itoa(i >> 20)
			C.TSSetClusterSetting(goToTSSlice([]byte(key)), goToTSSlice([]byte(fileSize)))
		}
        if key == "ts.wal.wal_level" {
            C.TSSetClusterSetting(goToTSSlice([]byte(key)), goToTSSlice([]byte(i)))
        }
		return setting.set(u.sv, int64(i))
	case *FloatSetting:
		f, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			return err
		}
		return setting.set(u.sv, f)
	case *DurationSetting:
		d, err := time.ParseDuration(rawValue)
		if err != nil {
			return err
		}
		if key == "ts.wal.flush_interval" {
			var walLevel string
			if d < 0 {
				walLevel = "0"
			} else if d >= 0 && d <= 200*time.Millisecond {
				walLevel = "2"
			} else {
				walLevel = "1"
			}
			C.TSSetClusterSetting(goToTSSlice([]byte("ts.wal.wal_level")), goToTSSlice([]byte(walLevel)))
		}
		return setting.set(u.sv, d)
	case *StateMachineSetting:
		return setting.set(u.sv, []byte(rawValue))
	}
	return nil
}

// ResetRemaining sets all settings not updated by the updater to their default values.
func (u updater) ResetRemaining() {
	for k, v := range registry {
		if _, ok := u.m[k]; !ok {
			v.setToDefault(u.sv)
		}
	}
}
