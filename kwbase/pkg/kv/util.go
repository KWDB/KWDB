// Copyright 2015 The Cockroach Authors.
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

package kv

import (
	"fmt"
	"reflect"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/apd"
)

// TODO(pmattis): The methods in this file needs tests.

func marshalKey(k interface{}) (roachpb.Key, error) {
	switch t := k.(type) {
	case *roachpb.Key:
		return *t, nil
	case roachpb.Key:
		return t, nil
	case *roachpb.RKey:
		return t.AsRawKey(), nil
	case roachpb.RKey:
		return t.AsRawKey(), nil
	case string:
		return roachpb.Key(t), nil
	case []byte:
		return roachpb.Key(t), nil
	}
	return nil, fmt.Errorf("unable to marshal key: %T %q", k, k)
}

// marshalValue returns a roachpb.Value initialized from the source
// interface{}, returning an error if the types are not compatible.
func marshalValue(v interface{}) (roachpb.Value, error) {
	var r roachpb.Value

	// Handle a few common types via a type switch.
	switch t := v.(type) {
	case *roachpb.Value:
		return *t, nil

	case nil:
		return r, nil

	case bool:
		r.SetBool(t)
		return r, nil

	case string:
		r.SetBytes([]byte(t))
		return r, nil

	case []byte:
		r.SetBytes(t)
		return r, nil

	case apd.Decimal:
		err := r.SetDecimal(&t)
		return r, err

	case roachpb.Key:
		r.SetBytes([]byte(t))
		return r, nil

	case time.Time:
		r.SetTime(t)
		return r, nil

	case duration.Duration:
		err := r.SetDuration(t)
		return r, err

	case protoutil.Message:
		err := r.SetProto(t)
		return r, err

	case roachpb.Value:
		panic("unexpected type roachpb.Value (use *roachpb.Value)")
	}

	// Handle all of the Go primitive types besides struct and pointers. This
	// switch also handles types based on a primitive type (e.g. "type MyInt
	// int").
	switch v := reflect.ValueOf(v); v.Kind() {
	case reflect.Bool:
		r.SetBool(v.Bool())
		return r, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		r.SetInt(v.Int())
		return r, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		r.SetInt(int64(v.Uint()))
		return r, nil

	case reflect.Float32, reflect.Float64:
		r.SetFloat(v.Float())
		return r, nil

	case reflect.String:
		r.SetBytes([]byte(v.String()))
		return r, nil
	}

	return r, fmt.Errorf("unable to marshal %T: %v", v, v)
}

// FollowerReadEnable is true means allow follower read.
var FollowerReadEnable = envutil.EnvOrDefaultBool("KWBASE_ENABLE_FOLLOWER_READ", false)

// TsFollowerReadThreshold controls whether ts replicas attempt to serve follower
// reads.
var TsFollowerReadThreshold = settings.RegisterNonNegativeFloatSetting(
	"kv.kvserver.qps_ts_follower_read_threshold",
	"allow ts replicas to serve follower reads if qps exceed the threshold",
	0,
)
