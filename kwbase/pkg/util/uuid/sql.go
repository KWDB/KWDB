// Copyright 2019 The Cockroach Authors.
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

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// Value implements the driver.Valuer interface.
func (u UUID) Value() (driver.Value, error) {
	return u.String(), nil
}

// Scan implements the sql.Scanner interface.
// A 16-byte slice will be handled by UnmarshalBinary, while
// a longer byte slice or a string will be handled by UnmarshalText.
func (u *UUID) Scan(src interface{}) error {
	switch src := src.(type) {
	case UUID: // support gorm convert from UUID to NullUUID
		*u = src
		return nil

	case []byte:
		if len(src) == Size {
			return u.UnmarshalBinary(src)
		}
		return u.UnmarshalText(src)

	case string:
		return u.UnmarshalText([]byte(src))
	}

	return fmt.Errorf("uuid: cannot convert %T to UUID", src)
}

// NullUUID can be used with the standard sql package to represent a
// UUID value that can be NULL in the database.
type NullUUID struct {
	UUID  UUID
	Valid bool
}

// Value implements the driver.Valuer interface.
func (u NullUUID) Value() (driver.Value, error) {
	if !u.Valid {
		return nil, nil
	}
	// Delegate to UUID Value function
	return u.UUID.Value()
}

// Scan implements the sql.Scanner interface.
func (u *NullUUID) Scan(src interface{}) error {
	if src == nil {
		u.UUID, u.Valid = Nil, false
		return nil
	}

	// Delegate to UUID Scan function
	u.Valid = true
	return u.UUID.Scan(src)
}

// MarshalJSON marshals the NullUUID as null or the nested UUID
func (u NullUUID) MarshalJSON() ([]byte, error) {
	if !u.Valid {
		return json.Marshal(nil)
	}

	return json.Marshal(u.UUID)
}

// UnmarshalJSON unmarshals a NullUUID
func (u *NullUUID) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, []byte("null")) {
		u.UUID, u.Valid = Nil, false
		return nil
	}

	if err := json.Unmarshal(b, &u.UUID); err != nil {
		return err
	}

	u.Valid = true

	return nil
}
