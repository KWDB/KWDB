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

import "github.com/pkg/errors"

// StringSetting is the interface of a setting variable that will be
// updated automatically when the corresponding cluster-wide setting
// of type "string" is updated.
type StringSetting struct {
	defaultValue string
	validateFn   func(*Values, string) error
	common
}

var _ extendedSetting = &StringSetting{}

func (s *StringSetting) String(sv *Values) string {
	return s.Get(sv)
}

// Encoded returns the encoded value of the current value of the setting.
func (s *StringSetting) Encoded(sv *Values) string {
	return s.String(sv)
}

// EncodedDefault returns the encoded value of the default value of the setting.
func (s *StringSetting) EncodedDefault() string {
	return s.defaultValue
}

// Typ returns the short (1 char) string denoting the type of setting.
func (*StringSetting) Typ() string {
	return "s"
}

// Default returns default value for setting.
func (s *StringSetting) Default() string {
	return s.defaultValue
}

// Defeat the linter.
var _ = (*StringSetting).Default

// Get retrieves the string value in the setting.
func (s *StringSetting) Get(sv *Values) string {
	loaded := sv.getGeneric(s.slotIdx)
	if loaded == nil {
		return ""
	}
	return loaded.(string)
}

// Validate that a value conforms with the validation function.
func (s *StringSetting) Validate(sv *Values, v string) error {
	if s.validateFn != nil {
		if err := s.validateFn(sv, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *StringSetting) set(sv *Values, v string) error {
	if err := s.Validate(sv, v); err != nil {
		return err
	}
	if s.Get(sv) != v {
		sv.setGeneric(s.slotIdx, v)
	}
	return nil
}

func (s *StringSetting) setToDefault(sv *Values) {
	if err := s.set(sv, s.defaultValue); err != nil {
		panic(err)
	}
}

// RegisterStringSetting defines a new setting with type string.
func RegisterStringSetting(key, desc string, defaultValue string) *StringSetting {
	return RegisterValidatedStringSetting(key, desc, defaultValue, nil)
}

// RegisterPublicStringSetting defines a new setting with type string and makes it public.
func RegisterPublicStringSetting(key, desc string, defaultValue string) *StringSetting {
	s := RegisterValidatedStringSetting(key, desc, defaultValue, nil)
	s.SetVisibility(Public)
	return s
}

// RegisterValidatedStringSetting defines a new setting with type string with a
// validation function.
func RegisterValidatedStringSetting(
	key, desc string, defaultValue string, validateFn func(*Values, string) error,
) *StringSetting {
	if validateFn != nil {
		if err := validateFn(nil, defaultValue); err != nil {
			panic(errors.Wrap(err, "invalid default"))
		}
	}
	setting := &StringSetting{
		defaultValue: defaultValue,
		validateFn:   validateFn,
	}
	// By default all string settings are considered to perhaps contain
	// PII and are thus non-reportable (to exclude them from telemetry
	// reports).
	setting.SetReportable(false)
	register(key, desc, setting)
	return setting
}
