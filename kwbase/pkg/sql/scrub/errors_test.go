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

package scrub_test

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/scrub"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestWrapError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	baseErr := errors.New("base error")
	err := scrub.WrapError(scrub.PhysicalError, baseErr)

	require.NotNil(t, err)
	require.Equal(t, scrub.PhysicalError, err.Code)
	require.Equal(t, baseErr, err.Cause())

	// Test Error() method
	expectedMsg := fmt.Sprintf("%s: %+v", scrub.PhysicalError, baseErr)
	require.Equal(t, expectedMsg, err.Error())
}

func TestIsScrubError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	baseErr := errors.New("base error")
	scrubErr := scrub.WrapError(scrub.PhysicalError, baseErr)

	require.True(t, scrub.IsScrubError(scrubErr))
	require.False(t, scrub.IsScrubError(baseErr))
	require.False(t, scrub.IsScrubError(nil))
}

func TestUnwrapScrubError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	baseErr := errors.New("base error")
	scrubErr := scrub.WrapError(scrub.PhysicalError, baseErr)

	// Unwrap scrub error
	require.Equal(t, baseErr, scrub.UnwrapScrubError(scrubErr))

	// Unwrap non-scrub error
	require.Equal(t, baseErr, scrub.UnwrapScrubError(baseErr))

	// Unwrap nil
	require.Nil(t, scrub.UnwrapScrubError(nil))
}

func TestFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	baseErr := errors.New("base error")
	scrubErr := scrub.WrapError(scrub.PhysicalError, baseErr)

	// Format with %v
	formatted := fmt.Sprintf("%v", scrubErr)
	require.Equal(t, "base error", formatted)

	// Format with %+v
	formattedPlus := fmt.Sprintf("%+v", scrubErr)
	require.Contains(t, formattedPlus, "base error")
	require.Contains(t, formattedPlus, "Wraps:")
	require.Contains(t, formattedPlus, "*scrub.Error")
}
