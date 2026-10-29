// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package serverpb

import (
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/status"
)

var _ error = (*Error)(nil)

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("ml error: code=%d msg=%s", e.Code, e.Message)
}

// NewError creates an error with a code.
func NewError(code Code, msg string) error {
	return &Error{
		Code:    code,
		Message: msg,
	}
}

// MatchErrorCode check error code.
func MatchErrorCode(err error, code Code) bool {
	errCode := GetCodeInternal(err, ComputeDefaultCode)

	return errCode == code
}

// GetCodeInternal get error code.
func GetCodeInternal(err error, computeDefaultCode func(err error) (code Code)) Code {
	code := Code_Uncategorized

	var c *Error

	if ok := errors.Is(err, c); ok {
		code = c.Code
	} else if newCode := computeDefaultCode(err); newCode != Code_Uncategorized {
		code = newCode
	}

	if c := errors.Unwrap(err); c != nil {
		innerCode := GetCodeInternal(c, computeDefaultCode)
		code = combineCodes(innerCode, code)
	}

	return code
}

// ComputeDefaultCode compute error code.
func ComputeDefaultCode(err error) Code {
	var mlError *Error
	if ok := errors.As(err, &mlError); ok {
		return mlError.Code
	}

	msg := status.Convert(err).Message()
	if matchCode(msg, Code_ModelVersionNotExist) {
		return Code_ModelVersionNotExist
	}

	return Code_Uncategorized
}

func combineCodes(innerCode, outerCode Code) Code {
	if outerCode == Code_Uncategorized {
		return innerCode
	}

	if innerCode != Code_Uncategorized {
		return innerCode
	}

	return outerCode
}

func matchCode(message string, code Code) bool {
	return strings.Contains(message, fmt.Sprintf("ml error: code=%d msg=", code))
}
