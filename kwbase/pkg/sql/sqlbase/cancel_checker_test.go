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

package sqlbase_test

import (
	"context"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// TestNewCancelChecker tests the NewCancelChecker function
func TestNewCancelChecker(t *testing.T) {
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)

	if cancelChecker == nil {
		t.Fatal("Expected non-nil CancelChecker")
	}
}

// TestCancelChecker_Check tests the Check method under normal conditions
func TestCancelChecker_Check(t *testing.T) {
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)

	// Should return nil when context is not cancelled
	for i := 0; i < 2000; i++ { // Run more than cancelCheckInterval (1024) to ensure multiple checks
		err := cancelChecker.Check()
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	}
}

// TestCancelChecker_Check_WithCancellation tests the Check method when context is cancelled
func TestCancelChecker_Check_WithCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelChecker := sqlbase.NewCancelChecker(ctx)

	// Initially should return nil
	err := cancelChecker.Check()
	if err != nil {
		t.Errorf("Expected no error initially, got %v", err)
	}

	// Cancel the context
	cancel()

	// Now it should return an error after some calls (due to the interval)
	// We need to call Check enough times to trigger the cancellation check
	var foundError bool
	for i := 0; i < 1024; i++ { // At most 1024 calls to ensure we hit the check
		err = cancelChecker.Check()
		if err != nil {
			foundError = true
			break
		}
	}

	if !foundError {
		t.Error("Expected to find cancellation error after context cancellation")
	}
}

// TestCancelChecker_Check_WithTimeout tests the Check method with a timeout context
func TestCancelChecker_Check_WithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	cancelChecker := sqlbase.NewCancelChecker(ctx)

	// Wait for the context to timeout
	time.Sleep(5 * time.Millisecond)

	// Now it should return an error after some calls (due to the interval)
	var foundError bool
	for i := 0; i < 1024; i++ { // At most 1024 calls to ensure we hit the check
		err := cancelChecker.Check()
		if err != nil {
			foundError = true
			break
		}
	}

	if !foundError {
		t.Error("Expected to find timeout error after context timeout")
	}
}

// TestCancelChecker_Reset tests the Reset method
func TestCancelChecker_Reset(t *testing.T) {
	ctx1 := context.Background()
	ctx2, cancel := context.WithCancel(context.Background())

	cancelChecker := sqlbase.NewCancelChecker(ctx1)

	// Initially should return nil
	err := cancelChecker.Check()
	if err != nil {
		t.Errorf("Expected no error with initial context, got %v", err)
	}

	// Reset with a cancelled context
	cancel() // Cancel ctx2
	cancelChecker.Reset(ctx2)

	// Now it should eventually return an error after reset
	var foundError bool
	for i := 0; i < 1024; i++ {
		err = cancelChecker.Check()
		if err != nil {
			foundError = true
			break
		}
	}

	if !foundError {
		t.Error("Expected to find cancellation error after reset with cancelled context")
	}
}

// TestCancelChecker_MultipleChecks tests multiple consecutive checks
func TestCancelChecker_MultipleChecks(t *testing.T) {
	ctx := context.Background()
	cancelChecker := sqlbase.NewCancelChecker(ctx)

	// Perform multiple checks in sequence
	for i := 0; i < 5000; i++ {
		err := cancelChecker.Check()
		if err != nil {
			t.Errorf("Unexpected error at iteration %d: %v", i, err)
		}
	}
}
