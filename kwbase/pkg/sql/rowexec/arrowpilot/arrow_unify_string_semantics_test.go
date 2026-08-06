// Copyright 2024 The KWDB Authors.
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

package arrowpilot

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestArrowUnifyProjectionStringSemantics verifies the edge-case semantics of
// the Arrow-native string function loops (TRIM / REPLACE / SUBSTRING /
// OVERLAY / SPLIT_PART) match the classic (rowexec/colexec) engine exactly.
// These kernels are hand-written Go vectorized loops inside the Arrow
// projection processor, deliberately mirroring the crdb/KWDB semantics (which
// differ from a naive byte-oriented implementation or from a direct port of the
// arrow-cpp kernels). See docs/arrow-unify-roadmap.md §6.4.
func TestArrowUnifyProjectionStringSemantics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	if _, err := db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = true"); err != nil {
		t.Fatalf("set projection enabled: %v", err)
	}
	defer func() {
		_, _ = db.Exec("SET CLUSTER SETTING sql.arrow_projection.enabled = false")
		execStmt(t, db, "DROP TABLE IF EXISTS ps_sem")
	}()

	execStmt(t, db, "CREATE TABLE ps_sem (s STRING)")
	// Mix of ASCII whitespace, a Unicode non-breaking space (U+00A0), and a
	// plain token so we can exercise both whitespace kinds and REPLACE edge
	// cases. Note: '\u00a0' is a Unicode whitespace that the classic
	// strings.TrimSpace strips but a naïve byte cut-set {" \t\n\v\f\r"} does
	// not.
	execStmt(t, db, fmt.Sprintf("INSERT INTO ps_sem VALUES (' a '), ('%s b %s'), ('hello')", "\u00a0", "\u00a0"))

	// --- TRIM: Unicode whitespace must be stripped (classic uses unicode.IsSpace) ---
	// ORDER BY s sorts the three source rows as: ' a ' < 'hello' < U+00A0 b U+00A0
	// (the non-breaking space is 0xC2 0xA0, which sorts after 'h').
	assertStr(t, db, "SELECT TRIM(s) FROM ps_sem ORDER BY s",
		[][]string{{"a"}, {"hello"}, {"b"}})
	assertStr(t, db, "SELECT TRIM(s, ' h') FROM ps_sem ORDER BY s",
		[][]string{{"a"}, {"ello"}, {"\u00a0 b \u00a0"}})

	// --- REPLACE with empty `from`: Go strings.Replace inserts `to` between
	// every character (classic semantics), not a no-op. ---
	assertStr(t, db, "SELECT REPLACE('abc', '', 'X')",
		[][]string{{"XaXbXcX"}})

	// --- SUBSTRING with negative length: must error (crdb raises
	// "negative substring length N not allowed"), not return empty string. ---
	_, err := db.Query("SELECT SUBSTRING(s, 1, -1) FROM ps_sem")
	require.Error(t, err, "substring with negative length should error")
	require.Contains(t, err.Error(), "negative substring length")

	// --- OVERLAY with non-positive start: must error (crdb raises
	// "'start' must be positive"), not clamp to 1. ---
	_, err = db.Query("SELECT OVERLAY(s PLACING 'x' FROM 0) FROM ps_sem")
	require.Error(t, err, "overlay with start <= 0 should error")
	require.Contains(t, err.Error(), "must be positive")

	// --- SPLIT_PART with non-positive field: must error (crdb raises
	// "field position N must be greater than zero"), not return NULL. ---
	_, err = db.Query("SELECT SPLIT_PART(s, ' ', 0) FROM ps_sem")
	require.Error(t, err, "split_part with field <= 0 should error")
	require.Contains(t, err.Error(), "must be greater than zero")

	// --- Sanity: normal SUBSTRING / OVERLAY / SPLIT_PART still work. ---
	assertStr(t, db, "SELECT SUBSTRING(s, 1, 3) FROM ps_sem ORDER BY s",
		[][]string{{" a "}, {"hel"}, {fmt.Sprintf("%s b", "\u00a0")}})
	assertStr(t, db, "SELECT SPLIT_PART('a.b.c', '.', 2)",
		[][]string{{"b"}})
}
