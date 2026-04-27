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

package rules_test

import (
	"context"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/rules"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func TestAuditEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// start test server
	params, _ := tests.CreateTestServerParams()
	s, DB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	//prepare audit
	createAuditStmt := "create audit stmttest ON DATABASE FOR CREATE TO root;"
	enableAuditStmt := "ALTER AUDIT stmttest enable;"
	openAuditStmt := "SET CLUSTER SETTING audit.enabled=true;"

	if _, err := DB.Exec(openAuditStmt); err != nil {
		t.Fatal(err)
	}

	if _, err := DB.Exec(createAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec(enableAuditStmt); err != nil {
		t.Fatal(err)
	}
	if _, err := DB.Exec("CREATE DATABASE test"); err != nil {
		t.Fatal(err)
	}
}

func TestConstants(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test constants
	if rules.AllStmt != "ALL" {
		t.Errorf("AllStmt = %q, want %q", rules.AllStmt, "ALL")
	}

	if rules.AllTar != "ALL" {
		t.Errorf("AllTar = %q, want %q", rules.AllTar, "ALL")
	}

	if rules.AllUser != "ALL" {
		t.Errorf("AllUser = %q, want %q", rules.AllUser, "ALL")
	}
}

func TestNewAuditStrategyInquirers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	_ = rules.NewAuditStrategyInquirers(ctx, nil)
}

func TestStrategyCheckAuditTargetID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := &rules.Strategy{
		TargetID: 123,
	}

	// Test matching ID
	if !st.CheckAuditTargetID(123) {
		t.Error("Strategy.CheckAuditTargetID should return true for matching ID")
	}

	// Test non-matching ID
	if st.CheckAuditTargetID(456) {
		t.Error("Strategy.CheckAuditTargetID should return false for non-matching ID")
	}
}

func TestStrategyCheckAuditTargetIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := &rules.Strategy{
		TargetID: 123,
	}

	// Test matching ID in list
	if !st.CheckAuditTargetIDs([]uint32{456, 123, 789}) {
		t.Error("Strategy.CheckAuditTargetIDs should return true when ID is in list")
	}

	// Test non-matching ID in list
	if st.CheckAuditTargetIDs([]uint32{456, 789, 999}) {
		t.Error("Strategy.CheckAuditTargetIDs should return false when ID is not in list")
	}

	// Test empty list
	if st.CheckAuditTargetIDs([]uint32{}) {
		t.Error("Strategy.CheckAuditTargetIDs should return false for empty list")
	}
}

func TestStrategyCheckAuditTargetType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with AllTar
	stAll := &rules.Strategy{
		TargetType: rules.AllTar,
	}
	if !stAll.CheckAuditTargetType(target.ObjectDatabase) {
		t.Error("Strategy.CheckAuditTargetType should return true when TargetType is AllTar")
	}

	// Test with matching target type
	stDB := &rules.Strategy{
		TargetType: string(target.ObjectDatabase),
	}
	if !stDB.CheckAuditTargetType(target.ObjectDatabase) {
		t.Error("Strategy.CheckAuditTargetType should return true for matching target type")
	}

	// Test with case-insensitive matching
	stDBCase := &rules.Strategy{
		TargetType: "database", // lowercase
	}
	if !stDBCase.CheckAuditTargetType(target.ObjectDatabase) {
		t.Error("Strategy.CheckAuditTargetType should return true for case-insensitive matching")
	}

	// Test with non-matching target type
	stTable := &rules.Strategy{
		TargetType: string(target.ObjectTable),
	}
	if stTable.CheckAuditTargetType(target.ObjectDatabase) {
		t.Error("Strategy.CheckAuditTargetType should return false for non-matching target type")
	}
}

func TestStrategyCheckAuditOperation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with AllOper
	stAll := &rules.Strategy{
		Operations: []string{string(target.AllOper)},
	}
	if !stAll.CheckAuditOperation(target.Create) {
		t.Error("Strategy.CheckAuditOperation should return true when operation is AllOper")
	}

	// Test with matching operation
	stCreate := &rules.Strategy{
		Operations: []string{string(target.Create)},
	}
	if !stCreate.CheckAuditOperation(target.Create) {
		t.Error("Strategy.CheckAuditOperation should return true for matching operation")
	}

	// Test with case-insensitive matching
	stCreateCase := &rules.Strategy{
		Operations: []string{"create"}, // lowercase
	}
	if !stCreateCase.CheckAuditOperation(target.Create) {
		t.Error("Strategy.CheckAuditOperation should return true for case-insensitive matching")
	}

	// Test with non-matching operation
	stAlter := &rules.Strategy{
		Operations: []string{string(target.Alter)},
	}
	if stAlter.CheckAuditOperation(target.Create) {
		t.Error("Strategy.CheckAuditOperation should return false for non-matching operation")
	}

	// Test with multiple operations
	stMultiple := &rules.Strategy{
		Operations: []string{string(target.Create), string(target.Alter), string(target.Drop)},
	}
	if !stMultiple.CheckAuditOperation(target.Alter) {
		t.Error("Strategy.CheckAuditOperation should return true when operation is in list")
	}
	if !stMultiple.CheckAuditOperation(target.Drop) {
		t.Error("Strategy.CheckAuditOperation should return true when operation is in list")
	}
	if stMultiple.CheckAuditOperation(target.Select) {
		t.Error("Strategy.CheckAuditOperation should return false when operation is not in list")
	}
}

func TestStrategyCheckAuditUser(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with AllUser
	stAll := &rules.Strategy{
		Operators: []string{rules.AllUser},
	}
	if !stAll.CheckAuditUser("test_user") {
		t.Error("Strategy.CheckAuditUser should return true when operator is AllUser")
	}

	// Test with matching user
	stUser := &rules.Strategy{
		Operators: []string{"test_user"},
	}
	if !stUser.CheckAuditUser("test_user") {
		t.Error("Strategy.CheckAuditUser should return true for matching user")
	}

	// Test with non-matching user
	stOtherUser := &rules.Strategy{
		Operators: []string{"other_user"},
	}
	if stOtherUser.CheckAuditUser("test_user") {
		t.Error("Strategy.CheckAuditUser should return false for non-matching user")
	}

	// Test with multiple users
	stMultiple := &rules.Strategy{
		Operators: []string{"user1", "user2", "user3"},
	}
	if !stMultiple.CheckAuditUser("user2") {
		t.Error("Strategy.CheckAuditUser should return true when user is in list")
	}
	if !stMultiple.CheckAuditUser("user3") {
		t.Error("Strategy.CheckAuditUser should return true when user is in list")
	}
	if stMultiple.CheckAuditUser("user4") {
		t.Error("Strategy.CheckAuditUser should return false when user is not in list")
	}
}

func TestStrategyCheckEventResult(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with AllStmt
	stAll := &rules.Strategy{
		EventResult: rules.AllStmt,
	}
	if !stAll.CheckEventResult("success") {
		t.Error("Strategy.CheckEventResult should return true when EventResult is AllStmt")
	}

	// Test with matching result
	stSuccess := &rules.Strategy{
		EventResult: "success",
	}
	if !stSuccess.CheckEventResult("success") {
		t.Error("Strategy.CheckEventResult should return true for matching result")
	}

	// Test with case-insensitive matching
	stSuccessCase := &rules.Strategy{
		EventResult: "SUCCESS",
	}
	if !stSuccessCase.CheckEventResult("success") {
		t.Error("Strategy.CheckEventResult should return true for case-insensitive matching")
	}

	// Test with result containing EventResult
	stContain := &rules.Strategy{
		EventResult: "succ",
	}
	if !stContain.CheckEventResult("success") {
		t.Error("Strategy.CheckEventResult should return true when result contains EventResult")
	}

	// Test with non-matching result
	stFailure := &rules.Strategy{
		EventResult: "failure",
	}
	if stFailure.CheckEventResult("success") {
		t.Error("Strategy.CheckEventResult should return false for non-matching result")
	}
}
