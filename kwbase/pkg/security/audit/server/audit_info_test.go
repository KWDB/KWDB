package server

import (
	"errors"
	"net"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

func TestMakeAuditInfo_Basic(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5432}

	ai := MakeAuditInfo(start,
		"alice",
		[]string{"admin", "dev"},
		target.OperationType("TEST_OP"),
		target.AuditObjectType("TEST_OBJ"),
		target.AuditLevelType(1),
		addr,
		nil,
	)

	if ai.User == nil {
		t.Fatalf("expected User not nil")
	}
	if ai.User.Username != "alice" {
		t.Fatalf("expected username alice, got %q", ai.User.Username)
	}
	if len(ai.User.Roles) != 2 || ai.User.Roles[0].Name != "admin" {
		t.Fatalf("unexpected roles: %#v", ai.User.Roles)
	}
	if ai.Client == nil {
		t.Fatalf("expected Client not nil")
	}
	if ai.Client.Address != addr.String() {
		t.Fatalf("expected client address %s, got %s", addr.String(), ai.Client.Address)
	}
	if ai.GetTargetType() != target.AuditObjectType("TEST_OBJ") {
		t.Fatalf("expected target type TEST_OBJ, got %v", ai.GetTargetType())
	}
	if ai.Result == nil {
		t.Fatalf("expected Result not nil")
	}
	if ai.Result.Status != ExecSuccess {
		t.Fatalf("expected result status %s, got %s", ExecSuccess, ai.Result.Status)
	}
	// elapsed should be non-negative
	if ai.Elapsed < 0 {
		t.Fatalf("unexpected negative elapsed: %v", ai.Elapsed)
	}
}

func TestSetResult_ErrorAndRows(t *testing.T) {
	var ai AuditInfo
	ai.SetResult(errors.New("boom"), 5)

	if ai.Result == nil {
		t.Fatalf("expected Result not nil")
	}
	if ai.Result.Status != ExecFail {
		t.Fatalf("expected status %s, got %s", ExecFail, ai.Result.Status)
	}
	if ai.Result.ErrMsg != "boom" {
		t.Fatalf("expected ErrMsg 'boom', got %q", ai.Result.ErrMsg)
	}
	if ai.Result.RowsAffected != 5 {
		t.Fatalf("expected RowsAffected 5, got %d", ai.Result.RowsAffected)
	}
}

func TestTarget_SetAndGet(t *testing.T) {
	var ai AuditInfo
	ai.SetTargetType(target.AuditObjectType("table"))
	ai.SetTarget(42, "users", []string{"col1", "col2"})

	ti := ai.GetTargetInfo()
	if ti == nil {
		t.Fatalf("expected target info not nil")
	}
	if ti.GetTargetType() != target.AuditObjectType("table") {
		t.Fatalf("expected target type 'table', got %v", ti.GetTargetType())
	}
	ids := ti.GetTargetIDs()
	found := false
	for _, id := range ids {
		if id == 42 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected target id 42 in %v", ids)
	}
	if ai.IsTargetEmpty() {
		t.Fatalf("expected target not empty")
	}
	if ti.IsTargetEmpty() {
		t.Fatalf("expected targetinfo not empty")
	}
}

func TestSetClient_FromSessionData(t *testing.T) {
	var ai AuditInfo
	addr := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 6000}
	sd := &sessiondata.SessionData{
		ApplicationName: "myapp",
		RemoteAddr:      addr,
	}
	ai.SetClient(sd)

	if ai.Client == nil {
		t.Fatalf("expected client not nil")
	}
	if ai.Client.AppName != "myapp" {
		t.Fatalf("expected AppName myapp, got %q", ai.Client.AppName)
	}
	if ai.Client.Address != addr.String() {
		t.Fatalf("expected Address %s, got %s", addr.String(), ai.Client.Address)
	}
}
