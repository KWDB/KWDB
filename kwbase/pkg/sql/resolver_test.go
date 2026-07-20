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

package sql

//import (
//	"context"
//	"testing"
//
//	"gitee.com/kwbasedb/kwbase/pkg/kv"
//	"gitee.com/kwbasedb/kwbase/pkg/security"
//	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
//	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
//	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
//	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
//)
//
//func Test_planner_LookupKWDBObject(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	// TODO: fix unit test failed
//	t.Skip()
//	ctx := context.Background()
//	params, _ := tests.CreateTestServerParams()
//	s, _, db := serverutils.StartServer(t, params)
//	defer s.Stopper().Stop(ctx)
//
//	execCfg := s.ExecutorConfig().(ExecutorConfig)
//	internalPlanner, cleanup := NewInternalPlanner(
//		"test",
//		kv.NewTxn(ctx, db, s.NodeID()),
//		security.RootUser,
//		&MemoryMetrics{},
//		&execCfg,
//	)
//	defer cleanup()
//	p := internalPlanner.(*GenericPlanner)
//
//	initStmts := []string{
//		`create tenant tenant1`,
//		`create portal tenant1.portal1 MEMCAPACITY 100 DISKCAPACITY 100 DIRPATH '.' WAL ENABLED FILE SPLIT TYPE 10 day`,
//		`create portal tenant1.portal2 MEMCAPACITY 100 DISKCAPACITY 100 DIRPATH '.' WAL ENABLED FILE SPLIT TYPE 10 day`,
//		`create device tenant1.portal1.device1 (e1 int, e2 float)`,
//		`create device tenant1.portal2.device1 (e1 int, e2 float)`,
//	}
//
//	for _, stmt := range initStmts {
//		if _, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Exec(ctx, "test lookup device", p.txn, stmt); err != nil {
//			t.Fatal(err)
//		}
//	}
//
//	type expect struct {
//		deviceCount int
//		deviceName  []string
//		found       bool
//		error       string
//	}
//
//	type input struct {
//		tenantName string
//		portalName string
//		deviceName string
//	}
//
//	testCase := []struct {
//		input
//		expect
//	}{
//		{input{"tenant1", "portal1", "device1"}, expect{1, []string{"device1"}, true, ""}},
//		{input{"tenant1", "portal3", "device1"}, expect{0, []string{}, false, "portal does not exist"}},
//		{input{"tenant1", "portal1", "device2"}, expect{0, []string{}, false, "device dose not exist"}},
//		{input{"tenant1", "*", "device1"}, expect{2, []string{"device1", "device1"}, true, ""}},
//	}
//
//	for _, v := range testCase {
//		t.Run("Lookup Device", func(t *testing.T) {
//			found, res, err := p.LookupKWDBObject(ctx, v.tenantName, v.portalName, v.input.deviceName)
//			if err != nil {
//				if err.Error() != v.error {
//					t.Fatalf("expected error:%v, got: %v ", v.expect.error, err)
//				}
//			}
//			if found != v.expect.found {
//				t.Fatalf("expected %v, got %v", v.found, found)
//			}
//			if ret, ok := res.(*sqlbase.Devices); ok {
//				if len(ret.DeviceList) != v.deviceCount {
//					t.Fatalf("expected %v device(s), got: %v device(s)", v.deviceCount, len(ret.DeviceList))
//				}
//				for idx, device := range ret.DeviceList {
//					if device.Device.DeviceName != v.expect.deviceName[idx] {
//						t.Fatalf("expected device name:%v, got: %v ", v.expect.deviceName, device.Device.DeviceName)
//					}
//				}
//			}
//		})
//	}
//
//}
