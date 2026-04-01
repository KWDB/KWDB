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

package tree

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestCreateTriggerFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateTrigger
		expected string
	}{
		{
			name: "basic before insert trigger",
			node: &CreateTrigger{
				Name:        "trig1",
				IfNotExists: false,
				ActionTime:  TriggerActionTimeBefore,
				Event:       TriggerEventInsert,
				TableName:   MakeTableName("test_db", "test_table"),
				Body: TriggerBody{
					Body: []Statement{
						&ProcSet{
							Name:  "a",
							Value: NewNumVal(nil, "1", false),
						},
					},
				},
			},
			expected: "CREATE TRIGGER trig1 BEFORE INSERT ON test_db.public.test_table FOR EACH ROW \nBEGIN\nSET a = 1;\nEND;",
		},
		{
			name: "if not exists after update with order",
			node: &CreateTrigger{
				Name:        "trig2",
				IfNotExists: true,
				ActionTime:  TriggerActionTimeAfter,
				Event:       TriggerEventUpdate,
				TableName:   MakeTableName("test_db", "test_table"),
				Order: &TriggerOrder{
					OrderType:    TriggerOrderTypeFollow,
					OtherTrigger: "trig1",
				},
				Body: TriggerBody{
					Body: []Statement{
						&ProcSet{
							Name:  "b",
							Value: NewNumVal(nil, "2", false),
						},
					},
				},
			},
			expected: "CREATE TRIGGER IF NOT EXISTS trig2 AFTER UPDATE ON test_db.public.test_table FOR EACH ROW FOLLOWS trig1\nBEGIN\nSET b = 2;\nEND;",
		},
		{
			name: "trigger with multiple statements",
			node: &CreateTrigger{
				Name:        "trig3",
				IfNotExists: false,
				ActionTime:  TriggerActionTimeBefore,
				Event:       TriggerEventDelete,
				TableName:   MakeTableName("test_db", "test_table"),
				Body: TriggerBody{
					Body: []Statement{
						&ProcSet{Name: "a", Value: NewNumVal(nil, "1", false)},
						&ProcSet{Name: "b", Value: NewNumVal(nil, "2", false)},
					},
				},
			},
			expected: "CREATE TRIGGER trig3 BEFORE DELETE ON test_db.public.test_table FOR EACH ROW \nBEGIN\nSET a = 1;\nSET b = 2;\nEND;",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestCreateTriggerPGFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name     string
		node     *CreateTriggerPG
		expected string
	}{
		{
			name: "pg trigger format",
			node: &CreateTriggerPG{
				Name:        "trig_pg",
				IfNotExists: true,
				ActionTime:  TriggerActionTimeAfter,
				Event:       TriggerEventInsert,
				TableName:   MakeTableName("test_db", "test_table"),
				Order: &TriggerOrder{
					OrderType:    TriggerOrderTypePrecede,
					OtherTrigger: "trig_other",
				},
				BodyStr: "\nBEGIN\n  SET a = 1;\nEND\n",
			},
			expected: "CREATE TRIGGER IF NOT EXISTS trig_pg AFTER INSERT ON test_db.public.test_table FOR EACH ROW PRECEDES trig_other$$\nBEGIN\n  SET a = 1;\nEND\n$$",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.node.Format(ctx)
			result := ctx.CloseAndGetString()
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestTriggerEventFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		event    TriggerEvent
		expected string
	}{
		{TriggerEventTypeUnknown, "UNKNOWN"},
		{TriggerEventInsert, "INSERT"},
		{TriggerEventUpdate, "UPDATE"},
		{TriggerEventDelete, "DELETE"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.event.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
			require.Equal(t, tc.expected, tc.event.String())
		})
	}
}

func TestTriggerActionTimeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		actionTime TriggerActionTime
		expected   string
	}{
		{TriggerActionTimeUnknown, "UNKNOWN"},
		{TriggerActionTimeBefore, "BEFORE"},
		{TriggerActionTimeAfter, "AFTER"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.actionTime.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}

func TestTriggerOrderTypeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		orderType TriggerOrderType
		expected  string
	}{
		{TriggerOrderTypeUnknown, "UNKNOWN"},
		{TriggerOrderTypeFollow, "FOLLOWS"},
		{TriggerOrderTypePrecede, "PRECEDES"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			ctx := NewFmtCtx(FmtSimple)
			tc.orderType.Format(ctx)
			require.Equal(t, tc.expected, ctx.CloseAndGetString())
		})
	}
}
