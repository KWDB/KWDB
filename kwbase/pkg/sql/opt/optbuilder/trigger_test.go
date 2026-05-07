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

package optbuilder

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestIsTriggerUpdate tests the isTriggerUpdate function
func TestIsTriggerUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name           string
		setupBuilder   func() *Builder
		expectedResult bool
	}{
		{
			name: "trigger is update",
			setupBuilder: func() *Builder {
				b := &Builder{
					TriggerInfo: &TriggerBuilder{
						TriggerTab: map[cat.StableID]TriggerTab{
							1: {
								CurTriggerTyp: tree.TriggerEventUpdate,
							},
						},
						CurTriggerTabID: 1,
					},
				}
				return b
			},
			expectedResult: true,
		},
		{
			name: "trigger is insert",
			setupBuilder: func() *Builder {
				b := &Builder{
					TriggerInfo: &TriggerBuilder{
						TriggerTab: map[cat.StableID]TriggerTab{
							1: {
								CurTriggerTyp: tree.TriggerEventInsert,
							},
						},
						CurTriggerTabID: 1,
					},
				}
				return b
			},
			expectedResult: false,
		},
		{
			name: "trigger is delete",
			setupBuilder: func() *Builder {
				b := &Builder{
					TriggerInfo: &TriggerBuilder{
						TriggerTab: map[cat.StableID]TriggerTab{
							1: {
								CurTriggerTyp: tree.TriggerEventDelete,
							},
						},
						CurTriggerTabID: 1,
					},
				}
				return b
			},
			expectedResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.setupBuilder()
			result := b.isTriggerUpdate()
			if result != tc.expectedResult {
				t.Errorf("Expected %v, got %v", tc.expectedResult, result)
			}
		})
	}
}

// TestColNewOldType tests the colNewOldType constants
func TestColNewOldType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test the bit flag values
	if newCol != 1 {
		t.Errorf("Expected newCol to be 1, got %d", newCol)
	}
	if oldCol != 2 {
		t.Errorf("Expected oldCol to be 2, got %d", oldCol)
	}

	// Test bitwise OR
	combined := newCol | oldCol
	if combined != 3 {
		t.Errorf("Expected combined value to be 3, got %d", combined)
	}
}

// TestTriggerTabStructure tests the TriggerTab structure
func TestTriggerTabStructure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	triggerTab := TriggerTab{
		ColMetas:       []opt.ColumnMeta{},
		CurTriggerTyp:  tree.TriggerEventInsert,
		CurActionTime:  tree.TriggerActionTimeBefore,
		PlaceholderTyp: tree.PlaceholderTypes{},
	}

	if triggerTab.CurTriggerTyp != tree.TriggerEventInsert {
		t.Errorf("Expected CurTriggerTyp to be TriggerEventInsert, got %v", triggerTab.CurTriggerTyp)
	}

	if triggerTab.CurActionTime != tree.TriggerActionTimeBefore {
		t.Errorf("Expected CurActionTime to be TriggerActionTimeBefore, got %v", triggerTab.CurActionTime)
	}
}

// TestTriggerColConstants tests the trigger column constants
func TestTriggerColConstants(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if triggerColNew != "new" {
		t.Errorf("Expected triggerColNew to be 'new', got %s", triggerColNew)
	}

	if triggerColOld != "old" {
		t.Errorf("Expected triggerColOld to be 'old', got %s", triggerColOld)
	}
}

// TestBuildProcCommandForTriggers tests the buildProcCommandForTriggers function
func TestBuildProcCommandForTriggers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// 测试用例1: 没有触发器的情况
	t.Run("no triggers", func(t *testing.T) {
		// 创建一个简单的 Builder 实例
		b := &Builder{
			TriggerInfo: &TriggerBuilder{
				TriggerTab: make(map[cat.StableID]TriggerTab),
			},
			semaCtx: &tree.SemaContext{
				TriggerColHolders: tree.PlaceholderInfo{},
			},
			TSInfo:    &TSBuilder{},
			TableType: make(map[tree.TableType]int),
		}

		// 创建一个简单的测试表 (需要实现 cat.Table 接口)
		testTab := &testTriggerTable{}

		// 初始化 outScope
		outScope := &scope{}
		outScope.builder = b

		mb := &mutationBuilder{
			b:             b,
			tab:           testTab,
			tabID:         opt.TableID(testTab.ID()),
			alias:         tree.TableName{},
			outScope:      outScope,
			targetColList: opt.ColList{},
			targetColSet:  opt.ColSet{},
			insertOrds:    []scopeOrdinal{},
		}

		// 调用函数
		result := mb.buildProcCommandForTriggers(tree.TriggerEventInsert)

		// 验证结果
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
		if len(result.Bodys) != 0 {
			t.Errorf("Expected 0 trigger commands, got %d", len(result.Bodys))
		}
	})

	// 测试用例2: 有触发器的情况
	t.Run("with triggers", func(t *testing.T) {
		// 创建一个简单的 Builder 实例
		b := &Builder{
			TriggerInfo: &TriggerBuilder{
				TriggerTab: make(map[cat.StableID]TriggerTab),
			},
			semaCtx: &tree.SemaContext{
				TriggerColHolders: tree.PlaceholderInfo{},
			},
			TSInfo:    &TSBuilder{},
			TableType: make(map[tree.TableType]int),
		}

		// 创建一个带有触发器的测试表
		testTab := &testTriggerTableWithTriggers{}

		// 初始化 outScope
		outScope := &scope{}
		outScope.builder = b

		mb := &mutationBuilder{
			b:             b,
			tab:           testTab,
			tabID:         opt.TableID(testTab.ID()),
			alias:         tree.TableName{},
			outScope:      outScope,
			targetColList: opt.ColList{},
			targetColSet:  opt.ColSet{},
			insertOrds:    []scopeOrdinal{},
		}

		// 调用函数
		result := mb.buildProcCommandForTriggers(tree.TriggerEventInsert)

		// 验证结果
		if result == nil {
			t.Errorf("Expected non-nil result")
		}
		// 这里我们期望有一个触发器命令
		if len(result.Bodys) != 1 {
			t.Errorf("Expected 1 trigger command, got %d", len(result.Bodys))
		}
	})
}

// testTriggerTableWithTriggers 是一个带有触发器的测试表实现
type testTriggerTableWithTriggers struct {
	testTriggerTable
}

// GetTriggers 返回一个触发器定义
func (t *testTriggerTableWithTriggers) GetTriggers(event tree.TriggerEvent) []cat.TriggerMeta {
	// 只在 INSERT 事件时返回触发器
	if event == tree.TriggerEventInsert {
		return []cat.TriggerMeta{
			{
				TriggerID:   1,
				TriggerName: "test_trigger",
				ActionTime:  tree.TriggerActionTimeBefore,
				Event:       tree.TriggerEventInsert,
				Body:        "CREATE TRIGGER test_trigger BEFORE INSERT ON test_table FOR EACH ROW BEGIN END;",
			},
		}
	}
	return nil
}

// testTriggerTable 是一个用于测试的简单 cat.Table 实现
type testTriggerTable struct{}

// testColumn 是一个用于测试的简单 cat.Column 实现
type testColumn struct {
	colID     cat.StableID
	colName   tree.Name
	datumType *types.T
}

// 实现 cat.Column 接口的方法
func (c *testColumn) ColID() cat.StableID {
	return c.colID
}

func (c *testColumn) ColName() tree.Name {
	return c.colName
}

func (c *testColumn) DatumType() *types.T {
	return c.datumType
}

func (c *testColumn) IsNullable() bool {
	return true
}

func (c *testColumn) IsHidden() bool {
	return false
}

func (c *testColumn) IsComputed() bool {
	return false
}

func (c *testColumn) IsGenerated() bool {
	return false
}

func (c *testColumn) IsVirtualComputed() bool {
	return false
}

func (c *testColumn) IsStored() bool {
	return false
}

func (c *testColumn) IsExpressionIndex() bool {
	return false
}

func (c *testColumn) IsPrimaryKey() bool {
	return false
}

func (c *testColumn) IsUnique() bool {
	return false
}

func (c *testColumn) IsIndexed() bool {
	return false
}

func (c *testColumn) IsForeignKey() bool {
	return false
}

func (c *testColumn) IsArray() bool {
	return false
}

func (c *testColumn) HasDefault() bool {
	return false
}

func (c *testColumn) DefaultExpr() tree.Expr {
	return nil
}

func (c *testColumn) ComputedExpr() tree.Expr {
	return nil
}

func (c *testColumn) GeneratedExpr() tree.Expr {
	return nil
}

func (c *testColumn) Collation() string {
	return ""
}

func (c *testColumn) FamilyID() int {
	return 0
}

func (c *testColumn) OriginColumn() cat.Column {
	return nil
}

func (c *testColumn) ColTypePrecision() int {
	return 0
}

func (c *testColumn) ColTypeStr() string {
	return c.datumType.String()
}

func (c *testColumn) ColTypeWidth() int {
	return 0
}

func (c *testColumn) ComputedExprStr() string {
	return ""
}

func (c *testColumn) DefaultExprStr() string {
	return ""
}

func (c *testColumn) IsOrdinaryTagCol() bool {
	return false
}

func (c *testColumn) IsPrimaryTagCol() bool {
	return false
}

func (c *testColumn) IsTagCol() bool {
	return false
}

func (c *testColumn) TsColStorgeLen() uint64 {
	return 0
}

// 实现 cat.Object 接口的方法
func (t *testTriggerTable) ID() cat.StableID {
	return cat.StableID(1)
}

func (t *testTriggerTable) PostgresDescriptorID() cat.StableID {
	return cat.StableID(1)
}

func (t *testTriggerTable) Equals(other cat.Object) bool {
	return false
}

// 实现 cat.DataSource 接口的方法
func (t *testTriggerTable) Name() tree.Name {
	return "test_table"
}

// 实现 cat.Table 接口的方法
func (t *testTriggerTable) IsVirtualTable() bool {
	return false
}

func (t *testTriggerTable) IsMaterializedView() bool {
	return false
}

func (t *testTriggerTable) IsInterleaved() bool {
	return false
}

func (t *testTriggerTable) ColumnCount() int {
	return 1 // 返回一个列
}

func (t *testTriggerTable) WritableColumnCount() int {
	return 1
}

func (t *testTriggerTable) DeletableColumnCount() int {
	return 1
}

func (t *testTriggerTable) Column(i int) cat.Column {
	// 返回一个简单的列
	return &testColumn{
		colID:     cat.StableID(1),
		colName:   "id",
		datumType: types.Int,
	}
}

func (t *testTriggerTable) IndexCount() int {
	return 0
}

func (t *testTriggerTable) WritableIndexCount() int {
	return 0
}

func (t *testTriggerTable) DeletableIndexCount() int {
	return 0
}

func (t *testTriggerTable) Index(i cat.IndexOrdinal) cat.Index {
	return nil
}

func (t *testTriggerTable) StatisticCount() int {
	return 0
}

func (t *testTriggerTable) Statistic(i int) cat.TableStatistic {
	return nil
}

func (t *testTriggerTable) CheckCount() int {
	return 0
}

func (t *testTriggerTable) Check(i int) cat.CheckConstraint {
	return cat.CheckConstraint{}
}

func (t *testTriggerTable) FamilyCount() int {
	return 0
}

func (t *testTriggerTable) Family(i int) cat.Family {
	return nil
}

func (t *testTriggerTable) OutboundForeignKeyCount() int {
	return 0
}

func (t *testTriggerTable) OutboundForeignKey(i int) cat.ForeignKeyConstraint {
	return nil
}

func (t *testTriggerTable) InboundForeignKeyCount() int {
	return 0
}

func (t *testTriggerTable) InboundForeignKey(i int) cat.ForeignKeyConstraint {
	return nil
}

func (t *testTriggerTable) GetTriggers(event tree.TriggerEvent) []cat.TriggerMeta {
	return nil // 返回空触发器列表
}

// 实现其他需要的方法
func (t *testTriggerTable) Zone() cat.Zone {
	return nil
}

func (t *testTriggerTable) IsTSTable() bool {
	return false
}

func (t *testTriggerTable) IsInstanceTable() bool {
	return false
}

func (t *testTriggerTable) HasPrimaryTag() bool {
	return false
}

func (t *testTriggerTable) GetPrimaryTagIndex() int {
	return -1
}

func (t *testTriggerTable) GetTSPrecision() int64 {
	return 0
}

func (t *testTriggerTable) IsAutoPartition() bool {
	return false
}

func (t *testTriggerTable) IsTagTable() bool {
	return false
}

func (t *testTriggerTable) GetParentTagID() cat.StableID {
	return 0
}

func (t *testTriggerTable) GetParentTableName() string {
	return ""
}

func (t *testTriggerTable) GetParentID() tree.ID {
	return 0
}

func (t *testTriggerTable) GetTSHashNum() uint64 {
	return 0
}

func (t *testTriggerTable) GetTSVersion() uint32 {
	return 0
}

func (t *testTriggerTable) GetTagMeta() []cat.TagMeta {
	return nil
}

func (t *testTriggerTable) SetTableName(name string) {
}

func (t *testTriggerTable) GetTableType() tree.TableType {
	return 0
}
