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

package sqltelemetry_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

func TestSerialColumnNormalizationCounter(t *testing.T) {
	inputTypes := []string{"INT", "BIGINT", "SMALLINT"}
	normTypes := []string{"SERIAL", "BIGSERIAL", "SMALLSERIAL"}

	for _, inputType := range inputTypes {
		for _, normType := range normTypes {
			counter := sqltelemetry.SerialColumnNormalizationCounter(inputType, normType)
			if counter == nil {
				t.Errorf("SerialColumnNormalizationCounter with inputType '%s' and normType '%s' should not return nil", inputType, normType)
			}
		}
	}
}

func TestSchemaNewTypeCounter(t *testing.T) {
	types := []string{"INT", "TEXT", "VARCHAR", "DATE", "TIMESTAMP"}

	for _, typ := range types {
		counter := sqltelemetry.SchemaNewTypeCounter(typ)
		if counter == nil {
			t.Errorf("SchemaNewTypeCounter with type '%s' should not return nil", typ)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaNewTypeCounter("")
	if counter == nil {
		t.Error("SchemaNewTypeCounter with empty string should not return nil")
	}
}

func TestCreateInterleavedTableCounter(t *testing.T) {
	counter := sqltelemetry.CreateInterleavedTableCounter
	if counter == nil {
		t.Error("CreateInterleavedTableCounter should not be nil")
	}
}

func TestCreateTempTableCounter(t *testing.T) {
	counter := sqltelemetry.CreateTempTableCounter
	if counter == nil {
		t.Error("CreateTempTableCounter should not be nil")
	}
}

func TestCreateTempSequenceCounter(t *testing.T) {
	counter := sqltelemetry.CreateTempSequenceCounter
	if counter == nil {
		t.Error("CreateTempSequenceCounter should not be nil")
	}
}

func TestCreateTempViewCounter(t *testing.T) {
	counter := sqltelemetry.CreateTempViewCounter
	if counter == nil {
		t.Error("CreateTempViewCounter should not be nil")
	}
}

func TestHashShardedIndexCounter(t *testing.T) {
	counter := sqltelemetry.HashShardedIndexCounter
	if counter == nil {
		t.Error("HashShardedIndexCounter should not be nil")
	}
}

func TestInvertedIndexCounter(t *testing.T) {
	counter := sqltelemetry.InvertedIndexCounter
	if counter == nil {
		t.Error("InvertedIndexCounter should not be nil")
	}
}

func TestTempObjectCleanerDeletionCounter(t *testing.T) {
	counter := sqltelemetry.TempObjectCleanerDeletionCounter
	if counter == nil {
		t.Error("TempObjectCleanerDeletionCounter should not be nil")
	}
}

func TestSchemaNewColumnTypeQualificationCounter(t *testing.T) {
	quals := []string{"PRIMARY KEY", "UNIQUE", "NOT NULL", "DEFAULT"}

	for _, qual := range quals {
		counter := sqltelemetry.SchemaNewColumnTypeQualificationCounter(qual)
		if counter == nil {
			t.Errorf("SchemaNewColumnTypeQualificationCounter with qualification '%s' should not return nil", qual)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaNewColumnTypeQualificationCounter("")
	if counter == nil {
		t.Error("SchemaNewColumnTypeQualificationCounter with empty string should not return nil")
	}
}

func TestSchemaChangeCreateCounter(t *testing.T) {
	types := []string{"table", "index", "view", "sequence"}

	for _, typ := range types {
		counter := sqltelemetry.SchemaChangeCreateCounter(typ)
		if counter == nil {
			t.Errorf("SchemaChangeCreateCounter with type '%s' should not return nil", typ)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaChangeCreateCounter("")
	if counter == nil {
		t.Error("SchemaChangeCreateCounter with empty string should not return nil")
	}
}

func TestSchemaChangeDropCounter(t *testing.T) {
	types := []string{"table", "index", "view", "sequence"}

	for _, typ := range types {
		counter := sqltelemetry.SchemaChangeDropCounter(typ)
		if counter == nil {
			t.Errorf("SchemaChangeDropCounter with type '%s' should not return nil", typ)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaChangeDropCounter("")
	if counter == nil {
		t.Error("SchemaChangeDropCounter with empty string should not return nil")
	}
}

func TestSchemaSetZoneConfigCounter(t *testing.T) {
	configNames := []string{"gc", "num_replicas", "constraints"}
	keyChanges := []string{"set", "remove", "update"}

	for _, configName := range configNames {
		for _, keyChange := range keyChanges {
			counter := sqltelemetry.SchemaSetZoneConfigCounter(configName, keyChange)
			if counter == nil {
				t.Errorf("SchemaSetZoneConfigCounter with configName '%s' and keyChange '%s' should not return nil", configName, keyChange)
			}
		}
	}

	// Test with empty strings
	counter := sqltelemetry.SchemaSetZoneConfigCounter("", "")
	if counter == nil {
		t.Error("SchemaSetZoneConfigCounter with empty strings should not return nil")
	}
}

func TestSchemaChangeAlterCounter(t *testing.T) {
	types := []string{"table", "database", "index", "view"}

	for _, typ := range types {
		counter := sqltelemetry.SchemaChangeAlterCounter(typ)
		if counter == nil {
			t.Errorf("SchemaChangeAlterCounter with type '%s' should not return nil", typ)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaChangeAlterCounter("")
	if counter == nil {
		t.Error("SchemaChangeAlterCounter with empty string should not return nil")
	}
}

func TestSchemaChangeAlterCounterWithExtra(t *testing.T) {
	types := []string{"table", "database"}
	extras := []string{"add_column", "drop_column", "rename"}

	for _, typ := range types {
		for _, extra := range extras {
			counter := sqltelemetry.SchemaChangeAlterCounterWithExtra(typ, extra)
			if counter == nil {
				t.Errorf("SchemaChangeAlterCounterWithExtra with type '%s' and extra '%s' should not return nil", typ, extra)
			}
		}
	}

	// Test with empty strings
	counter := sqltelemetry.SchemaChangeAlterCounterWithExtra("", "")
	if counter == nil {
		t.Error("SchemaChangeAlterCounterWithExtra with empty strings should not return nil")
	}
}

func TestSchemaSetAuditModeCounter(t *testing.T) {
	modes := []string{"all", "read_write", "none"}

	for _, mode := range modes {
		counter := sqltelemetry.SchemaSetAuditModeCounter(mode)
		if counter == nil {
			t.Errorf("SchemaSetAuditModeCounter with mode '%s' should not return nil", mode)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaSetAuditModeCounter("")
	if counter == nil {
		t.Error("SchemaSetAuditModeCounter with empty string should not return nil")
	}
}

func TestSchemaJobControlCounter(t *testing.T) {
	statuses := []string{"started", "completed", "failed", "paused", "resumed"}

	for _, status := range statuses {
		counter := sqltelemetry.SchemaJobControlCounter(status)
		if counter == nil {
			t.Errorf("SchemaJobControlCounter with status '%s' should not return nil", status)
		}
	}

	// Test with empty string
	counter := sqltelemetry.SchemaJobControlCounter("")
	if counter == nil {
		t.Error("SchemaJobControlCounter with empty string should not return nil")
	}
}

func TestSchemaChangeInExplicitTxnCounter(t *testing.T) {
	counter := sqltelemetry.SchemaChangeInExplicitTxnCounter
	if counter == nil {
		t.Error("SchemaChangeInExplicitTxnCounter should not be nil")
	}
}

func TestSecondaryIndexColumnFamiliesCounter(t *testing.T) {
	counter := sqltelemetry.SecondaryIndexColumnFamiliesCounter
	if counter == nil {
		t.Error("SecondaryIndexColumnFamiliesCounter should not be nil")
	}
}
