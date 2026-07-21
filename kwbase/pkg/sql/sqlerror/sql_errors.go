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

package sqlerror

import (
	"errors"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
)

var (
	// ErrEmptyDatabaseName indicates an empty database name was provided.
	ErrEmptyDatabaseName = pgerror.New(pgcode.Syntax, "empty database name")
	// ErrNoDatabase indicates no database was specified.
	ErrNoDatabase = pgerror.New(pgcode.InvalidName, "no database specified")
	// ErrNoSchema indicates no schema was specified.
	ErrNoSchema = pgerror.Newf(pgcode.InvalidName, "no schema specified")
	// ErrNoProcedure indicates no procedure was specified.
	ErrNoProcedure = pgerror.New(pgcode.InvalidName, "no procedure specified")
	// ErrNoTable indicates no table was specified.
	ErrNoTable = pgerror.New(pgcode.InvalidName, "no table specified")
	// ErrNoMatch indicates no object matched the query.
	ErrNoMatch = pgerror.New(pgcode.UndefinedObject, "no object matched")
	// ErrEmptyTriggerName indicates an empty trigger name was provided.
	ErrEmptyTriggerName = pgerror.New(pgcode.Syntax, "empty trigger name")
	// ErrNoPrimaryKey indicates the requested table does not have a primary key.
	ErrNoPrimaryKey = errors.New("requested table does not have a primary key")

	// ErrTableAdding indicates the table is being added.
	ErrTableAdding = errors.New("table is being added")
	// ErrTableDropped indicates the table is being dropped.
	ErrTableDropped = errors.New("table is being dropped")

	// InvalidClusterForShardedIndexError indicates hash sharded indexes can only be
	// created on a cluster that has fully migrated.
	InvalidClusterForShardedIndexError = pgerror.New(pgcode.FeatureNotSupported,
		"hash sharded indexes can only be created on a cluster that has fully migrated to version 20.1")

	// HashShardedIndexesDisabledError indicates hash sharded indexes require the
	// experimental_enable_hash_sharded_indexes cluster setting.
	HashShardedIndexesDisabledError = pgerror.New(pgcode.FeatureNotSupported,
		"hash sharded indexes require the experimental_enable_hash_sharded_indexes cluster setting")
)
