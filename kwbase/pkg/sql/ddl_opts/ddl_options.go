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

package ddlopts

type shouldOmitFKClausesFromCreate int

const (
	_ shouldOmitFKClausesFromCreate = iota
	// OmitFKClausesFromCreate will not include any foreign key information in the
	// create statement.
	OmitFKClausesFromCreate
	// IncludeFkClausesInCreate will include foreign key information in the create
	// statement, and error if a FK cannot be resolved.
	IncludeFkClausesInCreate
	// OmitMissingFKClausesFromCreate will include foreign key information only if they
	// can be resolved. If not, it will ignore those constraints.
	// This is used in the case when showing the create statement for
	// tables stored in backups. Not all relevant tables may have been
	// included in the back up, so some foreign key information may be
	// impossible to retrieve.
	OmitMissingFKClausesFromCreate
)

// ShowCreateDisplayOptions is a container struct holding the options that
// ShowCreate uses to determine how much information should be included in the
// CREATE statement.
type ShowCreateDisplayOptions struct {
	FKDisplayMode shouldOmitFKClausesFromCreate
	// Comment resolution requires looking up table data from system.comments
	// table. This is sometimes not possible. For example, in the context of a
	// SHOW BACKUP which may resolve the create statement, there is no mechanism
	// to read any table data from the backup (nor is there a guarantee that the
	// system.comments table is included in the backup at all).
	IgnoreComments bool
}

// AutoCommitOpt specifies the auto-commit behavior for DDL operations
type AutoCommitOpt int

const (
	// AutoCommitDisabled meand auto commit disabled
	AutoCommitDisabled AutoCommitOpt = 0
	// AutoCommitEnabled meand auto commit enabled
	AutoCommitEnabled AutoCommitOpt = 1
)
