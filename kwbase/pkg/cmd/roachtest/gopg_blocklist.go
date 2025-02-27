// Copyright 2019 The Cockroach Authors.
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
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package main

var gopgBlocklists = blocklistsForVersion{
	{"v19.2", "gopgBlockList19_2", gopgBlockList19_2, "gopgIgnoreList19_2", gopgIgnoreList19_2},
	{"v20.1", "gopgBlockList20_1", gopgBlockList20_1, "gopgIgnoreList20_1", gopgIgnoreList20_1},
	{"v20.2", "gopgBlockList20_2", gopgBlockList20_2, "gopgIgnoreList20_2", gopgIgnoreList20_2},
}

// These are lists of known gopg test errors and failures.
// When the gopg test suite is run, the results are compared to this list.
// Any failed test that is on this list is reported as FAIL - expected.
// Any failed test that is not on this list is reported as FAIL - unexpected.
//
// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.

var gopgBlockList20_2 = gopgBlockList20_1

var gopgBlockList20_1 = blocklist{
	"pg | BeforeQuery and AfterQuery CopyFrom | is called for CopyFrom with model":    "41608",
	"pg | BeforeQuery and AfterQuery CopyFrom | is called for CopyFrom without model": "41608",
	"pg | BeforeQuery and AfterQuery CopyTo | is called for CopyTo with model":        "41608",
	"pg | BeforeQuery and AfterQuery CopyTo | is called for CopyTo without model":     "41608",
	"pg | CopyFrom/CopyTo | copies corrupted data to a table":                         "41608",
	"pg | CopyFrom/CopyTo | copies data from a table and to a table":                  "41608",
	"pg | CountEstimate | works":                                      "17511",
	"pg | CountEstimate | works when there are no results":            "17511",
	"pg | CountEstimate | works with GROUP":                           "17511",
	"pg | CountEstimate | works with GROUP when there are no results": "17511",
	"pg | Listener | is closed when DB is closed":                     "41522",
	"pg | Listener | listens for notifications":                       "41522",
	"pg | Listener | reconnects on receive error":                     "41522",
	"pg | Listener | returns an error on timeout":                     "41522",
	"pg | Listener | supports concurrent Listen and Receive":          "41522",
	"v10.ExampleDB_Model_postgresArrayStructTag":                      "32552",
	"v10.TestBigColumn":                                               "41608",
	"v10.TestConversion":                                              "32552",
	"v10.TestGinkgo":                                                  "41522",
	"v10.TestGocheck":                                                 "17511",
	"v10.TestReadColumnValue":                                         "26925",
	"v10.TestUnixSocket":                                              "31113",
}

var gopgBlockList19_2 = blocklist{
	"pg | BeforeQuery and AfterQuery | CopyFrom is called for CopyFrom with model":         "5807",
	"pg | BeforeQuery and AfterQuery | CopyFrom is called for CopyFrom without model":      "5807",
	"pg | BeforeQuery and AfterQuery | CopyTo is called for CopyTo with model":             "5807",
	"pg | BeforeQuery and AfterQuery | CopyTo is called for CopyTo without model":          "5807",
	"pg | BeforeQuery and AfterQuery | Model is called for Model":                          "5807",
	"pg | BeforeQuery and AfterQuery | Query/Exec is called for Exec":                      "5807",
	"pg | BeforeQuery and AfterQuery | Query/Exec is called for Query":                     "5807",
	"pg | BeforeQuery and AfterQuery | model params is called for Model":                   "5807",
	"pg | CopyFrom/CopyTo | copies corrupted data to a table":                              "5807",
	"pg | CopyFrom/CopyTo | copies data from a table and to a table":                       "5807",
	"pg | CountEstimate | works":                                                           "17511",
	"pg | CountEstimate | works when there are no results":                                 "17511",
	"pg | CountEstimate | works with GROUP":                                                "17511",
	"pg | CountEstimate | works with GROUP when there are no results":                      "17511",
	"pg | DB nulls | nil ptr inserts non-null value":                                       "5807",
	"pg | DB nulls | nil ptr inserts null value":                                           "5807",
	"pg | DB nulls | sql.NullInt64 inserts non-null value":                                 "5807",
	"pg | DB nulls | sql.NullInt64 inserts null value":                                     "5807",
	"pg | DB uint64 in struct field | is appended and scanned as int64":                    "5807",
	"pg | DB.Select | selects bytea":                                                       "5807",
	"pg | DB.Select | selects into embedded struct pointer":                                "5807",
	"pg | HookTest | calls AfterSelect for a slice model":                                  "5807",
	"pg | HookTest | calls AfterSelect for a struct model":                                 "5807",
	"pg | HookTest | calls BeforeDelete and AfterDelete":                                   "5807",
	"pg | HookTest | calls BeforeInsert and AfterInsert":                                   "5807",
	"pg | HookTest | calls BeforeUpdate and AfterUpdate":                                   "5807",
	"pg | HookTest | does not call BeforeDelete and AfterDelete for nil model":             "5807",
	"pg | HookTest | does not call BeforeUpdate and AfterUpdate for nil model":             "5807",
	"pg | Listener | is closed when DB is closed":                                          "41522",
	"pg | Listener | listens for notifications":                                            "41522",
	"pg | Listener | reconnects on receive error":                                          "41522",
	"pg | Listener | returns an error on timeout":                                          "41522",
	"pg | Listener | supports concurrent Listen and Receive":                               "41522",
	"pg | soft delete with int column | model Deleted allows to select deleted model":      "5807",
	"pg | soft delete with int column | model ForceDelete deletes the model":               "5807",
	"pg | soft delete with int column | model soft deletes the model":                      "5807",
	"pg | soft delete with int column | nil model Deleted allows to select deleted model":  "5807",
	"pg | soft delete with int column | nil model ForceDelete deletes the model":           "5807",
	"pg | soft delete with int column | nil model soft deletes the model":                  "5807",
	"pg | soft delete with time column | model Deleted allows to select deleted model":     "5807",
	"pg | soft delete with time column | model ForceDelete deletes the model":              "5807",
	"pg | soft delete with time column | model soft deletes the model":                     "5807",
	"pg | soft delete with time column | nil model Deleted allows to select deleted model": "5807",
	"pg | soft delete with time column | nil model ForceDelete deletes the model":          "5807",
	"pg | soft delete with time column | nil model soft deletes the model":                 "5807",
	"v10.ExampleDB_Model_postgresArrayStructTag":                                           "32552",
	"v10.TestBigColumn":       "41608",
	"v10.TestConversion":      "32552",
	"v10.TestGinkgo":          "41522",
	"v10.TestGocheck":         "17511",
	"v10.TestReadColumnValue": "26925",
	"v10.TestUnixSocket":      "31113",
}

var gopgIgnoreList20_2 = gopgIgnoreList20_1

var gopgIgnoreList20_1 = gopgIgnoreList19_2

var gopgIgnoreList19_2 = blocklist{
	// These "fetching" tests assume a particular order when ORDER BY clause is
	// omitted from the query by the ORM itself.
	"pg | ORM slice model | fetches Book relations":       "41690",
	"pg | ORM slice model | fetches Genre relations":      "41690",
	"pg | ORM slice model | fetches Translation relation": "41690",
	"pg | ORM struct model | fetches Author relations":    "41690",
	"pg | ORM struct model | fetches Book relations":      "41690",
	"pg | ORM struct model | fetches Genre relations":     "41690",
	// Different error message for context cancellation timeout.
	"pg | OnConnect | does not panic on timeout": "41690",
	// These tests assume different transaction isolation level (READ COMMITTED).
	"pg | Tx | supports CopyFrom and CopyIn":             "41690",
	"pg | Tx | supports CopyFrom and CopyIn with errors": "41690",
	// These tests sometimes failed and we haven't diagnosed it
	"pg | DB race | SelectOrInsert with OnConflict is race free":    "unknown",
	"pg | DB race | SelectOrInsert without OnConflict is race free": "unknown",
}
