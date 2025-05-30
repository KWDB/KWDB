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

var pgxBlocklists = blocklistsForVersion{
	{"v19.2", "pgxBlocklist19_2", pgxBlocklist19_2, "pgxIgnorelist19_2", pgxIgnorelist19_2},
	{"v20.1", "pgxBlocklist20_1", pgxBlocklist20_1, "pgxIgnorelist20_1", pgxIgnorelist20_1},
	{"v20.2", "pgxBlocklist20_2", pgxBlocklist20_2, "pgxIgnorelist20_2", pgxIgnorelist20_2},
}

// Please keep these lists alphabetized for easy diffing.
// After a failed run, an updated version of this blocklist should be available
// in the test log.
var pgxBlocklist20_2 = blocklist{}

var pgxBlocklist20_1 = blocklist{
	"v4.Example_CustomType":                                        "27796",
	"v4.TestConnBeginBatchDeferredError":                           "31632",
	"v4.TestConnCopyFromJSON":                                      "19603",
	"v4.TestConnCopyFromLarge":                                     "19603",
	"v4.TestConnCopyFromSmall":                                     "19603",
	"v4.TestConnQueryDeferredError":                                "31632",
	"v4.TestConnQueryErrorWhileReturningRows":                      "26925",
	"v4.TestConnQueryReadRowMultipleTimes":                         "26925",
	"v4.TestConnQueryValues":                                       "26925",
	"v4.TestConnQueryValuesWithUnknownOID":                         "26925",
	"v4.TestConnSendBatch":                                         "44712",
	"v4.TestConnSendBatchWithPreparedStatement":                    "41558",
	"v4.TestConnSimpleProtocol":                                    "21286",
	"v4.TestConnSimpleProtocolRefusesNonStandardConformingStrings": "36215",
	"v4.TestConnSimpleProtocolRefusesNonUTF8ClientEncoding":        "37129",
	"v4.TestDomainType":                                            "27796",
	"v4.TestFatalRxError":                                          "35897",
	"v4.TestFatalTxError":                                          "35897",
	"v4.TestInetCIDRArrayTranscodeIP":                              "18846",
	"v4.TestInetCIDRArrayTranscodeIPNet":                           "18846",
	"v4.TestInetCIDRTranscodeIP":                                   "18846",
	"v4.TestInetCIDRTranscodeIPNet":                                "18846",
	"v4.TestInetCIDRTranscodeWithJustIP":                           "18846",
	"v4.TestLargeObjects":                                          "26725",
	"v4.TestLargeObjectsMultipleTransactions":                      "26725",
	"v4.TestLargeObjectsPreferSimpleProtocol":                      "26725",
	"v4.TestListenNotify":                                          "41522",
	"v4.TestListenNotifySelfNotification":                          "41522",
	"v4.TestListenNotifyWhileBusyIsSafe":                           "41522",
	"v4.TestQueryContextErrorWhileReceivingRows":                   "26925",
	"v4.TestRowDecode":                                             "26925",
	"v4.TestTransactionSuccessfulCommit":                           "31632",
	"v4.TestTransactionSuccessfulRollback":                         "31632",
	"v4.TestTxCommitSerializationFailure":                          "12701",
	"v4.TestTxCommitWhenTxBroken":                                  "31632",
	"v4.TestTxNestedTransactionCommit":                             "31632",
	"v4.TestTxNestedTransactionRollback":                           "31632",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult":   "27796",
}

var pgxBlocklist19_2 = blocklist{
	"v4.Example_CustomType":                                                   "27796",
	"v4.TestConnBeginBatchDeferredError":                                      "31632",
	"v4.TestConnCopyFromCopyFromSourceErrorEnd":                               "5807",
	"v4.TestConnCopyFromCopyFromSourceErrorMidway":                            "5807",
	"v4.TestConnCopyFromFailServerSideMidway":                                 "5807",
	"v4.TestConnCopyFromFailServerSideMidwayAbortsWithoutWaiting":             "5807",
	"v4.TestConnCopyFromJSON":                                                 "5807",
	"v4.TestConnCopyFromLarge":                                                "5807",
	"v4.TestConnCopyFromSlowFailRace":                                         "5807",
	"v4.TestConnCopyFromSmall":                                                "5807",
	"v4.TestConnQueryDatabaseSQLDriverValuerWithAutoGeneratedPointerReceiver": "5807",
	"v4.TestConnQueryDeferredError":                                           "31632",
	"v4.TestConnQueryErrorWhileReturningRows":                                 "26925",
	"v4.TestConnQueryReadRowMultipleTimes":                                    "26925",
	"v4.TestConnQueryValues":                                                  "26925",
	"v4.TestConnQueryValuesWithUnknownOID":                                    "24873",
	"v4.TestConnQueryWithoutResultSetCommandTag":                              "5807",
	"v4.TestConnSendBatch":                                                    "5807",
	"v4.TestConnSendBatchMany":                                                "5807",
	"v4.TestConnSendBatchQueryPartialReadInsert":                              "5807",
	"v4.TestConnSendBatchQueryRowInsert":                                      "5807",
	"v4.TestConnSendBatchWithPreparedStatement":                               "41558",
	"v4.TestConnSimpleProtocol":                                               "21286",
	"v4.TestConnSimpleProtocolRefusesNonStandardConformingStrings":            "36215",
	"v4.TestConnSimpleProtocolRefusesNonUTF8ClientEncoding":                   "37129",
	"v4.TestDomainType":                                                       "27796",
	"v4.TestExec":                                                             "5807",
	"v4.TestExecContextWithoutCancelation":                                    "5807",
	"v4.TestExecExtendedProtocol":                                             "unknown",
	"v4.TestExecSimpleProtocol":                                               "unknown",
	"v4.TestFatalRxError":                                                     "35897",
	"v4.TestFatalTxError":                                                     "35897",
	"v4.TestInetCIDRArrayTranscodeIP":                                         "18846",
	"v4.TestInetCIDRArrayTranscodeIPNet":                                      "18846",
	"v4.TestInetCIDRTranscodeIP":                                              "18846",
	"v4.TestInetCIDRTranscodeIPNet":                                           "18846",
	"v4.TestInetCIDRTranscodeWithJustIP":                                      "18846",
	"v4.TestInsertBoolArray":                                                  "5807",
	"v4.TestInsertTimestampArray":                                             "5807",
	"v4.TestLargeObjects":                                                     "26725",
	"v4.TestLargeObjectsMultipleTransactions":                                 "26725",
	"v4.TestLargeObjectsPreferSimpleProtocol":                                 "26725",
	"v4.TestListenNotify":                                                     "41522",
	"v4.TestListenNotifySelfNotification":                                     "41522",
	"v4.TestListenNotifyWhileBusyIsSafe":                                      "41522",
	"v4.TestQueryContextErrorWhileReceivingRows":                              "26925",
	"v4.TestRowDecode":                                                        "26925",
	"v4.TestTransactionSuccessfulCommit":                                      "31632",
	"v4.TestTransactionSuccessfulRollback":                                    "31632",
	"v4.TestTxCommitSerializationFailure":                                     "5807",
	"v4.TestTxCommitWhenTxBroken":                                             "31632",
	"v4.TestTxNestedTransactionCommit":                                        "31632",
	"v4.TestTxNestedTransactionRollback":                                      "31632",
	"v4.TestTxSendBatch":                                                      "5807",
	"v4.TestTxSendBatchRollback":                                              "5807",
	"v4.TestUnregisteredTypeUsableAsStringArgumentAndBaseResult":              "27796",
}

var pgxIgnorelist20_2 = pgxIgnorelist20_1

var pgxIgnorelist20_1 = blocklist{
	"v4.TestBeginIsoLevels":   "We don't support isolation levels",
	"v4.TestQueryEncodeError": "This test checks the exact error message",
}
var pgxIgnorelist19_2 = blocklist{
	"v4.TestBeginIsoLevels":   "We don't support isolation levels",
	"v4.TestQueryEncodeError": "This test checks the exact error message",
}
