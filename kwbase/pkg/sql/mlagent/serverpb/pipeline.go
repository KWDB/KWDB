// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package serverpb

const (
	// MLUser is for training service.
	MLUser = "kwdbmlusr"
	// MLDefaultDB is default db for connection of MLUser.
	MLDefaultDB = "defaultdb"
	// JobTypeTrain is type for training model.
	JobTypeTrain = "TRAIN"
	// JobTypeEvaluate is type for evaluate model.
	JobTypeEvaluate = "EVALUATE"
	// JobTypeBatchPredict is type for batch predict model.
	JobTypeBatchPredict = "BATCH_PREDICT"
	// JobTypeFineTune is type for fine-tuning model.
	JobTypeFineTune = "FINE_TUNE"

	// JobStatusPending is for jobs that have been created but on which work has
	// not yet started.
	JobStatusPending = "PENDING"
	// JobStatusRunning is for jobs that are currently in progress.
	JobStatusRunning = "RUNNING"
	// JobStatusSucceeded is for jobs that have successfully completed.
	JobStatusSucceeded = "SUCCEEDED"
	// JobStatusFailed is for jobs that failed.
	JobStatusFailed = "FAILED"
	// JobStatusCanceled is for jobs that were explicitly canceled by the user and
	// cannot be resumed.
	JobStatusCanceled = "CANCELED"
	// JobStatusUnknown is for jobs that have error on check job status
	JobStatusUnknown = "N/A"

	// OutputTableActionReplace is the action of delete existing data in the table.
	OutputTableActionReplace = "REPLACE"
	// OutputTableActionAppend is the action of append data in the table.
	OutputTableActionAppend = "APPEND"
	// OutputTableActionError is the action of return error on existing data in the table.
	OutputTableActionError = "ERROR"
)

// JobTypeMap used to check if job type is supported.
var JobTypeMap = map[string]struct{}{
	JobTypeTrain:        {},
	JobTypeEvaluate:     {},
	JobTypeBatchPredict: {},
	JobTypeFineTune:     {},
}

// JobStatusMap used to check if job status is supported.
var JobStatusMap = map[string]struct{}{
	JobStatusPending:   {},
	JobStatusRunning:   {},
	JobStatusSucceeded: {},
	JobStatusFailed:    {},
	JobStatusCanceled:  {},
}

// OutputTableAction used to check if output action is supported.
var OutputTableAction = map[string]struct{}{
	OutputTableActionReplace: {},
	OutputTableActionAppend:  {},
	OutputTableActionError:   {},
}
