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
#include "cm_assert.h"

#include <execinfo.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <mutex>
#include "cm_func.h"
#include "cm_kwdb_context.h"

namespace kwdbts {

// TODO(KWDB) Initialize the context global object and use it elsewhere by referring to
//  extern kwdbContext_p g_kwdb_context;
kwdbContext_t _ctx;
kwdbContext_p g_kwdb_context = &_ctx;

/* dump call stack to a file */
void dumpCallStack(FILE* file) {
  void *frames[MAX_STACK_TRACE_SIZE];
  int num_traces;
  char **traces;
  num_traces = backtrace(frames, MAX_STACK_TRACE_SIZE);
  traces = backtrace_symbols(frames, num_traces);
  if (traces) {
    for (int i = 0; i < num_traces; i++) {
      fprintf(file, "%s\n", traces[i]);
      // Add this printf to output specific errors when the CI pipeline triggers assert, as shown below.
      printf("%s\n", traces[i]);
    }
    fprintf(file, "--End of Call Stack--\n\n");
    printf("--End of Call Stack--\n\n");
  }
  free(traces);
}

// Print an error message
// TODO(KWDB) Call the error module interface and place error in the error stack
void push_error(int err, char* msg) {
  printf("%d:%s", err, msg);
}

// Record the online packets of the error location to the log
// TODO(KWDB) Adds information about thread coding
void dumpAssertContext(  //  KContextP ctx,
                       const char* assert_file,
                       const char* fun,
                       int line,
                       const char* x) {
  // Lock, control write operations to log files
  std::unique_lock<std::shared_mutex>  ulock(g_kwdb_context->mutex);
  // Opens the file in append mode
  // TODO(KWDB) When the file is opened during program initialization,
  //  the file is no longer opened and closed inside the function
  FILE* file = fopen(g_kwdb_context->assert_file_name, "a");
  // Write the current time of the error first
  time_t curr_time;
  char time_buffer[32];
  struct tm curr_time_info;
  curr_time = time(NULL);
  localtime_r(&curr_time, &curr_time_info);
  strftime(time_buffer, 32, "%Y-%m-%d %H:%M:%S", &curr_time_info);
  fprintf(file, "\n%s:\n", time_buffer);
  printf("\n%s:\n", time_buffer);
  // Then write the location of the error in the file and the error condition
  snprintf(g_kwdb_context->msg_buf, sizeof(g_kwdb_context->msg_buf),
      "\nAssert(%s:%s:%d):%s\n", assert_file, fun, line, x);
  fprintf(file, "%s", g_kwdb_context->msg_buf);
  printf("%s", g_kwdb_context->msg_buf);
  // Then write the call stack information of the current program
  dumpCallStack(file);
  push_error(ERR_ASSERT, g_kwdb_context->msg_buf);
  // Close file
  fclose(file);
}

}  //  namespace kwdbts
