#! /usr/bin/env expect -f

source [file join [file dirname $argv0] common.tcl]

start_test "Check kwbase demo --with-load runs the movr workload"

# Start demo with movr and the movr workload.
spawn $argv demo movr --with-load

eexpect "movr>"

# Try a few times, but expect that we eventually see the workload
# queries show up as the highest count queries in the system.

set workloadRunning 0

for {set i 0} {$i < 10} {incr i} {
  set timeout 1
  send "select key from kwdb_internal.node_statement_statistics order by count desc limit 1;\r"
  expect {
    "SELECT city, id FROM vehicles WHERE city = \$1" {
      set workloadRunning 1
      break
    }
    timeout {}
  }
}

if {!$workloadRunning} {
  report "Workload is not running"
  exit 1
}

interrupt
eexpect eof
end_test

# Ensure that kwbase demo with the movr workload can control the number of ranges that tables are split into.
start_test "Check that controlling ranges of the movr dataset works"
# Reset the timeout.
set timeout 30
spawn $argv demo movr --num-ranges=6

eexpect "movr>"

send "SELECT count(*) FROM \[SHOW RANGES FROM TABLE USERS\];\r"
eexpect "6"
eexpect "(1 row)"

interrupt
eexpect eof
end_test
