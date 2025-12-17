#!/bin/bash

#log level debug-1, info-2, warn-3, error-4, always-5
LOG_LEVEL=2

#log file
LOG_FILE=""

function format_print() {
  printf "                                                                                                                           \r"
}

function log_init() {
  local kw_log_path=$1
  local user=$2
  local kw_log_date=$(date '+%Y-%m-%d')
  if [ ! -e $kw_log_path/log ];then
    eval $local_cmd_prefix mkdir -p $kw_log_path/log
    sudo chown -R $user:$user $kw_log_path/log
    sudo chmod 755 $kw_log_path/log
  fi
  if [ ! -e "$kw_log_path/log/$kw_log_date" ];then
    eval $local_cmd_prefix touch $kw_log_path/log/$kw_log_date
    sudo chown $user:$user $kw_log_path/log/$kw_log_date
    sudo chmod 755 $kw_log_path/log/$kw_log_date
  fi
  LOG_FILE="$kw_log_path/log/$kw_log_date"
}

#debug log function
function log_debug(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 1  ] && echo $content >> $LOG_FILE && echo -e "\033[32m"  ${content}  "\033[0m"
}
#info log function
function log_info(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[INFO] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 2  ] && echo $content >> $LOG_FILE && echo -e "\033[32m"  ${content} "\033[0m"
}
#warning log function
function log_warn(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[WARN] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 3  ] && echo $content >> $LOG_FILE && echo -e "\033[33m" ${content} "\033[0m"
}
#error log function
function log_err(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 4  ] && echo $content >> $LOG_FILE && echo -e "\033[31m" ${content} "\033[0m"
}

#debug log function
function log_debug_without_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $@"
  [ $LOG_LEVEL -le 1  ] && echo $content >> $LOG_FILE
}
#info log function
function log_info_without_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[INFO] $(date '+%Y-%m-%d %H:%M:%S') $@"
  [ $LOG_LEVEL -le 2  ] && echo $content >> $LOG_FILE
}
#warning log function
function log_warn_without_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[WARN] $(date '+%Y-%m-%d %H:%M:%S') $@"
  [ $LOG_LEVEL -le 3  ] && echo $content >> $LOG_FILE
}
#error log function
function log_err_without_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $@"
  [ $LOG_LEVEL -le 4  ] && echo $content >> $LOG_FILE
}

#debug log function
function log_debug_only_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 1  ] && echo -e "\033[32m" ${content} "\033[0m"
}
#info log function
function log_info_only_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[INFO] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 2  ] && echo -e "\033[32m" ${content} "\033[0m"
}
#warning log function
function log_warn_only_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[WARN] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 3  ] && echo -e "\033[33m" ${content} "\033[0m"
}
#error log function
function log_err_only_console(){
  if [ -z "$LOG_FILE" ];then
    echo -e "\033[31m[ERROR]\033[0m Deploy log init failed."
    exit 1
  fi
  content="[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $@"
  format_print
  [ $LOG_LEVEL -le 4  ] && echo -e "\033[31m" ${content} "\033[0m"
}