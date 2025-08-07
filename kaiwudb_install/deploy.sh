#! /bin/bash

# basic golbal variables
g_deploy_path=$(cd $(dirname $0);pwd)
g_cur_usr=$(whoami)

func_info=""
# define export vars
declare -a global_vars

source $g_deploy_path/utils/kaiwudb_common.sh
source $g_deploy_path/utils/utils.sh
source $g_deploy_path/utils/kaiwudb_log.sh

function global_usage() {
  echo "Usage:
  deploy.sh Commands [Options]

Commands:
  install                  Install KaiwuDB
  uninstall                Uninstall KaiwuDB in local node
  cluster                  Cluster operate
  start                    Start KaiwuDB in local node
  stop                     Stop KaiwuDB in local node
  restart                  Restart KaiwuDB in local node
  status                   Get local node KaiwuDB status
Options:
  -h, --help
"
  exit 0
}

function install_usage() {
  eval set -- "$@"
  push_back "g_install_mode"
  if [ $# -eq 3 ];then
    case "$1" in
      --single)
        g_install_mode="single"
        return
        ;;
      --single-replica)
        g_install_mode="single-replication"
        return
        ;;
      --multi-replica)
        g_install_mode="multi-replication"
        return
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh install [Options]

Options:
  --single                  single mode(default)
  --single-replica          single replication mode
  --multi-replica           multi replication mode
  -h, --help                help message
"
  exit 0
}

function cluster_usage() {
  eval set -- "$@"
  push_back "g_cluster_opt"
  if [ $# -eq 3 ];then
    case "$1" in
      -i|--init)
        g_cluster_opt="init"
        return
        ;;
      -s|--status)
        g_cluster_opt="status"
        return
        ;;
      -h|--help)
        ;;
    esac
  fi

  echo "Usage:
  deploy.sh cluster [Options]
Options:
  -i, --init                     Start and init all cluster
  -s, --status                   Show all nodes status
  -h, --help                     help message
"
  exit 0
}

function join_usage() {
  local help_info=""
  g_secure="insecure"
  eval set -- "$@"
  while true;do
    case "$1" in
      --addr)
        if [ "$(addr_check $2)" = "no" ];then
          echo "Invalid parameter"
          exit 1
        else
          g_join_addr=$2
          shift 2
        fi
        ;;
      --tls)
        g_secure="tls"
        shift 1
        ;;
      --tlcp)
        g_secure="tlcp"
        shift 1
        ;;
      -h|--help)
        help_info=true
        break
        ;;
      --)
        break
    esac
  done

  if [ -z "$g_join_addr" ];then
    echo "Miss option --addr"
    exit 1
  fi
  if [ "$help_info" = "true" ];then
  echo "Usage:
  deploy.sh join [Options]
Options:
  --addr [VALUE]             Target cluster addr. eg:127.0.0.1:26257
  --tls,--tlcp               Using tls or tlcp mode
  -h, --help                 help message
"
  exit 0
  fi
}

function upgrade_usage() {
  local help_mes=false
  eval set -- "$@"
  while true ; do
    case "$1" in
      -l|--local)
        g_upgrade_opt=local
        shift 1
        ;;
      -a|--all)
        g_upgrade_opt=all
        shift 1
      ;;
      --bypass-version-check)
        g_ign_cmp=yes
        shift 1
      ;;
      -h|--help)
        help_mes=true
        break
        ;;
      --)
        break
    esac
  done
  if [ "$help_mes" = "true" ];then
    echo "Usage:
  deploy.sh upgrade [Options]

Options:
  -l, --local             local node upgrade
  -a, --all               whole cluster upgrade
  --bypass-version-check  ignore version compare check
  -h, --help              help message
"
  exit 0
  fi
}

function cmd_check() {
  local commands=("install" "uninstall" "start" "stop" "restart" "cluster" "limit" "status" "upgrade" "join" "decommission")
  local flag=false
  g_kw_cmd=""
  local global_flag=false
  local short_cmd="hsial"
  local long_cmd="help,single,init,status,cpu:,addr:,single-replica,multi-replica,local,all,bypass-version-check,tls,tlcp"
  local options=$(getopt -a -o $short_cmd -l $long_cmd -n "deploy.sh" -- "$@" 2>/dev/null)
  if [ $? -ne 0 ];then
    global_usage
  fi
  eval set -- "$options"
  while true; do
    case $1 in
      --)
        g_kw_cmd=$2
        break
      ;;
      -h|--help)
        global_flag=true
        shift 1
      ;;
      *)
        shift 1
        ;;
    esac
  done
  for item in ${commands[@]}
  do
    if [ "$g_kw_cmd" = "$item" ]; then
      flag=true
      break
    fi
  done
  if [ $flag = "false" ] || [[ -z "$g_kw_cmd" && $global_flag = "true" ]];then
    global_usage
  fi

  case "$g_kw_cmd" in
    install)
      install_usage $options
    ;;
    cluster)
      cluster_usage $options
    ;;
    limit)
      limit_usage $options
    ;;
    join)
      join_usage $options
    ;;
    upgrade)
      upgrade_usage $options
    ;;
  esac

}

function cfg_check() {
  # secure_mode check
  push_back "g_secure_mode"
  g_secure_mode=$(read_config_value $g_deploy_path/deploy.cfg global secure_mode)
  if [ "$g_secure_mode" != "tls" ] && [ "$g_secure_mode" != "tlcp" ] && [ "$g_secure_mode" != "insecure" ];then
    log_err "secure_mode is missing in deploy.cfg."
    exit 1
  fi
  # encrypto store
  push_back "g_encrypto_store"
  g_encrypto_store=$(read_config_value $g_deploy_path/deploy.cfg global encrypto_store)
  if [ "$g_encrypto_store" != "true" ] && [ "$g_encrypto_store" != "false" ];then
    if [ -z "$g_encrypto_store" ];then
      g_encrypto_store=false
    else
      log_err "encrypto_store is invalid."
      exit 1
    fi
  fi
  # management_user check
  push_back "g_user"
  g_user=$(read_config_value $g_deploy_path/deploy.cfg global management_user)
  if [ -z "$g_user" ];then
    log_err "management_user is missing in deploy.cfg."
    exit 1
  fi
  # rest_port check
  push_back "g_rest_port"
  g_rest_port=$(read_config_value $g_deploy_path/deploy.cfg global rest_port)
  if [ -z "$g_rest_port" ];then
    log_err "rest_port is missing in deploy.cfg."
    exit 1
  fi
  if [ $g_rest_port -le 0 -o $g_rest_port -gt 65535 ];then
    log_err "The rest_port value is incorrect, range [1-65535]."
    exit 1
  fi
  push_back "g_kwdb_port"
  g_kwdb_port=$(read_config_value $g_deploy_path/deploy.cfg global kaiwudb_port)
  if [ -z "$g_kwdb_port" ];then
    log_err "kaiwudb_port is missing in deploy.cfg."
    exit 1
  fi
  if [ $g_kwdb_port -le 0 -o $g_kwdb_port -gt 65535 ];then
    log_err "The kaiwudb_port value is incorrect, range [1-65535]."
    exit 1
  fi
  push_back "g_brpc_port"
  g_brpc_port=$(read_config_value $g_deploy_path/deploy.cfg global brpc_port)
  if [ -z "$g_brpc_port" ];then
    log_err "brpc_port is missing in deploy.cfg."
    exit 1
  fi
  if [ $g_brpc_port -le 0 -o $g_brpc_port -gt 65535 ];then
    log_err "The brpc_port value is incorrect, range [1-65535]."
    exit 1
  fi
  push_back "g_cpu_usage"
  g_cpu_usage=$(read_config_value $g_deploy_path/deploy.cfg global cpu)
  if [ -z "$g_cpu_usage" ];then
    g_cpu_usage=0
  fi
  # validate cpu usage
  echo $g_cpu_usage | awk '{if($1>1 || $1<0){exit 1}else{exit 0}}'
  if [ $? -ne 0 ];then
    log_err "cpu value is incorrect[0-1]."
    exit 1
  fi
  # data_root check
  push_back "g_data_root"
  g_data_root=$(read_config_value $g_deploy_path/deploy.cfg global data_root)
  if [ -z "$g_data_root" ];then
    log_err "data_root is missing in deploy.cfg."
    exit 1
  fi
  # local node addr check
  push_back "g_local_addr"
  g_local_addr=$(read_config_value $g_deploy_path/deploy.cfg local node_addr)
  if [ -z "$g_local_addr" ];then
    log_err "[local]node_addr is missing in deploy.cfg."
    exit 1
  fi
  if [ "$(addr_check $g_local_addr)" = "no" ];then
    log_err "The [local]node_addr format is incorrect."
    exit 1
  fi
  # cluster node check(optional)
  if grep -Eq "^\[cluster\]$" $g_deploy_path/deploy.cfg;then
    # g_cluster_deploy=true
    local cls_addrs=$(read_config_value $g_deploy_path/deploy.cfg cluster node_addr)
    g_cls_array=(${cls_addrs//,/ })
    # g_node_num=${#g_cls_array[@]}
    if [ ${#g_cls_array[@]} -eq 0 ];then
      log_err "[cluster]node_addr is missing in deploy.cfg."
      exit 1
    fi
    for ((i=0; i<${#g_cls_array[@]}; i++))
    do
      if [ "$(addr_check ${g_cls_array[$i]})" = "no" ];then
        log_err "The [cluster]node_addr("${g_cls_array[$i]}") format is incorrect."
        exit 1
      fi
    done
    g_ssh_port=$(read_config_value $g_deploy_path/deploy.cfg cluster ssh_port)
    if [ -z "$g_ssh_port" ];then
      log_err "ssh_port is missing in deploy.cfg."
      exit 1
    fi
    if [ $g_ssh_port -le 0 -o $g_ssh_port -gt 65535 ];then
      log_err "The ssh_port value is incorrect, range [1-65535]."
      exit 1
    fi
    g_ssh_user=$(read_config_value $g_deploy_path/deploy.cfg cluster ssh_user)
    if [ -z "$g_ssh_user" ];then
      log_err "ssh_user is missing in deploy.cfg."
      exit 1
    fi
  fi
}

# cmd line parameter parsing
cmd_check $@

# fetch local node passwd
local_privileged

# init log
log_init $g_deploy_path $g_cur_usr
push_back "LOG_FILE"
push_back "LOG_LEVEL"

if [ "$g_kw_cmd" = "install" ];then
  # pipe
  kw_pipe=$g_deploy_path/kw_pipe
  if [ ! -p $kw_pipe ]; then
    func_info=$(mkfifo $kw_pipe 2>&1)
    if [ $? -ne 0 ];then
      log_err "Pipe create failed: $func_info."
      exit 1
    fi
  fi
  exec 10<>$g_deploy_path/kw_pipe
  rm -rf $g_deploy_path/kw_pipe
  source $g_deploy_path/utils/kaiwudb_install.sh
  source $g_deploy_path/utils/kaiwudb_hardware.sh
  source $g_deploy_path/utils/process_bar.sh
  process_bar $$ &
  process_pid=$!
  echo "install#check:5" >&10
  # whether installing KaiwuDB
  if $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB is already installed."
    exit 1
  fi
  echo "hardware#check:10" >&10
  # hardware check
  cpu_info
  if [ $? -eq 1 ];then
    log_warn "The number of CPU cores does not meet the requirement. KaiwuDB may running failed."
  fi
  mem_info
  if [ $? -eq 2 ];then
    log_warn "The memory does not meet the requirement. KaiwuDB may running failed."
  fi
  verify_files
  echo "configure#file#check:15" >&10
  # parsing config file
  cfg_check
  echo "ssh-passwd-free#check:20" >&10
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "ssh_passwd_free"
  if [ $? -ne 0 ];then
    exit 1
  fi
  if [ ${#g_cls_array[*]} -ne 0 ];then
    echo "pause" >&10
    remote_privileged "${g_cls_array[0]}" $g_ssh_port $g_ssh_user
  fi
  
  echo "capture#SIGINT:30" >&10
  # capture SIGINT signal
  trap "rollback_all \"${g_cls_array[*]}\" $g_ssh_port $g_ssh_user && kill -9 $process_pid" INT
  echo "create#node#dir:35" >&10
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "node_dir"
  echo "local#dir#init:45" >&10
  # local node init directory
  func_info=$(init_directory)
  if [ $? -eq 2 ];then
    log_warn $func_info
  fi
  echo "node#dir#init:50" >&10
  # parallel init remote node directory
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "init_directory"
  echo "distribute#packages:55" >&10
  # distribute files
  parallel_distribute "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "$g_deploy_path/packages"
  if [ $? -ne 0 ];then
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "local#install:60" >&10
  #install packages
  func_info=$(install $g_local_addr)
  if [ $? -ne 0 ];then
    log_err "Install failed: $func_info"
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  else
    g_image_name=$func_info
  fi
  echo "node#install:65" >&10
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "install"
  if [ $? -ne 0 ];then
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "create#configure:70" >&10
  create_info_files "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
  if [ "$g_secure_mode" != "insecure" ];then
    echo "create#certs:75" >&10
    create_certificate
    if [ $? -ne 0 ];then
      rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
  fi
  create_encrypto_key
  if [ $? -ne 0 ];then
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "distribute#configure:80" >&10
  parallel_distribute "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "/etc/kaiwudb/*"
  if [ $? -ne 0 ];then
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "cp_files"
  if [ "$g_deploy_type" = "bare" ];then
    echo "pause" >&10
    management_passwd=$(read_passwd $g_user)
    format_print
    push_back "management_passwd"
    func_info=$(user_create)
    if [ $? -ne 0 ];then
      log_err "Create user failed: $func_info"
      rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
    parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "user_create"
    if [ $? -ne 0 ];then
      rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
      exit 1
    fi
  fi
  echo "create#service:85" >&10
  create_service $g_local_addr
  parallel_exec "${g_cls_array[*]}" $g_ssh_port $g_ssh_user "create_service"
  if [ $? -ne 0 ];then
    rollback_all "${g_cls_array[*]}" $g_ssh_port $g_ssh_user
    exit 1
  fi
  echo "compress#certs:95" >&10
  compress_certs
  echo "install#complete:100" >&10
  wait
  exec 10>&-
  echo -e "\e[1;32m[INSTALL COMPLETED]:\e[0mKaiwuDB has been installed successfully! To start KaiwuDB, please execute the command '\e[1;34msystemctl daemon-reload\e[0m'."
  exit 0
fi

if [ "$g_kw_cmd" = "uninstall" ];then
  source $g_deploy_path/utils/kaiwudb_uninstall.sh
  if ! $(install_check  >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if $(kw_status >/dev/null 2>&1);then
    log_err "Please stop KaiwuDB first."
    exit 1
  fi
  clear_opt="n"
  read -t60 -p "When uninstalling KaiwuDB, you can either delete or keep all user data. Please confirm your choice: Do you want to delete the data? (y/N)" -e clear_opt
  uninstall
  delete_user
  func_info=$(uninstall_dir)
  if [ $? -ne 0 ];then
    log_err "dir uninstall error:$func_info"
    exit 1
  fi
  echo -e "\e[1;32m[UNINSTALL COMPLETED]:\e[0mKaiwuDB has been uninstalled successfully."
  exit 0
fi

if [ "$g_kw_cmd" = "join" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check  >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if ! $(check_start_type);then
    log_err "The current mode is not supported join a cluster."
    exit 1
  fi
  if $(kw_status >/dev/null 2>&1);then
    log_err "KaiwuDB already running."
    exit 1
  fi
  func_info=$(rebuild_certs)
  if [ $? -ne 0 ];then
    log_err "Join failed: $func_info"
    exit 1
  fi
  func_info=$(modify_start_cmd)
  if [ $? -ne 0 ];then
    log_err "Join failed: $func_info"
    exit 1
  fi
  if ! $(kw_start >/dev/null 2>&1);then
    log_err "Start KaiwuDB failed. For more information, check kwbase's log."
    exit 1
  fi
  if ! $(cluster_status >/dev/null 2>&1);then
    log_err "Join to cluster failed. For more information, check kwbase's log."
    exit 1
  fi
  modify_cache
  echo -e "\e[1;32m[JOIN COMPLETED]:\e[0mNode has been joined to the cluster successfully."
  exit 0
fi

if [ "$g_kw_cmd" = "cluster" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if [ "$g_cluster_opt" = "init" ];then
    if ! $(install_check  >/dev/null 2>&1);then
      log_err "KaiwuDB does not exist. Please install KaiwuDB first."
      exit 1
    fi
    parse_addr
    # remote node passwd-free check
    parallel_exec "${ip_arr[*]}" $ssh_port $ssh_user "ssh_passwd_free"
    if [ $? -ne 0 ];then
      exit 1
    fi
    parallel_exec "${ip_arr[*]}" $ssh_port $ssh_user "install_check"
    if [ $? -ne 0 ];then
      exit 1
    fi
    if [ "$(running_type)" = "single" ];then
      log_err "The current mode is not supported cluster init."
      exit 1
    fi
    if [ ! -f /etc/kaiwudb/info/NODE ];then
      log_err "Cluster already init."
      exit 1
    fi
    if $(kw_status >/dev/null 2>&1);then
      log_err "KaiwuDB already running."
      exit 1
    fi
    if [ ${#ip_arr[*]} -ne 0 ];then
      remote_privileged "${ip_arr[0]}" $ssh_port $ssh_user
    fi
    parallel_exec "${ip_arr[*]}" $ssh_port $ssh_user "kw_status"
    if [ $? -ne 0 ];then
      exit 1
    fi
    if ! $(exec_start >/dev/null 2>&1);then
      log_err "Start KaiwuDB failed. For more information, check kwbase's log."
      exit 1
    fi
    parallel_exec "${ip_arr[*]}" $ssh_port $ssh_user "exec_start"
    if [ $? -ne 0 ];then
      exit 1
    fi
    func_info=$(cluster_init)
    if [ $? -ne 0 ];then
      log_err "Cluster init failed: $func_info"
      exit 1
    fi
    echo -e "\e[1;32m[INIT COMPLETED]:\e[0mCluster init successfully."
    exit 0
  elif [ "$g_cluster_opt" = "status" ];then
    if ! $(install_check  >/dev/null 2>&1);then
      log_err "KaiwuDB does not exist. Please install KaiwuDB first."
      exit 1
    fi
    if ! $(kw_status >/dev/null 2>&1);then
      log_err "KaiwuDB is not running."
      exit 1
    fi
    cluster_status
  fi
fi

if [ "$g_kw_cmd" = "decommission" ];then
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  if ! $(install_check  >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  
  if ! $(kw_status >/dev/null 2>&1);then
    log_err "KaiwuDB is not running, please start KaiwuDB first."
    exit 1
  fi
  decommission_from_cluster
  echo -e "\e[1;32m[DECOMMISSION COMPLETED]:\e[0mDecommission complete. Please wait a few minutes to check whether decommission from cluster succeeded."
  exit 0
fi

if [ "$g_kw_cmd" = "status" ];then
  if ! $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if ! $(kw_status >/dev/null 2>&1);then
    echo -e "\e[1;32m[STATUS COMPLETED]:\e[0mKaiwuDB is not runnning."
    exit 0
  else
    echo -e "\e[1;32m[STATUS COMPLETED]:\e[0mKaiwuDB is runnning now."
    exit 0
  fi
fi

if [ "$g_kw_cmd" = "start" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if $(kw_status >/dev/null 2>&1);then
    log_err "KaiwuDB is runnning now."
    exit 1
  fi
  if ! $(kw_start >/dev/null 2>&1);then
    log_err "Start KaiwuDB failed. For more information, check kwbase log."
    exit 1
  fi
  echo -e "\e[1;32m[START COMPLETED]:\e[0mKaiwuDB start successfully."
fi

if [ "$g_kw_cmd" = "stop" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if ! $(kw_status >/dev/null 2>&1);then
    log_err "KaiwuDB is not runnning now."
    exit 1
  fi
  if ! $(kw_stop);then
    log_err "Stop KaiwuDB failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  echo -e "\e[1;32m[STOP COMPLETED]:\e[0mKaiwuDB stop successfully."
fi

if [ "$g_kw_cmd" = "restart" ];then
  source $g_deploy_path/utils/kaiwudb_operate.sh
  if ! $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  if ! $(kw_status >/dev/null 2>&1);then
    log_err "KaiwuDB is not runnning now."
    exit 1
  fi
  if ! $(kw_restart);then
    log_err "Restart KaiwuDB failed. For more information, use 'journalctl -u kaiwudb' to view the system logs."
    exit 1
  fi
  echo -e "\e[1;32m[RESTART COMPLETED]:\e[0mKaiwuDB restart successfully."
fi

if [ "$g_kw_cmd" = "upgrade" ];then
  # ignore signal SIGINT
  trap "" INT
  source $g_deploy_path/utils/kaiwudb_install.sh
  source $g_deploy_path/utils/kaiwudb_upgrade.sh
  source $g_deploy_path/utils/kaiwudb_cluster.sh
  source $g_deploy_path/utils/kaiwudb_operate.sh
  # load process_bar function
  source $g_deploy_path/utils/process_bar.sh
  # create pipe file, and associate file-descriptor(10)
  kw_pipe=$g_deploy_path/kw_pipe
  if [ ! -p $kw_pipe ]; then
    func_info=$(mkfifo $kw_pipe 2>&1)
    if [ $? -ne 0 ];then
      log_err "Pipe create failed: $func_info."
      exit 1
    fi
  fi
  exec 10<>$g_deploy_path/kw_pipe
  # delete pipe file
  rm -rf $g_deploy_path/kw_pipe
  process_bar $$ &
  process_pid=$!
  echo "install#check:10" >&10
  # whether install KaiwuDB
  if ! $(install_check >/dev/null 2>&1);then
    log_err "KaiwuDB does not exist. Please install KaiwuDB first."
    exit 1
  fi
  echo "verify#packages:20" >&10
  verify_files
  echo "verify#install#type:30" >&10
  if [ "$g_deploy_type" != "$(install_type)" ];then
    log_err "Current use $(install_type), but the package is $g_deploy_type."
    exit 1
  fi
  # local node upgrade
  if [ "$g_upgrade_opt" = "local" ];then
    if [ "$g_ign_cmp" != "yes" ];then
      echo "version#compare:50" >&10
      func_info=$(version_compare)
      if [ $? -ne 0 ];then
        log_err "$func_info"
        exit 1
      fi
    fi
    # check kwbase whether running
    echo "whether#running:60" >&10
    if ! $(kw_status >/dev/null 2>&1);then
      # off-line upgrades
      echo "upgrade:70" >&10
      func_info=$(upgrade)
      if [ $? -ne 0 ];then
        log_err "Pipe create failed: $func_info."
        exit 1
      fi
    elif [ "$(running_type)" != "single" ];then
      # on-line upgrade
      echo "change#KaiwuDB#status:75" >&10
      func_info=$(change_kwdb_status)
      if [ $? -ne 0 ];then
        log_err "KaiwuDB change status failed: $func_info."
        exit 1
      fi
      echo "upgrade:80" >&10
      func_info=$(upgrade)
      if [ $? -ne 0 ];then
        log_err "KaiwuDB upgrade failed: $func_info."
        exit 1
      fi
      # modify start cmd and restart KaiwuDB
      echo "restart#KaiwuDB:90" >&10
      func_info=$(recover_node)
      if [ $? -ne 0 ];then
        log_err "Recover KaiwuDB failed: $func_info."
        exit 1
      fi
    else
      log_err "Current local node is not support online upgrade! Please stop KaiwuDB then try again."
      exit 1
    fi
    echo "upgrade#complete:100" >&10
    # wait bar process exit
    wait
    # close file descriptor
    exec 10>&-
    echo -e "\e[1;32m[UPGRADE COMPLETED]:\e[0mLocal node upgrade successfully."
  elif [ "$g_upgrade_opt" = "all" ];then
    # local node upgrade first.
    # check kwbase whether running
    if [ "$g_run_type" = "single" ];then
      log_err "Current local node is not support online upgrade! Please stop KaiwuDB then try again."
      exit 1
    fi
    echo "get#Kaiwudb#status:45" >&10
    if ! $(kw_status >/dev/null 2>&1);then
      # must be running
      log_err "KaiwuDB is not runnning, can not get cluster nodes status."
      exit 1
    else
      echo "get#all#nodes:50" >&10
      # local node upgrade first.
      get_cluster_nodes
      echo "ssh#passwd#free:55" >&10
      parallel_exec "${addr_array[*]}" $ssh_port $ssh_user "ssh_passwd_free"
      if [ $? -ne 0 ];then
        exit 1
      fi
      echo "pause" >&10
      if [ ${#addr_array[*]} -ne 0 ];then
        remote_privileged "${addr_array[0]}" $ssh_port $ssh_user
      fi
      # scp all packages
      echo "distribute#packages:60" >&10
      parallel_distribute "${addr_array[*]}" $ssh_port $ssh_user "$g_deploy_path/packages"
      if [ $? -ne 0 ];then
        exit 1
      fi
      if [ "$g_ign_cmp" != "yes" ];then
        echo "version#compare:70" >&10
        func_info=$(version_compare)
        if [ $? -ne 0 ];then
          log_err "$func_info"
        fi
        parallel_exec "${addr_array[*]}" $ssh_port $ssh_user "version_compare"
        if [ $? -ne 0 ];then
          exit 1
        fi
      fi
      echo "change#status:75" >&10
      func_info=$(change_kwdb_status)
      if [ $? -ne 0 ];then
        log_err "KaiwuDB change status failed: $func_info."
        exit 1
      fi
      echo "local#upgrade:80" >&10
      func_info=$(upgrade)
      if [ $? -ne 0 ];then
        log_err "KaiwuDB upgrade failed: $func_info."
        exit 1
      fi
      # modify start cmd and restart KaiwuDB
      echo "local#restart:85" >&10
      func_info=$(recover_node)
      if [ $? -ne 0 ];then
        log_err "Recover KaiwuDB failed: $func_info."
        exit 1
      fi
    fi
    echo "remote#upgrade:90" >&10
    # remote node upgrade
    for node in ${addr_array[*]}
    do
      sleep 5
      remote_exec $node $ssh_port $ssh_user "change_kwdb_status"
      remote_exec $node $ssh_port $ssh_user "upgrade"
      remote_exec $node $ssh_port $ssh_user "recover_node"
    done
    echo "upgrade#done:100" >&10
    wait
    exec 10>&-
    echo -e "\e[1;32m[UPGRADE COMPLETED]:\e[0mCluster upgrade completed."
  fi
fi