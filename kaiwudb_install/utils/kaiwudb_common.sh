#! /bin/bash

# verify the installation package and get package manager tool
function verify_files() {
  local ret=""
  if [ ! -d $g_deploy_path/packages ];then
    log_err "$g_deploy_path/packages: No such directory."
    exit 1
  fi
  local files=$(ls $g_deploy_path/packages)
  # determine the installation mode
  ret=$(echo $files | grep -wq "KaiwuDB.tar" && echo "yes" || echo "no")
  if [ "$ret" != "yes" ];then
    g_deploy_type="bare"
  else
    g_deploy_type="container"
  fi
  push_back "g_deploy_type"
  local packages_array=(server libcommon)
  # determine the package manager tool
  if [ "$g_deploy_type" = "bare" ];then
    dpkg --help >/dev/null 2>&1
    if [ $? -ne 0 ];then
      g_package_tool="rpm"
    else
      g_package_tool="dpkg"
    fi
    push_back "g_package_tool"
    for item in ${packages_array[@]}
    do
      ret=$(echo "$files" | grep -qo "$item" &&  echo "yes" || echo "no")
      if [ "$ret" = "no" ];then
        log_err "Package $item does not exist."
        exit 1
      fi
    done
  fi
}

# whether installing KaiwuDB
function install_check() {
  if [ ! -e /etc/systemd/system/kaiwudb.service ];then
    echo "KaiwuDB is not install."
    return 1
  fi
  echo "KaiwuDB is already installed."
  return 0
}

# IP verification
function addr_check() {
  local addr=(${1//:/ })
  local valid=$(echo ${addr[0]}| awk -F. '$1<=255&&$2<=255&&$3<=255&&$4<=255{print "yes"}')
  if echo ${addr[0]}| grep -E "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$" > /dev/null; then
    if [ "${valid:-no}" = "yes" ]; then
      echo "yes"
    else
      echo "no"
    fi
  else
    echo "no"
  fi
}

# ssh passwd-free check
function ssh_passwd_free() {
  exit 0
}

# bare or container
function install_type() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local type=$(sed -n "1p" /etc/kaiwudb/info/MODE)
    if [ "$type" != "bare" -a "$type" != "container" ];then
      return 1
    fi
    echo "$type"
    return 0
  else
    return 1
  fi
}

function install_dir() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local dir=$(sed -n "3p" /etc/kaiwudb/info/MODE)
    echo "$dir"
    return 0
  else
    return 1
  fi
}

# single or single-replication or multi-replication
function running_type() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local type=$(sed -n "2p" /etc/kaiwudb/info/MODE)
    if [ "$type" != "single" -a "$type" != "single-replication" -a "$type" != "multi-replication" ];then
      return 1
    fi
    echo "$type"
    return 0
  else
    return 1
  fi
}

function kw_data_dir() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local dir=$(sed -n "4p" /etc/kaiwudb/info/MODE)
    echo "$dir"
    return 0
  else
    return 1
  fi
}

function container_image() {
  if [ -f /etc/kaiwudb/info/MODE ];then
    local image_name=$(sed -n "3p" /etc/kaiwudb/info/MODE)
    echo "$image_name"
    return 0
  else
    return 1
  fi
}

function user_name() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local user_name=$(sed -n "7p" /etc/kaiwudb/info/MODE)
    echo "$user_name"
    return 0
  else
    return 1
  fi
}

function secure_mode() {
  if [ -f /etc/kaiwudb/info/MODE ];then
    local secure_mode=$(sed -n "8p" /etc/kaiwudb/info/MODE)
    echo "$secure_mode"
    return 0
  else
    if [ -e /etc/kaiwudb/certs/tlcp_ca.crt ];then
      echo "tlcp"
    elif [ -e /etc/kaiwudb/certs/ca.crt ];then
      echo "tls"
    else
      echo "insecure"
    fi
    return 0
  fi
}

function local_addr() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local addr=$(sed -n '$p' /etc/kaiwudb/info/MODE)
    echo "$addr"
    return 0
  else
    return 1
  fi
}

function local_port() {
	if [ -f /etc/kaiwudb/info/MODE ];then
    local port=$(sed -n "5p" /etc/kaiwudb/info/MODE)
    echo "$port"
    return 0
  else
    return 1
  fi
}

function node_dir() {
  cd ~
  if [ ! -d "kaiwudb_files" ];then
    mkdir -p kaiwudb_files
  else
    eval $node_cmd_prefix rm -rf kaiwudb_files
    mkdir -p kaiwudb_files
  fi
}

# Whether Kaiwudb is running
function kw_status() {
  if [ "$(install_type)" = "bare" ];then
    local main_pid=0
    # bare mode check PID
    main_pid=$(systemctl show --property MainPID --value kaiwudb)
    if [ $main_pid -ne 0 ];then
      echo "KaiwuDB already running."
      return 0
    fi
  else
    local stat=$(docker ps -a --filter name=kaiwudb-container --format {{.Status}} | awk '{if($0~/^(Up).*/){print "yes";}else{print "no";}}')
    if [ "$stat" = "yes" ];then
      echo "KaiwuDB already running."
      return 0
    fi
  fi
  echo "KaiwuDB is not running."
  return 1
}

function whether_running() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    local cmd="$prefix -u $(user_name) bash -c \"./kwbase node status --host=127.0.0.1:$(local_port) $(secure_opt)\""
    eval $cmd 2>&1
  else
    docker exec -it kaiwudb-container bash -c "./kwbase node status $(secure_opt)" 2>&1
  fi
    return $?
}

function rollback() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
    eval $prefix rm -rf ~/kaiwudb_files
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$g_package_tool" = "dpkg" ];then
    eval $prefix dpkg -r kaiwudb-server >/dev/null 2>&1
    eval $prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
    eval $prefix dpkg -r kwdb-server >/dev/null 2>&1
    eval $prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
  elif [ "$g_package_tool" = "rpm" ];then
    eval $prefix rpm -e kaiwudb-server >/dev/null 2>&1
    eval $prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
    eval $prefix rpm -e kwdb-server >/dev/null 2>&1
    eval $prefix rpm -e kwdb-libcommon >/dev/null 2>&1
  fi
  if [ -f /etc/kaiwudb/info/MODE ];then
    eval $prefix userdel -r $(user_name) >/dev/null 2>&1
    sudo sed -i "/^$(user_name) ALL=(ALL)  NOPASSWD: ALL$/d" /etc/sudoers
    eval $prefix tar -zcvf ~/kaiwudb_files.tar.gz  -C/etc kaiwudb >/dev/null 2>&1
    eval $prefix tar -zcvf ~/kw_data.tar.gz  $(kw_data_dir) >/dev/null 2>&1
  fi
}

function privileged() {
  local user=$(whoami)
  if [ "$user" != "root" ];then
    # passwd-free check
    timeout --foreground -k 1 1s sudo -s -p "" exit >/dev/null 2>&1
    if [ $? -ne 0 ];then
      return 1
    else
      return 0
    fi
  else
    return 0
  fi
}

function read_passwd() {
  local passwd=""
  read -s -t60 -p "Please input $1's password: " passwd
  echo $passwd
}

function local_privileged() {
  if privileged;then
    local_cmd_prefix="sudo"
  else
    local_cmd_prefix="echo '$(read_passwd $g_cur_usr)' | sudo -S -p \"\""
    echo
    eval $local_cmd_prefix -k -s >/dev/null 2>&1
    if [ $? -ne 0 ];then
      echo -e "\033[31m[ERROR]\033[0m Incorrect password." >&2
      exit 1
    fi
  fi
  eval $local_cmd_prefix bash -c "exit" >/dev/null 2>&1
  if [ $? -ne 0 ];then
    echo -e "\033[31m[ERROR]\033[0m Can not use command: 'sudo bash'." >&2
    exit 1
  fi
}

function remote_privileged() {
  ssh -q -p $2 $3@$1 "$(declare -f privileged); privileged"
  if [ $? -eq 0 ];then
    node_cmd_prefix="sudo"
  else
    node_cmd_prefix="echo '$(read_passwd $3)' | sudo -S -p \"\""
    echo
    ssh -q -p $2 $3@$1 "$node_cmd_prefix -s \"\"" >/dev/null 2>&1
    if [ $? -ne 0 ];then
      echo -e "\033[31m[ERROR]\033[0m Incorrect password." >&2
      exit 1
    fi
  fi
  push_back "node_cmd_prefix"
}

function remote_exec() {
  local ret=""
  local ret_value=""
  cd $g_deploy_path
  ret=$(ssh -q -p $2 $3@$1 "$(declare -f);$(declare -p ${global_vars[*]});export REMOTE=ON;$4 $1 $2 $3")
  ret_value=$?
  if [ "$4" = "kw_status" ];then
    if [ $ret_value -eq 0 ];then
      log_err "$4 exec failed in $1: $ret"
      exit 1
    fi
  else
    if [ $ret_value -eq 1 ];then
      log_err "$4 exec failed in $1: $ret"
      exit 1
    fi
    if [ $ret_value -eq 2 ];then
      log_warn "warning in $1: $ret"
    fi
  fi
}

function parallel_exec() {
  declare -a array
  local node_array=($1)
  local ret_value=""
  cd $g_deploy_path
  if [ ${#node_array[@]} -ne 0 ];then
    for ((i=0; i<${#node_array[@]}; i++))
    do
      array[$i]="${node_array[$i]} $2 $3"
    done
    echo ${array[@]} | xargs -n3 | xargs -P 5 -I {} bash -c "$(declare -p ${global_vars[*]});$(declare -f);$(declare -p global_vars);remote_exec {} $4"
    ret_value=$?
  fi
  unset array
  return $ret_value
}

function distribute_files() {
  local ret=""
  ret=$(scp -r -P $2 $4 $3@$1:~/kaiwudb_files/ 2>&1)
  if [ $? -ne 0 ];then
    log_err "Distribute files failed in $1: $ret."
    exit 1
  fi
}

function parallel_distribute() {
  declare -a array
  cd $g_deploy_path
  local node_array=($1)
  local ret_value=""
  if [ ${#node_array[@]} -ne 0 ];then
    for ((i=0; i<${#node_array[@]}; i++))
    do
      array[$i]="${node_array[$i]} $2 $3"
    done
    echo ${array[@]} | xargs -n3 | xargs -P 5 -I {} bash -c "$(declare -p ${global_vars[*]});$(declare -f);$(declare -p global_vars);distribute_files {} \"$4\""
    ret_value=$?
  fi
  unset array
  return $ret_value
}

function rollback_all() {
  declare -a array
  local node_array=($1)
  local ret_value=""
  cd $g_deploy_path
  if [ ${#node_array[@]} -ne 0 ];then
    # remote node rollback
    for ((i=0; i<${#node_array[@]}; i++))
    do
      array[$i]="${node_array[$i]} $2 $3"
    done
    rollback
    echo ${array[@]} | xargs -n3 | xargs -P 5 -I {} bash -c "$(declare -p ${global_vars[*]});$(declare -f);$(declare -p global_vars);remote_exec {} rollback"
  else
    # local node rollback
    rollback
  fi
  unset array
  return $ret_value
}

function push_back() {
  global_vars+=("$1")
}

function secure_opt(){
  if [ "$(install_type)" = "bare" ];then
    if [ "$(secure_mode)" = "tls" ];then
      echo "--certs-dir=/etc/kaiwudb/certs"
    elif [ "$(secure_mode)" = "tlcp" ];then
      echo "--certs-dir=/etc/kaiwudb/certs --tlcp"
    else
      echo "--insecure"
    fi
  else
    if [ "$(secure_mode)" = "tls" ];then
      echo "--certs-dir=/kaiwudb/certs"
    elif [ "$(secure_mode)" = "tlcp" ];then
      echo "--certs-dir=/kaiwudb/certs --tlcp"
    else
      echo "--insecure"
    fi
  fi
}