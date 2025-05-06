#! /bin/bash

# init directory
function init_directory() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  eval $prefix rm -rf /etc/kaiwudb
  sudo mkdir -p /etc/kaiwudb/certs
  sudo mkdir -p /etc/kaiwudb/script
  sudo mkdir -p /etc/kaiwudb/info
  if [ -e $g_data_root ];then
    if [ "$(ls -A $g_data_root)" != "" ];then
      echo "Data dir is not empty"
      return 2
    fi
  else
    eval $prefix mkdir -p $g_data_root
  fi
}

# container mode: create docker-compose.yml
function create_compose() {
  if [ "$g_secure_mode" = "insecure" ];then
    local secure_param="--insecure"
  else
    if [ "$g_secure_mode" = "tls" ];then
      local secure_param="--certs-dir=/kaiwudb/certs"
    else
      local secure_param="--certs-dir=/kaiwudb/certs --tlcp"
    fi
  fi
  local start_type="start-single-node"
  local opt_join=""
  case $g_install_mode in
    single)
      start_type="start-single-node"
      ;;
    single-replication)
      start_type="start-single-replica"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
    multi-replication)
      start_type="start"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
  esac
  local cpus=""
  # cal cpu usage
  if [ "$g_cpu_usage" != "0" ];then
    local cores=$(cat /proc/cpuinfo | grep "processor" | sort -u | wc -l)
    local cpu_usage=$(echo $g_cpu_usage $cores | awk '{printf("%.3f",$1*$2)}')
    cpus="deploy:
      resources:
        limits:
          cpus: '$cpu_usage'"
  fi
  if [ "$g_encrypto_store" = "true" ];then
    local encrypto_opt="--store-encryption=path=/kaiwudb/deploy/kaiwudb-container,key=/kaiwudb/certs/sm4.key,cipher=sm4"
  fi
    sudo bash -c "echo \"version: '3.3'
services:
  kaiwudb-container:
    image: $2
    container_name: kaiwudb-container
    hostname: kaiwudb-container
    ports:
      - $g_rest_port:8080
      - $g_kwdb_port:26257
    $cpus
    ulimits:
      memlock: -1
      nofile: 1048576
    volumes:
      - /etc/kaiwudb/certs:/kaiwudb/certs
      - $g_data_root:/kaiwudb/deploy/kaiwudb-container
      - /dev:/dev
    networks: 
      - default
    restart: on-failure
    ipc: shareable
    privileged: true
    environment:
      - LD_LIBRARY_PATH=/kaiwudb/lib
    tty: true
    working_dir: /kaiwudb/bin
    command: 
      - /bin/bash
      - -c
      - |
        /kaiwudb/bin/kwbase  $start_type $secure_param --listen-addr=0.0.0.0:26257 --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:8080 --store=/kaiwudb/deploy/kaiwudb-container $encrypto_opt $opt_join
\" >/etc/kaiwudb/script/docker-compose.yml"
}

function install() {
  local ret=""
  local base_dir=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
    base_dir=~/kaiwudb_files
  else
    prefix=$local_cmd_prefix
    base_dir="$g_deploy_path"
  fi
  eval $prefix -s "exit" >/dev/null 2>&1
  if [ "$g_deploy_type" = "bare" ];then
    local kw_server=$(ls $base_dir/packages | grep "server" 2>/dev/null)
    local kw_libcommon=$(ls $base_dir/packages | grep "libcommon" 2>/dev/null)
    cd $base_dir/packages
    if [ "$g_package_tool" = "dpkg" ];then
      ret=$(eval $prefix dpkg -i ./$kw_libcommon 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
      ret=$(eval $prefix dpkg -i ./$kw_server 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
    elif [ "$g_package_tool" = "rpm" ];then
      ret=$(eval $prefix rpm -ivh --replacepkgs ./$kw_libcommon 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
      ret=$(eval $prefix rpm -ivh --replacepkgs ./$kw_server 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
    fi
    
  else
    # check whether docker is installed
    docker --help >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "Command not found: docker."
      return 1
    fi
    if ! $(which docker-compose >/dev/null 2>&1); then
      echo "Command not found: docker-compose."
      return 1
    fi
    cd $base_dir/packages
    # load docker images
    ret=$(docker load < KaiwuDB.tar 2>&1)
    if [ $? -ne 0 ]; then
      echo "$ret"
      return 1
    fi
    # get the image name
    local image=$(echo "$ret" | awk -F": " '{print $2}')
    create_compose $1 $image
    ret=$(eval $prefix chmod +x /etc/kaiwudb/script/docker-compose.yml 2>&1)
    if [ $? -ne 0 ];then
      echo "$ret"
      return 1
    fi
    echo $image
    
  fi
  ret=$(eval $prefix cp -f $base_dir/packages/.version /etc/kaiwudb/info 2>&1)
  if [ $? -ne 0 ];then
    echo "$ret"
    return 1
  fi
  return 0
}

function create_info_files() {
  log_info_without_console "start create /etc/kaiwudb/info/MODE /etc/kaiwudb/info/NODE."
  if [ -e /etc/kaiwudb/info/MODE ];then
    eval $local_cmd_prefix rm /etc/kaiwudb/info/*
  fi
  eval $local_cmd_prefix touch /etc/kaiwudb/info/MODE
  sudo touch /etc/kaiwudb/script/kaiwudb_env
  sudo bash -c "echo $g_deploy_type >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_install_mode >> /etc/kaiwudb/info/MODE"
  if [ "$g_deploy_type" = "bare" ];then
    sudo bash -c "echo /usr/local/kaiwudb >> /etc/kaiwudb/info/MODE"
  else
    sudo bash -c "echo $g_image_name >> /etc/kaiwudb/info/MODE"
  fi
  sudo bash -c "echo $g_data_root >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_kwdb_port >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_rest_port >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_user >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_secure_mode >> /etc/kaiwudb/info/MODE"
  sudo bash -c "echo $g_encrypto_store >> /etc/kaiwudb/info/MODE"
  if [ "$g_install_mode" != "single" ];then
    sudo touch /etc/kaiwudb/info/NODE
    sudo bash -c "echo \"${g_cls_array[*]}\" >> /etc/kaiwudb/info/NODE"
    sudo bash -c "echo $g_ssh_port >> /etc/kaiwudb/info/NODE"
    sudo bash -c "echo $g_ssh_user >> /etc/kaiwudb/info/NODE"
  fi

  sudo bash -c "echo KAIWUDB_START_ARG=\\\"\\\" > /etc/kaiwudb/script/kaiwudb_env"
  log_info_without_console "create /etc/kaiwudb/info/MODE /etc/kaiwudb/info/NODE successfully."
}

# Create certificate
function create_certificate() {
  local ret=""
  if [ "$g_secure_mode" = "insecure" ];then
    local secure_param="--insecure"
  else
    if [ "$g_secure_mode" = "tls" ];then
      local secure_param=""
    else
      local secure_param="--tlcp"
    fi
  fi
  log_info_without_console "start create certificate files in /etc/kaiwudb/certts."
  if [ "$g_deploy_type" = "bare" ];then
    local KWDB_CA_KEY_FILE=/etc/kaiwudb/certs/ca.key
    local KWDB_CA_CRT_FILE=/etc/kaiwudb/certs/ca.crt
    local KWDB_NODE_KEY_FILE=/etc/kaiwudb/certs/node.key
    local KWDB_NODE_CRT_FILE=/etc/kaiwudb/certs/node.crt
    local KWDB_ROOT_KEY_FILE=/etc/kaiwudb/certs/client.root.key
    local KWDB_ROOT_CRT_FILE=/etc/kaiwudb/certs/client.root.crt
    sudo ldconfig
    cd /usr/local/kaiwudb/bin
    if [ ! -f "${KWDB_CA_KEY_FILE}" -o ! -f "${KWDB_CA_CRT_FILE}" ]; then
      ret=$(sudo bash -c "./kwbase cert create-ca --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} $secure_param 2>&1")
      if [ $? -ne 0 ]; then
        log_err "CA certificate creats failed: $ret"
        return 1
      fi
    fi

    if [ ! -f "${KWDB_NODE_KEY_FILE}" -o ! -f "${KWDB_NODE_CRT_FILE}" ]; then
      ret=$(sudo bash -c "./kwbase cert create-node $g_local_addr ${g_cls_array[*]} 127.0.0.1 0.0.0.0 localhost --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} $secure_param 2>&1")
      if [ $? -ne 0 ]; then
        log_err "Node certificate creats failed: $ret"
        return 1
      fi
    fi

    if [ ! -f "${KWDB_ROOT_KEY_FILE}" -o ! -f "${KWDB_ROOT_CRT_FILE}" ]; then
      ret=$(sudo bash -c "./kwbase cert create-client root --certs-dir=/etc/kaiwudb/certs --ca-key=${KWDB_CA_KEY_FILE} $secure_param 2>&1")
      if [ $? -ne 0 ]; then
        log_err "Client certificate creats failed: $ret"
        return 1
      fi
    fi
  else
    sudo rm -rf /etc/kaiwudb/certs/*
    ret=$(docker run --rm -it --privileged --name init -e SE_MODE=$secure_param -v/etc/kaiwudb/certs:/kaiwudb/certs -v$g_deploy_path/utils/container_shell.sh:/kaiwudb/container_shell.sh $g_image_name bash -c "/kaiwudb/container_shell.sh $g_local_addr \"${g_cls_array[*]}\"" 2>&1)
    if [ $? -ne 0 ];then
        log_err_without_console "$ret"
        log_err "Container creats certificate failed. Please check the log to see more information."
        return 1
    fi
  fi
  sudo chmod 755 /etc/kaiwudb/certs/*
  log_info_without_console "create certificate files in /etc/kaiwudb/certts successfully."
}

function create_encrypto_key() {
  if [ "$g_encrypto_store" = "false" ];then
    return 0
  fi
  local ret=""
  if [ "$g_deploy_type" = "bare" ];then
    cd /usr/local/kaiwudb/bin
    ret=$(sudo bash -c "./kwbase gen encryption-key -s 128 /etc/kaiwudb/certs/sm4.key 2>&1")
    if [ $? -ne 0 ]; then
      log_err "Create encrypto key failed: $ret"
      return 1
    fi
  else 
    ret=$(docker run --rm -it --privileged -w /kaiwudb/bin -v/etc/kaiwudb/certs:/kaiwudb/certs $(install_dir) bash -c "
          ./kwbase gen encryption-key -s 128 /kaiwudb/certs/sm4.key" 2>&1)
    if [ $? -ne 0 ]; then
      log_err "Create encrypto key failed: $ret"
      return 1
    fi
  fi
  sudo chmod 755 /etc/kaiwudb/certs/sm4.key
}

# create user and group
function user_create() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  eval $prefix -s "exit" >/dev/null 2>&1
  local ret=""
  # create group if not exists
  egrep "^$g_user:" /etc/group >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    eval $prefix groupadd $g_user >/dev/null 2>&1
  fi

  # create user if not exists
  egrep "^$g_user:" /etc/passwd >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    ret=$(eval $prefix useradd -g $g_user -m -s /bin/bash $g_user 2>&1)
    if [ $? -ne 0 ]; then
      echo "$ret"
      return 1
    fi
    if [ -n "$management_passwd" ];then
      ret=$(sudo bash -c "echo $g_user:$management_passwd | chpasswd 2>&1")
      if [ $? -ne 0 ]; then
        echo "$ret"
        return 1
      fi
    fi
  fi
  ret=$(sudo sed -n "/^$g_user ALL=(ALL)  NOPASSWD: ALL/p" /etc/sudoers)
  if [ -z "$ret" ];then
    sudo bash -c "echo \"$g_user ALL=(ALL)  NOPASSWD: ALL\" >> /etc/sudoers" >/dev/null 2>&1
  fi
  # change owner
  eval $prefix chown -R $g_user:$g_user /usr/local/kaiwudb >/dev/null 2>&1
  sudo chown -R $g_user:$g_user $g_data_root >/dev/null 2>&1
  sudo chown -R $g_user:$g_user /etc/kaiwudb >/dev/null 2>&1
}

function create_service() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  eval $prefix touch /etc/systemd/system/kaiwudb.service
  sudo bash -c "echo $1 >> /etc/kaiwudb/info/MODE"
  local start_type=""
  local opt_join=""
  case $g_install_mode in
    single)
      start_type="start-single-node"
      ;;
    single-replication)
      start_type="start-single-replica"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
    multi-replication)
      start_type="start"
      opt_join="--join $g_local_addr:$g_kwdb_port"
      ;;
  esac
	if [ "$g_deploy_type" = "bare" ];then
    if [ "$g_secure_mode" != "insecure" ];then
      if [ "$g_secure_mode" = "tls" ];then
        local secure_param="--certs-dir=/etc/kaiwudb/certs"
      elif [ "$g_secure_mode" = "tlcp" ];then
        local secure_param="--certs-dir=/etc/kaiwudb/certs --tlcp"
      fi
    else
      local secure_param="--insecure"
    fi
    if [ "$g_cpu_usage" != "0" ];then
      local cores=$(cat /proc/cpuinfo | grep "processor" | sort -u| wc -l )
      local cpu_usage=$(echo $g_cpu_usage $cores | awk '{printf("%.1f", $1*$2*100)}')
      local cpus="CPUQuota=$cpu_usage%"
    fi
    if [ "$g_encrypto_store" = "true" ];then
      local encrypto_opt="--store-encryption=path=$g_data_root,key=/etc/kaiwudb/certs/sm4.key,cipher=sm4"
    fi
    sudo bash -c "echo \"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=$g_user
Group=$g_user
Type=simple
LimitMEMLOCK=infinity
LimitNOFILE=1048576
$cpus
WorkingDirectory=/usr/local/kaiwudb/bin
EnvironmentFile=/etc/kaiwudb/script/kaiwudb_env
ExecStartPre=$(which sudo) $(which sysctl) -w vm.max_map_count=10000000
ExecStart=/usr/local/kaiwudb/bin/kwbase $start_type \\\$KAIWUDB_START_ARG $secure_param --listen-addr=0.0.0.0:$g_kwdb_port --advertise-addr=$1:$g_kwdb_port --http-addr=0.0.0.0:$g_rest_port --store=$g_data_root $encrypto_opt $opt_join
ExecStop=/bin/kill \\\$MAINPID
KillMode=control-group
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\" > /etc/systemd/system/kaiwudb.service"
	else
		sudo bash -c "echo \"
[Unit]
Description=KaiwuDB Service
After=network.target
Wants=network.target
[Service]
User=root
Group=root
Type=simple
LimitMEMLOCK=infinity
LimitNOFILE=1048576
WorkingDirectory=/etc/kaiwudb/script
ExecStartPre=$(which sysctl) -w vm.max_map_count=10000000
ExecStart=$(which docker-compose) --compatibility up
ExecStop=$(which docker-compose) stop
# Restart=on-failure
# ExecStopPost=sudo umount -a -t squashfs
#RuntimeMaxSec=2592000
# Don't restart in the case of configuration error
RestartPreventExitStatus=INVALIDARGUMENT
[Install]
WantedBy=multi-user.target\" > /etc/systemd/system/kaiwudb.service"
	fi
  if [ "$g_secure_mode" != "insecure" ];then
    sudo chmod 700 /etc/kaiwudb/certs/*.key >/dev/null 2>&1
    sudo chmod 644 /etc/kaiwudb/certs/*.crt >/dev/null 2>&1
  fi
}

function compress_certs() {
  local ret=""
  local crt=""
  if [ "$g_secure_mode" != "insecure" ];then
    log_info_without_console "start compress certs to $g_deploy_path/kaiwudb_certs.tar.gz"
    crt=$(basename $(ls /etc/kaiwudb/certs/*ca*.crt))
    if [ "$g_encrypto_store" = "true" ];then
      encrypto_key="sm4.key"
    fi
    ret=$(eval $local_cmd_prefix tar -zcvf $g_deploy_path/kaiwudb_certs.tar.gz -C/etc/kaiwudb/certs ca.key $crt $encrypto_key 2>&1)
    if [ $? -ne 0 ];then
      log_warn "Compress ca failed: $ret. Please manually compress the CA certificate."
      return
    fi
    log_info_without_console "compress certs success"
  fi
}

function cp_files() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  cd ~/kaiwudb_files
  eval $prefix cp -rf ./certs /etc/kaiwudb
  eval $prefix cp -rf ./info /etc/kaiwudb
  eval $prefix cp -rf ./script/kaiwudb_env /etc/kaiwudb/script
  if [ "$g_secure_mode" != "insecure" ];then
    sudo chmod 700 /etc/kaiwudb/certs/*.key >/dev/null 2>&1
    sudo chmod 644 /etc/kaiwudb/certs/*.crt >/dev/null 2>&1
  fi
  eval $prefix cp -rf ~/kaiwudb_files/*
}
