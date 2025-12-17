#! /bin/bash

# uninstall pkg
function uninstall() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    dpkg --help >/dev/null 2>&1
    if [ $? -ne 0 ];then
      local manager="rpm"
    else
      local manager="dpkg"
    fi
    if [ "$manager" = "dpkg" ];then
      eval $prefix dpkg -r kaiwudb-server >/dev/null 2>&1
      eval $prefix dpkg -r kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-server >/dev/null 2>&1
      eval $prefix dpkg -r kwdb-libcommon >/dev/null 2>&1
    elif [ "$manager" = "rpm" ];then
      eval $prefix rpm -e kaiwudb-server >/dev/null 2>&1
      eval $prefix rpm -e kaiwudb-libcommon >/dev/null 2>&1
      eval $prefix rpm -e kwdb-server >/dev/null 2>&1
      eval $prefix rpm -e kwdb-libcommon >/dev/null 2>&1
    fi
    sudo rm -rf /usr/local/kaiwudb >/dev/null 2>&1
  else
    local image=$(docker ps -a --filter name=kaiwudb-container --format {{.Image}})
    if [ -n "$image" ];then
      docker rm kaiwudb-container > /dev/null 2>&1
    else
      local image=$(container_image)
    fi
    docker rmi $image > /dev/null 2>&1
  fi
}

function umount_loop() {
  local ret=""
  echo "Please wait, the device is being uninstalled."
  arr=$(losetup -a | awk '{print $3}' | tr -d '()')
  for loop in ${arr};do
    if [[ $loop =~ ^$1 ]];then
      ret=$(eval $prefix umount $loop 2>&1)
      if [ $? -ne 0 ];then
        echo "$ret"
        return 1
      fi
    fi
  done
}

function uninstall_dir() {
  local ret=""
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  local data_root=$(kw_data_dir)
  eval $prefix rm -rf /etc/systemd/system/kaiwudb.service /usr/bin/kw-status /usr/bin/kw-sql
  if [ -n "$data_root" ];then
    if [ "$clear_opt" = "yes" ] || [ "$clear_opt" = "y" ]; then
      ret=$(umount_loop $data_root)
      if [ $? -ne 0 ];then
        echo $ret
        return 1
      fi
      ret=$(eval $prefix rm -rf $data_root /etc/kaiwudb 2>&1)
      if [ $? -ne 0 ];then
        echo $ret
        return 1
      fi
    fi
  fi
}

function delete_user() {
  if [ "$REMOTE" = "ON" ];then
    prefix=$node_cmd_prefix
  else
    prefix=$local_cmd_prefix
  fi
  if [ "$(install_type)" = "bare" ];then
    # if bare mode， delete user and group
    local kw_name=$(user_name)
    eval $prefix userdel -r $kw_name >/dev/null 2>&1
    sudo sed -i "/^$kw_name ALL=(ALL)  NOPASSWD: ALL$/d" /etc/sudoers
  fi
}