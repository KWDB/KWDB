
color_purple='\033[0;35m '
color_red='\033[0;31m '
color_blue='\033[0;34m '
color_green='\033[0;32m '
color_disable=' \033[0m'

export INFO=$color_purple
export PRIMARY=$color_blue
export FAIL=$color_red
export SUCC=$color_green
export NC=$color_disable

function update_config_section() {
  path=$1
  section=$2
  new_section=$3
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi

  sed -i -e "s/^\[$section\]$/\[$new_section\]/" $path
  return 0
}

function update_config_value() {
  path=$1
  section=$2
  key=$3
  value=$4
  if ! test -e $path ; then 
    echo "$path not exist!"
    return 1 
  fi  
  # update first matched {^$key=} in $section
  # Explanation: /^\[$section\]$/,/^$key=*/ search Range(inclusive)
  # {s/\(^$key=\).*/\1$value/} replace script in Range
  sed -i -e "/^\[$section\]$/,/^$key=*/{s/\(^$key=\).*/\1$value/}" $path
  return 0
}

function delete_config_value() {
  path=$1
  key=$2
  if ! test -e $path ; then
    echo "$path not exist!"
    return 1
  fi
  # delete matched "$key="" in any sections
  sed -i -e "/$key/d" $path
  return 0
}

function read_config_value() {
  path=$1
  section=$2
  key=$3
  if ! test -e $path ; then 
    echo "$path not exist!"
    return 1 
  fi
  sed -n "/^\[$section\]$/,/^$key=*/{s/^$key=//p}" $path
  return 0
}


# input_me_cfg comment section key defaultvalue
function input_cfg() {
  comment=$1
  section=$2
  key=$3
  value=$(read_config_value $cfgpath $section $key)  
  default=${4:-$value}
  echo -e -n "${comment}${INFO}-[$section]:$key:(cur:$value)(def:$default)${NC}:"
  read value
  if [ "$value" = "" ]; then value=$default; fi
  update_config_value $cfgpath $section $key $value
  value=$(read_config_value $cfgpath $section $key)
  echo -e "${SUCC}[$section]:$key:${NC}${value}"
}

function scan_docker_network() {
  network_name=$1
  cnt=$2
  subnet=192.168.192.0/20
  # Check if network exists
  if ! docker network inspect $network_name >/dev/null 2>&1; then
    # Create a docker network temporarily
    docker network create $network_name >/dev/null 2>&1
    # Obtain the subnet and gateway of the network
    subnet=$(docker network inspect --format='{{range .IPAM.Config}}{{.Subnet}}{{end}}' $network_name)
    gateway=$(docker network inspect --format='{{range .IPAM.Config}}{{.Gateway}}{{end}}' $network_name)
    # Delete the temporary network and recreate it
    docker network rm $network_name >/dev/null 2>&1
    docker network create --subnet=$subnet --gateway=$gateway $network_name >/dev/null 2>&1
  fi

  # Get subnet CIDR of network
  subnet=$(docker network inspect --format '{{(index .IPAM.Config 0).Subnet}}' $network_name)
  # Get IP addresses in use
  used_ips=$(docker network inspect --format='{{range $id, $c := .Containers}}{{.IPv4Address}}{{end}}' $network_name)

  # Scan subnet for available IPs
  available_ips=()
  ip_template=${subnet%%/*}
  ip_template=${ip_template%\.*}
  for i in $(seq 2 254); do
    ip="$ip_template.$i"
    if [[ ! $used_ips =~ $ip ]]; then
      available_ips+=($ip)
    fi
    if [ ${#available_ips[@]} -eq $cnt ]; then
      break
    fi
  done

  echo "${available_ips[@]}"
}
