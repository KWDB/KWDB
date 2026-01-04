#! /bin/bash
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

set -euo pipefail

run_containers() {
    # 1. start single node container
    container_name=${CONTAINER_NAME_SINGLE}
    if [ $(docker ps -a | grep ${container_name} | wc -l) -eq 0 ]; then
        echo "create ${container_name} container"
        docker run --name=${container_name} \
            --hostname=${host_ip} \
            --cpuset-cpus=0-15 \
            --volume /var/run/docker.sock:/var/run/docker.sock \
            --volume /dev:/dev \
            --volume ${NODE_HOME_SINGLE}:/home/inspur/src/gitee.com \
            --volume ${REPORT_DIR}:/home/inspur/src/reports \
            --volume /usr/bin/docker:/usr/bin/docker \
            --network=host \
            --privileged \
            --workdir=/home/inspur/src/github.com \
            --runtime=runc \
            --memory="34359738368" \
            --detach=true \
            ${KWDB_DOCKER_IMAGE} \
            /bin/bash -c 'while true; do sleep 10000; done'
    else
        echo "${container_name} container already exists"
        if [ $(docker inspect -f '{{.State.Running}}' ${container_name}) != "true" ]; then
            echo "start ${container_name} container"
            docker start ${container_name}
        else
            echo "${container_name} container is already running"
        fi
    fi

    container_name=${CONTAINER_NAME_1}
    if [ $(docker ps -a | grep ${container_name} | wc -l) -eq 0 ]; then
        echo "create ${container_name} container"
        docker run --name=${container_name} \
            --hostname=${host_ip} \
            --cpuset-cpus=16-31 \
            --volume /var/run/docker.sock:/var/run/docker.sock \
            --volume /dev:/dev \
            --volume ${NODE_HOME_1}:/home/inspur/src/gitee.com \
            --volume ${REPORT_DIR}:/home/inspur/src/reports \
            --volume /usr/bin/docker:/usr/bin/docker \
            --network=host \
            --privileged \
            --workdir=/home/inspur/src/github.com \
            --runtime=runc \
            --memory="34359738368" \
            --detach=true \
            ${KWDB_DOCKER_IMAGE} \
            /bin/bash -c 'while true; do sleep 10000; done'
    else
        echo "${container_name} container already exists"
        if [ $(docker inspect -f '{{.State.Running}}' ${container_name}) != "true" ]; then
            echo "start ${container_name} container"
            docker start ${container_name}
        else
            echo "${container_name} container is already running"
        fi
    fi

    container_name=${CONTAINER_NAME_2}
    if [ $(docker ps -a | grep ${container_name} | wc -l) -eq 0 ]; then
        echo "create ${container_name} container"
        docker run --name=${container_name} \
            --hostname=${host_ip} \
            --cpuset-cpus=32-47 \
            --volume /var/run/docker.sock:/var/run/docker.sock \
            --volume /dev:/dev \
            --volume ${NODE_HOME_2}:/home/inspur/src/gitee.com \
            --volume ${REPORT_DIR}:/home/inspur/src/reports \
            --volume /usr/bin/docker:/usr/bin/docker \
            --network=host \
            --privileged \
            --workdir=/home/inspur/src/github.com \
            --runtime=runc \
            --memory="34359738368" \
            --detach=true \
            ${KWDB_DOCKER_IMAGE} \
            /bin/bash -c 'while true; do sleep 10000; done'
    else
        echo "${container_name} container already exists"
        if [ $(docker inspect -f '{{.State.Running}}' ${container_name}) != "true" ]; then
            echo "start ${container_name} container"
            docker start ${container_name}
        else
            echo "${container_name} container is already running"
        fi
    fi

    container_name=${CONTAINER_NAME_3}
    if [ $(docker ps -a | grep ${container_name} | wc -l) -eq 0 ]; then
        echo "create ${container_name} container"
        docker run --name=${container_name} \
            --hostname=${host_ip} \
            --cpuset-cpus=48-63 \
            --volume /var/run/docker.sock:/var/run/docker.sock \
            --volume /dev:/dev \
            --volume ${NODE_HOME_3}:/home/inspur/src/gitee.com \
            --volume ${REPORT_DIR}:/home/inspur/src/reports \
            --volume /usr/bin/docker:/usr/bin/docker \
            --volume ${cur_path}:/home/inspur/src/scripts \
            --network=host \
            --privileged \
            --workdir=/home/inspur/src/github.com \
            --runtime=runc \
            --memory="34359738368" \
            --detach=true \
            ${KWDB_DOCKER_IMAGE} \
            /bin/bash -c 'while true; do sleep 10000; done'
    else
        echo "${container_name} container already exists"
        if [ $(docker inspect -f '{{.State.Running}}' ${container_name}) != "true" ]; then
            echo "start ${container_name} container"
            docker start ${container_name}
        else
            echo "${container_name} container is already running"
        fi
    fi

    return $?
}