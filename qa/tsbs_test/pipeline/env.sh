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

OPEN_SOURCE="false"
CLONE_DIR=${CLONE_DIR:-"/home/inspur/src/gitee.com"}
REPORT_DIR="/data1/reports"

KWDB_DOCKER_IMAGE="devops.inspur.com:80/isg/zdp/repo/local_repo/zdp/builder/kaiwudb_amd64_ub_2204:3.0.0"

CONTAINER_NAME_SINGLE="tsbs_test_single"
NODE_HOME_SINGLE="/data5/tsbs_test/gitee.com" 

CONTAINER_NAME_1="tsbs_test_1"
NODE_HOME_1="/data2/tsbs_test/gitee.com"
CONTAINER_NAME_2="tsbs_test_2"
NODE_HOME_2="/data3/tsbs_test/gitee.com"
CONTAINER_NAME_3="tsbs_test_3"
NODE_HOME_3="/data4/tsbs_test/gitee.com"

