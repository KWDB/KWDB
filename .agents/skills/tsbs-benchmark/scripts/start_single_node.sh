source $(git rev-parse --show-toplevel)/.agents/skills/tsbs-benchmark/scripts/utils.sh

start_single_node tsbs_n1

$KWBIN sql --insecure --host="$host_ip:$listenport" < ${PROJ_BASE_DIR}/.agents/skills/tsbs-benchmark/scripts/cluster-settings/general.sql
