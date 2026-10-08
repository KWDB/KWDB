#!/bin/bash -e
source ./utils.sh

if [ $# -lt 1 ]
then
  echo -e "Failed!\n\tusage: $0 username"
  exit 1
fi
script_dir=$(pwd)
username=$1
host=$2

if [[ -z "${KWDB_ROOT}" ]]; then
  KWDB_ROOT=$(cd ..; pwd)
fi

KWDB_BIN_DIR=${KWDB_ROOT}/bin
KWDB_LIB_DIR=${KWDB_ROOT}/lib
KWDB_CERTS_DIR=${KWDB_ROOT}/certs

KWDB_CA_KEY_FILE=${KWDB_CERTS_DIR}/ca.key
KWDB_CA_CRT_FILE=${KWDB_CERTS_DIR}/ca.crt
KWDB_NODE_KEY_FILE=${KWDB_CERTS_DIR}/node.key
KWDB_NODE_CRT_FILE=${KWDB_CERTS_DIR}/node.crt
KWDB_ROOT_KEY_FILE=${KWDB_CERTS_DIR}/client.$username.key
KWDB_ROOT_CRT_FILE=${KWDB_CERTS_DIR}/client.$username.crt
KWDB_ROOT_PK8_FILE=${KWDB_CERTS_DIR}/client.$username.pk8

if [ ! -d "${KWDB_CERTS_DIR}" ]; then
  mkdir -p ${KWDB_CERTS_DIR}
fi

if [ ! -f "${KWDB_CA_KEY_FILE}" -o ! -f "${KWDB_CA_CRT_FILE}" ]; then
  rm -rf ${KWDB_CA_KEY_FILE} ${KWDB_CA_CRT_FILE}
  LD_LIBRARY_PATH=${KWDB_LIB_DIR}:${LD_LIBRARY_PATH} ./kwbase cert create-ca --certs-dir=${KWDB_CERTS_DIR} --ca-key=${KWDB_CA_KEY_FILE}
  echo "CA certificate generation finished!"
fi

if [ -n "$host" ]; then
  rm -rf ${KWDB_NODE_KEY_FILE} ${KWDB_NODE_CRT_FILE}
  LD_LIBRARY_PATH=${KWDB_LIB_DIR}:${LD_LIBRARY_PATH} ./kwbase cert create-node $host --certs-dir=${KWDB_CERTS_DIR} --ca-key=${KWDB_CA_KEY_FILE}
  echo "node certificate generation finished!"
fi

if [ ! -f "${KWDB_ROOT_KEY_FILE}" -o ! -f "${KWDB_ROOT_CRT_FILE}" ]; then
  rm -rf ${KWDB_ROOT_KEY_FILE} ${KWDB_ROOT_CRT_FILE}
  LD_LIBRARY_PATH=${KWDB_LIB_DIR}:${LD_LIBRARY_PATH} ./kwbase cert create-client $username --certs-dir=${KWDB_CERTS_DIR} --ca-key=${KWDB_CA_KEY_FILE}
  openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in ${KWDB_ROOT_KEY_FILE} -out ${KWDB_ROOT_PK8_FILE}
  chmod 644 ${KWDB_ROOT_PK8_FILE}
  echo "client certificate for $username user generation finished!"
fi

echo "certificate generation finished!"
