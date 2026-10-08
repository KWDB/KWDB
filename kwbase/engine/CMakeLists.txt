# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# C++ storage/execution engines under kwbase.
# common: shared C++ base library (migrated from repo-root common/)
# log_engine: time-series shard RaftLogStore (replica-layer log storage)
# ts_engine: time-series WAL/segment + AE physical operators (migrated from repo-root kwdbts2)

add_subdirectory(common ${CMAKE_BINARY_DIR}/common)
add_subdirectory(log_engine ${CMAKE_BINARY_DIR}/log_engine)
add_subdirectory(ts_engine ${CMAKE_BINARY_DIR}/ts_engine)
if (WITH_TESTS)
  enable_testing()
endif ()

# ts_engine sets protobuf toolchain via PARENT_SCOPE; forward to repo root for kwbase.
set(PROTOBUF_C ${PROTOBUF_C} PARENT_SCOPE)
set(PROTOBUF_INC ${PROTOBUF_INC} PARENT_SCOPE)
set(LIBPROTOBUF ${LIBPROTOBUF} PARENT_SCOPE)
