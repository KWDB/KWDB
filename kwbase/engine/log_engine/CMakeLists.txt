# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# Time-series shard RaftLogStore (replica-layer log storage).

set(LOG_ENGINE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(LOG_ENGINE_INCLUDE ${LOG_ENGINE_DIR}/include PARENT_SCOPE)
set(LOG_ENGINE_SRC
    ${LOG_ENGINE_DIR}/src/raft_store.cpp
    ${LOG_ENGINE_DIR}/src/raft_log_api.cpp
    PARENT_SCOPE)
set(LOG_ENGINE_TEST_SRC ${LOG_ENGINE_DIR}/tests/raft_store_test.cpp PARENT_SCOPE)
