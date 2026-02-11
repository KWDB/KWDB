# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.
cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

project(KWDBTS-bench)
message(STATUS "Project:KWDBTS-bench")

# project version
if (NOT DEFINED VERSION)
    set(VERSION "unknown")
endif ()
message(STATUS "KWDBTS-bench version:${VERSION}")
add_definitions(-DPROJECT_VERSION=\"${VERSION}\")

# commit id
if (NOT DEFINED GIT_COMMIT_ID)
    execute_process(
            COMMAND	git log -1 --format=%H
            WORKING_DIRECTORY ../../..
            OUTPUT_VARIABLE GIT_COMMIT_ID
    )
    string(REPLACE "\n" ""  GIT_COMMIT_ID "${GIT_COMMIT_ID}")
    if (GIT_COMMIT_ID STREQUAL "")
        set(GIT_COMMIT_ID "unknown")
    endif ()
endif ()
message(STATUS "Git commit:${GIT_COMMIT_ID}")
add_definitions(-DGIT_COMMIT_ID=\"${GIT_COMMIT_ID}\")

add_definitions(-DKAIWU)
add_definitions(-DIOT_MODE)
add_definitions(-DTHREAD_SAFE)
add_definitions(-DNDEBUG)
add_definitions(-DNEW_COUNT)

# Used for third-party library search and introduction
include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

if (NOT WIN32)
    add_definitions(-DOS_LINUX -Werror)
endif ()

# gcc version verification
message(STATUS "GCC version:" ${CMAKE_CXX_COMPILER_VERSION})

if (NOT DEFINED CMAKE_CXX_STANDARD)
    # We require a C++17 compliant compiler
    set(CMAKE_CXX_STANDARD 17)
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wno-format-truncation")

# Whether to enable span trace plug-in compilation
option(ENABLE_TRACING "Compile with tracing plugin." ON)
if(ENABLE_TRACING)
    message(STATUS "Compile with tracing plugin, Add K_OPENTRACING definition")
    add_definitions(-DK_OPENTRACING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -U__FILE_NAME__ -D__FILE_NAME__='\"$(subst $(dir $<),,$<)\"'")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -U__MODULE_NAME__ -D__MODULE_NAME__='\"$(lastword $(subst /, ,$(dir $<)))\"'")
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wreturn-type")

# find protobuf
find_package(Protobuf REQUIRED)

if (NOT KWDB_PROJECT_DIR)
    set(KWDB_PROJECT_DIR ${PROJECT_SOURCE_DIR}/../../../)
    message(STATUS  "KWDB_PROJECT_DIR=${KWDB_PROJECT_DIR}")
endif ()

set(KWDBTS_PROJ_DIR ${KWDB_PROJECT_DIR}/kwdbts2)


option(WITH_DEFINITION "Add macro definition control" K_DEBUG)
if (NOT WITH_DEFINITION)
    set(WITH_DEFINITION K_RELEASE)
endif()
if (WITH_DEFINITION STREQUAL "K_DEBUG")
    message(STATUS "Add K_DEBUG definition")
    add_definitions(-DK_DEBUG)
elseif (WITH_DEFINITION STREQUAL "K_RELEASE")
    message(STATUS "Add K_RELEASE definition")
    add_definitions(-DK_RELEASE)
else()
    message(FATAL_ERROR "-DWITH_DEFINITION is not set correctly, please set to K_DEBUG or K_RELEASE")
endif ()

# Macro definition used in code
add_definitions(-DDATA_DIR="${CMAKE_SOURCE_DIR}/qa/stress_tests/kwdbts-bench2/ts_data")

get_filename_component(PROJECT_BINARY_DIR_PARENT_DIR1 ${PROJECT_BINARY_DIR} DIRECTORY)
get_filename_component(PROJECT_BINARY_DIR_PARENT_DIR2 ${PROJECT_BINARY_DIR_PARENT_DIR1} DIRECTORY)
get_filename_component(PROJECT_BINARY_DIR_PARENT_DIR3 ${PROJECT_BINARY_DIR_PARENT_DIR2} DIRECTORY)
set(BRPC_INCLUDE_DIR ${NATIVE}/kwdbts2/third_party/include)

# embed will use the include file
set(embed_includes worker ${PROTOBUF_INCLUDE_DIR} src/worker
        ${KWDB_PROJECT_DIR}/common/src/include
        ${KWDB_PROJECT_DIR}/common/src/h
        ${KWDBTS_PROJ_DIR}/include
        ${KWDBTS_PROJ_DIR}/mmap/include
        ${KWDBTS_PROJ_DIR}/common/include
        ${KWDBTS_PROJ_DIR}/engine/include
        ${KWDBTS_PROJ_DIR}/ts_engine/include
        ${KWDBTS_PROJ_DIR}/storage/include
        ${KWDBTS_PROJ_DIR}/roachpb
        ${KWDBTS_PROJ_DIR}/latch/include
        ${KWDBTS_PROJ_DIR}/brpc/include
        ${BRPC_INCLUDE_DIR}
        ${KWDBTS_PROJ_DIR}/exec/include
        )
#message("embed_includes:${embed_includes}")

## Compile to generate an executable file
aux_source_directory(src/worker WORKER_SRCS)

aux_source_directory(src/worker/embed EMBED_SRCS)
add_executable(kdp_bench src/kdp_bench.cpp ${WORKER_SRCS} ${EMBED_SRCS})
target_include_directories(kdp_bench PRIVATE ${embed_includes})
target_link_libraries(kdp_bench PUBLIC kwdbts2 common ssl atomic z)

INSTALL(FILES
        ${PROJECT_BINARY_DIR}/kdp_bench
        DESTINATION ${CMAKE_INSTALL_PREFIX}/stress_tests
        PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_WRITE GROUP_READ
        WORLD_EXECUTE WORLD_WRITE WORLD_READ)

FIND_PACKAGE(OpenMP REQUIRED)
if(OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()
  
