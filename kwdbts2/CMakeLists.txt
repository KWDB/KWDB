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

cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

# KWDBTS2
project(KWDBTS2)
message(STATUS "Project:KWDBTS2")

# project version
if (NOT DEFINED VERSION)
  set(VERSION "unknown")
endif ()
message(STATUS "KWDBTS2 version:${VERSION}")
add_definitions(-DPROJECT_VERSION=\"${VERSION}\")

# commit id
if (NOT DEFINED GIT_COMMIT_ID)
  execute_process(
    COMMAND	git log -1 --format=%H
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
    OUTPUT_VARIABLE GIT_COMMIT_ID
  )
  string(REPLACE "\n" ""  GIT_COMMIT_ID "${GIT_COMMIT_ID}")
  if (GIT_COMMIT_ID STREQUAL "")
    set(GIT_COMMIT_ID "unknown")
  endif ()
endif ()
message(STATUS "Git commit:${GIT_COMMIT_ID}")
add_definitions(-DGIT_COMMIT_ID=\"${GIT_COMMIT_ID}\")

# Used for third-party library search and introduction
include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

if (NOT WIN32)
  add_definitions(-DOS_LINUX -Wno-error)
endif ()

# gcc version verification
message(STATUS "GCC version:" ${CMAKE_CXX_COMPILER_VERSION})
if (CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS 7.3 OR 
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.3)
        message(WARNING "GNU C/C++ compiler version should greater and equal than 7.3")
    endif()
    if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 13.2 OR
        CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 13.2)
        message(WARNING "GNU C/C++ compiler version should less and equal than 13.2")
    endif()
endif()

if (NOT DEFINED CMAKE_CXX_STANDARD)
  # We require a C++17 compliant compiler
  set(CMAKE_CXX_STANDARD 17)
endif ()

if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build Type" FORCE)
endif ()
message(STATUS "Build type:${CMAKE_BUILD_TYPE}")

# Add K_DEBUG and K_RELEASE macros
if (NOT WITH_DEFINITION)
    set(WITH_DEFINITION K_DEBUG)
endif()
if (WITH_DEFINITION STREQUAL "K_DEBUG")
    message(STATUS "Add K_DEBUG definition")
    add_definitions(-DK_DEBUG)
    set(KMALLOC_DEBUGGER ON)
elseif (WITH_DEFINITION STREQUAL "K_RELEASE")
    message(STATUS "Add K_RELEASE definition")
    add_definitions(-DK_RELEASE)
else()
    message(FATAL_ERROR "-DWITH_DEFINITION is not set correctly, please set to K_DEBUG or K_RELEASE")
endif ()

# Add K_DO_NOT_SHIP control to mask all code that doesn't need to be published
if (K_DO_NOT_SHIP)
    message(STATUS "K_DO_NOT_SHIP:ON, block code that does not need to be released")
    add_definitions(-DK_DO_NOT_SHIP)
else()
    message(STATUS "K_DO_NOT_SHIP:OFF, do not block code that does not need to be released")
endif()

# Whether to enable the performance statistics of key methods
if(ENABLE_STATS)
    message(STATUS "Compile with operation statistics.")
    add_definitions(-DKWDB_STATS_ON)
endif()

if (WITH_ASAN)
  message(STATUS "WITH_ASAN:ON, enable asan check, release build should not enable the option")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fno-omit-frame-pointer -fsanitize-recover=address")
  set(LIBASAN_OPTION "-Wl,--copy-dt-needed-entries,-ldl -lasan")
else()
  set(LIBASAN_OPTION "")
endif()

if (KMALLOC_DEBUGGER)
    message(STATUS "Enable memory pool analysis")
    add_definitions(-DKMALLOC_DEBUGGER)
endif ()

# Whether to use shared_mem_info to manage concurrent memory access
if (KMALLOC_SHARED_REFCOUNT)
  add_definitions(-DKMALLOC_SHARED_REFCOUNT)
endif ()

# Whether to record the memory that is not released by the process and release it when the process crashes
if (KMALLOC_RELEASE_PROCESS_MEMORY)
    add_definitions(-DKMALLOC_RELEASE_PROCESS_MEMORY)
endif ()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wreturn-type")

if (ENABLE_COVERAGE)
  message(STATUS "Code coverage check:${ENABLE_COVERAGE}")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  add_definitions(-DENABLE_COVERAGE)
endif ()

# Add code compilation options for mmap
add_definitions(-DTHREAD_SAFE)
# add_definitions(-DNDEBUG)
add_definitions(-DNEW_COUNT)

# try to do some estimate on parallel digree used by make -j
# first, check total memory, assuming we can have one compile job for every 4GB mem
# second, take nproc result.  We take the smaller one as -j parameter
execute_process(
    COMMAND bash -c "free -t -g | tail -n 1 | awk '{print $2}' | xargs -I {} expr {} / 4"
    RESULT_VARIABLE result1
    OUTPUT_VARIABLE value1
    ERROR_VARIABLE error_output1
)

execute_process(
    COMMAND nproc
    RESULT_VARIABLE result2
    OUTPUT_VARIABLE value2
    ERROR_VARIABLE error_output1
)

# taking a pretty big value as init
set(DEDUCTED_PARALLEL_DEGREE "1024")

# if result1 is 0, assign the result to DEDUCTED_PARALLEL_DEGREE
if (${result1} EQUAL 0)
    string(STRIP "${value1}" value1)
    math(EXPR mem_by_four "${value1}")
    message(STATUS "Parallel degree estimation by memory is ${mem_by_four}")
    set(DEDUCTED_PARALLEL_DEGREE ${mem_by_four})
endif ()

# if nproc returns, and if it's smaller than current value, take it
if (${result2} EQUAL 0)
    string(STRIP "${value2}" value2)
    math(EXPR proc_num "${value2}")
    message(STATUS "Parallel degree by nproc is ${proc_num}")
    if (${DEDUCTED_PARALLEL_DEGREE} GREATER ${proc_num})
      set(DEDUCTED_PARALLEL_DEGREE ${proc_num})
    endif ()
endif ()

# to prevent the original big value.  In case it's still very big
# the above two steps fail.  Take 4 as safe value
if (${DEDUCTED_PARALLEL_DEGREE} GREATER 1023)
    set(DEDUCTED_PARALLEL_DEGREE 4)
endif ()
message(STATUS "Final decision on parallel degree is: ${DEDUCTED_PARALLEL_DEGREE}")

FIND_PACKAGE(OpenMP REQUIRED)
if(OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif()

# Build brpc external project
message(STATUS "Building brpc from source: ${PROJECT_SOURCE_DIR}/third_party/brpc-1.9.0")

set(BRPC_VERSION 1.9.0)
set(BRPC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party/brpc-${BRPC_VERSION}")
set(BRPC_PREFIX_DIR "${CMAKE_CURRENT_BINARY_DIR}/brpc-${BRPC_VERSION}")
set(BRPC_BUILD_DIR "${BRPC_PREFIX_DIR}/build")
set(BRPC_OUTPUT_DIR "${BRPC_PREFIX_DIR}/output")

include(ExternalProject)

ExternalProject_Add(
    brpc
    SOURCE_DIR "${BRPC_SOURCE_DIR}"
    PREFIX "${BRPC_PREFIX_DIR}"
    BINARY_DIR "${BRPC_BUILD_DIR}"
    CONFIGURE_COMMAND
        ${CMAKE_COMMAND} -E copy_directory "${BRPC_SOURCE_DIR}" "${BRPC_BUILD_DIR}"
        COMMAND cd "${BRPC_BUILD_DIR}" && sh ./config_brpc.sh --headers=/usr/include "--libs=/usr/lib /usr/lib64"
    BUILD_COMMAND
        cd "${BRPC_BUILD_DIR}" && make -j${DEDUCTED_PARALLEL_DEGREE}
    BUILD_IN_SOURCE 0
    INSTALL_COMMAND
        ${CMAKE_COMMAND} -E make_directory "${BRPC_OUTPUT_DIR}/include"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${BRPC_OUTPUT_DIR}/lib"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${BRPC_BUILD_DIR}/output/include" "${BRPC_OUTPUT_DIR}/include"
        COMMAND ${CMAKE_COMMAND} -E copy_directory "${BRPC_BUILD_DIR}/output/lib" "${BRPC_OUTPUT_DIR}/lib"
)

set(MODULE_NAME kwdbts2)
set(KWDBTS2_SERVER_LIB kwdbts2)

set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)

# include file of kwdbts2
set(TS_INCLUDE
        ../common/src/h
        ../common/src/include
        ../common/src/roachpb
        third_party/gtest-1.8.1/fused-src
        third_party/brpc-1.9.0/src
        ${BRPC_OUTPUT_DIR}/include
        include
        engine/include
        common/include
        storage/include
        mmap/include
        exec/include
        statistic/include
        brpc/include
        )
if (NOT KWBASE_OSS)
    list(APPEND TS_INCLUDE icl/autonomy/include)
endif()
# kwdbts2 module cpp file
if (KWBASE_OSS)
file(GLOB_RECURSE KWDBTS2_SERVER_LIB_SRC
        common/src/*.cpp
        exec/src/*.cpp
        storage/src/*.cpp
        engine/*.cpp
        mmap/src/*.cpp
        mmap/src/lib/*.c
        statistic/src/*.cpp
        brpc/src/*.cpp
        )
else ()
    file(GLOB_RECURSE KWDBTS2_SERVER_LIB_SRC
            common/src/*.cpp
            exec/src/*.cpp
            storage/src/*.cpp
            engine/*.cpp
            mmap/src/*.cpp
            mmap/src/lib/*.c
            statistic/src/*.cpp
            icl/autonomy/src/*.cpp
            brpc/src/*.cpp
    )
endif()
list(FILTER KWDBTS2_SERVER_LIB_SRC EXCLUDE REGEX ".*/tests/.*")

if (ENABLE_COVERAGE)
  # If code coverage detection is enabled, the system collects coverage statistics on the main entry file
  list(APPEND KWDBTS2_SERVER_LIB_SRC ${KWDBTS2_SERVER_EXEC_SRCS})
endif()
# Add the server module source files to the dynamic library
add_library(${KWDBTS2_SERVER_LIB} SHARED ${KWDBTS2_SERVER_LIB_SRC})

# Set brpc library dependency
add_dependencies(${KWDBTS2_SERVER_LIB} brpc)
target_link_libraries(${KWDBTS2_SERVER_LIB} PRIVATE ${BRPC_OUTPUT_DIR}/lib/libbrpc.a)
target_link_libraries(${KWDBTS2_SERVER_LIB} PRIVATE gflags ssl lz4)

# Import the header files required by the server module
target_include_directories(${KWDBTS2_SERVER_LIB} PRIVATE
    ${PROTOBUF_INCLUDE_DIR}
    ${TS_INCLUDE}
    ${BRPC_OUTPUT_DIR}/include
	)

# Compile and generate test case executables
if (WITH_TESTS)
  set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
  enable_testing()
  add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -V)
  add_definitions(-DWITH_TESTS)

  if (KWBASE_OSS)
      file(GLOB_RECURSE KWDBTS2_SERVER_TEST_SRC
              mmap/tests/*.cpp
              storage/tests/*.cpp
              exec/tests/*.cpp
              engine/tests/*.cpp
              statistic/tests/*.cpp
              brpc/tests/*.cpp
              )
  else ()
      file(GLOB_RECURSE KWDBTS2_SERVER_TEST_SRC
              mmap/tests/*.cpp
              storage/tests/*.cpp
              exec/tests/*.cpp
              engine/tests/*.cpp
              statistic/tests/*.cpp
              icl/autonomy/tests/*.cpp
              brpc/tests/*.cpp
      )
  endif()
  foreach (tsrc ${KWDBTS2_SERVER_TEST_SRC})
    # Build target name from filename (eg: icl_db_test.cc for icl/db_test.cc).
    get_filename_component(filename ${tsrc} NAME_WE)
    get_filename_component(dirname ${tsrc} DIRECTORY)
    get_filename_component(dircmake ${tsrc} DIRECTORY)
    string(REPLACE "${PROJECT_SOURCE_DIR}/" "" dirname "${dirname}")
    string(REPLACE "/" "_" dirname "${dirname}")
    if ("${dirname}" STREQUAL "")
      set(tname ${filename})
    else ()
      set(tname ${dirname}_${filename})
    endif ()
    #start make test[i]
    if(EXISTS ${dircmake}/CMakeLists.txt)
        add_subdirectory(${dircmake})
    else ()
        add_executable(${tname} ${tsrc})
        target_include_directories(${tname}
                PRIVATE ${TS_INCLUDE}
                ${PROTOBUF_INCLUDE_DIR}
                third_party/gtest-1.8.1/fused-src
                )

        target_link_libraries(${tname} PRIVATE ${KWDBTS2_SERVER_LIB} ${PROTOBUF_LIB} common gtest dl rt)
        set_target_properties(${tname} PROPERTIES
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
                COMPILE_OPTIONS "-Wno-error;-Wall;-Wno-sign-compare"
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir
                )
    endif()
    #end make test[i]
    add_test(NAME ${tname} 
      COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir/${tname}
      WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir)
    add_dependencies(check ${tname})
  endforeach ()
endif ()

# Older versions of gcc required an explicit link to stdc++fs
if (CMAKE_COMPILER_IS_GNUCC AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.1)
        target_link_libraries(${KWDBTS2_SERVER_LIB} PRIVATE stdc++fs)
endif ()

INSTALL(TARGETS ${KWDBTS2_SERVER_LIB}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)

add_custom_target(cov
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/shell/code_coverage.sh ${CMAKE_BINARY_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/shell/run_test.sh ${CMAKE_BINARY_DIR}
        COMMAND ${CMAKE_BINARY_DIR}/run_test.sh ${CMAKE_BINARY_DIR} exec
        COMMAND ${CMAKE_BINARY_DIR}/code_coverage.sh ${CMAKE_BINARY_DIR}
        )

add_custom_target(memcheck
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/shell/code_memcheck.sh ${CMAKE_BINARY_DIR}
    COMMAND ${CMAKE_BINARY_DIR}/code_memcheck.sh ${CMAKE_BINARY_DIR}
)
