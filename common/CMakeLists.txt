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

# LLVM options
set(LLVM_DETECTOR "")
if (KMALLOC_INSTRUMENTATION)
    set (CMAKE_C_COMPILER "/usr/bin/clang")
    set (CMAKE_CXX_COMPILER "/usr/bin/clang++")
    add_subdirectory(src/instrumentation)
    add_definitions(-Wno-varargs)
    set(LLVM_DETECTOR "detector")
    # LLVM requires K_DEBUG
    set(WITH_DEFINITION K_DEBUG)
    add_definitions(-DKMALLOC_INSTRUMENTATION)
endif ()

# COMMON
project(COMMON)
message(STATUS "Project:COMMON")

# project version
if (NOT DEFINED VERSION)
    set(VERSION "unknown")
endif ()
message(STATUS "COMMON version:${VERSION}")
add_definitions(-DPROJECT_VERSION=\"${VERSION}\")

if (NOT WIN32)
    add_definitions(-DOS_LINUX -Werror)
endif ()

# gcc version verification
if (KMALLOC_INSTRUMENTATION)
    message(STATUS "CLANG version:" ${CMAKE_CXX_COMPILER_VERSION})
else()
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

# Add K_DEBUG_FILE_INFO to control the error module information output
option(K_DEBUG_FILE_INFO "display the complete error information" OFF)
if(K_DEBUG_FILE_INFO)
    message(STATUS "K_DEBUG_FILE_INFO:ON, The complete error information is displayed")
    add_definitions(-DK_DEBUG_FILE_INFO)
else()
    message(STATUS "K_DEBUG_FILE_INFO:OFF, The complete error information is not displayed")
endif()

# Add K_DO_NOT_SHIP control to mask all code that doesn't need to be published
if (K_DO_NOT_SHIP)
    message(STATUS "K_DO_NOT_SHIP:ON, block code that does not need to be released")
    add_definitions(-DK_DO_NOT_SHIP)
else()
    message(STATUS "K_DO_NOT_SHIP:OFF, do not block code that does not need to be released")
endif()

if (KMALLOC_DEBUGGER)
    message(STATUS "Enable memory pool analysis")
    add_definitions(-DKMALLOC_DEBUGGER)
endif ()

# Whether to use shared_mem_info to manage concurrent memory access
if (KMALLOC_SHARED_REFCOUNT)
    add_definitions(-DKMALLOC_SHARED_REFCOUNT)
endif ()

option(KMALLOC_PERFORMANCE_OPTIMIZATION "enable kmalloc high performance mode" OFF)

# Whether to enable the memory pool high performance mode
# After this function is enabled, functions such as TRACE/LOG cannot be used, but the memory pool performance is improved
if (KMALLOC_PERFORMANCE_OPTIMIZATION)
    add_definitions(-DKMALLOC_PERFORMANCE_OPTIMIZATION)
endif ()

# Whether to record the memory that is not released by the process and release it when the process crashes
if (KMALLOC_RELEASE_PROCESS_MEMORY)
    add_definitions(-DKMALLOC_RELEASE_PROCESS_MEMORY)
endif ()

# Whether to enable span trace plug-in compilation
if(ENABLE_TRACING)
    message(STATUS "Compile with tracing plugin, Add K_OPENTRACING definition")
    add_definitions(-DK_OPENTRACING)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -U__FILE_NAME__ -D__FILE_NAME__='\"$(subst $(dir $<),,$<)\"'")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -U__MODULE_NAME__ -D__MODULE_NAME__='\"$(lastword $(subst /, ,$(dir $<)))\"'")
endif()

# Whether to use lock-free tracker, for lock-free data structure memory management purpose
if(LOCKFREE_TRACKER)
    message(STATUS "Use lock-free tracker, for lock-free data structure memory management purpose")
    add_definitions(-DLOCKFREE_TRACKER)
endif()
# Adding the LATCH debug option is disabled by default to prevent performance testing
option(ENABLE_LATCH_DEBUG "enable latch debug" OFF)
if (ENABLE_LATCH_DEBUG)
    message(STATUS "ENABLE_LATCH_DEBUG:ON, enable latch debug")
    add_definitions(-DENABLE_LATCH_DEBUG)
endif()
# Specifies the directory where the common module is compiled and generated
set(MODULE_NAME common)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)


function(generate_proto protofile)
  message(STATUS "protofile:${protofile}")
  get_filename_component(protofolder ${protofile} PATH)
  get_filename_component(protoname ${protofile} NAME_WLE)
  message(STATUS "protoname:${protoname}")
  if (EXISTS "${protofolder}/${protoname}.pb.cc")
      message("${protoname}.pb.cc is exists")
      message("mkdir -p ${CMAKE_BINARY_DIR}/tmp")
      message("cp -arf ${protofile} ${CMAKE_BINARY_DIR}/tmp/${protoname}.proto")
      message("${PROTOBUF_C} ${CMAKE_BINARY_DIR}/tmp/${protoname}.proto --cpp_out=${CMAKE_BINARY_DIR}/tmp -I ${CMAKE_BINARY_DIR}/tmp -I ${protofolder}")
      EXECUTE_PROCESS(
          COMMAND mkdir -p ${CMAKE_BINARY_DIR}/tmp
          COMMAND cp -arf ${protofile} ${CMAKE_BINARY_DIR}/tmp/
          COMMAND ${PROTOBUF_C} ${CMAKE_BINARY_DIR}/tmp/${protoname}.proto --cpp_out=${CMAKE_BINARY_DIR}/tmp -I ${CMAKE_BINARY_DIR}/tmp -I ${protofolder}
      )
      message("check diff file ${CMAKE_BINARY_DIR}/tmp/${protoname}.pb.cc ${protofolder}/${protoname}.pb.cc")
      EXECUTE_PROCESS(
	      COMMAND diff ${CMAKE_BINARY_DIR}/tmp/${protoname}.pb.cc ${protofolder}/${protoname}.pb.cc
	      COMMAND diff ${CMAKE_BINARY_DIR}/tmp/${protoname}.pb.h ${protofolder}/${protoname}.pb.h
	      RESULT_VARIABLE changed
	      OUTPUT_QUIET
	      )
      if ( ${changed} EQUAL 1)
	      message(STATUS "${protofile} has changed")
        EXECUTE_PROCESS(
            COMMAND ${PROTOBUF_C} ${protofile} --cpp_out=${protofolder} -I ${protofolder}
        )
      else()
	      message(STATUS "${protofile} has nothing to be done")
      endif()
      EXECUTE_PROCESS(
         COMMAND rm -rf ${CMAKE_BINARY_DIR}/tmp
      )
  else()
      message("${protoname}.pb.cc does not exist")
      EXECUTE_PROCESS(
          COMMAND ${PROTOBUF_C} ${protofile} --cpp_out=${protofolder} -I ${protofolder}
      )
  endif()
endfunction(generate_proto)

# Generate C++ files from proto files
file(GLOB_RECURSE NART_PROTOS_EE
  src/roachpb/ee_pb_plan.proto)
file(GLOB_RECURSE NART_PROTOS_META
  src/roachpb/me_metadata.proto)
file(GLOB_RECURSE NART_PROTOS_BR
  src/roachpb/br_internal_service.proto)
generate_proto(${NART_PROTOS_META})
generate_proto(${NART_PROTOS_EE})
generate_proto(${NART_PROTOS_BR})

file(GLOB COMMON_SRC
        src/mem/*.cpp
        src/log/*.cpp
        src/trace/*.cpp
        src/error/*.cpp
        src/common/*.cpp
        src/data_struct/*.cpp
        src/latch/*.cpp
        src/thread/*.cpp
        src/roachpb/*.cc)

list(FILTER COMMON_SRC EXCLUDE REGEX ".*/test/.*")

# Build brpc external project in common module
message(STATUS "Building brpc from source in common module: ${PROJECT_SOURCE_DIR}/third_party/brpc-1.9.0")

set(BRPC_VERSION 1.9.0)
set(BRPC_SOURCE_DIR "${PROJECT_SOURCE_DIR}/third_party/brpc-${BRPC_VERSION}")
set(BRPC_PREFIX_DIR "${CMAKE_CURRENT_BINARY_DIR}/brpc-${BRPC_VERSION}")
set(BRPC_BUILD_DIR "${BRPC_PREFIX_DIR}/build")
set(BRPC_OUTPUT_DIR "${BRPC_PREFIX_DIR}/output")

# try to do some estimate on parallel degree used by make -j
# first, check total memory, assuming we can have one compile job for every 4GB mem
# second, take nproc result. We take the smaller one as -j parameter
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

# to prevent the original big value. In case it's still very big
# the above two steps fail. Take 4 as safe value
if (${DEDUCTED_PARALLEL_DEGREE} GREATER 1023)
    set(DEDUCTED_PARALLEL_DEGREE 4)
endif ()
message(STATUS "Final decision on parallel degree for brpc is: ${DEDUCTED_PARALLEL_DEGREE}")

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

# common
set(COMMON_LIB common)
add_library(${COMMON_LIB} SHARED ${COMMON_SRC})
target_include_directories(${COMMON_LIB}
        PUBLIC 
            src/h
            ${BRPC_OUTPUT_DIR}/include
            third_party/brpc-1.9.0/src
            third_party/gtest-1.8.1/fused-src
            ../kwdbts2/latch/include
        PRIVATE 
            src/include
            src/roachpb
            ../kwdbts2/common/include
        )

# Set brpc library dependency for common lib
add_dependencies(${COMMON_LIB} brpc)
target_link_libraries(${COMMON_LIB} ${BRPC_OUTPUT_DIR}/lib/libbrpc.a gflags ssl lz4 protobuf dl)

if (KMALLOC_OVERRIDE)
    set(COMMON_SRC_OVERRIDE ${COMMON_SRC})
    list(FILTER COMMON_SRC_OVERRIDE EXCLUDE REGEX ".*cgo.*")

    set(COMMON_OVERRIDE_LIB common_override)
    add_library(${COMMON_OVERRIDE_LIB} SHARED ${COMMON_SRC_OVERRIDE})
    add_dependencies(${COMMON_OVERRIDE_LIB} brpc)
    target_compile_definitions(${COMMON_OVERRIDE_LIB} PUBLIC KMALLOC_OVERRIDE)
    target_include_directories(${COMMON_OVERRIDE_LIB}
            PUBLIC 
                src/h
                ${BRPC_OUTPUT_DIR}/include
                third_party/brpc-1.9.0/src
            PRIVATE 
                src/include
                src/roachpb
                ../kwdbts2/common/include
                third_party/gtest-1.8.1/fused-src
            )
    target_link_libraries(${COMMON_OVERRIDE_LIB} ${BRPC_OUTPUT_DIR}/lib/libbrpc.a gflags ssl lz4 protobuf dl)
endif()

# Compile and generate test case executables
option(WITH_TESTS "build with tests" ON)
message(STATUS "Build with tests:${WITH_TESTS}")

add_subdirectory(third_party/gtest-1.8.1/fused-src/gtest gtest.out)

if (WITH_TESTS)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/tests)
    enable_testing()
    add_custom_target(commoncheck COMMAND ${CMAKE_CTEST_COMMAND} -V)
    add_definitions(-DWITH_TESTS)

    file(GLOB KWDB_COMMON_TEST_SRC
            src/common/test/*.cpp
            src/data_struct/test/*.cpp
            src/thread/test/*.cpp
            )

    if (K_DO_NOT_SHIP)
        list(FILTER KWDB_COMMON_TEST_SRC EXCLUDE REGEX "src/common/test/cm_fault_injection_test.cpp")
        list(FILTER KWDB_COMMON_TEST_SRC EXCLUDE REGEX "src/mem/test/mm_debugger_server_test.cpp")
    endif()

    foreach (tsrc ${KWDB_COMMON_TEST_SRC})
        get_filename_component(filename ${tsrc} NAME_WE)
        get_filename_component(dirname ${tsrc} DIRECTORY)
        string(REPLACE "${PROJECT_SOURCE_DIR}/" "" dirname "${dirname}")
        string(REPLACE "/" "_" dirname "${dirname}")
        if ("${dirname}" STREQUAL "")
            set(tname kwdb_common_${filename})
        else ()
            set(tname kwdb_common_${dirname}_${filename})
        endif ()
        add_executable(${tname} ${tsrc})
        target_include_directories(${tname}
                PUBLIC src/h
                PRIVATE src/include
                PRIVATE src/roachpb
                PRIVATE ../kwdbts2/common/include
                PRIVATE src/mem/test/h
                third_party/gtest-1.8.1/fused-src
                third_party/brpc-1.9.0/src
                )
        if (KMALLOC_INSTRUMENTATION)
            set_target_properties(${tname} PROPERTIES COMPILE_FLAGS
            "-g -Wno-tautological-pointer-compare -flegacy-pass-manager \
            -Xclang -load -Xclang ${CMAKE_BINARY_DIR}/src/instrumentation/llvm-pass/libUserPass.so")
            add_dependencies(${tname} UserPass detector)
        endif ()
        target_link_libraries(${tname} gtest common pthread atomic ${LLVM_DETECTOR})
        set_target_properties(${tname} PROPERTIES
                CXX_STANDARD_REQUIRED YES
                CXX_EXTENSIONS NO
                COMPILE_OPTIONS "-Wno-error;-Wall;-Wno-sign-compare"
                RUNTIME_OUTPUT_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir
                )
        add_test(NAME ${tname} 
          COMMAND ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir/${tname}
          WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${tname}.dir)
        add_dependencies(commoncheck ${tname})
    endforeach ()
endif ()

install(TARGETS ${COMMON_LIB} ${COMMON_OVERRIDE_LIB}
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

