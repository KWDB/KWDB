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
message(STATUS "GCC version:" ${CMAKE_CXX_COMPILER_VERSION})
if (CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_C_COMPILER_VERSION VERSION_LESS 7.3 OR 
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.3)
        message(WARNING "GNU C/C++ compiler version should greater and equal than 7.3")
    endif()
    if(CMAKE_C_COMPILER_VERSION VERSION_GREATER 13.2 OR
        CMAKE_CXX_COMPILER_VERSION VERSION_LESS 13.2)
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

if (KMALLOC_DEBUGGER)
    message(STATUS "Enable memory pool analysis")
    add_definitions(-DKMALLOC_DEBUGGER)
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

file(GLOB COMMON_SRC
        src/mem/*.cpp
        src/log/*.cpp
        src/trace/*.cpp
        src/error/*.cpp
        src/common/*.cpp
        src/data_struct/*.cpp
        src/latch/*.cpp
        src/thread/*.cpp)

list(FILTER COMMON_SRC EXCLUDE REGEX ".*/test/.*")

# common
set(COMMON_LIB common)
add_library(${COMMON_LIB} SHARED ${COMMON_SRC})
target_include_directories(${COMMON_LIB}
        PUBLIC src/h
        PRIVATE src/include
        PRIVATE ../kwdbts2/common/include
        PUBLIC ../kwdbts2/latch/include
        ../kwdbts2/third_party/gtest-1.8.1/fused-src
        )
target_link_libraries(${COMMON_LIB} dl)

if (KMALLOC_OVERRIDE)
    set(COMMON_SRC_OVERRIDE ${COMMON_SRC})
    list(FILTER COMMON_SRC_OVERRIDE EXCLUDE REGEX ".*cgo.*")

    set(COMMON_OVERRIDE_LIB common_override)
    add_library(${COMMON_OVERRIDE_LIB} SHARED ${COMMON_SRC_OVERRIDE})
    target_compile_definitions(${COMMON_OVERRIDE_LIB} PUBLIC KMALLOC_OVERRIDE)
    target_include_directories(${COMMON_OVERRIDE_LIB}
            PUBLIC src/h
            PRIVATE src/include
            PRIVATE ../kwdbts2/common/include
            ../kwdbts2/third_party/gtest-1.8.1/fused-src
            )
    target_link_libraries(${COMMON_OVERRIDE_LIB} protobuf dl)
endif()

# Compile and generate test case executables
option(WITH_TESTS "build with tests" ON)
message(STATUS "Build with tests:${WITH_TESTS}")

add_subdirectory(../kwdbts2/third_party/gtest-1.8.1/fused-src/gtest gtest.out)

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
                PRIVATE ../kwdbts2/common/include
                PRIVATE src/mem/test/h
                ../kwdbts2/third_party/gtest-1.8.1/fused-src
                )
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
