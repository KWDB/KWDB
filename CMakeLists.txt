cmake_minimum_required(VERSION 3.6 FATAL_ERROR)

PROJECT(KWDB)
message(STATUS "Project:KWDB")

set(KWDB_PROJECT_DIR ${CMAKE_CURRENT_SOURCE_DIR})

if (NOT DEFINED CMAKE_BUILD_PARALLEL_LEVEL)
    set(PARALLEL_LEVEL "")
else()
    set(PARALLEL_LEVEL "-j${CMAKE_BUILD_PARALLEL_LEVEL}")
endif()

message(STATUS "CMAKE_BUILD_PARALLEL_LEVEL:${CMAKE_BUILD_PARALLEL_LEVEL}")

# project version
if (NOT DEFINED VERSION)
    set(VERSION "unknown")
endif ()
message(STATUS "KWDB version:${VERSION}")

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

add_definitions(-DIOT_MODE)

# Used for third-party library search and introduction
include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

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

# Whether to enable memory pool analysis parameter
option(KMALLOC_DEBUGGER "Enable memory pool analysis" OFF)

option(MULTI_CLIENT_BENCHMARK "disable pubsub context reuse" OFF)
if (MULTI_CLIENT_BENCHMARK)
    message(STATUS "PubSubContext reuse disabled")
    add_definitions(-DMULTI_CLIENT_BENCHMARK)
else()
    message(STATUS "PubSubContext reuse enabled")
endif()

option(KWBASE_OSS "Whether to compile an open-source edition" ON)
if (KWBASE_OSS)
    message(STATUS "Compile open-source edition")
    add_definitions(-DKWBASE_OSS)
else()
    message(STATUS "Compile enterprise edition")
endif()

# Add K_DEBUG and K_RELEASE macros
option(WITH_DEFINITION "Add macro definition control" K_DEBUG)
if(NOT WITH_DEFINITION)
    if (CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
        set(WITH_DEFINITION K_RELEASE)
    else()
        set(WITH_DEFINITION K_DEBUG)
    endif()
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
option(K_DO_NOT_SHIP "Block code that does not need to be released" OFF)
if (K_DO_NOT_SHIP)
    message(STATUS "K_DO_NOT_SHIP:ON, block code that does not need to be released")
    add_definitions(-DK_DO_NOT_SHIP)
else()
    message(STATUS "K_DO_NOT_SHIP:OFF, do not block code that does not need to be released")
endif()

option(WITH_ASAN "enable asan check" OFF)
if (WITH_ASAN)
  message(STATUS "WITH_ASAN:ON, enable asan check, release build should not enable the option")
  set(WITH_ASAN ON)
endif()

if (KMALLOC_DEBUGGER)
    message(STATUS "Enable memory pool analysis")
    add_definitions(-DKMALLOC_DEBUGGER)
endif ()

# Whether shared_mem_info should be used to manage concurrent memory access
if (KMALLOC_SHARED_REFCOUNT)
    add_definitions(-DKMALLOC_SHARED_REFCOUNT)
endif ()

# Whether to record the memory that is not released by the process and release it when the process crashes
option(KMALLOC_RELEASE_PROCESS_MEMORY "Enable releasing process memory feature." OFF)
if (KMALLOC_RELEASE_PROCESS_MEMORY)
    add_definitions(-DKMALLOC_RELEASE_PROCESS_MEMORY)
endif ()

# Whether to enable span trace plug-in compilation
option(ENABLE_TRACING "Compile with tracing plugin." OFF)

# Whether to enable the performance statistics of key methods
option(ENABLE_STATS "Compile with operation statistics." OFF)
if (K_DO_NOT_SHIP)
    set(ENABLE_STATS OFF)
endif ()

# Whether memory pool override is enabled
option(KMALLOC_OVERRIDE "Enable memory pool libc override" OFF)
message(STATUS "Memory pool libc override: ${KMALLOC_OVERRIDE}")

# Whether to enable the shared memory pool
option(KMALLOC "Enable memory pool start" OFF)
if (KMALLOC)
    add_definitions(-DKMALLOC)
endif ()

# whether to use lock-free tracker, for lock-free data structure memory management purpose
option(LOCKFREE_TRACKER "Use lock-free tracker." OFF)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread -Wreturn-type")

# compile kwbase
option(BUILD_KWBASE "compile kwbase" ON)
message(STATUS "Build KWBase:${BUILD_KWBASE}")

# add code coverage check
option(ENABLE_COVERAGE "code coverage check" OFF)
if (BUILD_KWBASE)
    set(ENABLE_COVERAGE OFF)
endif()
if (ENABLE_COVERAGE)
    message(STATUS "Code coverage check:${ENABLE_COVERAGE}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
    add_definitions(-DENABLE_COVERAGE)
endif ()

# installation path prefix
if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX ${CMAKE_SOURCE_DIR}/install CACHE PATH "default install prefix" FORCE)
endif()
message(STATUS "Install prefix: ${CMAKE_INSTALL_PREFIX}")
# compile with external protobuf
# option(PROTOBUF_DIR "Compile with external protobuf." OFF)

if ("${PROTOBUF_DIR}" STREQUAL "")
  # Find the protobuf library installed on your system
  find_package(Protobuf REQUIRED)
  if (PROTOBUF_FOUND)
    message(STATUS "protobuf found")
    set(PROTOBUF_LIB protobuf)
    set(PROTOBUF_C protoc)
  else ()
    message(FATAL_ERROR "Cannot find Protobuf")
  endif ()
else()
  if (NOT EXISTS ${PROTOBUF_DIR}/include)
    message(FATAL_ERROR "Protobuf include directory not found : ${PROTOBUF_DIR}/include")
  endif()
  include_directories(${PROTOBUF_DIR}/include)
  if (NOT EXISTS ${PROTOBUF_DIR}/lib)
    message(FATAL_ERROR "Protobuf library directory not found : ${PROTOBUF_DIR}/lib")
  endif()
  link_directories(${PROTOBUF_DIR}/lib)
  if (NOT EXISTS ${PROTOBUF_DIR}/bin/protoc)
    message(FATAL_ERROR "Protobuf compiler not found : ${PROTOBUF_DIR}/bin/protoc")
  endif()
  set(PROTOBUF_C ${PROTOBUF_DIR}/bin/protoc)

endif()

# compile unit test
option(WITH_TESTS "build with tests" OFF)
message(STATUS "Build with tests:${WITH_TESTS}")
# support OPENSSL
option(WITH_OPENSSL "build with OPENSSL" OFF)
message(STATUS "Build with OPENSSL:${WITH_OPENSSL}")

add_subdirectory(common)
# compile kwdbts2
add_subdirectory(kwdbts2)

# compile kwbase
if (BUILD_KWBASE)
    set(KWBASE_MAKEFILE "Makefile")
    if(NOT KWBASE_OSS)
        set(KWBASE_MAKEFILE "Makefile_ent")
    endif()

    add_custom_target(
        kwbase ALL
        COMMAND make build -f ${KWBASE_MAKEFILE}
        PROTOBUF_DIR=${PROTOBUF_DIR}
        OUTPUT_PATH=${CMAKE_BINARY_DIR}/bin
        KWDB_LIB_DIR=${CMAKE_BINARY_DIR}/lib WITH_ASAN=${WITH_ASAN}
        GOPATH=$ENV{GOPATH} KWBASE_MAKEFILE=${KWBASE_MAKEFILE}
        ${PARALLEL_LEVEL}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/kwbase
        COMMENT "====== compile kwbase ======"
        DEPENDS kwdbts2 common
        )
endif()

if (WITH_TESTS)
    add_subdirectory(qa/stress_tests/kwdbts-bench2)
    enable_testing()
endif()
# install script
file(GLOB_RECURSE UTIL_SCRIPTS
        common/util/script/query_status.sh
        common/util/script/query_kwbase_status.sh
        common/util/script/setup_cert_file.sh
        common/util/script/err_inject.sh
        common/util/script/df.sh
        common/util/script/utils.sh
        )
INSTALL(FILES ${UTIL_SCRIPTS}
        DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
        PERMISSIONS
        OWNER_EXECUTE OWNER_WRITE OWNER_READ
        GROUP_EXECUTE GROUP_WRITE GROUP_READ
        WORLD_EXECUTE WORLD_WRITE WORLD_READ)

# install kwbase
if (BUILD_KWBASE)
    INSTALL(FILES
            ${CMAKE_BINARY_DIR}/bin/kwbase
            DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
            PERMISSIONS
            OWNER_EXECUTE OWNER_WRITE OWNER_READ
            GROUP_EXECUTE GROUP_WRITE GROUP_READ
            WORLD_EXECUTE WORLD_WRITE WORLD_READ)
endif()
