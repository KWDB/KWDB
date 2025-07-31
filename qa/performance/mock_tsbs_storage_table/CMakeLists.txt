cmake_minimum_required(VERSION 3.16)
project(mock_tsbs_storage_table)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS -pthread)
set(WITH_TESTS OFF CACHE BOOL INTERNAL)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
    set(WITH_DEFINITION K_DEBUG CACHE STRING INTERNAL)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer")
else ()
    set(WITH_DEFINITION K_RELEASE CACHE STRING INTERNAL)
endif ()

set(ZDP ${CMAKE_CURRENT_SOURCE_DIR}/../../../)
set(LINK_DIR ${CMAKE_CURRENT_BINARY_DIR}/lib)

# build common kwdbts2
add_subdirectory(${ZDP}/common common)
add_subdirectory(${ZDP}/kwdbts2 kwdbts2)

set(INCLUDE
    ${ZDP}/common/src/h
    ${ZDP}/common/src/include
    ${ZDP}/kwdbts2/third_party/gtest-1.8.1/fused-src
    ${ZDP}/kwdbts2/third_party/phmap
    ${ZDP}/kwdbts2/include
    ${ZDP}/kwdbts2/roachpb
    ${ZDP}/kwdbts2/engine/include
    ${ZDP}/kwdbts2/common/include
    ${ZDP}/kwdbts2/storage/include
    ${ZDP}/kwdbts2/mmap/include
    ${ZDP}/kwdbts2/exec/include
    ${ZDP}/kwdbts2/statistic/include
    ${ZDP}/kwdbts2/brpc/include
    ${CMAKE_CURRENT_SOURCE_DIR}/
    ${CMAKE_CURRENT_SOURCE_DIR}/tag
    ${CMAKE_CURRENT_SOURCE_DIR}/metric
)

if (NOT PROTOBUF_DIR)
    find_package(Protobuf REQUIRED)
    if (PROTOBUF_FOUND)
        message(STATUS "protobuf found")
        set(PROTOBUF_LIB protobuf)
        set(PROTOBUF_C protoc)
    else ()
        message(FATAL_ERROR "Cannot find Protobuf")
    endif ()
else()
    if (NOT PROTOBUF_INCLUDE_DIR)
        message(FATAL_ERROR "Must set -DPROTOBUF_INCLUDE_DIR")
    endif ()
    set(PROTOBUF_LIB ${PROTOBUF_DIR}/libprotobuf.a)
    set(PROTOBUF_C ${PROTOBUF_DIR}/protoc)
    if(EXISTS "${PROTOBUF_LIB}" AND  EXISTS "${PROTOBUF_C}")
        message(STATUS "kwbase protobuf found : ${PROTOBUF_LIB}")
    else()
        message(FATAL_ERROR "Cannot find kwbase Protobuf in ${PROTOBUF_DIR}")
    endif()
endif()

link_directories(${LINK_DIR})

file(GLOB_RECURSE CPP_FILES "${CMAKE_CURRENT_SOURCE_DIR}/metric/*.cpp" "${CMAKE_CURRENT_SOURCE_DIR}/tag/*.cpp")
foreach (cpp ${CPP_FILES})
    get_filename_component(dir ${cpp} DIRECTORY)
    get_filename_component(name ${cpp} NAME_WLE)
    get_filename_component(base_dir ${dir} NAME)
    set(bin_name "${base_dir}_${name}")
    message("BIN_NAME=" ${bin_name})
    add_executable(${bin_name} ${dir}/${name}.cpp)
    target_include_directories(${bin_name} PRIVATE ${INCLUDE})
    target_link_libraries(${bin_name} ${PROTOBUF_LIB} kwdbts2 common gtest dl rt)
    set_target_properties(${bin_name} PROPERTIES
            CXX_STANDARD_REQUIRED YES
            CXX_EXTENSIONS NO
            COMPILE_OPTIONS "-Wno-error;-Wall;-Wno-sign-compare"
    )
endforeach ()
