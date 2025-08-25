cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME mysql_scanner)
project(${TARGET_NAME})

set(EXTENSION_NAME ${TARGET_NAME}_extension)

find_path(MYSQL_INCLUDE_DIR mysql.h
    PATHS
        "/usr/include/mysql"
        "/usr/local/include/mysql"
        "/usr/local/mysql/include"
)

find_library(MYSQL_LIBRARIES mysqlclient)

include_directories(${MYSQL_INCLUDE_DIR})

include_directories(include)

add_subdirectory(storage)

set(ALL_OBJECT_FILES
    mysql_connection.cpp
    mysql_execute.cpp
    mysql_extension.cpp
    mysql_filter_pushdown.cpp
    mysql_scanner.cpp
    mysql_storage.cpp
    mysql_utils.cpp
    storage/mysql_catalog.cpp
    storage/mysql_catalog_set.cpp
    storage/mysql_clear_cache.cpp
    storage/mysql_execute_query.cpp
    storage/mysql_index.cpp
    storage/mysql_index_entry.cpp
    storage/mysql_index_set.cpp
    storage/mysql_insert.cpp
    storage/mysql_optimizer.cpp
    storage/mysql_schema_entry.cpp
    storage/mysql_schema_set.cpp
    storage/mysql_table_entry.cpp
    storage/mysql_table_set.cpp
    storage/mysql_transaction.cpp
    storage/mysql_transaction_manager.cpp)    

build_static_extension_with_depends(${TARGET_NAME} ${MYSQL_LIBRARIES} ${ALL_OBJECT_FILES})
set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Loadable binary
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE include ${MYSQL_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME}_loadable_extension ${MYSQL_LIBRARIES})

install(
  TARGETS mysql_scanner_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")