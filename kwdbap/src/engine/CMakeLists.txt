add_library_unity(
        duckdb_engine
        OBJECT
        db.cpp
        duckdb_exec.cpp
        ap_parse_query.cpp
        ap_table_scan.cpp
        ap_filters.cpp
        ap_aggregate.cpp
        ap_processors.cpp
        plan_transform.cpp
        )
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_engine>
        PARENT_SCOPE)