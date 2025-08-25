add_library_unity(
  duckdb_engine
  OBJECT
  db.cpp
  duckdb_exec.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_engine>
    PARENT_SCOPE)