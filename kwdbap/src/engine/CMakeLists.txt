add_library_unity(
  duckdb_engine
  OBJECT
  db.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_engine>
    PARENT_SCOPE)