create database test_udf;
use test_udf;
delimiter \\
CREATE FUNCTION add(a INT, b INT)
    RETURNS INT
    LANGUAGE sql
BEGIN
  IF a > 1 then
SELECT a + b;
else
SELECT 2*(a + b);
ENDIF;
END \\
delimiter ;
select add(1,2) = 6;
select add(2,1) = 3;

CREATE TABLE t_udf(a INT, b INT);
INSERT INTO t_udf VALUES (1, 2), (3, 4);
SELECT count(*) = 2
FROM t_udf
where add(a, b) = 6 or add(a, b) = 7;

CREATE FUNCTION add2(a INT, b INT) RETURNS INT LANGUAGE sql BEGIN SELECT a + b; END;

delimiter \\
CREATE FUNCTION calc_sum(a INT, b INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
   DECLARE c INT;
   SET c = a + b;
SELECT c;
END \\
delimiter ;
SELECT calc_sum(10, 20) = 30;

delimiter \\
CREATE FUNCTION calc_if(a INT, b INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
IF a > 1 THEN
SELECT a + b;
ELSE
SELECT 2 * (a + b);
ENDIF;
END \\
delimiter ;
SELECT calc_if(1, 2) = 6;
SELECT calc_if(2, 2) = 4;

delimiter \\
CREATE FUNCTION calc_loop(n INT)
RETURNS INT
LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE s INT;
SET i = 0;
SET s = 0;
WHILE i < n DO
SET i = i + 1;
SET s = s + i;
ENDWHILE;
SELECT s;
END \\
delimiter ;
SELECT calc_loop(5) = 15;

delimiter \\
CREATE FUNCTION add_twice(a INT, b INT)
RETURNS INT
LANGUAGE SQL
BEGIN
SELECT add(a, b) * 2;
END \\
delimiter ;
SELECT add_twice(1, 2) = 12;

SHOW CREATE FUNCTION add;

-- PARAMETER TEST
delimiter \\
CREATE FUNCTION udf_decimal(a DECIMAL, b DECIMAL)
RETURNS DECIMAL
LANGUAGE SQL
BEGIN
SELECT a + b;
END \\
delimiter ;
SELECT udf_decimal(1.5, 2.5);

delimiter \\
CREATE FUNCTION udf_string(a STRING, b STRING)
RETURNS STRING
LANGUAGE SQL
BEGIN SELECT a || b;
END \\
delimiter ;
SELECT udf_string('hello', 'world');

delimiter \\
CREATE FUNCTION udf_timestamp(a TIMESTAMP)
RETURNS TIMESTAMP
LANGUAGE SQL
BEGIN SELECT a;
END \\
delimiter ;
SELECT udf_timestamp('2026-01-01 10:00:00'::TIMESTAMP);



-- Error case
CREATE FUNCTION invalid_missing_body(a INT) RETURNS INT LANGUAGE SQL;
CREATE FUNCTION invalid_dup_param(a INT, a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION invalid_param_type(a BOOL) RETURNS INT LANGUAGE SQL BEGIN SELECT 1; END;
CREATE FUNCTION invalid_return_type(a INT) RETURNS BOOL LANGUAGE SQL BEGIN SELECT a > 1; END;
CREATE FUNCTION invalid_return_mismatch(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT 'abc'; END;
SELECT invalid_return_mismatch(1);
delimiter \\
CREATE FUNCTION invalid_multi_column()
RETURNS INT
LANGUAGE SQL
BEGIN SELECT 1, 2;
END \\
delimiter ;
SELECT invalid_multi_column();

DROP TABLE IF EXISTS t_multi_row;
CREATE TABLE t_multi_row(a INT);
INSERT INTO t_multi_row VALUES (1), (2);
delimiter \\
CREATE FUNCTION invalid_multi_row()
RETURNS INT
LANGUAGE SQL
BEGIN
SELECT a FROM t_multi_row;
END \\
delimiter ;
SELECT invalid_multi_row();

DROP TABLE IF EXISTS t_func_insert;
CREATE TABLE t_func_insert(a INT);
delimiter \\
CREATE FUNCTION invalid_insert()
    RETURNS INT
    LANGUAGE SQL
BEGIN
INSERT INTO t_func_insert VALUES (1);
SELECT 1;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION invalid_commit()
    RETURNS INT
    LANGUAGE SQL
BEGIN
COMMIT;
SELECT 1;
END \\
delimiter ;

SHOW CREATE FUNCTION not_exists_func;

--函数名重复
delimiter \\
CREATE FUNCTION overload_test(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT a;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION overload_test(a INT, b INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT a + b;
END \\
delimiter ;

CREATE FUNCTION sum(a INT) RETURNS INT LANGUAGE sql BEGIN SELECT a + b; END;
CREATE FUNCTION abs(a int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function abs(a)
  if a > 0 then
        return a
    end
  return -a
end'
END;
CREATE FUNCTION abs(a INT) RETURNS INT LANGUAGE sql BEGIN SELECT a + b; END;

delimiter \\
CREATE FUNCTION public.schema_func(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT a;
END \\
delimiter ;

DROP FUNCTION add;
CREATE FUNCTION add(a INT, b INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + b; END;
CREATE FUNCTION no_arg_func() RETURNS INT LANGUAGE SQL BEGIN SELECT 100; END;
SELECT no_arg_func() = 100;

WITH q AS ( SELECT add(1, 2) AS v ) SELECT v = 3 FROM q;
SELECT ( SELECT add(3, 4) ) = 7;
CREATE TABLE t_udf_group(a INT, b INT);
INSERT INTO t_udf_group VALUES (1, 2), (1, 2), (3, 4);
SELECT count(*) = 1 FROM ( SELECT add(a, b) AS v, count(*) AS c FROM t_udf_group GROUP BY add(a, b) HAVING add(a, b) = 3 AND count(*) = 2 );
SELECT add(a, b) FROM t_udf_group ORDER BY add(a, b);

CREATE FUNCTION udf_int2(a INT2) RETURNS INT2 LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_int4(a INT4) RETURNS INT4 LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_int8(a INT8) RETURNS INT8 LANGUAGE SQL BEGIN SELECT a; END;
SELECT udf_int2(1::INT2) = 1::INT2;
SELECT udf_int4(1::INT4) = 1::INT4;
SELECT udf_int8(1::INT8) = 1::INT8;

CREATE FUNCTION udf_float4(a FLOAT4) RETURNS FLOAT4 LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_float8(a FLOAT8) RETURNS FLOAT8 LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_decimal(a DECIMAL, b DECIMAL) RETURNS DECIMAL LANGUAGE SQL BEGIN SELECT a + b; END;
SELECT udf_float4(1.5::FLOAT4) = 1.5::FLOAT4;
SELECT udf_float8(1.5::FLOAT8) = 1.5::FLOAT8;
SELECT udf_decimal(1.5, 2.5) = 4.0;

CREATE FUNCTION udf_string2(a STRING, b STRING) RETURNS STRING LANGUAGE SQL BEGIN SELECT a || b; END;
CREATE FUNCTION udf_text(a TEXT) RETURNS TEXT LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_varchar(a VARCHAR) RETURNS VARCHAR LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_char(a CHAR) RETURNS CHAR LANGUAGE SQL BEGIN SELECT a; END;
SELECT udf_string2('hello', 'world') = 'helloworld';
SELECT udf_text('abc') = 'abc';
SELECT udf_varchar('abc') = 'abc';
SELECT udf_char('a') = 'a';

CREATE FUNCTION udf_citext(a CITEXT) RETURNS CITEXT LANGUAGE SQL BEGIN SELECT a; END;
SELECT udf_citext('TeSt'::CITEXT) = 'test'::CITEXT;

CREATE FUNCTION udf_timestamp(a TIMESTAMP) RETURNS TIMESTAMP LANGUAGE SQL BEGIN SELECT a; END;
CREATE FUNCTION udf_timestamptz(a TIMESTAMPTZ) RETURNS TIMESTAMPTZ LANGUAGE SQL BEGIN SELECT a; END;
SELECT udf_timestamp('2026-01-01 10:00:00'::TIMESTAMP) = '2026-01-01 10:00:00'::TIMESTAMP;
SELECT udf_timestamptz('2026-01-01 10:00:00+00:00'::TIMESTAMPTZ) = '2026-01-01 10:00:00+00:00'::TIMESTAMPTZ;

CREATE FUNCTION udf_return_null() RETURNS INT LANGUAGE SQL BEGIN SELECT NULL::INT; END;
SELECT udf_return_null() IS NULL;

SELECT count(*) = 1 FROM "".kwdb_internal.kwdb_functions WHERE function_name = 'add' AND language = 'SQL' AND function_body LIKE '%CREATE FUNCTION add%';
CREATE FUNCTION lua_abs2(a INT) RETURNS FLOAT LANGUAGE LUA BEGIN 'function lua_abs2(a) if a > 0 then return a end return -a end' END;
SHOW CREATE FUNCTION lua_abs2;
SELECT count(*) = 1 FROM "".kwdb_internal.kwdb_functions WHERE function_name = 'lua_abs2' AND language = 'LUA' AND function_body LIKE '%function lua_abs2%';

SELECT add('a', 'b');
SELECT add(1, 2, 3);
CREATE FUNCTION udf_set_error(a CHAR) RETURNS CHAR LANGUAGE SQL BEGIN declare c bool; END;

-- COMPLEX CASE TEST
-- 1. UDF result as another UDF argument
SELECT calc_sum(add(1, 2), add2(3, 4)) = 10;
SELECT add(add(1, 2), add2(3, 4)) = 10;
SELECT add(calc_sum(1, 2), calc_if(2, 2)) = 7;

-- 2. UDF result as builtin function argument
SELECT length(udf_string2('hello', 'world')) = 10;
SELECT substring(udf_string2('abcdef', 'gh') FROM 2 FOR 3) = 'bcd';
SELECT abs(add(-10, 3)) = 14;
SELECT round(udf_decimal(1.2, 2.3)) = 4;

-- 3. UDF result used in CASE expression
SELECT CASE WHEN add(1, 2) = 3 THEN 'ok' ELSE 'bad' END = 'ok';
SELECT CASE WHEN calc_if(1, 2) = 6 THEN add(1, 1) ELSE add(2, 2) END = 2;

-- 4. UDF result used as IN / BETWEEN expression argument
SELECT add(1, 2) IN (1, 2, 3);
SELECT add(1, 2) BETWEEN 1 AND 5;

-- 5. UDF result used as another SQL UDF parameter
CREATE FUNCTION udf_param_chain(a INT, b INT, c INT) RETURNS INT LANGUAGE SQL BEGIN SELECT add(calc_sum(a, b), c); END;
SELECT udf_param_chain(1, 2, 3) = 6;
SELECT udf_param_chain(add(1, 1), add2(2, 3), calc_if(2, 1)) = 10;

-- 6. Nested SQL UDF calls: level1 -> level2 -> level3
CREATE FUNCTION nested_level1(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 1; END;
CREATE FUNCTION nested_level2(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT nested_level1(a) * 2; END;
CREATE FUNCTION nested_level3(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT nested_level2(nested_level1(a)) + nested_level1(a); END;
SELECT nested_level3(2) = 11;

-- 7. Nested SQL UDF call with IF branch
CREATE FUNCTION nested_if_func(a INT, b INT) RETURNS INT LANGUAGE SQL BEGIN IF add(a, b) > 10 THEN SELECT add(a, b); ELSE SELECT add(add(a, b), add(a, b)); ENDIF; END;
SELECT nested_if_func(3, 4) = 14;
SELECT nested_if_func(6, 7) = 13;

-- 8. Nested SQL UDF call with WHILE loop
CREATE FUNCTION nested_loop_sum(n INT) RETURNS INT LANGUAGE SQL BEGIN DECLARE i INT; DECLARE s INT; SET i = 0; SET s = 0; WHILE i < n DO SET i = add(i, 1); SET s = add(s, i); ENDWHILE; SELECT s; END;
SELECT nested_loop_sum(5) = 15;

-- 9. UDF result as CALL procedure parameter
CREATE PROCEDURE proc_accept_int(x INT) BEGIN SELECT x; END;
CALL proc_accept_int(add(1, 2));
CALL proc_accept_int(calc_sum(add(1, 2), add2(3, 4)));

-- 10. Multiple UDF expressions as CALL procedure parameters
CREATE PROCEDURE proc_accept_two_ints(x INT, y INT) BEGIN SELECT x + y; END;
CALL proc_accept_two_ints(add(1, 2), add2(3, 4));
CALL proc_accept_two_ints(calc_if(1, 2), calc_loop(5));

-- 11. Procedure body calls SQL UDF
CREATE PROCEDURE proc_call_udf(a INT, b INT) BEGIN SELECT add(a, b); END;
CALL proc_call_udf(10, 20);

-- 12. Procedure body uses SQL UDF in DECLARE/SET/IF flow
CREATE PROCEDURE proc_call_udf_flow(a INT, b INT) BEGIN DECLARE c INT; SET c = add(a, b); IF c > 10 THEN SELECT c; ELSE SELECT add(c, c); ENDIF; END;
CALL proc_call_udf_flow(1, 2);
CALL proc_call_udf_flow(6, 7);

-- 13. SQL UDF called from procedure, and procedure parameter is also UDF result
CALL proc_call_udf(add(1, 2), calc_sum(3, 4));
CALL proc_call_udf_flow(add(1, 2), calc_loop(3));

-- 14. UDF used in subquery inside another UDF
CREATE TABLE t_nested_udf(a INT, b INT);
INSERT INTO t_nested_udf VALUES (1, 2), (3, 4), (5, 6);
CREATE FUNCTION nested_subquery_func(x INT) RETURNS INT LANGUAGE SQL BEGIN SELECT count(*)::INT FROM t_nested_udf WHERE add(a, b) > x; END;
SELECT nested_subquery_func(3) = 2;
SELECT nested_subquery_func(10) = 1;

-- 15. UDF used in EXISTS subquery
SELECT EXISTS ( SELECT 1 FROM t_nested_udf WHERE add(a, b) = 7 );

-- 16. UDF used in JOIN condition
CREATE TABLE t_nested_udf_2(c INT);
INSERT INTO t_nested_udf_2 VALUES (3), (7), (11);
SELECT count(*) = 3 FROM t_nested_udf t1 JOIN t_nested_udf_2 t2 ON add(t1.a, t1.b) = t2.c;

-- 17. UDF used in aggregate expression
SELECT sum(add(a, b)) = 21 FROM t_nested_udf;

-- 18. UDF used in HAVING with aggregate
SELECT count(*) = 1 FROM ( SELECT add(a, b) AS v FROM t_nested_udf GROUP BY add(a, b) HAVING add(a, b) = 7 );

-- 19. Error case: CALL procedure parameter uses a UDF with wrong return type
CREATE FUNCTION udf_string_for_proc() RETURNS STRING LANGUAGE SQL BEGIN SELECT 'abc'; END;
CALL proc_accept_int(udf_string_for_proc());

-- 20. Error case: nested UDF returns non-scalar result
CREATE FUNCTION nested_bad_scalar() RETURNS INT LANGUAGE SQL BEGIN SELECT invalid_multi_column(); END;
SELECT nested_bad_scalar();

-- 21. DROP inner function then call outer function
DROP FUNCTION nested_level1;
SELECT nested_level2(1);

CREATE FUNCTION nested_level1(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 1; END;
SELECT nested_level2(1) = 4;

-- 22. SHOW CREATE FUNCTION for nested UDF
SHOW CREATE FUNCTION nested_level2;
SHOW CREATE FUNCTION nested_level3;
SHOW CREATE FUNCTION nested_if_func;
SHOW CREATE FUNCTION nested_loop_sum;

-- 23. WHILE + LEAVE: leave loop when condition is met
delimiter \\
CREATE FUNCTION loop_leave_sum(n INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE s INT;

SET i = 0;
SET s = 0;

label my_loop: WHILE i < n DO
  SET i = i + 1;

  IF i > 3 THEN
    LEAVE my_loop;
  ENDIF;

  SET s = s + i;
ENDWHILE my_loop;

SELECT s;
END \\
delimiter ;
SELECT loop_leave_sum(10) = 6;
SELECT loop_leave_sum(2) = 3;

-- 24. Nested WHILE + LEAVE inner loop only
delimiter \\
CREATE FUNCTION nested_loop_leave(n INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE j INT;
DECLARE s INT;

SET j = 0;
SET s = 0;
  label inner_loop: WHILE j < 10 DO
    SET j = j + 1;

    IF j > 2 THEN
      LEAVE inner_loop;
    ENDIF;

    SET s = s + 1;
  ENDWHILE inner_loop;

SELECT s;
END \\
delimiter ;
SELECT nested_loop_leave(3) = 2;

-- 25. LEAVE outer loop from inner IF
delimiter \\
CREATE FUNCTION leave_outer_loop(n INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE s INT;

SET i = 0;
SET s = 0;

label outer_loop: WHILE i < n DO
  SET i = i + 1;

  IF i = 4 THEN
    LEAVE outer_loop;
  ENDIF;

  SET s = s + i;
ENDWHILE outer_loop;

SELECT s;
END \\
delimiter ;

-- 1 + 2 + 3 = 6
SELECT leave_outer_loop(10) = 6;

-- 26. WHILE body should not execute when condition is false initially
delimiter \\
CREATE FUNCTION loop_zero_case(n INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE s INT;
SET i = 0;
SET s = 100;

WHILE i < n DO
  SET i = i + 1;
  SET s = s + i;
ENDWHILE;

SELECT s;
END \\
delimiter ;

SELECT loop_zero_case(0) = 100;

-- 27. ELSIF branch
delimiter \\
CREATE FUNCTION calc_elsif(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
IF a < 0 THEN
SELECT -1;
ELSIF a = 0 THEN
SELECT 0;
ELSE
SELECT 1;
ENDIF;
END \\
delimiter ;

SELECT calc_elsif(-1) = -1;
SELECT calc_elsif(0) = 0;
SELECT calc_elsif(1) = 1;

-- 28. Multiple ELSIF branches
delimiter \\
CREATE FUNCTION calc_multi_elsif(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
IF a < 0 THEN
SELECT -1;
ELSIF a = 0 THEN
SELECT 0;
ELSIF a = 1 THEN
SELECT 10;
ELSE
SELECT 100;
ENDIF;
END \\
delimiter ;

SELECT calc_multi_elsif(-5) = -1;
SELECT calc_multi_elsif(0) = 0;
SELECT calc_multi_elsif(1) = 10;
SELECT calc_multi_elsif(2) = 100;


-- 29. UDF call should be case-insensitive by function name
SELECT ADD(1, 2) = 3;
SELECT Add(2, 3) = 5;


-- 30. SQL UDF used in SELECT list with column alias
SELECT add(1, 2) AS udf_result;


-- 31. SQL UDF used in expression with arithmetic operators
SELECT add(1, 2) * add2(3, 4) = 21;
SELECT add(add(1, 1), add(add2(1, 2), 3)) = 8;


-- 32. SQL UDF used with NULL argument
-- Expected behavior depends on NullableArgs=false.
-- Usually scalar function should return NULL when any argument is NULL.
SELECT add(NULL::INT, 1) IS NULL;
SELECT calc_sum(NULL::INT, 1) IS NULL;


-- 33. SQL UDF returns value from table with WHERE filter
CREATE TABLE t_udf_lookup(k INT, v INT);
INSERT INTO t_udf_lookup VALUES (1, 10), (2, 20), (3, 30);

delimiter \\
CREATE FUNCTION lookup_value(x INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT v FROM t_udf_lookup WHERE t_udf_lookup.k = x;
END \\
delimiter ;

SELECT lookup_value(1) = 10;
SELECT lookup_value(2) = 20;


-- 34. SQL UDF with table aggregate and UDF expression together
CREATE FUNCTION sum_add_result() RETURNS INT LANGUAGE SQL BEGIN SELECT sum(add(k, v))::INT FROM t_udf_lookup;END;

-- (1+10) + (2+20) + (3+30) = 66
SELECT sum_add_result() = 66;


-- 35. SQL UDF in ORDER BY with LIMIT
SELECT add(k, v)
FROM t_udf_lookup
ORDER BY add(k, v) DESC
    LIMIT 1;


-- 36. SQL UDF in DISTINCT
SELECT count(*) = 3
FROM (
         SELECT DISTINCT add(k, v) AS x
         FROM t_udf_lookup
     );


-- 37. SQL UDF inside EXISTS
SELECT EXISTS (
    SELECT 1
    FROM t_udf_lookup
    WHERE add(k, v) = 22
);


-- 38. SQL UDF inside scalar subquery
SELECT (
           SELECT add(k, v)
FROM t_udf_lookup
WHERE k = 3
    ) = 33;


-- 39. DROP FUNCTION then function call should fail immediately
CREATE FUNCTION drop_call_check(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 1;END;

SELECT drop_call_check(1) = 2;
DROP FUNCTION drop_call_check;

-- Expected: unknown function / function does not exist.
SELECT drop_call_check(1);


-- 40. DROP FUNCTION then SHOW CREATE FUNCTION should fail
CREATE FUNCTION drop_show_check(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a; END;

SHOW CREATE FUNCTION drop_show_check;
DROP FUNCTION drop_show_check;

-- Expected: function does not exist.
SHOW CREATE FUNCTION drop_show_check;


-- 41. DROP FUNCTION then recreate with same name
CREATE FUNCTION recreate_check(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 1; END;

SELECT recreate_check(1) = 2;
DROP FUNCTION recreate_check;

CREATE FUNCTION recreate_check(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 2; END;

SELECT recreate_check(1) = 3;


-- 42. SQL UDF with nested IF + WHILE + LEAVE
delimiter \\
CREATE FUNCTION complex_control_flow(n INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE i INT;
DECLARE s INT;

SET i = 0;
SET s = 0;

label main_loop: WHILE i < n DO
  SET i = i + 1;

  IF i = 2 THEN
    SET s = s + 20;
  ELSIF i = 4 THEN
    LEAVE main_loop;
ELSE
    SET s = s + i;
  ENDIF;
ENDWHILE main_loop;

SELECT s;
END \\
delimiter ;

-- i=1 => +1, i=2 => +20, i=3 => +3, i=4 leave; total 24.
SELECT complex_control_flow(10) = 24;


-- 43. Error case: LEAVE unknown label
delimiter \\
CREATE FUNCTION invalid_leave_label()
    RETURNS INT
    LANGUAGE SQL
BEGIN
WHILE 1 < 2 DO
  LEAVE not_exists_label;
ENDWHILE;
SELECT 1;
END \\
delimiter ;

DROP FUNCTION loop_leave_sum;
DROP FUNCTION nested_loop_leave;
DROP FUNCTION leave_outer_loop;
DROP FUNCTION loop_zero_case;
DROP FUNCTION calc_elsif;
DROP FUNCTION calc_multi_elsif;
DROP FUNCTION lookup_value;
DROP FUNCTION sum_add_result;
DROP FUNCTION recreate_check;
DROP FUNCTION complex_control_flow;
DROP FUNCTION udf_param_chain;
DROP FUNCTION nested_level3;
DROP FUNCTION nested_level2;
DROP FUNCTION nested_level1;
DROP FUNCTION nested_if_func;
DROP FUNCTION nested_loop_sum;
DROP FUNCTION nested_subquery_func;
DROP FUNCTION udf_string_for_proc;
DROP FUNCTION nested_bad_scalar;
DROP PROCEDURE proc_accept_int;
DROP PROCEDURE proc_accept_two_ints;
DROP PROCEDURE proc_call_udf;
DROP PROCEDURE proc_call_udf_flow;
DROP TABLE IF EXISTS t_nested_udf;
DROP TABLE IF EXISTS t_nested_udf_2;

DROP FUNCTION no_arg_func;
DROP FUNCTION udf_int2;
DROP FUNCTION udf_int4;
DROP FUNCTION udf_int8;
DROP FUNCTION udf_float4;
DROP FUNCTION udf_float8;
DROP FUNCTION udf_decimal;
DROP FUNCTION udf_text;
DROP FUNCTION udf_varchar;
DROP FUNCTION udf_char;
DROP FUNCTION udf_citext;
DROP FUNCTION udf_timestamptz;
DROP FUNCTION udf_return_null;
DROP FUNCTION lua_abs2;
DROP FUNCTION overload_test;
DROP FUNCTION add;
DROP FUNCTION add2;
DROP FUNCTION calc_sum;
DROP FUNCTION calc_if;
DROP FUNCTION calc_loop;
DROP FUNCTION add_twice;
DROP FUNCTION udf_string;
DROP FUNCTION udf_string2;
DROP FUNCTION udf_timestamp;
DROP FUNCTION invalid_multi_column;
DROP FUNCTION invalid_multi_row;
DROP FUNCTION invalid_return_mismatch;
SHOW FUNCTIONS;
SHOW PROCEDURES;
DROP DATABASE test_udf cascade;

-- GLOBAL FUNCTION NAME TEST
DROP DATABASE IF EXISTS udf_global_db1 CASCADE;
DROP DATABASE IF EXISTS udf_global_db2 CASCADE;

CREATE DATABASE udf_global_db1;
CREATE DATABASE udf_global_db2;

USE udf_global_db1;

CREATE FUNCTION global_unique_func(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 1;END;

SELECT global_unique_func(1) = 2;

USE udf_global_db2;

-- Expected error: function named 'global_unique_func' already exists.
CREATE FUNCTION global_unique_func(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a + 100; END;

-- Global UDF should still be callable by unqualified name if current logic is global.
SELECT global_unique_func(1) = 2;

DROP FUNCTION global_unique_func;
DROP DATABASE udf_global_db1 CASCADE;
DROP DATABASE udf_global_db2 CASCADE;

-- UNSUPPORTED STATEMENT TEST
DROP TABLE IF EXISTS t_udf_dml;
CREATE TABLE t_udf_dml(a INT);
INSERT INTO t_udf_dml VALUES (1);

-- Expected error: UPDATE is not supported in SQL function.
CREATE FUNCTION invalid_update() RETURNS INT LANGUAGE SQL BEGIN UPDATE t_udf_dml SET a = 2; SELECT 1;END;

-- Expected error: DELETE is not supported in SQL function.
CREATE FUNCTION invalid_delete() RETURNS INT LANGUAGE SQL BEGIN DELETE FROM t_udf_dml WHERE a = 1; SELECT 1;END;

-- Expected error: UPSERT is not supported in SQL function.
CREATE FUNCTION invalid_upsert() RETURNS INT LANGUAGE SQL BEGIN UPSERT INTO t_udf_dml VALUES (2); SELECT 1; END;

-- Expected error: ROLLBACK is not supported in SQL function.
CREATE FUNCTION invalid_rollback() RETURNS INT LANGUAGE SQL BEGIN ROLLBACK; SELECT 1;END;

-- Expected error: PREPARE is not supported in SQL function.
CREATE FUNCTION invalid_prepare() RETURNS INT LANGUAGE SQL BEGIN PREPARE stmt AS SELECT 1; SELECT 1; END;

-- Expected error: EXECUTE is not supported in SQL function.
CREATE FUNCTION invalid_execute() RETURNS INT LANGUAGE SQL BEGIN EXECUTE stmt;SELECT 1;END;

DROP TABLE IF EXISTS t_udf_dml;

-- EMPTY BODY / ZERO ROW RETURN TEST

CREATE FUNCTION empty_body_func() RETURNS INT LANGUAGE SQL BEGIN END;

-- Expected error: sql function empty_body_func does not return a scalar result.
SELECT empty_body_func();

CREATE TABLE t_udf_zero_row(a INT);

CREATE FUNCTION zero_row_func() RETURNS INT LANGUAGE SQL BEGIN SELECT a FROM t_udf_zero_row WHERE a = 1;END;

-- Expected error: sql function zero_row_func does not return a scalar result.
SELECT zero_row_func();

DROP FUNCTION empty_body_func;
DROP FUNCTION zero_row_func;
DROP TABLE t_udf_zero_row;

delimiter \\
CREATE FUNCTION invalid_dup_local_var()
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE c INT;
DECLARE c INT;
SELECT c;
END \\
delimiter ;

-- Expected error: unknown variable / column.
delimiter \\
CREATE FUNCTION invalid_set_unknown_var()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SET not_exists_var = 1;
SELECT 1;
END \\
delimiter ;

-- Expected error: assignment/type mismatch.
delimiter \\
CREATE FUNCTION invalid_set_type_mismatch()
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE c INT;
SET c = 'abc';
SELECT c;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION declare_default_null()
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE c INT;
SELECT c;
END \\
delimiter ;

SELECT declare_default_null() IS NULL;

DROP FUNCTION declare_default_null;

-- PARAMETER NAME CASE TEST

-- Expected error: parameter name "a" used more than once.
delimiter \\
CREATE FUNCTION invalid_dup_param_case(a INT, A INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT a;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION invalid_declare_same_as_param(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
DECLARE a INT;
SELECT a;
END \\
delimiter ;
drop function invalid_declare_same_as_param;

-- SET undeclared variable.
delimiter \\
CREATE FUNCTION invalid_set_undeclared(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SET c = a + 1;
SELECT c;
END \\
delimiter ;

-- SELECT references unknown variable.
delimiter \\
CREATE FUNCTION invalid_unknown_var(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT not_exists_var;
END \\
delimiter ;

-- Return type mismatch with numeric family.
delimiter \\
CREATE FUNCTION invalid_decimal_to_int()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT 1.5::DECIMAL;
END \\
delimiter ;
SELECT invalid_decimal_to_int();
drop function invalid_decimal_to_int;

-- Return type mismatch with timestamp/string.
CREATE FUNCTION invalid_string_to_timestamp() RETURNS TIMESTAMP LANGUAGE SQL BEGIN SELECT 'abc';END;
SELECT invalid_string_to_timestamp();
drop function invalid_string_to_timestamp;

-- SHOW CREATE after DROP should fail.
CREATE FUNCTION show_after_drop(a INT) RETURNS INT LANGUAGE SQL BEGIN SELECT a;END;

SHOW CREATE FUNCTION show_after_drop;
DROP FUNCTION show_after_drop;

-- Expected error: function show_after_drop does not exist.
SHOW CREATE FUNCTION show_after_drop;

SHOW FUNCTIONS;
SHOW PROCEDURES;

-- ADDITIONAL POSITIVE CASES
-- 1. No argument SQL UDF with expression.
delimiter \\
CREATE FUNCTION no_arg_expr()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT 1 + 2;
END \\
delimiter ;

SELECT no_arg_expr() = 3;
SHOW CREATE FUNCTION no_arg_expr;


-- 2. SQL UDF with NULL branch.
delimiter \\
CREATE FUNCTION null_branch(a INT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
IF a IS NULL THEN
SELECT NULL::INT;
ELSE
SELECT a + 1;
ENDIF;
END \\
delimiter ;

SELECT null_branch(NULL::INT) IS NULL;
SELECT null_branch(1) = 2;


-- 3. SQL UDF with TEXT/VARCHAR/CHAR mixed expression.
delimiter \\
CREATE FUNCTION string_mix(a TEXT, b VARCHAR, c CHAR)
    RETURNS STRING
    LANGUAGE SQL
BEGIN
SELECT a || b || c;
END \\
delimiter ;

SELECT string_mix('a', 'b', 'c') = 'abc';


-- 4. SQL UDF with CITEXT comparison.
delimiter \\
CREATE FUNCTION citext_eq(a CITEXT, b CITEXT)
    RETURNS INT
    LANGUAGE SQL
BEGIN
IF a = b THEN
SELECT 1;
ELSE
SELECT 0;
ENDIF;
END \\
delimiter ;

SELECT citext_eq('Hello'::CITEXT, 'hello'::CITEXT) = 1;


-- 5. SQL UDF with TIMESTAMPTZ return.
delimiter \\
CREATE FUNCTION timestamptz_identity(a TIMESTAMPTZ)
    RETURNS TIMESTAMPTZ
    LANGUAGE SQL
BEGIN
SELECT a;
END \\
delimiter ;

SELECT timestamptz_identity('2026-01-01 10:00:00+00:00'::TIMESTAMPTZ)
           = '2026-01-01 10:00:00+00:00'::TIMESTAMPTZ;


-- 6. SQL UDF with aggregate returns one scalar row.
DROP TABLE IF EXISTS t_udf_agg_extra;
CREATE TABLE t_udf_agg_extra(a INT);
INSERT INTO t_udf_agg_extra VALUES (1), (2), (3);

delimiter \\
CREATE FUNCTION agg_scalar_extra()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT sum(a)::INT FROM t_udf_agg_extra;
END \\
delimiter ;

SELECT agg_scalar_extra() = 6;


-- 7. LUA UDF show create and kwdb_functions body.
CREATE FUNCTION lua_show_extra(a INT)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function lua_show_extra(a)
  return a + 100
end'
END;

SELECT lua_show_extra(1) = 101;
SHOW CREATE FUNCTION lua_show_extra;

SELECT count(*) = 1
FROM "".kwdb_internal.kwdb_functions
WHERE function_name = 'lua_show_extra'
  AND language = 'LUA'
  AND function_body LIKE '%return a + 100%';


-- 8. SQL UDF and LUA UDF interaction: SQL calls LUA.
CREATE FUNCTION lua_base_extra(a INT)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function lua_base_extra(a)
  return a + 5
end'
END;

delimiter \\
CREATE FUNCTION sql_call_lua_extra(a INT)
    RETURNS FLOAT
    LANGUAGE SQL
BEGIN
SELECT lua_base_extra(a) * 2;
END \\
delimiter ;

SELECT sql_call_lua_extra(10) = 30;


-- Cleanup for added positive cases.
DROP FUNCTION no_arg_expr;
DROP FUNCTION null_branch;
DROP FUNCTION string_mix;
DROP FUNCTION citext_eq;
DROP FUNCTION timestamptz_identity;
DROP FUNCTION agg_scalar_extra;
DROP FUNCTION lua_show_extra;
DROP FUNCTION lua_base_extra;
DROP FUNCTION sql_call_lua_extra;

-- BUG1
delimiter \\
CREATE FUNCTION "Bug2_Zombie"()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT 1;
END \\
delimiter ;

SHOW FUNCTIONS;
SHOW CREATE FUNCTION bug2_zombie;
SHOW CREATE FUNCTION Bug2_Zombie;
SHOW CREATE FUNCTION "Bug2_Zombie";
SELECT bug2_zombie();
SELECT Bug2_Zombie();
SELECT "Bug2_Zombie"();

delimiter \\
CREATE FUNCTION "Bug2_Zombie"()
    RETURNS INT
    LANGUAGE SQL
BEGIN
SELECT 2;
END \\
delimiter ;

DROP FUNCTION bug2_zombie;
DROP FUNCTION "Bug2_Zombie";

CREATE FUNCTION Case_Normal() RETURNS INT LANGUAGE SQL BEGIN SELECT 10; END;

SELECT case_normal();
SELECT CASE_NORMAL();

DROP FUNCTION case_normal;

CREATE FUNCTION "Lua_Bug2_Zombie"(a INT)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function Lua_Bug2_Zombie(a)
  return a + 10
end'
END;

SHOW FUNCTION "Lua_Bug2_Zombie";
SHOW CREATE FUNCTION "Lua_Bug2_Zombie";

SELECT lua_bug2_zombie(1);
SELECT Lua_Bug2_Zombie(1);
SELECT "Lua_Bug2_Zombie"(1);

DROP FUNCTION Lua_Bug2_Zombie;
DROP FUNCTION "Lua_Bug2_Zombie";

-- BUG2
delimiter \\
CREATE FUNCTION bug_a(x INT) RETURNS INT LANGUAGE SQL
BEGIN
SELECT x;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION bug_b(x INT) RETURNS INT LANGUAGE SQL
BEGIN
SELECT bug_a(x) + 1;
END \\
delimiter ;

DROP FUNCTION bug_a;
delimiter \\
CREATE FUNCTION bug_a(x INT) RETURNS INT LANGUAGE SQL
BEGIN
SELECT bug_b(x) + 1;
END \\
delimiter ;

SELECT bug_a(1);
drop function bug_a;
drop function bug_b;

-- BUG3
delimiter \\
CREATE FUNCTION bug1_null_skip(x INT) RETURNS INT LANGUAGE SQL
BEGIN
DECLARE v INT;
SET v = 999;
SELECT v;
END \\
delimiter ;

SELECT bug1_null_skip(100);
SELECT bug1_null_skip(NULL);
drop function bug1_null_skip;

-- BUG4
delimiter \\
CREATE FUNCTION bug14_info_schema() RETURNS INT LANGUAGE SQL
BEGIN
SELECT count(*) FROM information_schema.tables;
END \\
delimiter ;

SELECT bug14_info_schema() >= 0;
DROP FUNCTION bug14_info_schema;

delimiter \\
CREATE FUNCTION bug14_pg_catalog() RETURNS INT LANGUAGE SQL
BEGIN
SELECT count(*) FROM pg_catalog.pg_tables;
END \\
delimiter ;

SELECT bug14_pg_catalog() >= 0;
DROP FUNCTION bug14_pg_catalog;

-- BUG5
CREATE FUNCTION f_lua_add(x INT, y INT) RETURNS INT LANGUAGE LUA
BEGIN
'function f_lua_add(x, y)
return x + y
end'
END;

SHOW CREATE FUNCTION f_lua_add;
DROP FUNCTION f_lua_add;

-- BUG6
CREATE TS DATABASE test_win;
SET timezone = 8;

CREATE TABLE test_win.t1(k_timestamp TIMESTAMP NOT NULL, v INT, status INT)
    TAGS(tag1 INT NOT NULL) PRIMARY TAGS(tag1);
INSERT INTO test_win.t1 VALUES('2024-06-01 01:00:00', 10, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 02:00:00', 50, 0, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 03:00:00', 80, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 04:00:00', 20, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 05:00:00', 60, 0, 1);

-- 2. time_window
delimiter \\
CREATE FUNCTION f_crash_tw() RETURNS INT LANGUAGE SQL
BEGIN
SELECT COUNT(*) FROM test_win.t1
GROUP BY time_window(k_timestamp, '2h', '1h') LIMIT 1;
END \\
delimiter ;

SELECT f_crash_tw() >= 0;

-- 3. event_window
delimiter \\
CREATE FUNCTION f_crash_ew() RETURNS INT LANGUAGE SQL
BEGIN
SELECT COUNT(*) FROM test_win.t1
GROUP BY event_window(v > 50, v <= 50) LIMIT 1;
END \\
delimiter ;

SELECT f_crash_ew() >= 0;

-- 4. state_window
delimiter \\
CREATE FUNCTION f_crash_sw() RETURNS INT LANGUAGE SQL
BEGIN
SELECT COUNT(*) FROM test_win.t1
GROUP BY state_window(status) LIMIT 1;
END \\
delimiter ;
SELECT f_crash_sw() >= 0;
SELECT COUNT(*) >=0  FROM test_win.t1 GROUP BY time_window(k_timestamp, '2h', '1h');

-- session_window / count_window
delimiter \\
CREATE FUNCTION f_ok_sw() RETURNS INT LANGUAGE SQL
BEGIN
SELECT COUNT(*) FROM test_win.t1
GROUP BY session_window(k_timestamp, '2h') LIMIT 1;
END \\
delimiter ;

SELECT f_ok_sw() >= 0;

DROP FUNCTION f_crash_tw;
DROP FUNCTION f_crash_ew;
DROP FUNCTION f_crash_sw;
DROP FUNCTION f_ok_sw;
DROP TABLE test_win.t1;
DROP DATABASE test_win CASCADE;

-- BUG7
-- CHAR(1)
delimiter \\
CREATE FUNCTION bug15_char() RETURNS CHAR(1) LANGUAGE SQL
BEGIN
SELECT 'AB';
END \\
delimiter ;
SELECT bug15_char();
DROP FUNCTION bug15_char;

-- VARCHAR(3)
delimiter \\
CREATE FUNCTION bug15_varchar() RETURNS VARCHAR(3) LANGUAGE SQL
BEGIN
SELECT 'abcdef';
END \\
delimiter ;
SELECT bug15_varchar();
DROP FUNCTION bug15_varchar;

-- NCHAR(1)
delimiter \\
CREATE FUNCTION bug15_nchar() RETURNS NCHAR(1) LANGUAGE SQL
BEGIN
SELECT 'ABCD';
END \\
delimiter ;
SELECT bug15_nchar();
DROP FUNCTION bug15_nchar;

-- NVARCHAR(3)
delimiter \\
CREATE FUNCTION bug15_nvarchar() RETURNS NVARCHAR(3) LANGUAGE SQL
BEGIN
SELECT 'abcdef';
END \\
delimiter ;
SELECT bug15_nvarchar();
DROP FUNCTION bug15_nvarchar;

-- DECIMAL(5,2)
delimiter \\
CREATE FUNCTION bug15_decimal() RETURNS DECIMAL(5,2) LANGUAGE SQL
BEGIN
SELECT 9999.999;
END \\
delimiter ;
SELECT bug15_decimal();
DROP FUNCTION bug15_decimal;

-- INT2 (max 32767)
delimiter \\
CREATE FUNCTION bug15_int2() RETURNS INT2 LANGUAGE SQL
BEGIN
SELECT 99999;
END \\
delimiter ;
SELECT bug15_int2();
DROP FUNCTION bug15_int2;

-- INT4 (max 2147483647)
delimiter \\
CREATE FUNCTION bug15_int4() RETURNS INT4 LANGUAGE SQL
BEGIN
SELECT 9999999999;
END \\
delimiter ;
SELECT bug15_int4();
DROP FUNCTION bug15_int4;

-- FLOAT4 溢出 → +Inf
delimiter \\
CREATE FUNCTION bug15_float4() RETURNS FLOAT4 LANGUAGE SQL
BEGIN
SELECT 1.0e50::FLOAT8;
END \\
delimiter ;
SELECT bug15_float4();
DROP FUNCTION bug15_float4;

-- FLOAT8 溢出 → +Inf
delimiter \\
CREATE FUNCTION bug15_float8() RETURNS FLOAT8 LANGUAGE SQL
BEGIN
SELECT 1.0e400::FLOAT8;
END \\
delimiter ;
SELECT bug15_float8();
DROP FUNCTION bug15_float8;

-- TIMESTAMP year over flow
delimiter \\
CREATE FUNCTION bug15_timestamp() RETURNS TIMESTAMP LANGUAGE SQL
BEGIN
SELECT '99999-01-01'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp();
DROP FUNCTION bug15_timestamp;

-- TIMESTAMPTZ year over flow
delimiter \\
CREATE FUNCTION bug15_timestamptz() RETURNS TIMESTAMPTZ LANGUAGE SQL
BEGIN
SELECT '99999-01-01'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz();
DROP FUNCTION bug15_timestamptz;

-- TIMESTAMP precision 0-9
delimiter \\
CREATE FUNCTION bug15_timestamp_p0() RETURNS TIMESTAMP(0) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p0();
DROP FUNCTION bug15_timestamp_p0;

delimiter \\
CREATE FUNCTION bug15_timestamp_p1() RETURNS TIMESTAMP(1) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p1();
DROP FUNCTION bug15_timestamp_p1;

delimiter \\
CREATE FUNCTION bug15_timestamp_p2() RETURNS TIMESTAMP(2) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p2();
DROP FUNCTION bug15_timestamp_p2;

delimiter \\
CREATE FUNCTION bug15_timestamp_p3() RETURNS TIMESTAMP(3) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p3();
DROP FUNCTION bug15_timestamp_p3;

delimiter \\
CREATE FUNCTION bug15_timestamp_p4() RETURNS TIMESTAMP(4) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p4();
DROP FUNCTION bug15_timestamp_p4;

delimiter \\
CREATE FUNCTION bug15_timestamp_p5() RETURNS TIMESTAMP(5) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p5();
DROP FUNCTION bug15_timestamp_p5;

delimiter \\
CREATE FUNCTION bug15_timestamp_p6() RETURNS TIMESTAMP(6) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p6();
DROP FUNCTION bug15_timestamp_p6;

delimiter \\
CREATE FUNCTION bug15_timestamp_p7() RETURNS TIMESTAMP(7) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p7();
DROP FUNCTION bug15_timestamp_p7;

delimiter \\
CREATE FUNCTION bug15_timestamp_p8() RETURNS TIMESTAMP(8) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p8();
DROP FUNCTION bug15_timestamp_p8;

delimiter \\
CREATE FUNCTION bug15_timestamp_p9() RETURNS TIMESTAMP(9) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789'::TIMESTAMP;
END \\
delimiter ;
SELECT bug15_timestamp_p9();
DROP FUNCTION bug15_timestamp_p9;


-- TIMESTAMPTZ precision 0-9
delimiter \\
CREATE FUNCTION bug15_timestamptz_p0() RETURNS TIMESTAMPTZ(0) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p0();
DROP FUNCTION bug15_timestamptz_p0;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p1() RETURNS TIMESTAMPTZ(1) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p1();
DROP FUNCTION bug15_timestamptz_p1;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p2() RETURNS TIMESTAMPTZ(2) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p2();
DROP FUNCTION bug15_timestamptz_p2;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p3() RETURNS TIMESTAMPTZ(3) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p3();
DROP FUNCTION bug15_timestamptz_p3;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p4() RETURNS TIMESTAMPTZ(4) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p4();
DROP FUNCTION bug15_timestamptz_p4;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p5() RETURNS TIMESTAMPTZ(5) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p5();
DROP FUNCTION bug15_timestamptz_p5;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p6() RETURNS TIMESTAMPTZ(6) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p6();
DROP FUNCTION bug15_timestamptz_p6;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p7() RETURNS TIMESTAMPTZ(7) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p7();
DROP FUNCTION bug15_timestamptz_p7;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p8() RETURNS TIMESTAMPTZ(8) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p8();
DROP FUNCTION bug15_timestamptz_p8;

delimiter \\
CREATE FUNCTION bug15_timestamptz_p9() RETURNS TIMESTAMPTZ(9) LANGUAGE SQL
BEGIN
SELECT '2024-01-01 10:00:00.123456789+00:00'::TIMESTAMPTZ;
END \\
delimiter ;
SELECT bug15_timestamptz_p9();
DROP FUNCTION bug15_timestamptz_p9;

-- BUG8
CREATE DATABASE bug29_test;
USE bug29_test;

CREATE TABLE t_sales (id INT PRIMARY KEY, product VARCHAR(50), amount DECIMAL(10,2), qty INT);

INSERT INTO t_sales VALUES
                        (1, 'Widget', 100.00, 5),
                        (2, 'Gadget', 200.00, 3),
                        (3, 'Doohickey', 300.00, 2);

delimiter \\
CREATE FUNCTION f_total(amount DECIMAL, qty INT) RETURNS DECIMAL LANGUAGE SQL
BEGIN
SELECT amount * qty;
END \\
delimiter ;

delimiter \\
CREATE FUNCTION f_scale(x DECIMAL) RETURNS DECIMAL LANGUAGE SQL
BEGIN
SELECT CASE WHEN x > 100 THEN x * 0.9 ELSE x END;
END \\
delimiter ;

SELECT f_total(100.00, 5) ;
SELECT f_scale(150.00);
SELECT id, product, f_total(amount, qty) as total FROM t_sales order by id;

CREATE TABLE t_copy AS
SELECT id, product, f_total(amount, qty) as total, f_scale(amount) as scaled FROM t_sales;

SELECT count(*) FROM t_copy;

DROP TABLE t_copy;
DROP FUNCTION f_total;
DROP FUNCTION f_scale;
DROP TABLE t_sales;
USE defaultdb;
DROP DATABASE bug29_test CASCADE;

-- BUG9
CREATE TS DATABASE test_win;
CREATE TABLE test_win.t1(k_timestamp TIMESTAMP NOT NULL, v INT, status INT)TAGS(tag1 INT NOT NULL) PRIMARY TAGS(tag1);
INSERT INTO test_win.t1 VALUES('2024-06-01 01:00:00', 10, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 02:00:00', 50, 0, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 03:00:00', 80, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 04:00:00', 20, 1, 1);
INSERT INTO test_win.t1 VALUES('2024-06-01 05:00:00', 60, 0, 1);
CREATE FUNCTION f_crash_tw() RETURNS INT LANGUAGE SQL BEGIN SELECT COUNT(*) FROM test_win.t1 GROUP BY time_window(k_timestamp, '2h', '1h') LIMIT 1;END;
SELECT f_crash_tw();
CREATE FUNCTION f_crash_ew() RETURNS INT LANGUAGE SQL BEGIN SELECT COUNT(*) FROM test_win.t1 GROUP BY event_window(v > 50, v <= 50) LIMIT 1;END;
SELECT f_crash_ew();
CREATE FUNCTION f_crash_sw() RETURNS INT LANGUAGE SQL BEGIN SELECT COUNT(*) FROM test_win.t1 GROUP BY state_window(status) LIMIT 1;END;
SELECT f_crash_sw();
CREATE FUNCTION f_ok_sw() RETURNS INT LANGUAGE SQL BEGIN SELECT COUNT(*) FROM test_win.t1 GROUP BY session_window(k_timestamp, '2h') LIMIT 1;END;
SELECT f_ok_sw();
show create function f_crash_tw;
show function f_crash_tw;
show create function f_crash_ew;
show function f_crash_ew;
show create function f_crash_sw;
show function f_crash_sw;
show create function f_ok_sw;
show function f_ok_sw;
DROP FUNCTION  f_crash_tw;
DROP FUNCTION  f_crash_ew;
DROP FUNCTION  f_crash_sw;
DROP FUNCTION  f_ok_sw;
drop database test_win cascade;

-- BUG10
CREATE TS DATABASE IF NOT EXISTS test_udf_null;
CREATE TABLE test_udf_null.sensor_null (ts TIMESTAMP NOT NULL, val_int INT4, val_float FLOAT8, val_varchar VARCHAR(50), status INT4) TAGS (device VARCHAR(32) NOT NULL, location VARCHAR(32)) PRIMARY TAGS (device);
INSERT INTO test_udf_null.sensor_null VALUES ('2024-01-01 00:00:00', 100, 3.14, 'normal', 1, 'dev_1', 'room_a'), ('2024-01-01 00:01:00', NULL, 2.71, 'null_int', 0, 'dev_1', 'room_a'), ('2024-01-01 00:02:00', 200, NULL, 'null_float', 2, 'dev_1', 'room_a'), ('2024-01-01 00:03:00', 150, 1.41, NULL, 3, 'dev_1', 'room_a'), ('2024-01-01 00:04:00', NULL, NULL, NULL, NULL, 'dev_1', 'room_a'), ('2024-01-01 00:05:00', 300, 9.99, 'mixed', 1, 'dev_1', 'room_a'), ('2024-01-01 00:06:00', 100, 5.0, 'div_by_zero', 0, 'dev_1', 'room_a');

CREATE FUNCTION f_lua_add(x INT, y INT) RETURNS INT LANGUAGE LUA BEGIN 'function f_lua_add(x, y) return x + y end' END;
CREATE FUNCTION f_sql_divide(a INT4, b INT4) RETURNS FLOAT8 LANGUAGE SQL BEGIN SELECT a::FLOAT8 / b; END;

SELECT device, ts, val_int, f_lua_add(val_int, 100) AS result FROM test_udf_null.sensor_null ORDER BY device, ts;

SELECT ts, val_int, status, f_sql_divide(val_int, status) AS div_result FROM test_udf_null.sensor_null ORDER BY ts;
DROP TABLE test_udf_null.sensor_null;
DROP FUNCTION f_lua_add;
DROP FUNCTION f_sql_divide;
drop database test_udf_null cascade;
SHOW FUNCTIONS;
SHOW PROCEDURES;