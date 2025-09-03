-- 创建测试数据库
DROP DATABASE IF EXISTS test_udvar;
CREATE DATABASE test_udvar;
USE test_udvar;

CREATE SEQUENCE user_id_seq;
DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
                           employee_id INT PRIMARY KEY DEFAULT nextval('user_id_seq'),
                           name VARCHAR(50),
                           salary DECIMAL(10,2),
                           department VARCHAR(50),
                           hire_date DATE
);

INSERT INTO employees (name, salary, department, hire_date) VALUES
                                                                ('Alice', 5000.00, 'Sales', '2023-01-15'),
                                                                ('Bob', 6000.00, 'Sales', '2023-02-20'),
                                                                ('Charlie', 6000.00, 'HR', '2023-03-05'),
                                                                ('David', 5500.00, 'Sales', '2023-01-25'),
                                                                ('Eve', 7000.00, 'Engineering', '2023-04-10'),
                                                                ('Frank', 4500.00, 'HR', '2023-02-10'),
                                                                ('Grace', 8000.00, 'Engineering', '2023-03-15');

CREATE SEQUENCE user_id_seq1;
DROP TABLE IF EXISTS products;
CREATE TABLE products (
                          product_id INT PRIMARY KEY DEFAULT nextval('user_id_seq1'),
                          product_name VARCHAR(50),
                          price DECIMAL(10,2),
                          category VARCHAR(50)
);

INSERT INTO products (product_name, price, category) VALUES
                                                         ('Laptop', 1000.00, 'Electronics'),
                                                         ('Smartphone', 800.00, 'Electronics'),
                                                         ('Headphones', 200.00, 'Electronics'),
                                                         ('Desk', 150.00, 'Furniture'),
                                                         ('Chair', 100.00, 'Furniture'),
                                                         ('Monitor', 300.00, 'Electronics');

SET @num := 100;
SELECT @num AS num_value;
SHOW @num;
SET @num := 200;
SELECT @num AS new_num_value;
SHOW @num;
SET @NUM := 201;
SELECT @NUM;
SHOW @NUM;
SET @"NUM" := 202;
SELECT @"NUM";
SHOW @"NUM";


SET @first_salary := 0.0;
SELECT salary FROM employees ORDER BY employee_id LIMIT 1;
SELECT salary INTO @first_salary FROM employees ORDER BY employee_id LIMIT 1;
SELECT @first_salary AS first_salary;
SELECT salary + 1 FROM employees ORDER BY employee_id LIMIT 1;
SELECT salary + 1 INTO @FIRST_salary FROM employees ORDER BY employee_id LIMIT 1;
SELECT @FIRST_salary;
SELECT salary + 2 FROM employees ORDER BY employee_id LIMIT 1;
SELECT salary + 2 INTO @"FIRST_salary" FROM employees ORDER BY employee_id LIMIT 1;
SELECT @"FIRST_salary";
SELECT salary + 3 FROM employees ORDER BY employee_id LIMIT 1;
set @FIRST_salary = (SELECT salary + 3 FROM employees ORDER BY employee_id LIMIT 1);
SELECT @FIRST_salary;
SELECT salary + 4 FROM employees ORDER BY employee_id LIMIT 1;
set @"FIRST_salary" = (SELECT salary + 4 FROM employees ORDER BY employee_id LIMIT 1);
SELECT @"FIRST_salary";
SHOW @first_salary;
SHOW @FIRST_salary;
SHOW @FIRST_salar;

SET @first_product := '';
SELECT product_name FROM products ORDER BY product_id LIMIT 1;
SELECT product_name INTO @first_product FROM products ORDER BY product_id LIMIT 1;
SELECT @first_product AS first_product_name;
SHOW @first_product;


SET @first_name := '';
SET @first_salary2 := 0.0;
SELECT name, salary FROM employees WHERE employee_id = 1;
SELECT name, salary INTO @first_name, @first_salary2 FROM employees WHERE employee_id = 1;
SELECT @first_name AS first_name, @first_salary2 AS first_salary2;

SELECT name, salary INTO @only_name FROM employees WHERE employee_id = 1;


SET @a := 10;
SET @b := 3;
SELECT @a + @b AS sum, @a - @b AS diff, @a * @b AS prod, @a / @b AS quot;
SET @a := 20;
SET @b := 5;
SET @c := 2;
SELECT (@a - @b) + (@a * @c) - (@a / @b) AS mixed_expr;
SET @a := 10;
SET @b := 3;
SELECT @a % @b AS mod_result;
SELECT MOD(@a, @b) AS mod_func;
SET @a := 10;
SET @b := 3;
SET @c := 2;
SELECT @a + @b * @c AS expr1, (@a + @b) * @c AS expr2;
SET @a := 15;
SET @b := 4;
SET @c := 3;
SELECT ((@a + @b) * (@c - 1)) / (@a - 5) AS nested_expr;
SET @a := -5;
SET @b := 3;
SELECT @a + @b AS sum_neg, @a * @b AS prod_neg;
SET @x := 10.5;
SET @y := 4.2;
SELECT @x / @y AS division, @x - @y AS diff_float;
SET @base := 2;
SET @exp := 3;
SELECT POW(@base, @exp) AS power_val, POW(@base, @exp) + 1 AS power_plus;
SET @a := 20;
SET @b := 3;
SET @c := 2;
SET @d := 2;
SELECT (@a - @b) * (@c + @d) / (MOD(@b, @d) + 1) AS complex_expr;
SET @a := 50;
SET @b := 7;
SET @c := 3;
SELECT ((@a + @b) - (@a / @c)) % @b AS mixed_result;


SET @num_str := '123';
SELECT CAST(@num_str AS INT) + 7 AS explicit_result;
SELECT @num_str + 7 AS implicit_result;
SET @init_date := '2023-01-01';
SELECT @init_date AS init_date;
SELECT hire_date FROM employees ORDER BY hire_date LIMIT 1;
SELECT hire_date::STRING INTO @init_date FROM employees ORDER BY hire_date LIMIT 1;
SELECT @init_date AS first_hire_date;

SET @min_salary := 5500;
SELECT employee_id, name, salary FROM employees WHERE salary > 5500;
SELECT employee_id, name, salary FROM employees WHERE salary > @min_salary;
SET @dept := 'Sales';
SELECT employee_id, name, salary FROM employees WHERE salary > 5000 AND department = 'Sales';
--SELECT employee_id, name, salary FROM employees WHERE salary > 5000 AND department = @dept;

SET @sales_count := 0;
SELECT COUNT(*) FROM employees WHERE department = 'Sales' GROUP BY department LIMIT 1;
SELECT COUNT(*) INTO @sales_count FROM employees WHERE department = 'Sales' GROUP BY department LIMIT 1;
SELECT @sales_count AS sales_count;
SELECT department, AVG(salary) AS avg_salary FROM employees GROUP BY department HAVING AVG(salary) > (SELECT AVG(salary) FROM employees) LIMIT 1;

SET @max_salary := 0.0;
SELECT salary FROM employees ORDER BY salary DESC LIMIT 1;
SELECT salary INTO @max_salary FROM employees ORDER BY salary DESC LIMIT 1;
SELECT @max_salary AS highest_salary;

SET @unique_dept := '';
SELECT DISTINCT department FROM employees WHERE department IN ('Sales', 'HR') LIMIT 1;
SELECT DISTINCT department INTO @unique_dept FROM employees WHERE department IN ('Sales', 'HR') LIMIT 1;
SELECT @unique_dept AS unique_department;

SET @total_emp := 0;
SELECT COUNT(*) FROM employees;
SELECT (SELECT COUNT(*) FROM employees) INTO @total_emp;
SELECT @total_emp AS total_employees;
SELECT name FROM employees ORDER BY employee_id;
SELECT name INTO @emp_name FROM employees ORDER BY employee_id;


SET @tbl := 'employees';
SET @col := 'salary';
SET @cond := 'salary >= 6000';
SET @dyn_sql := CONCAT('SELECT employee_id, name, ', @col,
                        ' FROM ', @tbl,
                        ' WHERE ', @cond,
                        ' ORDER BY ', @col, ' DESC LIMIT 2');
SELECT @dyn_sql;
PREPARE stmt AS @dyn_sql;
EXECUTE stmt;
SELECT employee_id, name, salary FROM employees WHERE salary >= 6000 ORDER BY salary DESC LIMIT 2;
DEALLOCATE PREPARE stmt;

SET @avg_salary := 0.0;
SELECT AVG(salary) FROM employees;
SET @avg_salary := (SELECT AVG(salary) FROM employees);
SELECT employee_id, name, salary FROM employees WHERE salary > @avg_salary;


SET @nonexistent := 'default';
SELECT name FROM employees WHERE employee_id = -1;
SELECT name INTO @nonexistent FROM employees WHERE employee_id = -1;
SELECT name INTO @nonexistent FROM employees;
SELECT name FROM employees;
SELECT @nonexistent AS result_when_no_row;

SET @last_name := '';
SELECT name FROM employees ORDER BY employee_id;
SELECT name INTO @last_name FROM employees ORDER BY employee_id;
SELECT @last_name AS last_employee_name;

-- SELECT @x, @x := 100, @x FROM employees LIMIT 1;


SELECT @uninit_var + 10 AS result;

SET @var := 100;
SET @var := 'Hello';
SELECT @var AS result;

SET @str := 'abc';
SET @str := 123;
SELECT @str AS result;



DROP DATABASE IF EXISTS test_ts_udvar;
CREATE TS DATABASE test_ts_udvar;
USE test_ts_udvar;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1(
                   ts TIMESTAMP NOT NULL,
                   a INT
) TAGS(
  b INT NOT NULL,
  c INT
) PRIMARY TAGS(b);
INSERT INTO t1 VALUES ('2024-03-06 10:00:00', 10, 101, 1001);
INSERT INTO t1 VALUES ('2024-03-06 10:05:00', 20, 102, 1002);
INSERT INTO t1 VALUES ('2024-03-06 10:10:00', 30, 101, 1003);
INSERT INTO t1 VALUES ('2024-03-06 10:15:00', 40, 103, 1001);
INSERT INTO t1 VALUES ('2024-03-06 10:20:00', 50, 102, 1002);

-- SET @rownum := 0;
-- EXPLAIN SELECT ts, a, @rownum := a + @rownum AS row_number FROM t1 where (@rownum := 1 + @rownum) < 3 ORDER BY ts;
-- SELECT ts, a, @rownum := a + @rownum AS row_number FROM t1 where (@rownum := 1 + @rownum) < 3 ORDER BY ts;
-- SELECT @rownum;
-- explain SELECT a, @total_salary := @total_salary + a AS total_salary FROM t1;

SET @first_a := 0;
SELECT a INTO @first_a FROM t1 ORDER BY ts LIMIT 1;
SELECT a FROM t1 ORDER BY ts LIMIT 1;
SELECT @first_a AS first_a_value;

SET @first_a2 := 0;
SET @dummy := 0;
SELECT a, a+5 INTO @first_a2, @dummy FROM t1 WHERE ts = '2024-03-06 10:10:00';
SELECT a, a+5 FROM t1 WHERE ts = '2024-03-06 10:10:00';
SELECT @first_a2 AS first_a2, @dummy AS dummy_value;

SET @first_tag_b := '';
SELECT CAST(b AS CHAR) INTO @first_tag_b FROM t1 WHERE ts = '2024-03-06 10:05:00' LIMIT 1;
SELECT CAST(b AS CHAR) INTO @first_tag_b FROM t1 WHERE ts = '2024-03-06 10:05:00';
SELECT CAST(b AS CHAR) FROM t1 WHERE ts = '2024-03-06 10:05:00' LIMIT 1;
SELECT CAST(b AS CHAR) FROM t1 WHERE ts = '2024-03-06 10:05:00';
SELECT @first_tag_b AS first_tag_b;

SET @str1 := '';
SET @str2 := '';
SELECT CAST(b AS CHAR), CAST(c AS CHAR) INTO @str1, @str2
FROM t1 WHERE ts = '2024-03-06 10:15:00' LIMIT 1;
SELECT @str1 AS tag_b_str, @str2 AS tag_c_str;

SET @first_ts := '2000-01-01 00:00:00';
SELECT ts::STRING INTO @first_ts FROM t1 ORDER BY ts LIMIT 1;
SELECT ts FROM t1 ORDER BY ts LIMIT 1;
SELECT @first_ts AS first_ts_value;

SET @first_ts_1 := '2000-01-01 00:00:00'::timestamptz;
SELECT ts INTO @first_ts_1 FROM t1 ORDER BY ts LIMIT 1;
SELECT ts FROM t1 ORDER BY ts LIMIT 1;
SELECT @first_ts_1 AS first_ts_value;
SET @first_ts_1 = (SELECT ts FROM t1 ORDER BY ts LIMIT 1);
SELECT @first_ts_1 AS first_ts_value;

SET @avg_salary := 0.0;
SET @avg_salary := (SELECT AVG(a) FROM t1);
SELECT @avg_salary AS avg_a;
SELECT AVG(a) FROM t1;

SET @total_a := 0.0;
SELECT SUM(a) INTO @total_a FROM t1;
SELECT SUM(a) FROM t1;
SELECT @total_a AS total_a;

SET @tbl := 't1';
SET @col := 'a';
SET @cond := 'a >= 30';
SET @dyn_sql := CONCAT('SELECT ts, a FROM ', @tbl, ' WHERE ', @cond, ' ORDER BY a DESC LIMIT 2');
SELECT @dyn_sql;
PREPARE stmt AS @dyn_sql;
EXECUTE stmt;
SELECT ts, a FROM t1 WHERE a >= 30 ORDER BY a DESC LIMIT 2;
DEALLOCATE PREPARE stmt;

SET @grp := 'b';
SET @dyn_sql := CONCAT('SELECT ', @grp, ', COUNT(*) as cnt FROM t1 GROUP BY ', @grp, ' ORDER BY ', @grp, ' LIMIT 1');
SET @DYN_sql := CONCAT('SELECT ', @grp, ', COUNT(*) as cnt FROM t1 GROUP BY ', @grp, ' ORDER BY ', @grp, ' LIMIT 1');
SET @"DYN_sql" := CONCAT('SELECT ', @grp, ', COUNT(*) as cnt FROM t1 GROUP BY ', @grp, ' ORDER BY ', @grp, ' LIMIT 1');
SELECT @dyn_sql;
PREPARE stmt AS @dyn_sql;
EXECUTE stmt;
PREPARE stmt1 AS @DYN_sql;
EXECUTE stmt1;
PREPARE stmt2 AS @"DYN_sql";
EXECUTE stmt2;
SELECT b, COUNT(*) as cnt FROM t1 GROUP BY b ORDER BY b LIMIT 1;
PREPARE stmt3 AS @unexist_sql;
DEALLOCATE PREPARE stmt;

SET @tag_c = 1003;
INSERT INTO t1 VALUES ('2024-03-06 10:20:00', 50, 104, @tag_c);
SET @tag_c = 10030;
UPDATE t1 SET c = @tag_c WHERE b = 104;
SET @tag_c = 104;
DELETE FROM t1 WHERE b = @tag_c;

DROP DATABASE test_ts_udvar CASCADE;

SHOW @qwe;
SET @qwe = MAX(1);
SET @qwe = 'ss';
SET @qwe = NULL;
SET @qwe = null;
SET @qwe = 'null';
USE test_udvar;
create table test_udv1(a string);
SELECT a into @qwe from test_udv1;
SHOW @qwe;
SELECT @qwe;
INSERT INTO test_udv1 VALUES('Q'),('W');
SELECT a into @qwe from test_udv1;
SELECT @qwe;
DROP DATABASE test_udvar CASCADE;