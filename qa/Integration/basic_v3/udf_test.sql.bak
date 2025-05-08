--Native udf case
create database test;
use test;
Create table t1(k_timestamp timestamp not null,c1 int,c2 int);
set timezone = 8;
Insert into t1 values ('2024-1-1 1:00:00',1,2);
Insert into t1 values ('2024-1-1 1:00:00',2,4);
Insert into t1 values ('2024-1-1 2:00:00',6,3);
Insert into t1 values ('2024-1-1 5:00:00',8,12);
Insert into t1 values ('2024-1-1 5:00:00',0,3);
--Create a power consumption growth rate function
CREATE FUNCTION calculate_growth_rate(previous_consumption int, current_consumption int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function calculate_growth_rate(previous_consumption, current_consumption)
  if previous_consumption == 0 then
        return nil
    end
  return (current_consumption - previous_consumption) / previous_consumption
end'
END;

select calculate_growth_rate(c1,c2) from t1 where k_timestamp >= '2024-1-1 1:00:00' and k_timestamp <= '2024-1-1 5:00:00';

show functions;
show function calculate_growth_rate;
-- error
show function "";
-- error, function does not exist or no permissions
show function xxxyyyzzz;

-- An error occurred when creating a function with the same name
CREATE FUNCTION calculate_growth_rate(previous_consumption int, current_consumption int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function calculate_growth_rate(previous_consumption, current_consumption)
  if previous_consumption == 0 then
        return nil
    end
  return (current_consumption - previous_consumption) / previous_consumption
end'
END;

-- Error message for creating a custom function with the same name as the kwdb native function
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

-- Create syntax error
CREATE FUNCTION ""(a int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function  (a)
  if a > 0 then
        return a
    end
  return -a
end'
END;

drop function calculate_growth_rate;
select calculate_growth_rate(c1,c2) from t1 where k_timestamp >= '2024-1-1 1:00:00' and k_timestamp <= '2024-1-1 5:00:00';

DELETE FROM system.user_defined_function WHERE true;
use default;
drop database test cascade;

--- bug-45930
BEGIN;
CREATE FUNCTION test_txn(previous_consumption int, current_consumption int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function test_txn(previous_consumption, current_consumption)
  if previous_consumption == 0 then
        return nil
    end
  return (current_consumption - previous_consumption) / previous_consumption
end'
END;
COMMIT;
CREATE FUNCTION test_txn(previous_consumption int, current_consumption int)
    RETURNS FLOAT
    LANGUAGE LUA
BEGIN
'function test_txn(previous_consumption, current_consumption)
  if previous_consumption == 0 then
        return nil
    end
  return (current_consumption - previous_consumption) / previous_consumption
end'
END;
BEGIN;
DROP FUNCTION test_txn;
COMMIT;
SHOW FUNCTIONS;
DROP FUNCTION test_txn;

--testcase0001 Tests the input parameter types of the creation function timestamp
-- Create a function to calculate the timestamp difference
CREATE FUNCTION time_difference_to_string(start_time timestamp, end_time timestamp)
    RETURNS varchar(100)
    LANGUAGE LUA
BEGIN
'function time_difference_to_string(start_time, end_time)
    -- Calculates the difference between two timestamps in seconds
    local difference = math.abs(end_time - start_time)

    local days = math.floor(difference / (60 * 60 * 24))
    local hours = math.floor((difference % (60 * 60 * 24)) / (60 * 60))
    local minutes = math.floor((difference % (60 * 60)) / 60)
    local seconds = difference % 60

    local result = days .. "天 " .. hours .. "小时 " .. minutes .. "分钟 " .. seconds .. "秒"
    return result
end'
END;
-- Creates a function that increases the specified number of hours at a given time
CREATE FUNCTION time_add_hour(input_time timestamp, hours_to_add double)
    RETURNS timestamp
    LANGUAGE LUA
BEGIN
'function time_add_hour(input_time, hours_to_add)
    local new_time = input_time + (hours_to_add * 3600)
    return new_time
end'
END;
--select system.user_defined_functions
select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;
SHOW FUNCTIONS;
SHOW FUNCTION time_add_hour;
select time_add_hour('2024-01-01 10:00:00',1);
--select time_add_hour(now());
select time_add_hour(time_add_hour('2024-01-01 10:00:00',1),1);
select time_difference_to_string('2024-01-01 10:00:00','2024-01-02 11:00:00');
--ts table
create ts database test_udf;
create table test_udf.tb1(ts timestamptz not null,e1 timestamp)tags(t1 int not null)primary tags(t1);
insert into test_udf.tb1 values(now(),'2024-01-01 10:00:00',1);
insert into test_udf.tb1 values(now(),'2024-01-01 12:00:00',2);
insert into test_udf.tb1 values(now(),'2024-01-01 13:00:00',3);
select time_add_hour(e1,1) from test_udf.tb1 order by e1;
select time_difference_to_string(e1,'2024-01-01 10:00:00') from test_udf.tb1 order by e1;
select count(e1),min(e1),max(e1),time_add_hour(e1,1) as udf_add ,last_row(e1) from test_udf.tb1 group by e1 having time_add_hour(e1,1) > '2024-01-01 12:00:00' order by e1;
select count(e1),min(e1),max(e1),time_difference_to_string(e1,'2024-01-01 10:00:00') as udf_time_diff ,last_row(e1) from test_udf.tb1 where time_add_hour(e1,1) >= '2024-01-01 12:00:00' group by e1 order by e1;
drop table test_udf.tb1;
drop database test_udf cascade;
--relation table
create  database test_udf_re;
create table test_udf_re.r1(e1 timestamp);
insert into test_udf_re.r1 values('2024-01-01 10:00:00');
insert into test_udf_re.r1 values('2024-01-01 12:00:00');
insert into test_udf_re.r1 values('2024-01-01 13:00:00');
select time_add_hour(e1,1) from test_udf_re.r1 order by e1;
select time_difference_to_string(e1,'2024-01-01 10:00:00') from test_udf_re.r1 order by e1;
select count(e1),min(e1),max(e1) ,time_add_hour(e1,1) as udf_add from test_udf_re.r1 group by e1 having time_add_hour(e1,1) > '2024-01-01 12:00:00' order by e1;
select count(e1),min(e1),max(e1),time_difference_to_string(e1,'2024-01-01 10:00:00') as udf_time_diff,time_add_hour(e1,1) as udf_add from test_udf_re.r1 where time_add_hour(e1,1) >= '2024-01-01 12:00:00' group by e1 order by e1;
drop table test_udf_re.r1 ;
drop database test_udf_re cascade;
--boundary value test
select time_add_hour('1970-01-01 00:00:00',1);
select time_add_hour('1969-01-01 00:00:00',2);
select time_add_hour('2262-04-11 23:47:16.854',3);
select time_add_hour('2262-04-11 23:47:16.857',4);
select time_add_hour(null,null);
select time_difference_to_string('1970-01-01 00:00:00','1970-01-01 00:00:00');
select time_difference_to_string('1969-01-01 00:00:00','1970-01-01 00:00:00');
select time_difference_to_string('2262-04-11 23:47:16.854','1970-01-01 00:00:00');
select time_difference_to_string('2262-04-11 23:47:16.857','1970-01-01 00:00:00');
select time_difference_to_string(null,null);
--exception handle
select time_add_hour(-1,1);
select time_add_hour('test',1);
select time_add_hour(123456,1);
select time_add_hour(1.1,1);
select time_add_hour(false,1);
select time_difference_to_string(-1,1);
select time_difference_to_string('test',1);
select time_difference_to_string(123456,1);
select time_difference_to_string(1.1,1);
select time_difference_to_string(false,1);

DROP FUNCTION time_add_hour;
DROP FUNCTION time_difference_to_string;
SHOW FUNCTIONS;
SHOW FUNCTION time_add_hour;

--testcase0002 smallint
CREATE FUNCTION multiply(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function multiply(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;
select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;
SHOW FUNCTIONS;
SHOW FUNCTION multiply;
select multiply(3);
select multiply(multiply(2));
create ts database test_udf;
create table test_udf.tb1(ts timestamptz not null,e1 smallint)tags(t1 int not null)primary tags(t1);
insert into test_udf.tb1 values(now(),0,1);
insert into test_udf.tb1 values(now(),10,2);
insert into test_udf.tb1 values(now(),200,3);
CREATE FUNCTION complex_calculation(input_val smallint)
    RETURNS smallint
    LANGUAGE LUA
BEGIN
'
function complex_calculation(input_val)
    local result = 0
    if input_val < 10 then
        result = input_val * input_val
    elseif input_val >= 10 and input_val <= 100 then
        result = math.floor(math.sqrt(input_val) * 2)
    else
        result = math.floor(math.log(input_val) * 3)
    end
return result
end
'
END;
select complex_calculation(e1) from test_udf.tb1 order by e1;
select multiply(e1) from test_udf.tb1 order by e1;
select count(e1),min(e1),max(e1),complex_calculation(e1) as udf_cal ,last_row(e1) from test_udf.tb1 group by e1 having complex_calculation(e1) > 2 order by e1;
select count(e1),min(e1),max(e1),multiply(e1) as udf_add ,last_row(e1) from test_udf.tb1 group by e1 having multiply(e1) > 2 order by e1;
drop table test_udf.tb1;
drop database test_udf cascade;

create  database test_udf_re;
create table test_udf_re.r1(e1 smallint);
insert into test_udf_re.r1 values(0);
insert into test_udf_re.r1 values(10);
insert into test_udf_re.r1 values(200);
select complex_calculation(e1) from test_udf_re.r1 order by e1;
select multiply(e1) from test_udf_re.r1 order by e1;
select count(e1),min(e1),max(e1) ,multiply(e1) as udf_add from test_udf_re.r1 group by e1 having multiply(e1) > 2 order by e1;
select count(e1),min(e1),max(e1) ,complex_calculation(e1) as udf_cal from test_udf_re.r1 group by e1 having complex_calculation(e1) > 2 order by e1;
drop table test_udf_re.r1 ;
drop database test_udf_re cascade;

select multiply(-32768);
select multiply(-32769);
select multiply(32767);
select multiply(32768);
select multiply(null);
select complex_calculation(-32768);
select complex_calculation(-32769);
select complex_calculation(32767);
select complex_calculation(32768);
select complex_calculation(null);

select multiply(now());
select multiply('test');
select multiply(1.2);
select multiply(true);
select multiply('2011-11-11');
select complex_calculation(now());
select complex_calculation('test');
select complex_calculation(1.2);
select complex_calculation(true);
select complex_calculation('2011-11-11');

DROP FUNCTION multiply;
DROP FUNCTION complex_calculation;
SHOW FUNCTIONS;
SHOW FUNCTION multiply;

--testcase0003 int

CREATE TS DATABASE test_udf;
CREATE TABLE test_udf.tb1(ts timestamptz NOT NULL, e1 int) TAGS(t1 int NOT NULL) PRIMARY TAGS(t1);
INSERT INTO test_udf.tb1 VALUES(NOW(), 10, 1);
INSERT INTO test_udf.tb1 VALUES(NOW(), 20, 2);
INSERT INTO test_udf.tb1 VALUES(NOW(), 28, 3);

CREATE FUNCTION is_perfect_number(num int)
    RETURNS int
    LANGUAGE LUA
BEGIN
'function is_perfect_number(num)
    local function find_divisors(n)
        local sum = 1
        for i = 2, math.sqrt(n) do
            if n % i == 0 then
                sum = sum + i
                if i ~= n / i then
                    sum = sum + n / i
                end
            end
        end
        return sum
    end


    local divisors_sum = find_divisors(num)


    if num > 1 and divisors_sum == num then
        return num
    else
        return nil
    end
end'
END;

CREATE FUNCTION square(nu_int int)
    RETURNS int
    LANGUAGE LUA
BEGIN
'function square(nu_int)
    local re = nu_int * nu_int
    return re
end'
END;

CREATE FUNCTION sum_two_ints(a int, b int)
    RETURNS int
    LANGUAGE LUA
BEGIN
'function sum_two_ints(a, b)
    local re = a + b
    return re
end'
END;

SELECT square(4);
SELECT sum_two_ints(3, 5);
SELECT is_perfect_number(28);

SELECT function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, comments FROM system.user_defined_function;

SHOW FUNCTIONS;

SHOW FUNCTION square;
SHOW FUNCTION sum_two_ints;
SELECT square(e1) FROM test_udf.tb1 order by e1;
SELECT sum_two_ints(e1, e1) AS double_e1 FROM test_udf.tb1 order by e1;
SELECT is_perfect_number(e1) FROM test_udf.tb1 where is_perfect_number(e1) > 0 order by e1;
drop table test_udf.tb1;
drop database test_udf cascade;

CREATE DATABASE test_udf_re;
CREATE TABLE test_udf_re.r1(e1 int);
INSERT INTO test_udf_re.r1 VALUES(10);
INSERT INTO test_udf_re.r1 VALUES(20);
SELECT square(e1) FROM test_udf_re.r1 order by e1;
SELECT sum_two_ints(e1, e1) AS double_e1 FROM test_udf_re.r1 order by e1;
SELECT is_perfect_number(e1) FROM test_udf_re.r1 where is_perfect_number(e1) > 0 order by e1;
drop table test_udf_re.r1 ;
drop database test_udf_re cascade;

SELECT square(-2147483648);
SELECT square(2147483647);
SELECT square(null);
SELECT sum_two_ints(-2147483648, 2147483647);
SELECT sum_two_ints(-2147483648, 2147483648);
SELECT sum_two_ints(null,null);
SELECT is_perfect_number(-2147483648);
SELECT is_perfect_number(-2147483649);
SELECT is_perfect_number(2147483647);
SELECT is_perfect_number(2147483648);
SELECT is_perfect_number(null);

SELECT square(NOW());
SELECT square('test');
SELECT square(1.2);
SELECT square(TRUE);
SELECT square('2011-11-11');

DROP FUNCTION square;
DROP FUNCTION sum_two_ints;
DROP FUNCTION is_perfect_number;
SHOW FUNCTIONS;

--testcase0004 bigint

CREATE TS DATABASE test_udf;
CREATE TABLE test_udf.tb1(ts timestamptz NOT NULL, e1 bigint) TAGS(t1 int NOT NULL) PRIMARY TAGS(t1);
INSERT INTO test_udf.tb1 VALUES(NOW(),10,1);

CREATE FUNCTION fibonacci(n bigint)
    RETURNS bigint
    LANGUAGE LUA
BEGIN
'function fibonacci(n)
    if n <= 1 then return n end
    local fib = {0, 1}
    for i = 3, n + 1 do
        fib[i] = fib[i - 1] + fib[i - 2]
    end
    return fib[n + 1]
end'
END;

CREATE FUNCTION sum_of_factors(num bigint)
    RETURNS bigint
    LANGUAGE LUA
BEGIN
'function sum_of_factors(num)
    local sum = 1
    for i = 2, num do
        if num % i == 0 then
            sum = sum + i
        end
    end
    return sum
end'
END;

SELECT sum_of_factors(28);
SELECT fibonacci(10);

SELECT function_name,argument_types,return_type,types_length,function_body,function_type,language,db_name,creator,comments FROM system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION fibonacci;
SHOW FUNCTION sum_of_factors;
SELECT fibonacci(e1) FROM test_udf.tb1;
SELECT sum_of_factors(e1) FROM test_udf.tb1;
drop table test_udf.tb1;
drop database test_udf cascade;

CREATE DATABASE test_udf_re;
CREATE TABLE test_udf_re.r1(e1 bigint);
INSERT INTO test_udf_re.r1 VALUES(10);
SELECT fibonacci(e1) FROM test_udf_re.r1;
SELECT sum_of_factors(e1) FROM test_udf_re.r1;
drop table test_udf_re.r1 ;
drop database test_udf_re cascade;

select fibonacci(12200160415121876738);
SELECT fibonacci(93);
SELECT sum_of_factors(100);
select fibonacci(null);
SELECT fibonacci(null);
SELECT sum_of_factors(null);

SELECT fibonacci('test');
SELECT sum_of_factors(now());

DROP FUNCTION fibonacci;
DROP FUNCTION sum_of_factors;
SHOW FUNCTIONS;

--testcase0005 float

CREATE TS DATABASE test_udf;
CREATE TABLE test_udf.tb1(ts timestamptz NOT NULL, e1 float4) TAGS(t1 int NOT NULL) PRIMARY TAGS(t1);
INSERT INTO test_udf.tb1 VALUES(NOW(), 5, 1);

CREATE FUNCTION calculate_circle_area(radius float4)
    RETURNS float4
    LANGUAGE LUA
BEGIN
'function calculate_circle_area(radius)
    local pi = 3.141592653589793
    local area = pi * radius * radius
    return area
end'
END;

CREATE FUNCTION calculate_stddev(numbers_str varchar)
    RETURNS float4
    LANGUAGE LUA
BEGIN
'function calculate_stddev(numbers_str)
    local sum = 0
    local numbers = {}
    local count = 0
    for number in string.gmatch(numbers_str, "([^,]+)") do
        local num = tonumber(number)
        if not num then

          return nil
        end
        table.insert(numbers, num)
        sum = sum + num
        count = count + 1
    end
    local mean = sum / count
    local variance_sum = 0
    for _, num in ipairs(numbers) do
        variance_sum = variance_sum + ((num - mean) ^ 2)
    end
    local variance = variance_sum / count
    return math.sqrt(variance)
end'
END;

SELECT calculate_circle_area(5);
SELECT calculate_stddev('1,2,3,4,5');

SELECT function_name,argument_types,return_type,types_length,function_body,function_type,language,db_name,creator,comments FROM system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION calculate_circle_area;
SHOW FUNCTION calculate_stddev;
SELECT calculate_circle_area(e1) FROM test_udf.tb1;
drop table test_udf.tb1;
drop database test_udf cascade;

CREATE DATABASE test_udf_re;
CREATE TABLE test_udf_re.r1(e1 varchar);
INSERT INTO test_udf_re.r1 VALUES('1,2,3,4,5');
SELECT calculate_stddev(e1) FROM test_udf_re.r1;
drop table test_udf_re.r1 ;
drop database test_udf_re cascade;

SELECT calculate_circle_area(1.0407460546591726e+6);
SELECT calculate_circle_area(1.0407460546591726e+20);
SELECT calculate_circle_area(null);
SELECT calculate_circle_area(null);

SELECT calculate_circle_area('test');
SELECT calculate_stddev('invalid, data');

DROP FUNCTION calculate_circle_area;
DROP FUNCTION calculate_stddev;
SHOW FUNCTIONS;

--testcase0006 double
CREATE FUNCTION calculate_distance(x1 double, y1 double, x2 double, y2 double)
    RETURNS double
    LANGUAGE LUA
BEGIN
'function calculate_distance(x1, y1, x2, y2)
    local dx = x2 - x1
    local dy = y2 - y1
    return math.sqrt(dx * dx + dy * dy)
end'
END;

CREATE FUNCTION calculate_factorial(n double)
    RETURNS double
    LANGUAGE LUA
BEGIN
'function calculate_factorial(n)
    local result = 1
    n = math.floor(n)
    if n < 0 or n > math.huge then
        return nil
    end
    for i = 2, n do
        result = result * i
        if result > math.huge then
            return result
        end
    end
    return result
end'
END;

SELECT function_name,argument_types,return_type,types_length,function_body,function_type,language,db_name,creator,comments FROM system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION calculate_distance;
SHOW FUNCTION calculate_factorial;

SELECT calculate_distance(0, 0, 1, 1);
SELECT calculate_factorial(5);
SELECT calculate_factorial(5.7);

CREATE TS DATABASE test_udf;
CREATE TABLE test_udf.tb1(ts timestamptz NOT NULL, e1 double,e2 double,e3 double, e4 double) TAGS(t1 int NOT NULL) PRIMARY TAGS(t1);
INSERT INTO test_udf.tb1 VALUES(NOW(), 0, 0, 1, 1, 1);
INSERT INTO test_udf.tb1 VALUES(NOW(), 5, 5, 5, 5, 2);
SELECT calculate_distance(e1, e2, e3, e4) FROM test_udf.tb1 order by e1;
SELECT calculate_factorial(e1) FROM test_udf.tb1 order by e1;
drop table test_udf.tb1;
drop database test_udf cascade;

CREATE DATABASE test_udf_re;
CREATE TABLE test_udf_re.r1(x1 double, y1 double, x2 double, y2 double);
INSERT INTO test_udf_re.r1 VALUES(0, 0, 1, 1);
SELECT calculate_distance(x1, y1, x2, y2) FROM test_udf_re.r1;
SELECT calculate_factorial(x1) FROM test_udf_re.r1;
drop table test_udf_re.r1;
drop database test_udf_re cascade;

SELECT calculate_factorial(1.79769313486231570814527423731704356798071e+308);
SELECT calculate_factorial(1.79769313486231570814527423731704356798071e+309);
SELECT calculate_factorial(170);
SELECT calculate_factorial(171);
SELECT calculate_factorial(null);
SELECT calculate_factorial(null);
SELECT calculate_factorial(null);
SELECT calculate_factorial(null);

SELECT calculate_factorial(-1);
DROP FUNCTION calculate_distance;
DROP FUNCTION calculate_factorial;
SHOW FUNCTIONS;

--testcase0007 char
drop database if exists test_udf_char cascade;
drop database if exists test_udf_re_char cascade;
CREATE FUNCTION check_substring(str char, substr char)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function check_substring(str, substr)
    local start_pos, end_pos = string.find(str, substr)
    if start_pos then
        return start_pos
    else
        return -1
    end
end
'
END;

select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION check_substring;

select check_substring('abcdefg', 'cde');

create ts database test_udf_char;
create table test_udf_char.tb1(ts timestamptz not null,e1 char(10), e2 char(10))tags(tag1 int not null)primary tags(tag1);
insert into test_udf_char.tb1 values('2024-01-01 11:00:00','abc', 'b', 1);
insert into test_udf_char.tb1 values('2024-01-01 12:00:00','def', 'e' ,2);
insert into test_udf_char.tb1 values('2024-01-01 13:00:00','ghi', 'h' ,3);
select check_substring(e1, e2) from test_udf_char.tb1 order by e1;
select count(e1), check_substring(e1, e2) as loc ,last_row(e1) from test_udf_char.tb1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_char.tb1;
drop database test_udf_char cascade;

create  database test_udf_re_char;
create table test_udf_re_char.r1(e1 char(10), e2 char(10));
insert into  test_udf_re_char.r1 values('abc', 'b');
insert into  test_udf_re_char.r1 values('def', 'e');
insert into  test_udf_re_char.r1 values('ghi', 'h');
select check_substring(e1, e2) from test_udf_re_char.r1 order by e1;
select count(e1), check_substring(e1, e2) as loc from test_udf_re_char.r1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_re_char.r1 ;
drop database test_udf_re_char cascade;

select check_substring('abcdef', 'a');
select check_substring('abcdef', 'f');
select check_substring('abcdef', 'g');
select check_substring('abcdef', ' ');
select check_substring('abcdef', '' );
select check_substring('  '    , ' ');
select check_substring('  '    , '' );
select check_substring(''      , '' );
select check_substring(''      , ' ');
select check_substring(null, null);

select check_substring(-1, 1);
select check_substring('test');
select check_substring(123456, 'a');
select check_substring(1.1, '2');
select check_substring(false);

DROP FUNCTION check_substring;
SHOW FUNCTIONS;

--testcase0008 varchar

drop database if exists test_udf_varchar cascade;
drop database if exists test_udf_re_varchar cascade;
CREATE FUNCTION check_substring(str varchar, substr varchar)
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function check_substring(str, substr)
    local start_pos, end_pos = string.find(str, substr)
    if start_pos then
        return start_pos
    else
        return -1
    end
end
'
END;

select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION check_substring;

select check_substring('abcdefg', 'cde');

create ts database test_udf_varchar;
create table test_udf_varchar.tb1(ts timestamptz not null,e1 varchar(10), e2 varchar(10))tags(tag1 int not null)primary tags(tag1);
insert into test_udf_varchar.tb1 values('2024-01-01 11:00:00','abc', 'b', 1);
insert into test_udf_varchar.tb1 values('2024-01-01 12:00:00','def', 'e' ,2);
insert into test_udf_varchar.tb1 values('2024-01-01 13:00:00','ghi', 'h' ,3);
select check_substring(e1, e2) from test_udf_varchar.tb1 order by e1;
select count(e1), check_substring(e1, e2) as loc ,last_row(e1) from test_udf_varchar.tb1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_varchar.tb1;
drop database test_udf_varchar cascade;

create  database test_udf_re_varchar;
create table test_udf_re_varchar.r1(e1 char(10), e2 char(10));
insert into  test_udf_re_varchar.r1 values('abc', 'b');
insert into  test_udf_re_varchar.r1 values('def', 'e');
insert into  test_udf_re_varchar.r1 values('ghi', 'h');
select check_substring(e1, e2) from test_udf_re_varchar.r1 order by e1;
select count(e1), check_substring(e1, e2) as loc from test_udf_re_varchar.r1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_re_varchar.r1 ;
drop database test_udf_re_varchar cascade;

select check_substring('abcdef', 'a');
select check_substring('abcdef', 'f');
select check_substring('abcdef', 'g');
select check_substring('abcdef', ' ');
select check_substring('abcdef', '' );
select check_substring('  '    , ' ');
select check_substring('  '    , '' );
select check_substring(''      , '' );
select check_substring(''      , ' ');

select check_substring(-1, 1);
select check_substring('test');
select check_substring(123456, 'a');
select check_substring(1.1, '2');
select check_substring(false);

DROP FUNCTION check_substring;
SHOW FUNCTIONS;


--testcase0009 nchar

drop database if exists test_udf_nchar cascade;
drop database if exists test_udf_re_nchar cascade;
CREATE FUNCTION check_substring(str nchar(10), substr nchar(10))
    RETURNs int
    LANGUAGE LUA
BEGIN
'
function check_substring(str, substr)
    local start_pos, end_pos = string.find(str, substr)
    if start_pos then
        return start_pos
    else
        return -1
    end
end
'
END;

select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION check_substring;

select check_substring('abcdefg', 'cde');

create ts database test_udf_nchar;
create table test_udf_nchar.tb1(ts timestamptz not null,e1 nchar(10), e2 nchar(10))tags(tag1 int not null)primary tags(tag1);
insert into test_udf_nchar.tb1 values('2024-01-01 11:00:00','abc', 'b', 1);
insert into test_udf_nchar.tb1 values('2024-01-01 12:00:00','def', 'e' ,2);
insert into test_udf_nchar.tb1 values('2024-01-01 13:00:00','ghi', 'h' ,3);
select check_substring(e1, e2) from test_udf_nchar.tb1 order by e1;
select count(e1), check_substring(e1, e2) as loc ,last_row(e1) from test_udf_nchar.tb1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_nchar.tb1;
drop database test_udf_nchar cascade;

create  database test_udf_re_nchar;
create table test_udf_re_nchar.r1(e1 char(10), e2 char(10));
insert into  test_udf_re_nchar.r1 values('abc', 'b');
insert into  test_udf_re_nchar.r1 values('def', 'e');
insert into  test_udf_re_nchar.r1 values('ghi', 'h');
select check_substring(e1, e2) from test_udf_re_nchar.r1 order by e1;
select count(e1), check_substring(e1, e2) as loc from test_udf_re_nchar.r1 group by e1, e2 having check_substring(e1, e2) >= 0 order by e1;
drop table test_udf_re_nchar.r1 ;
drop database test_udf_re_nchar cascade;

select check_substring('abcdef', 'a');
select check_substring('abcdef', 'f');
select check_substring('abcdef', 'g');
select check_substring('abcdef', ' ');
select check_substring('abcdef', '' );
select check_substring('  '    , ' ');
select check_substring('  '    , '' );
select check_substring(''      , '' );
select check_substring(''      , ' ');

select check_substring(-1, 1);
select check_substring('test');
select check_substring(123456, 'a');
select check_substring(1.1, '2');
select check_substring(false);

DROP FUNCTION check_substring;
SHOW FUNCTIONS;


--testcase0010 test creates multiple functions
CREATE FUNCTION m1(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m1(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;

CREATE FUNCTION m2(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m2(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;


CREATE FUNCTION m3(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m3(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;



CREATE FUNCTION m4(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m4(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;



CREATE FUNCTION m5(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m5(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;



CREATE FUNCTION m6(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m6(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;



CREATE FUNCTION m7(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m7(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;


CREATE FUNCTION m8(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m8(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;


CREATE FUNCTION m9(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m9(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;


CREATE FUNCTION m10(nu_int smallint)
    RETURNs smallint
    LANGUAGE LUA
BEGIN
'function m10(nu_int)
    local re = nu_int + nu_int
    return re
end'
END;

select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;

SHOW FUNCTIONS;
SHOW FUNCTION m1;
SHOW FUNCTION m2;
SHOW FUNCTION m3;
SHOW FUNCTION m4;
SHOW FUNCTION m5;
SHOW FUNCTION m6;
SHOW FUNCTION m7;
SHOW FUNCTION m8;
SHOW FUNCTION m9;
SHOW FUNCTION m10;


DROP FUNCTION m1;
DROP FUNCTION m2;
DROP FUNCTION m3;
DROP FUNCTION m4;
DROP FUNCTION m5;
DROP FUNCTION m6;
DROP FUNCTION m7;
DROP FUNCTION m8;
DROP FUNCTION m9;
DROP FUNCTION m10;
select function_name, argument_types, return_type, types_length, function_body, function_type, language, db_name, creator, version, comments from system.user_defined_function;

--testcase0011 test exception
CREATE FUNCTION time_add_hour(timestamp_str )
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;


CREATE FUNCTION time_add_hour(timestamp_str timestamp)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end
END;


--testcase0012 udf type is not unsupported
CREATE FUNCTION time_add_hour(timestamp_str string)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;


CREATE FUNCTION time_add_hour(timestamp_str timestamp)
    RETURNs string
    LANGUAGE LUA
BEGIN
'function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;

--testcase0024 function special character
CREATE FUNCTION singleQuotes(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function singleQuotes(s1)
    s1 = ''''
    return s1
end'
END;


CREATE FUNCTION singleQuotes(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function singleQuotes(s1)
    s1 = ''
    return s1
end'
END;


CREATE FUNCTION singleQuotes(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function singleQuotes(s1)
    s1 = "\"
    return s1
end'
END;



CREATE FUNCTION singleQuotes(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function singleQuotes(s1)
    s1 = "\\"
    return s1
end'
END;



CREATE FUNCTION singleQuotes(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function singleQuotes(s1)
    s1 = \
    return s1
end'
END;
DROP FUNCTION singleQuotes;


--testcase0013 function creation with same name
CREATE FUNCTION time_add_hour(timestamp_str timestamp)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;

CREATE FUNCTION time_add_hour(timestamp_str timestamp)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function time_add_hour(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;
drop function time_add_hour;



--testcase0014 same name as the original function
CREATE FUNCTION max(timestamp_str timestamp)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function max(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;


--testcase0015 the function name is different from the structure name
CREATE FUNCTION test1(timestamp_str timestamp)
    RETURNs timestamp
    LANGUAGE LUA
BEGIN
'function test2(timestamp_str)
    local new_time = timestamp_str + 3600
    return new_time
end'
END;


--testcase0016 udf execute fail
CREATE FUNCTION t1(a char)
    RETURNs char
    LANGUAGE LUA
BEGIN
'function t1(a)
    local c = a * 100000000
    return c
end'
END;

select t1('a');


--testcase0017 params types error
CREATE FUNCTION t1(a int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'function t1(a)
    local c = a * 100000000
    return c
end'
END;
select t1('a');



--testcase0018 params number error
CREATE FUNCTION t1(a int)
    RETURNs int
    LANGUAGE LUA
BEGIN
'function t1(a)
    local c = a * 100000000
    return c
end'
END;
select t1(1,2);
drop function t1;


--testcase0019 drop nonexistent functions
drop function testt;

--testcase0020 drop database support functions
drop function max;
drop function min;
drop function sum;
drop function count;
drop function now;
drop function last_row;
SHOW FUNCTIONS;

--testcase0021 LUA function returns multiple values
CREATE FUNCTION test21(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function test21(s1)
    s1 = "\\"
    return s1, ""
end'
END;

CREATE FUNCTION test21(s1 int)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function test21(s1)
    if s1 == 1 then
      return "",""
    else
      return "123"
    end
    return "123"
end'
END;

CREATE FUNCTION test21(s1 int)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function test21(s1)
    while i <= 5 do
      i = i + 1
      if i == 3 then
         return "123",""
      end
    end
    return "123"
end'
END;

--testcase0022 the number of function body parameters does not match
CREATE FUNCTION test21(s1 char)
    RETURNS char
    LANGUAGE LUA
BEGIN
'function test21()
    s1 = "\\"
    return s1, ""
end'
END;

SHOW FUNCTIONS;