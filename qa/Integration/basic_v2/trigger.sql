--1. 基础insert
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);

create trigger trig1 before insert on tt for each row begin insert into tt1 values (new.a); insert into tt2 values (new.b);end;
create trigger trig2 after insert on tt for each row begin delete from tt1 where a=new.a; delete from tt2 where b=new.b;end;
show create trigger trig1 on tt;

insert into tt values (123,456);
select * from tt;
select * from tt1;
select * from tt2;

-- 1. insert and transaction rollback
drop table if exists tt;
create table tt(a int, b int);
create trigger trig1 before insert on tt for each row begin insert into tt1 values (1); insert into tt2 values (123123123123); end;
insert into tt values (123, 456);
select * from tt;
select * from tt1;
select * from tt2;

drop trigger trig1 on tt;
create trigger trig1 before insert on tt for each row begin insert into tt1 values (1); end;
insert into tt values (123123123123,123123123123);
select * from tt;
select * from tt1;

create trigger trig2 after insert on tt for each row begin insert into tt1 values (123123123123);end;
insert into tt values (123, 456);
select * from tt;
select * from tt1;

--2. 基础delete
drop table if exists tt,tt1,tt2;
create table tt(a int primary key, b int);
create index idx1 on tt(b);
create table tt1(a int);
create table tt2(b int);

insert into tt values (66,7);
insert into tt1 values (7);
insert into tt2 values (66);

select * from tt;
select * from tt1;
select * from tt2;

create trigger trig1 before delete on tt for each row begin delete from tt1 where a = old.b; delete from tt2 where b = old.a;end;

create trigger trig2 after delete on tt for each row begin insert into tt1 values (old.a); insert into tt2 values (old.b);end;

delete from tt where a=66;
select * from tt;
select * from tt1;
select * from tt2;

-- 2. delete and transaction rollback
drop trigger trig1 on tt;
drop table if exists tt;
create table tt(a int primary key, b int);
insert into tt values (123, 456);
insert into tt1 values (123);
create trigger trig1 before delete on tt for each row begin delete from tt1 where a=old.a; insert into tt1 values (123123123123); end;
delete from tt where a=123;
select * from tt;
select * from tt1;

drop trigger trig1 on tt;
create trigger trig1 before delete on tt for each row begin delete from tt1 where a=old.a; end;
delete from tt where a='stest';
select * from tt;
select * from tt1;

create trigger trig2 after delete on tt for each row begin insert into tt1 values (123123123123);end;
delete from tt where a=123;
select * from tt;
select * from tt1;

--3. 基础update
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);

create trigger trig1 before update on tt for each row begin insert into tt1 values (new.b); insert into tt2 values (old.b);end;

create trigger trig2 after update on tt for each row begin insert into tt1 values (old.b); insert into tt2 values (old.a);end;

insert into tt values (123,456);

update tt set b=789 where b= 456;

select * from tt; --123,789
select * from tt1; --789,456
select * from tt2; --456,123

-- 3. update and transaction rollback
drop table if exists tt;
create table tt(a int, b int);
create trigger trig1 before update on tt for each row begin insert into tt1 values (123123123123); end;
insert into tt values (123, 456);
update tt set b=567 where a= 123;
select * from tt;
select * from tt1;

drop trigger trig1 on tt;
create trigger trig1 before update on tt for each row begin insert into tt1 values (old.a); end;
update tt set b=123123123123 where a= 123;
select * from tt;
select * from tt1;

create trigger trig2 after update on tt for each row begin insert into tt1 values (123123123123);end;
update tt set b=567 where a= 123;
select * from tt;
select * from tt1;

--4. Insert on conflict, 不会触发触发器
drop table if exists tt,tt1,tt2;
create table tt(a int primary key, b int unique);
create table tt1(a int);
create table tt2(b int);

-- upsert时无冲突，则等同于insert语句
create trigger trig1 before insert on tt for each row begin insert into tt1 values (new.a); insert into tt2 values (new.b);end;

-- upsert时有冲突，则等同于update语句
create trigger trig2 before update on tt for each row begin insert into tt1 values (new.b); insert into tt2 values (old.b);end;

insert into tt values (123,456); -- 触发insert触发器
select * from tt;  -- 123,456
select * from tt1; -- 123
select * from tt2; -- 456

insert into tt values (123,456) on conflict (b) do update set b=789; -- 不触发触发器
select * from tt;  -- 123,789
select * from tt1; -- 123
select * from tt2; -- 456

--5. 基础upsert/insert on conflict， 报错，因为不支持写在create trigger中
drop table if exists tt,tt1,tt2;
create table tt(a int primary key, b int);
create table tt1(a int);
create table tt2(b int);

-- 报错， insert on conflict不支持
create trigger trig1 before insert on tt for each row begin insert into tt1 values (new.a) on conflict(a) do nothing; end;

-- 报错， 语法错误
create trigger trig2 before update on tt for each row begin upsert tt1 values (new.b);end;

upsert into tt values (123,456);
select * from tt;  -- 123,456
select * from tt1; -- 空
select * from tt2; -- 空

upsert into tt values (123,789);
select * from tt;  -- 123,789
select * from tt1; -- 空
select * from tt2; -- 空

--6. 检查trigger之间是否存在循环依赖（从而导致死循环）
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);

create trigger trig1 before insert on tt for each row begin insert into tt1 values (new.a); insert into tt2 values (new.a);end;
create trigger trig1 before insert on tt1 for each row begin insert into tt2 values (new.a);end;

create trigger trig1 before insert on tt2 for each row begin insert into tt values (new.b);end; --create成功
insert into tt values (1,2); --报错，因为tt被循环引用了
--7. 级联更新表
drop table if exists sales;
drop table if exists sales_summary;
CREATE TABLE sales (sale_id INT, amount int);
CREATE TABLE sales_summary (total_sales int, sale_id int);

CREATE TRIGGER update_summary
    AFTER INSERT ON sales
    FOR EACH ROW
BEGIN UPDATE sales_summary SET total_sales = total_sales + NEW.amount, sale_id = sale_id + new.sale_id where true; END;;

-- 测试步骤：
insert into sales_summary values (0,0);
INSERT INTO sales VALUES (1, 200), (2, 300);
SELECT * FROM sales_summary; -- 验证结果为500

--8. Before insert，自动填充字段
-- 测试点：字段自动填充
drop table if exists orders ;
CREATE TABLE orders (
id INT PRIMARY KEY,
amount DECIMAL(10,2),
created_at timestamp);


CREATE TRIGGER set_order_date
    BEFORE INSERT ON orders
    FOR EACH ROW
BEGIN SET NEW.created_at = '2025-07-10 01:30:22'; END;

-- 测试步骤：
INSERT INTO orders (id, amount) VALUES (1, 100.50);
SELECT created_at FROM orders;

--9. Before update, 防止非法修改
-- 测试点：数据完整性保护
drop table if exists employees ;
CREATE TABLE employees (
id INT PRIMARY KEY,
salary DECIMAL(10,2)
);

CREATE TRIGGER prevent_salary_decrease
    BEFORE UPDATE ON employees
    FOR EACH ROW
BEGIN IF NEW.salary < OLD.salary THEN SET new.salary = old.salary; ENDIF; END;


-- 测试步骤：
INSERT INTO employees VALUES (1, 5000);
UPDATE employees SET salary = 4000 WHERE id=1;
select * from employees; --期望是5000

drop table if exists employees ;
CREATE TABLE employees (
id INT PRIMARY KEY,
name STRING,
age INT,
salary DECIMAL(10,2)
);

CREATE TRIGGER prevent_salary_decrease
    BEFORE UPDATE ON employees
    FOR EACH ROW
BEGIN IF NEW.salary < OLD.salary THEN SET new.salary = old.salary; ENDIF; END;


-- 测试步骤：
INSERT INTO employees VALUES (1, 'name1', 99, 5000);
UPDATE employees SET salary = 4000 WHERE id=1;
select * from employees; --期望是5000

--10. After  delete, 同步删除关联数据
-- 测试点：级联数据删除
drop table if exists users;
drop table if exists logs ;
CREATE TABLE users (user_id INT PRIMARY KEY);
CREATE TABLE logs (log_id INT, user_id INT, action VARCHAR(20));


CREATE TRIGGER delete_user_logs
    AFTER DELETE ON users
    FOR EACH ROW
BEGIN DELETE FROM logs WHERE user_id = OLD.user_id; END;;


-- 测试步骤：
INSERT INTO users VALUES (101);
INSERT INTO logs VALUES (1, 101, 'login'), (2, 101, 'logout');
DELETE FROM users WHERE user_id=101;
SELECT COUNT(*) FROM logs; -- 验证结果为0

--11. Before  insert, 空值处理
-- 测试点：空值转换
drop table if exists products;
CREATE TABLE products (
id INT,
name VARCHAR(50) NOT NULL
);

CREATE TRIGGER set_default_name
    BEFORE INSERT ON products
    FOR EACH ROW
BEGIN IF NEW.name IS NULL THEN SET NEW.name = 'Unnamed'; ENDIF; END;


-- 测试步骤：
INSERT INTO products (id) VALUES (100);
SELECT name FROM products; -- 验证结果为'Unnamed'

--12. 触发器执行顺序验证
-- 测试点：执行优先级
drop table if exists audit_log ;
CREATE TABLE audit_log (message VARCHAR(100));
drop table if exists orders;
create table orders (amount int);

-- 触发器1（BEFORE INSERT）

CREATE TRIGGER trig1
    BEFORE INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;


-- 触发器2（AFTER INSERT）
CREATE TRIGGER trig2
    AFTER INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('AFTER INSERT'); END;


-- 测试步骤：
INSERT INTO orders (amount) VALUES (50);
SELECT * FROM audit_log; -- 验证顺序: BEFORE → AFTER

--13. 创建非法执行顺序的触发器
-- 测试点：触发器执行优先级的非法设置
drop table if exists audit_log ;
CREATE TABLE audit_log (message VARCHAR(100));
drop table if exists orders;
create table orders (amount int);

-- 触发器1（BEFORE INSERT）
CREATE TRIGGER trig1
    after INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;


-- 触发器2（AFTER INSERT）
CREATE TRIGGER trig2
    before INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('AFTER INSERT'); END;


-- 触发器11（BEFORE INSERT）
CREATE TRIGGER trig11
    after INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;


-- 触发器22（AFTER INSERT）
CREATE TRIGGER trig22
    before INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO audit_log VALUES ('AFTER INSERT'); END;

select trigger_name,trigger_action_time,trigger_order from [show triggers from orders]; -- trig2的order是1，trig22的order是2， trig1的order是3, trig11的order是4,

-- 报错：Referenced trigger 'trig2' for the given action time does not exist.
CREATE TRIGGER trig3
    after INSERT ON orders
    FOR EACH ROW precedes trig2
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;

-- 报错：Referenced trigger 'trig1' for the given action time does not exist.
CREATE TRIGGER trig4
    before INSERT ON orders
    FOR EACH ROW follows trig1
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;


-- 成功
CREATE TRIGGER trig3
    after INSERT ON orders
    FOR EACH ROW precedes trig1
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;


-- 成功
CREATE TRIGGER trig4
    before INSERT ON orders
    FOR EACH ROW follows trig2
BEGIN INSERT INTO audit_log VALUES ('BEFORE INSERT'); END;

select trigger_name,trigger_action_time,trigger_order from [show triggers from orders]; --顺序为：trig2，trig4，trig22，trig3，trig1，trig11

--14. 递归触发器预防
drop table if exists counter;
-- 测试点：递归检测
CREATE TABLE counter (val INT);
CREATE TRIGGER recursive_trigger
    AFTER UPDATE ON counter
    FOR EACH ROW
BEGIN UPDATE counter SET val = val + 1 where val = 1; END;


-- 测试步骤：
INSERT INTO counter VALUES (1);
UPDATE counter SET val = 2 where val = 1; -- 验证递归深度限制（MySQL默认禁止）

--15. 跨数据库trigger
drop database if exists other_db cascade;
create database other_db;
create table other_db.logs(id int);
drop table if exists orders;
create table orders(id int);
-- 测试点：权限与作用域

CREATE TRIGGER cross_db_trigger
    AFTER INSERT ON orders
    FOR EACH ROW
BEGIN INSERT INTO other_db.logs VALUES (NEW.id); END;

-- 测试步骤：
insert into orders values (123); --根据用户权限，应该成功
select * from other_db.logs; --根据用户权限，结果应为123

--16. OLD/NEW修改
drop table if exists orders;
create table orders(amount float);
-- 测试点：修改插入数据

CREATE TRIGGER modify_data
    BEFORE INSERT ON orders
    FOR EACH ROW
BEGIN SET NEW.amount = NEW.amount * 0.9; END;

-- 测试步骤：
INSERT INTO orders (amount) VALUES (100);
SELECT amount FROM orders; -- 验证结果为90

--17. trigger内部使用自定义变量
drop table if exists orders;
drop table if exists other_table;
create table orders(id int);
create table other_table(id int);
set @udv1 = 123;
-- 测试点：内部使用自定义变量
CREATE TRIGGER dynamic_sql
    AFTER INSERT ON orders
    FOR EACH ROW
BEGIN insert into other_table values (@udv1); END;


-- 测试步骤：
INSERT INTO orders (id) VALUES (75);
select * from other_table; -- 结果为123

--18. precedes/follows
drop table if exists orders;
drop table if exists other;
create table orders(amount float);
create table other(a float);

CREATE TRIGGER test_precedes
    before INSERT ON orders
    FOR EACH ROW
BEGIN set new.amount = new.amount+1; insert into other values (new.amount); END;


-- 报错 not_exist不存在
CREATE TRIGGER test_precedes_1
    BEFORE INSERT ON orders
    FOR EACH ROW PRECEDES not_exist
BEGIN set new.amount = new.amount+1; END;

CREATE TRIGGER test_precedes_1
    before INSERT ON orders
    FOR EACH ROW PRECEDES test_precedes
BEGIN set new.amount = new.amount + 999 ; insert into other values (new.amount); END;

insert into orders values (1);
select * from orders; -- 1001
select * from other; -- 1000 \n 1001

--19. precedes/follows, 测试before和after的new是否通用
drop table if exists orders;
drop table if exists other;
create table orders(amount float);
create table other(a float);

CREATE TRIGGER test_precedes before INSERT ON orders FOR EACH ROW BEGIN set new.amount = new.amount+1;  insert into other values (new.amount);END;


-- 报错 not_exist不存在
CREATE TRIGGER test_precedes_1 BEFORE INSERT ON orders FOR EACH ROW PRECEDES not_exist BEGIN set new.amount = new.amount+1;END;

CREATE TRIGGER test_precedes_1
    after INSERT ON orders
    FOR EACH ROW
BEGIN set new.amount = new.amount + 999; insert into other values (new.amount);END;

insert into orders values (1);
select * from orders; -- 2
select * from other; -- 2 \n 1001

--20. trigger中使用select into(待修改)
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);
set @udv = 1;
insert into tt1 values (666);

create trigger trig1 before insert on tt for each row begin select a into @udv from tt1; insert into tt2 values (@udv + new.b);  end;
insert into tt values (1,111);

select * from tt;
select * from tt2;

--21. trigger中使用new.a与当前表的a列做运算
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);
insert into tt1 values (666);

create trigger trig1 before insert on tt for each row begin insert into tt2 select new.a + a from tt1;  end;

insert into tt values (1,111);

select * from tt; --1,111
select * from tt2; -- 667
drop table if exists tt,tt1,tt2;

--22. 源表删列后，进行before insert trigger触发，包括new/old
drop table if exists tt,tt1,tt2;
create table tt(a int, b int);
create table tt1(a int);
create table tt2(b int);

create trigger trig1 before insert on tt for each row begin if new.b < 0 then insert into tt2 values (new.b); set new.b=0; endif; insert into tt1 values (new.b); end;
insert into tt values (1,-1);
select * from tt; -- 1,0
select * from tt1; -- 0
select * from tt2; -- -1

alter table tt drop column a;
insert into tt values (-123);
select * from tt; -- 0 \n 0
select * from tt1; -- 0 \n 0
select * from tt2; -- -1 \n -123

--23. 源表删列后，进行after update trigger触发，包括new/old
drop table if exists tt,tt1,tt2;
create table tt(a int, b int, c int);
create table tt1(a int, c int);
create table tt2(a int, c int);
insert into tt values (1,2,3);

create trigger trig1 before update on tt for each row begin if new.a < 0 then set new.a=0; insert into tt2 values (new.a, new.c); endif; insert into tt1 values (old.a, old.c); end;

update tt set a=-1, b=6, c=7 where a=1;
select * from tt; -- 0,6,7
select * from tt1; -- 1,3
select * from tt2; -- 0,7

alter table tt drop column b;

update tt set a=999, c=777 where a=0;
select * from tt; -- 999, 777
select * from tt1; -- 1,3 \n 0,7
select * from tt2; -- 0,7

--24. 源表删列后，进行before delete trigger触发，包括new/old
drop table if exists tt,tt1,tt2;
create table tt(a int, b int, c int);
create table tt1(a int, b int);
insert into tt values (1,2,3);
create trigger trig1 before delete on tt for each row begin if old.b > 0 then set old.b = 777; insert into tt1 values (old.a, old.b); endif;end;

delete from tt where a=1;
select * from tt; -- 空
select * from tt1; -- 1, 777

alter table tt drop column c;
insert into tt values (2,3);
delete from tt where a=2;
select * from tt; -- 空
select * from tt1; -- 1,777 \n 2,777

--25. 源表增列后，进行before update trigger触发，包括new/old
drop table if exists tt,tt1,tt2;
create table tt(a int, b int, c int);
create table tt1(a int, c int);
create table tt2(a int, c int);
insert into tt values (1,2,3);

create trigger trig1 before update on tt for each row begin if new.a < 0 then set new.a=0; insert into tt2 values (new.a, new.c); endif; insert into tt1 values (old.a, old.c); end;

update tt set a=-1, b=6, c=7 where a=1;
select * from tt; -- 0,6,7
select * from tt1; -- 1,3
select * from tt2; -- 0,7

alter table tt add column d string;

update tt set a=999, c=777 where a=0;
select * from tt; -- 999, 6, 777, NULL
select * from tt1; -- 1,3 \n 0,7
select * from tt2; -- 0,7

drop table if exists tt,tt1,tt2;
USE defaultdb;
DROP DATABASE IF EXISTS test_trigger_dxy cascade;
CREATE DATABASE test_trigger_dxy;
CREATE TABLE test_trigger_dxy.t1(
tp  TIMESTAMPTZ NOT NULL,
id  INT NOT NULL,
e1  INT2,
e2  INT,
e3  INT8,
e4  FLOAT4,
e5  FLOAT8,
e6  BOOL,
e7  TIMESTAMP,
e8  CHAR(1023),
e9  NCHAR(255),
e10 VARCHAR(4096),
e11 CHAR,
e12 NCHAR,
e13 NVARCHAR(4096),
e14 VARBYTES,
e15 VARCHAR,
e16 NVARCHAR,
e17 BLOB,
e18 CLOB
);
CREATE TABLE test_trigger_dxy.t2(
tp  TIMESTAMPTZ NOT NULL,
id  INT NOT NULL,
e1  INT2,
e2  INT,
e3  INT8,
e4  FLOAT4,
e5  FLOAT8,
e6  BOOL,
e7  TIMESTAMP,
e8  CHAR(1023),
e9  NCHAR(255),
e10 VARCHAR(4096),
e11 CHAR,
e12 NCHAR,
e13 NVARCHAR(4096),
e14 VARBYTES,
e15 VARCHAR,
e16 NVARCHAR,
e17 BLOB,
e18 CLOB
);
CREATE TABLE test_trigger_dxy.t3(
tp  TIMESTAMPTZ NOT NULL,
id  INT NOT NULL,
e1  INT2,
e2  INT,
e3  INT8,
e4  FLOAT4,
e5  FLOAT8,
e6  BOOL,
e7  TIMESTAMP,
e8  CHAR(1023),
e9  NCHAR(255),
e10 VARCHAR(4096),
e11 CHAR,
e12 NCHAR,
e13 NVARCHAR(4096),
e14 VARBYTES,
e15 VARCHAR,
e16 NVARCHAR,
e17 BLOB,
e18 CLOB
);
INSERT INTO test_trigger_dxy.t1 VALUES('1970-01-01 00:00:00+00:00',1 ,0,0,0,0,0,true,'1970-01-01 00:00:00+00:00','','','','','','','','','','','');
select id,tp from test_trigger_dxy.t1;

DROP TRIGGER IF EXISTS trig1 ON test_trigger_dxy.t1;
CREATE TRIGGER trig1
BEFORE DELETE ON test_trigger_dxy.t1
FOR EACH ROW BEGIN INSERT INTO test_trigger_dxy.t2(id,tp) SELECT 11, OLD.tp FROM test_trigger_dxy.t1 ORDER BY id LIMIT 1; END;
delete from test_trigger_dxy.t1 where id=1;
select id,tp from test_trigger_dxy.t1;
select id,tp from test_trigger_dxy.t2;


SET @dxy='0000-1-1 00:00:00';
DROP TRIGGER IF EXISTS trig1 ON test_trigger_dxy.t1;
CREATE trigger trig1
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN UPDATE test_trigger_dxy.t2 SET id=200 WHERE tp='1970-1-1 00:00:00.000' RETURNING tp; END;

CREATE trigger trig1
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN UPDATE test_trigger_dxy.t2 SET id=200 WHERE tp='1970-1-1 00:00:00.000' RETURNING tp::string INTO @dxy; END ;

INSERT INTO test_trigger_dxy.t1 VALUES('2262-05-11 23:47:16+00:00' ,21,-1 ,1 ,-1 ,1.125 ,-2.125 ,false,'2020-01-01 12:00:00+00:00' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'' ,'' ,'' ,'BUG' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'BUG' ,'BUG' );
SELECT @dxy;

DROP TRIGGER IF EXISTS trig1 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig2 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig3 ON test_trigger_dxy.t1;
CREATE trigger trig1
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN INSERT INTO test_trigger_dxy.t2 VALUES('2001-12-09 09:48:12.3+00:00' ,9 , null ,null ,null ,null ,null ,null ,null ,'BEFORE' ,null ,null ,null ,null ,null ,null ,null ,null ,null ,null );END ;

CREATE trigger trig3
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN INSERT INTO test_trigger_dxy.t2 VALUES('2001-12-09 09:48:12.3+00:00' ,7 , null ,null ,null ,null ,null ,null ,null ,'BEFORE' ,null ,null ,null ,null ,null ,null ,null ,null ,null ,null );END ;

CREATE trigger trig2
BEFORE DELETE
ON test_trigger_dxy.t1 FOR EACH ROW
PRECEDES trig1 BEGIN INSERT INTO test_trigger_dxy.t2 VALUES('2001-12-09 09:48:12.3+00:00' ,8 , null ,null ,null ,null ,null ,null ,null ,'BEFORE' ,null ,null ,null ,null ,null ,null ,null ,null ,null ,null );END;

DROP TRIGGER IF EXISTS trig1 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig2 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig3 ON test_trigger_dxy.t1;
CREATE trigger trig2
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN SET NEW.e1=NULL;INSERT INTO test_trigger_dxy.t2(tp,id,e1) VALUES('2970-1-1 00:00:00',NEW.id,NEW.e1);END;

select id,tp,e1 from test_trigger_dxy.t1;
select id,tp,e1 from test_trigger_dxy.t2;
INSERT INTO test_trigger_dxy.t1 VALUES('2262-05-11 23:47:16+00:00' ,21,-1 ,1 ,-1 ,1.125 ,-2.125 ,false,'2020-01-01 12:00:00+00:00' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'' ,'' ,'' ,'BUG' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'BUG' ,'BUG' );
select id,tp,e1 from test_trigger_dxy.t1;
select id,tp,e1 from test_trigger_dxy.t2;

DROP TRIGGER IF EXISTS trig1 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig2 ON test_trigger_dxy.t1;
DROP TRIGGER IF EXISTS trig3 ON test_trigger_dxy.t1;
prepare p3 as CREATE trigger trig1
BEFORE INSERT
ON test_trigger_dxy.t1 FOR EACH ROW BEGIN INSERT INTO test_trigger_dxy.t2 VALUES('2001-12-09 09:48:12.3+00:00',NEW.id, -2,null,null,null,null,null,null ,null,null,null ,null ,null ,null ,null,null,null,null ,null );END ;

execute p3;
show triggers from test_trigger_dxy.t1;
select id,tp,e1 from test_trigger_dxy.t2 order by id;
INSERT INTO test_trigger_dxy.t1 VALUES('2262-05-11 23:47:16+00:00' ,21,-1 ,1 ,-1 ,1.125 ,-2.125 ,false,'2020-01-01 12:00:00+00:00' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'' ,'' ,'' ,'BUG' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'\0\0中文te@@~eng TE./。\0\0\0' ,'BUG' ,'BUG' );
select id,tp,e1 from test_trigger_dxy.t2 order by id;

DROP DATABASE IF EXISTS test_trigger_dxy cascade;

drop database IF EXISTS d3 cascade;
CREATE DATABASE d3;
use d3;
CREATE TABLE d3.t3(
  id  INT NOT NULL,
  ts  timestamp
);
insert into d3.t3 values (1, '2025-01-01 12:00:00'),(2, '2025-01-02 12:00:00');
SET @dxy11 = 5;

CREATE TRIGGER trig_dxy11
    BEFORE UPDATE ON d3.t3
    FOR EACH ROW
    $$ BEGIN
SELECT id INTO @dxy11 FROM d3.t3 WHERE id=1 LIMIT 1;END $$;

UPDATE d3.t3 SET id=200 where ts > '2025-01-02 10:00:00';
select @dxy11;
DROP DATABASE IF EXISTS d3 cascade;

