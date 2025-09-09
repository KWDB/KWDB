drop database if exists d1;
create database d1;
use d1;

CREATE TABLE d1.type(
    id           INT primary key,
    eint2        INT2,
    eint4        INT4,
    eint8        INT8,
    efloat4      FLOAT4,
    efloat8      FLOAT8,
    edecimal     DECIMAL,
    ebool        BOOL,
    ebit         BIT,
    evarbit      VARBIT,
    ebytes       BYTES,
    evarbytes    VARBYTES,
    eblob        BLOB,
    estring      STRING,
    echar        CHAR(100),
    evarchar     VARCHAR,
    ecstring     STRING COLLATE en,
    envarchar    NVARCHAR,
    eclob        CLOB,
    edate        DATE,
    etime        TIME,
    etimestamp   TIMESTAMP,
    etimestamptz TIMESTAMPTZ,
    einterval    INTERVAL,
    einet        INET,
    euuid        UUID,
    ejsonb       JSONB,
    eoid         OID
);
insert into type values
(1,2,4,8,4.4,8.8,1000,true,bit'1',bit'0','bytes','varbytes','blob','string','char','varchar',null,'nvarchar','clob','2025-01-01','00:00:00.123456','2025-01-01 00:00:00.123456','2025-01-01 00:00:00.123456','1h2m','127.0.0.1','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11','{"key": "value"}',10000);

insert into type values
(2,20,40,80,44,88.88,2000,false,bit'101',bit'10','bytes2','varbytes2','blob2','string2','char2','varchar2',null,'nvarchar2','clob2','2025-01-02','00:00:02.123456','2025-01-02 00:00:02.123456','2025-01-02 00:00:02.123456','2h4m','127.0.0.2','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12','{"key1": "value1"}',20000);

select * from type order by id;

create procedure p1() begin select 1; end;
call p1();
drop procedure p1;

create procedure p2() begin select abs(-1); end;
call p2();
drop procedure p2;

create procedure p3() begin select length('abcd'); end;
call p3();
drop procedure p3;

create procedure p4() begin update type set eint2=eint2*10 where id=1; select eint2 from type where id=1; update type set eint2=2 where id=1; select eint2 from type where id=1; end;
call p4();
drop procedure p4;


create procedure p5() begin update type set estring='modified' where id=2; select * from type where id=2; update type set estring='string2' where id=2; end;
call p5();
drop procedure p5;

create procedure p6() begin insert into type select 3,3,3,3,3.3,3.3,3,true,bit'1',bit'0','b','vb','b','s','c','vc',null,'nv','c','2025-01-03','00:00:03','2025-01-03 00:00:03','2025-01-03 00:00:03','3h','127.0.0.3','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13','{"k": "v"}',30000; select * from type where id=3; delete from type where id=3; end;
call p6();
drop procedure p6;

create procedure p7() begin update type set efloat8=efloat8*2 where id=1; select efloat8 from type where id=1; update type set efloat8=8.8 where id=1; end;
call p7();
drop procedure p7;

create procedure p8() begin delete from type where id=2; select count(*) from type; insert into type values (2,20,40,80,44,88.88,2000,false,bit'101',bit'10','bytes2','varbytes2','blob2','string2','char2','varchar2',null,'nvarchar2','clob2','2025-01-02','00:00:02.123456','2025-01-02 00:00:02.123456','2025-01-02 00:00:02.123456','2h4m','127.0.0.2','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12','{"key1": "value1"}',20000); end;
call p8();
drop procedure p8;

create procedure p9() begin update type set edate='2025-01-10' where id=1; select edate from type where id=1; update type set edate='2025-01-01' where id=1; end;
call p9();
drop procedure p9;

create procedure p10() begin update type set eint4=eint4+100 where id=1; select eint4 from type where id=1; update type set eint4=4 where id=1; end;
call p10();
drop procedure p10;

create procedure p11() begin insert into type values (3,3,3,3,3.3,3.3,3,true,bit'1','0','b','vb','b','s','c','vc',null,'nv','c','2025-01-03','00:00:03','2025-01-03 00:00:03','2025-01-03 00:00:03','3h','127.0.0.3','a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13','{"k": "v"}',30000); select * from type where id=3; delete from type where id=3; end;
call p11();
drop procedure p11;

create procedure p12() begin update type set ebool=not ebool where id=2; select ebool from type where id=2; update type set ebool=false where id=2; end;
call p12();
drop procedure p12;

create procedure p13() begin update type set ejsonb='{"new_key": "new_value"}' where id=1; select ejsonb from type where id=1; update type set ejsonb='{"key": "value"}' where id=1; end;
call p13();
drop procedure p13;

create procedure p14() begin update type set etimestamp='2025-02-01 12:00:00' where id=1; select etimestamp from type where id=1; update type set etimestamp='2025-01-01 00:00:00.123456' where id=1; end;
call p14();
drop procedure p14;

create procedure p15() begin update type set envarchar='modified_nvarchar' where id=2; select envarchar from type where id=2; update type set envarchar='nvarchar2' where id=2; end;
call p15();
drop procedure p15;

create procedure p16() begin update type set eoid=eoid+1000 where id=1; select eoid from type where id=1; update type set eoid=10000 where id=1; end;
call p16();
drop procedure p16;

create procedure p17() begin update type set einterval='5h' where id=2; select einterval from type where id=2; update type set einterval='2h4m' where id=2; end;
call p17();
drop procedure p17;

create procedure p18() begin update type set euuid='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a99' where id=1; select euuid from type where id=1; update type set euuid='a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' where id=1; end;
call p18();
drop procedure p18;

create procedure p19() begin update type set echar='modified_chars' where id=2; select echar from type where id=2; update type set echar='char2       ' where id=2; end;
call p19();
drop procedure p19;

create procedure p20() begin update type set echar='modified_chars' where id=2; select echar from type where id=2; update type set echar='char2       ' where id=2; end;
call p20();
drop procedure p20;

CREATE PROCEDURE p21(id INT,value FLOAT8,type INT) BEGIN DECLARE err INT DEFAULT 0;DECLARE orig_value FLOAT8;DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN SET err = 1;ROLLBACK;ENDHANDLER;START TRANSACTION;SELECT efloat8 INTO orig_value FROM type order by id limit 1;IF type = 1 THEN UPDATE type SET efloat8 = efloat8 + value WHERE id = id;ELSIF type = 2 THEN UPDATE type SET efloat8 = efloat8 * value WHERE id = id;ELSE UPDATE type SET estring = CONCAT(estring, '_TEST') WHERE id = id;ENDIF;IF type IN (1, 2) THEN UPDATE type SET efloat8 = orig_value WHERE id = id;ELSE UPDATE type SET estring = SUBSTRING(estring, 1, LENGTH(estring) - 5) WHERE id = id;ENDIF; select * from type order by id; COMMIT;END;
CALL p21(2, 2.5, 2);
drop procedure p21;

CREATE PROCEDURE p22(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 + p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p22(1, 10.5);
DROP PROCEDURE p22;

CREATE PROCEDURE p23(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 * p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p23(2, 2.5);
DROP PROCEDURE p23;

CREATE PROCEDURE p24(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(estring, p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p24(1, '_TEST');
DROP PROCEDURE p24;

CREATE PROCEDURE p25(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p25(1, 2);
DROP PROCEDURE p25;

CREATE PROCEDURE p26(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, p_old_str, p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p26(1, 'old', 'new');
DROP PROCEDURE p26;

CREATE PROCEDURE p27(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold; COMMIT; END;
CALL p27(50);
DROP PROCEDURE p27;

CREATE PROCEDURE p28(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 + p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p28(1, 10.5);
DROP PROCEDURE p28;

CREATE PROCEDURE p29(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 * p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p29(2, 2.5);
DROP PROCEDURE p29;

CREATE PROCEDURE p30(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p30(1, 2);
DROP PROCEDURE p30;

CREATE PROCEDURE p31(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val > p_threshold THEN UPDATE type SET efloat8 = efloat8 + p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 - p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p31(1, 10, 5, 3);
DROP PROCEDURE p31;

CREATE PROCEDURE p32(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, p_old_str, p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p32(1, 'old', 'new');
DROP PROCEDURE p32;

CREATE PROCEDURE p33(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold; COMMIT; END;
CALL p33(50);
DROP PROCEDURE p33;

CREATE PROCEDURE p34(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = GREATEST(orig_val1, orig_val2) WHERE id = p_id1; UPDATE type SET efloat8 = LEAST(orig_val1, orig_val2) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p34(3, 7);
DROP PROCEDURE p34;

CREATE PROCEDURE p35(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(UPPER(SUBSTRING(estring, 1, LENGTH(p_old_str))), LOWER(SUBSTRING(estring, LENGTH(p_old_str)+1))) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p35(1, 'HELLO', 'world');
DROP PROCEDURE p35;

CREATE PROCEDURE p36(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 < p_threshold ORDER BY efloat8 ASC; COMMIT; END;
CALL p36(25);
DROP PROCEDURE p36;

CREATE PROCEDURE p37(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(REVERSE(estring), p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p37(1, '_REV');
DROP PROCEDURE p37;

CREATE PROCEDURE p38(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 + p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p38(1, 10.5);
DROP PROCEDURE p38;

CREATE PROCEDURE p39(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 * p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p39(2, 2.5);
DROP PROCEDURE p39;

CREATE PROCEDURE p40(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(estring, p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p40(1, '_TEST');
DROP PROCEDURE p40;

CREATE PROCEDURE p41(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 + p_value) * 2 WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p41(1, 2.1);
DROP PROCEDURE p41;

CREATE PROCEDURE p42(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 - p_value) / 3 WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p42(2, 6.3);
DROP PROCEDURE p42;

CREATE PROCEDURE p43(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p43(1, 2);
DROP PROCEDURE p43;

CREATE PROCEDURE p44(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val > p_threshold THEN UPDATE type SET efloat8 = efloat8 + p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 - p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p44(1, 10, 5, 3);
DROP PROCEDURE p44;

CREATE PROCEDURE p45(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, p_old_str, p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p45(1, 'old', 'new');
DROP PROCEDURE p45;

CREATE PROCEDURE p46(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold; COMMIT; END;
CALL p46(50);
DROP PROCEDURE p46;

CREATE PROCEDURE p47(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold ORDER BY efloat8 DESC; COMMIT; END;
CALL p47(35);
DROP PROCEDURE p47;

CREATE PROCEDURE p48(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 - p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p48(1, 5.2);
DROP PROCEDURE p48;

CREATE PROCEDURE p49(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 / p_value WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p49(2, 2.0);
DROP PROCEDURE p49;

CREATE PROCEDURE p50(p_id INT, p_prefix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(p_prefix, estring) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p50(1, 'PRE_');
DROP PROCEDURE p50;

CREATE PROCEDURE p51(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, SUBSTRING(estring, LENGTH(p_old_str)), p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p51(1, 'TEST', 'REPLACED');
DROP PROCEDURE p51;

CREATE PROCEDURE p52(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold ORDER BY efloat8 DESC; COMMIT; END;
CALL p52(35);
DROP PROCEDURE p52;

CREATE PROCEDURE p53(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val1 * 2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 * 2 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p53(1, 2);
DROP PROCEDURE p53;

CREATE PROCEDURE p54(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val < p_threshold THEN UPDATE type SET efloat8 = efloat8 + p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 - p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p54(1, 20, 8, 4);
DROP PROCEDURE p54;

CREATE PROCEDURE p55(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, p_old_str, p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p55(1, 'new', 'updated');
DROP PROCEDURE p55;

CREATE PROCEDURE p56(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 < p_threshold; COMMIT; END;
CALL p56(30);
DROP PROCEDURE p56;

CREATE PROCEDURE p57(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val > p_threshold THEN UPDATE type SET efloat8 = efloat8 * p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 / p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p57(1, 25, 1.5, 2);
DROP PROCEDURE p57;

CREATE PROCEDURE p58(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 + (p_value * 2) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p58(1, 3.7);
DROP PROCEDURE p58;

CREATE PROCEDURE p59(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 - (p_value / 2) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p59(2, 4.2);
DROP PROCEDURE p59;

CREATE PROCEDURE p60(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(estring, '_', p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p60(1, 'SUFFIX');
DROP PROCEDURE p60;

CREATE PROCEDURE p61(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 / (p_value + 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p61(2, 0.5);
DROP PROCEDURE p61;

CREATE PROCEDURE p62(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = SQRT(orig_val1) WHERE id = p_id1; UPDATE type SET efloat8 = SQRT(orig_val2) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p62(4, 9);
DROP PROCEDURE p62;

CREATE PROCEDURE p63(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val1 + orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 - orig_val2 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p63(1, 2);
DROP PROCEDURE p63;

CREATE PROCEDURE p64(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val BETWEEN p_threshold/2 AND p_threshold*2 THEN UPDATE type SET efloat8 = efloat8 + p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 - p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p64(1, 15, 6, 3);
DROP PROCEDURE p64;

CREATE PROCEDURE p65(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(p_new_str, SUBSTRING(estring, LENGTH(p_old_str)+1)) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p65(1, 'PRE_', 'POST_');
DROP PROCEDURE p65;

CREATE PROCEDURE p66(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 BETWEEN p_threshold-10 AND p_threshold+10; COMMIT; END;
CALL p66(40);
DROP PROCEDURE p66;

CREATE PROCEDURE p67(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 * (p_value + 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p67(1, 1.2);
DROP PROCEDURE p67;


CREATE PROCEDURE p68(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 * (p_value + 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p68(1, 1.2);
DROP PROCEDURE p68;

CREATE PROCEDURE p69(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = efloat8 / (p_value + 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p69(2, 0.5);
DROP PROCEDURE p69;

CREATE PROCEDURE p70(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(SUBSTRING(estring, 1, LENGTH(estring)/2), p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p70(1, '_HALF');
DROP PROCEDURE p70;

CREATE PROCEDURE p71(p_id INT, p_value BOOL) BEGIN DECLARE orig_val BOOL; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT ebool INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET ebool = IF(orig_val AND p_value, FALSE, TRUE) WHERE id = p_id; SELECT ebool AS result FROM type WHERE id = p_id; UPDATE type SET ebool = orig_val WHERE id = p_id; COMMIT; END;
CALL p71(1, FALSE);
DROP PROCEDURE p71;

CREATE PROCEDURE p72(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 + p_value) / (p_value - 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p72(1, 2.2);
DROP PROCEDURE p72;

CREATE PROCEDURE p73(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = SQRT(orig_val1) WHERE id = p_id1; UPDATE type SET efloat8 = SQRT(orig_val2) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p73(4, 9);
DROP PROCEDURE p73;

CREATE PROCEDURE p74(p_id INT, p_threshold FLOAT8, p_increase FLOAT8, p_decrease FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE new_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val > p_threshold THEN UPDATE type SET efloat8 = efloat8 * p_increase WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 / p_decrease WHERE id = p_id; ENDIF; SELECT efloat8 INTO new_val FROM type WHERE id = p_id; SELECT new_val AS result; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p74(1, 25, 1.5, 2);
DROP PROCEDURE p74;

CREATE PROCEDURE p75(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = REPLACE(estring, SUBSTRING(estring, LENGTH(p_old_str)), p_new_str) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p75(1, 'TEST', 'REPLACED');
DROP PROCEDURE p75;

CREATE PROCEDURE p76(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold ORDER BY efloat8 DESC; COMMIT; END;
CALL p76(35);
DROP PROCEDURE p76;

CREATE PROCEDURE p77(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold ORDER BY efloat8 ASC; COMMIT; END;
CALL p77(40);
DROP PROCEDURE p77;

CREATE PROCEDURE p78(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 + p_value) * 2 WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p78(1, 2.1);
DROP PROCEDURE p78;

CREATE PROCEDURE p79(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 - p_value) / 3 WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p79(2, 6.3);
DROP PROCEDURE p79;

CREATE PROCEDURE p80(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(REVERSE(estring), p_suffix) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p80(1, '_REV');
DROP PROCEDURE p80;

CREATE PROCEDURE p81(p_id INT, p_value BOOL) BEGIN DECLARE orig_val BOOL; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT ebool INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET ebool = IF(p_value, NOT orig_val, orig_val) WHERE id = p_id; SELECT ebool AS result FROM type WHERE id = p_id; UPDATE type SET ebool = orig_val WHERE id = p_id; COMMIT; END;
CALL p81(1, TRUE);
DROP PROCEDURE p81;

CREATE PROCEDURE p82(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(REPLACE(SUBSTRING(estring, 1, LENGTH(p_old_str)), p_old_str, p_new_str), SUBSTRING(estring, LENGTH(p_old_str)+1)) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p82(1, 'OLD', 'NEW');
DROP PROCEDURE p82;

CREATE PROCEDURE p83(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = GREATEST(orig_val1, orig_val2) WHERE id = p_id1; UPDATE type SET efloat8 = LEAST(orig_val1, orig_val2) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p83(3, 7);
DROP PROCEDURE p83;

CREATE PROCEDURE p84(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = POWER(orig_val1, 2) WHERE id = p_id1; UPDATE type SET efloat8 = POWER(orig_val2, 3) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p84(2, 3);
DROP PROCEDURE p84;

CREATE PROCEDURE p85(p_id INT, p_old_str VARCHAR(50), p_new_str VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(UPPER(SUBSTRING(estring, 1, LENGTH(p_old_str))), LOWER(SUBSTRING(estring, LENGTH(p_old_str)+1))) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p85(1, 'HELLO', 'world');
DROP PROCEDURE p85;

CREATE PROCEDURE p86(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 < p_threshold ORDER BY efloat8 ASC; COMMIT; END;
CALL p86(25);
DROP PROCEDURE p86;

CREATE PROCEDURE p87(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(LOWER(SUBSTRING(estring, 1, LENGTH(p_suffix))), UPPER(SUBSTRING(estring, LENGTH(p_suffix)+1))) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p87(1, 'HELLO');
DROP PROCEDURE p87;

CREATE PROCEDURE p88(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 NOT BETWEEN p_threshold-5 AND p_threshold+5; COMMIT; END;
CALL p88(30);
DROP PROCEDURE p88;

CREATE PROCEDURE p89(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 + p_value) * (p_value + 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p89(1, 1.8);
DROP PROCEDURE p89;

CREATE PROCEDURE p90(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 - p_value) / (p_value + 2) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p90(2, 3.5);
DROP PROCEDURE p90;

CREATE PROCEDURE p91(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 NOT BETWEEN p_threshold-5 AND p_threshold+5; COMMIT; END;
CALL p91(30);
DROP PROCEDURE p91;

CREATE PROCEDURE p92(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8 default 0; DECLARE val2 FLOAT8 default 0; DECLARE orig_val1 FLOAT8 default 0; DECLARE orig_val2 FLOAT8 default 0; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val1 + orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 - orig_val2 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p92(2, 3);
DROP PROCEDURE p92;

CREATE PROCEDURE p93(p_threshold FLOAT8) BEGIN DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT id, efloat8 FROM type WHERE efloat8 > p_threshold ORDER BY efloat8 ASC; COMMIT; END;
CALL p93(40);
DROP PROCEDURE p93;

CREATE PROCEDURE p94(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 + p_value) / (p_value - 1) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p94(1, 2.2);
DROP PROCEDURE p94;

CREATE PROCEDURE p95(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 - p_value) * (p_value + 0.5) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p95(2, 4.7);
DROP PROCEDURE p95;

CREATE PROCEDURE p96(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = POWER(orig_val1, 2) WHERE id = p_id1; UPDATE type SET efloat8 = POWER(orig_val2, 3) WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p96(2, 3);
DROP PROCEDURE p96;

CREATE PROCEDURE p97(p_id INT, p_value FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; UPDATE type SET efloat8 = (efloat8 - p_value) / (p_value + 2) WHERE id = p_id; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p97(2, 3.5);
DROP PROCEDURE p97;

CREATE PROCEDURE p98(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(LOWER(SUBSTRING(estring, 1, LENGTH(p_suffix))), UPPER(SUBSTRING(estring, LENGTH(p_suffix)+1))) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p98(1, 'HELLO');
DROP PROCEDURE p98;

CREATE PROCEDURE p99(p_id INT, p_threshold FLOAT8, p_multiplier FLOAT8) BEGIN DECLARE orig_val FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val FROM type WHERE id = p_id LIMIT 1; IF orig_val > p_threshold THEN UPDATE type SET efloat8 = efloat8 * p_multiplier WHERE id = p_id; ELSE UPDATE type SET efloat8 = efloat8 / p_multiplier WHERE id = p_id; ENDIF; SELECT efloat8 AS result FROM type WHERE id = p_id; UPDATE type SET efloat8 = orig_val WHERE id = p_id; COMMIT; END;
CALL p99(1, 25, 1.5);
DROP PROCEDURE p99;

CREATE PROCEDURE p100(p_id INT, p_suffix VARCHAR(50)) BEGIN DECLARE orig_str VARCHAR(50); DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT estring INTO orig_str FROM type WHERE id = p_id LIMIT 1; UPDATE type SET estring = CONCAT(SUBSTRING(estring, 1, LENGTH(estring)/5), p_suffix, SUBSTRING(estring, LENGTH(estring)/5+1)) WHERE id = p_id; SELECT estring AS result FROM type WHERE id = p_id; UPDATE type SET estring = orig_str WHERE id = p_id; COMMIT; END;
CALL p100(1, '_FIFTH');
DROP PROCEDURE p100;

CREATE PROCEDURE p101() BEGIN DECLARE total INT DEFAULT 0; DECLARE counter INT DEFAULT 1; WHILE counter <= 10 DO SET total = total + counter; SET counter = counter + 1; ENDWHILE; SELECT total; END;
CALL p101();
DROP PROCEDURE p101;

CREATE PROCEDURE p102(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE original_value INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO original_value FROM type WHERE id = current_id; IF original_value < 10 THEN UPDATE type SET eint4 = eint4 * 2 WHERE id = current_id; elsif original_value >= 10 AND original_value < 50 THEN UPDATE type SET eint4 = CAST(eint4 / 2 AS INT4) WHERE id = current_id; ELSE UPDATE type SET eint4 = eint4 + 10 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; select * from type order by id; ROLLBACK; END;
CALL p102(2);
DROP PROCEDURE p102;

CREATE PROCEDURE p103(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE str_val STRING; START TRANSACTION; WHILE current_id <= max_id DO SELECT estring INTO str_val FROM type WHERE id = current_id; IF LENGTH(str_val) < 10 THEN UPDATE type SET estring = CONCAT(estring, '_append') WHERE id = current_id; elsif LENGTH(str_val) >= 10 AND LENGTH(str_val) < 20 THEN UPDATE type SET estring = SUBSTRING(estring, 1, 5) WHERE id = current_id; ELSE UPDATE type SET estring = UPPER(estring) WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p103(2);
DROP PROCEDURE IF EXISTS p103;

CREATE PROCEDURE p104(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE date_val timestamp; START TRANSACTION; WHILE current_id <= max_id DO SELECT etimestamp INTO date_val FROM type WHERE id = current_id; IF date_val < '2025-01-10' THEN UPDATE type SET etimestamp = etimestamp + INTERVAL '1 day' WHERE id = current_id; elsif date_val >= '2025-01-10' AND date_val < '2025-01-20' THEN UPDATE type SET etimestamp = etimestamp - INTERVAL '3 days' WHERE id = current_id; ELSE UPDATE type SET edate = CURRENT_DATE WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p104(2);
DROP PROCEDURE IF EXISTS p104;

CREATE PROCEDURE p105(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE count_val INT DEFAULT 0; START TRANSACTION; WHILE current_id <= max_id DO IF max_id < 20 THEN SET count_val = count_val + 1; UPDATE type SET eint8 = eint8 + count_val WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p105(2);
DROP PROCEDURE IF EXISTS p105;

CREATE PROCEDURE p106(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE json_val JSONB; START TRANSACTION; WHILE current_id <= max_id DO SELECT ejsonb INTO json_val FROM type WHERE id = current_id; IF json_val ? 'key' THEN UPDATE type SET ejsonb = jsonb_set(ejsonb, '{new_key}', '"new_value"') WHERE id = current_id; elsif json_val ? 'key1' THEN UPDATE type SET ejsonb = jsonb_set(ejsonb, '{key1}', '"updated_value"') WHERE id = current_id; ELSE UPDATE type SET ejsonb = '{"default": "value"}' WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p106(2);
DROP PROCEDURE IF EXISTS p106;

CREATE PROCEDURE p107(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint2 FROM type WHERE id = current_id) % 2 = 0 THEN UPDATE type SET euuid = gen_random_uuid() WHERE id = current_id; else UPDATE type SET euuid = '00000000-0000-0000-0000-000000000000' WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p107(2);
DROP PROCEDURE IF EXISTS p107;

CREATE PROCEDURE p108(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE int_val INT4; DECLARE str_len INT; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4, LENGTH(estring) INTO int_val, str_len FROM type WHERE id = current_id; IF int_val < 10 AND str_len < 15 THEN UPDATE type SET efloat4 = efloat4 * 1.5 WHERE id = current_id; elsif int_val >= 10 OR str_len >= 15 THEN UPDATE type SET ebool = NOT ebool WHERE id = current_id; else UPDATE type SET eint8 = eint8 + 100 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p108(2);
DROP PROCEDURE IF EXISTS p108;

CREATE PROCEDURE p109(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val > 0 THEN IF val < 50 THEN UPDATE type SET eint4 = val * 2 WHERE id = current_id; elsif val >= 50 AND val < 100 THEN UPDATE type SET eint4 = val / 2 WHERE id = current_id; else UPDATE type SET eint4 = val - 10 WHERE id = current_id; endif; else UPDATE type SET eint4 = 0 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p109(2);
DROP PROCEDURE IF EXISTS p109;

CREATE PROCEDURE p110(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) IN (10, 20, 30) THEN UPDATE type SET evarchar = CONCAT(evarchar, '_matched') WHERE id = current_id; elsif (SELECT eint4 FROM type WHERE id = current_id) BETWEEN 40 AND 60 THEN UPDATE type SET evarchar = SUBSTRING(evarchar, 1, 10) WHERE id = current_id; else UPDATE type SET evarchar = 'default_value' WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p110(2);
DROP PROCEDURE IF EXISTS p110;

CREATE PROCEDURE p111(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE result INT4 DEFAULT 1; START TRANSACTION; WHILE current_id <= max_id DO SET result = result * (SELECT eint4 FROM type WHERE id = current_id); IF result > 1000 THEN UPDATE type SET eint8 = eint8 + result WHERE id = current_id; SET result = 1; elsif result < 100 THEN UPDATE type SET eint8 = eint8 - result WHERE id = current_id; else UPDATE type SET eint8 = eint8 * result WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p111(2);
DROP PROCEDURE IF EXISTS p111;

CREATE PROCEDURE p112(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE ref_val INT; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO ref_val FROM type WHERE id = current_id; IF ref_val > 5 THEN UPDATE type t SET eint8 = eint8 + (SELECT eint2 FROM type WHERE id = ref_val % max_id) WHERE t.id = current_id; elsif ref_val < 5 THEN UPDATE type SET eint8 = eint8 - 100 WHERE id = current_id; else UPDATE type SET eint8 = eint8 * 2 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p112(2);
DROP PROCEDURE IF EXISTS p112;

CREATE PROCEDURE p113(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE int_val INT4; DECLARE str_val VARCHAR; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4, evarchar INTO int_val, str_val FROM type WHERE id = current_id; IF int_val > 10 THEN UPDATE type SET eint4 = int_val * 3, evarchar = CONCAT(str_val, '_triple') WHERE id = current_id; elsif int_val < 5 THEN UPDATE type SET eint4 = CAST(CAST(int_val AS FLOAT8) / 2 AS INT4), evarchar = SUBSTRING(str_val, 1, 5) WHERE id = current_id; ELSE UPDATE type SET eint4 = int_val + 5, evarchar = REPLACE(str_val, 'a', 'x') WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p113(2);
DROP PROCEDURE IF EXISTS p113;

CREATE PROCEDURE p114(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE str_len INT; START TRANSACTION; WHILE current_id <= max_id DO SELECT LENGTH(estring) INTO str_len FROM type WHERE id = current_id; IF str_len > 15 THEN UPDATE type SET estring = LEFT(estring, 10) WHERE id = current_id; elsif str_len < 5 THEN UPDATE type SET estring = REPEAT('x', 8) WHERE id = current_id; ELSE UPDATE type SET estring = CONCAT('pre_', estring, '_post') WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p114(2);
DROP PROCEDURE IF EXISTS p114;

CREATE PROCEDURE p115(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val BETWEEN 1 AND 10 THEN UPDATE type SET eint4 = 100 WHERE id = current_id; elsif val BETWEEN 11 AND 20 THEN UPDATE type SET eint4 = 200 WHERE id = current_id; elsif val BETWEEN 21 AND 30 THEN UPDATE type SET eint4 = 300 WHERE id = current_id; ELSE UPDATE type SET eint4 = 0 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p115(2);
DROP PROCEDURE IF EXISTS p115;

CREATE PROCEDURE p116(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE total INT4 DEFAULT 0; START TRANSACTION; WHILE current_id <= max_id DO SET total = total + (SELECT eint4 FROM type WHERE id = current_id); IF total > 100 THEN UPDATE type SET eint8 = total WHERE id = current_id; SET total = 0; ELSE UPDATE type SET eint8 = total * 2 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p116(2);
DROP PROCEDURE IF EXISTS p116;

CREATE PROCEDURE p117(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE str_val VARCHAR; START TRANSACTION; WHILE current_id <= max_id DO SELECT evarchar INTO str_val FROM type WHERE id = current_id; IF str_val LIKE '%test%' THEN UPDATE type SET evarchar = REPLACE(str_val, 'test', 'demo') WHERE id = current_id; elsif LENGTH(str_val) > 20 THEN UPDATE type SET evarchar = SUBSTRING(str_val, 5, 15) WHERE id = current_id; ELSE UPDATE type SET evarchar = CONCAT('new_', str_val) WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p117(2);
DROP PROCEDURE IF EXISTS p117;

CREATE PROCEDURE p118(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val % 3 = 0 THEN UPDATE type SET eint4 = val >> 1 WHERE id = current_id; elsif val % 3 = 1 THEN UPDATE type SET eint4 = val << 1 WHERE id = current_id; ELSE UPDATE type SET eint4 = (val << 1) + 1 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p118(2);
DROP PROCEDURE IF EXISTS p118;

CREATE PROCEDURE p119(max_id INT, prefix TEXT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val > 10 THEN UPDATE type SET estring = CONCAT(prefix, '_', estring) WHERE id = current_id; ELSE UPDATE type SET estring = CONCAT('default_', estring) WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p119 executed' AS status, max_id AS input_param, prefix AS prefix_used; ROLLBACK; END;
CALL p119(2, 'custom');
DROP PROCEDURE IF EXISTS p119;

CREATE PROCEDURE p120(min_val INT4, max_val INT4, replace_str TEXT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; START TRANSACTION; WHILE current_id <= 10 DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val >= min_val AND val <= max_val THEN UPDATE type SET evarchar = replace_str WHERE id = current_id; ELSE UPDATE type SET evarchar = CONCAT('out_', evarchar) WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p120 executed' AS status, COUNT(*) AS rows_affected FROM type WHERE evarchar = replace_str; ROLLBACK; END;
CALL p120(5, 20, 'in_range');
DROP PROCEDURE IF EXISTS p120;

CREATE PROCEDURE p121(replace_from TEXT, replace_to TEXT, multiplier INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE str_val TEXT; START TRANSACTION; WHILE current_id <= 5 DO SELECT evarchar INTO str_val FROM type WHERE id = current_id; IF str_val LIKE CONCAT('%', replace_from, '%') THEN UPDATE type SET evarchar = REPLACE(evarchar, replace_from, replace_to), eint4 = eint4 * multiplier WHERE id = current_id; ELSE UPDATE type SET eint4 = eint4 + 1 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p121 executed' AS status, SUM(eint4) AS total_eint4 FROM type; ROLLBACK; END;
CALL p121('old', 'new', 3);
DROP PROCEDURE IF EXISTS p121;

CREATE PROCEDURE p122(threshold INT4, str_match TEXT, default_val TEXT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE val INT4; DECLARE str_val TEXT; START TRANSACTION; WHILE current_id <= 3 DO SELECT eint4, evarchar INTO val, str_val FROM type WHERE id = current_id; IF val > threshold AND str_val LIKE CONCAT('%', str_match, '%') THEN UPDATE type SET evarchar = CONCAT('high_', str_val), eint4 = val * 2 WHERE id = current_id; elsif val <= threshold AND NOT (str_val LIKE CONCAT('%', str_match, '%')) THEN UPDATE type SET evarchar = default_val, eint4 = eint4 >> 1 WHERE id = current_id; ELSE UPDATE type SET evarchar = CONCAT('mid_', str_val), eint4 = val + threshold WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p122 executed' AS status, MAX(eint4) AS max_value, MIN(eint4) AS min_value FROM type; ROLLBACK; END;
CALL p122(15, 'test', 'default');
DROP PROCEDURE IF EXISTS p122;

CREATE PROCEDURE p123(start_id INT, end_id INT, op_type TEXT) BEGIN DECLARE current_id INT DEFAULT start_id; DECLARE affected_rows INT DEFAULT 0; START TRANSACTION; WHILE current_id <= end_id DO IF op_type = 'multiply' THEN UPDATE type SET eint4 = eint4 * 2 WHERE id = current_id; elsif op_type = 'divide' THEN UPDATE type SET eint4 = eint4 >> 1 WHERE id = current_id; ELSE UPDATE type SET eint4 = eint4 + 5 WHERE id = current_id; endif; SET affected_rows = affected_rows + ROW_COUNT(); SET current_id = current_id + 1; endwhile; SELECT 'p123 executed' AS status, op_type AS operation, affected_rows AS rows_updated; ROLLBACK; END;
CALL p123(1, 2, 'divide');
DROP PROCEDURE IF EXISTS p123;

CREATE PROCEDURE p124(id INT, multiplier INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val * multiplier WHERE id = id; SELECT 'p124 executed' AS status, id AS target_id, multiplier AS multiplier_used; ROLLBACK; END;
CALL p124(1, 5);
DROP PROCEDURE IF EXISTS p124;

CREATE PROCEDURE p125(id INT, prefix TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = CONCAT(prefix, estring) WHERE id = id; SELECT 'p125 executed' AS status, id AS target_id, prefix AS prefix_added; ROLLBACK; END;
CALL p125(2, 'pre_');
DROP PROCEDURE IF EXISTS p125;

CREATE PROCEDURE p126(min_id INT, max_id INT, threshold INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) > threshold THEN UPDATE type SET eint4 = eint4 - threshold WHERE id = current_id; ELSE UPDATE type SET eint4 = eint4 + threshold WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p126 executed' AS status, COUNT(*) AS rows_updated FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p126(1, 3, 10);
DROP PROCEDURE IF EXISTS p126;

CREATE PROCEDURE p127(id INT, old_str TEXT, new_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, old_str, new_str) WHERE id = id; SELECT 'p127 executed' AS status, id AS target_id, old_str AS replaced_text; ROLLBACK; END;
CALL p127(1, 'test', 'demo');
DROP PROCEDURE IF EXISTS p127;

CREATE PROCEDURE p128(min_id INT, max_id INT) BEGIN DECLARE current_id INT DEFAULT min_id; DECLARE val INT4; START TRANSACTION; WHILE current_id <= max_id DO SELECT eint4 INTO val FROM type WHERE id = current_id; IF val < 10 THEN UPDATE type SET eint4 = 10 WHERE id = current_id; elsif val BETWEEN 10 AND 20 THEN UPDATE type SET eint4 = 20 WHERE id = current_id; ELSE UPDATE type SET eint4 = 30 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p128 executed' AS status, MIN(eint4) AS min_value, MAX(eint4) AS max_value FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p128(1, 5);
DROP PROCEDURE IF EXISTS p128;

CREATE PROCEDURE p129(id INT, shift_val INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val << shift_val WHERE id = id; SELECT 'p129 executed' AS status, id AS target_id, shift_val AS shift_amount, (SELECT eint4 FROM type WHERE id = id) AS new_value; ROLLBACK; END;
CALL p129(1, 2);
DROP PROCEDURE IF EXISTS p129;

CREATE PROCEDURE p130(min_id INT, max_id INT, append_str TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET estring = CONCAT(estring, append_str) WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p130 executed' AS status, COUNT(*) AS rows_affected FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p130(1, 3, '_appended');
DROP PROCEDURE IF EXISTS p130;

CREATE PROCEDURE p131(id INT, divisor INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val >> divisor WHERE id = id; SELECT 'p131 executed' AS status, id AS target_id, divisor AS divisor_used, (SELECT eint4 FROM type WHERE id = id) AS result; ROLLBACK; END;
CALL p131(1, 1);
DROP PROCEDURE IF EXISTS p131;

CREATE PROCEDURE p132(min_id INT, max_id INT, factor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * factor WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p132 executed' AS status, SUM(eint4) AS total_sum FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p132(1, 2, 3);
DROP PROCEDURE IF EXISTS p132;

CREATE PROCEDURE p133(id INT, substr_len INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; UPDATE type SET estring = SUBSTRING(estring, 1, substr_len) WHERE id = id; SELECT 'p133 executed' AS status, id AS target_id, LENGTH(estring) AS new_length FROM type WHERE id = id; ROLLBACK; END;
CALL p133(1, 5);
DROP PROCEDURE IF EXISTS p133;

CREATE PROCEDURE p134(min_id INT, max_id INT, threshold INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) < threshold THEN UPDATE type SET eint4 = threshold WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p134 executed' AS status, COUNT(*) AS rows_updated FROM type WHERE eint4 = threshold AND id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p134(1, 4, 20);
DROP PROCEDURE IF EXISTS p134;

CREATE PROCEDURE p135(id INT, replace_char TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, 'a', replace_char) WHERE id = id; SELECT 'p135 executed' AS status, id AS target_id, (SELECT estring FROM type WHERE id = id) AS updated_string; ROLLBACK; END;
CALL p135(1, 'X');
DROP PROCEDURE IF EXISTS p135;

CREATE PROCEDURE p136(min_id INT, max_id INT, offset_val INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 + offset_val WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p136 executed' AS status, AVG(eint4) AS average_value FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p136(1, 3, 5);
DROP PROCEDURE IF EXISTS p136;

CREATE PROCEDURE p137(id INT, prefix TEXT, suffix TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = CONCAT(prefix, estring, suffix) WHERE id = id; SELECT 'p137 executed' AS status, id AS target_id, (SELECT estring FROM type WHERE id = id) AS full_string; ROLLBACK; END;
CALL p137(1, '[', ']');
DROP PROCEDURE IF EXISTS p137;

CREATE PROCEDURE p138(min_id INT, max_id INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * eint4 WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p138 executed' AS status, MAX(eint4) AS max_squared FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p138(1, 2);
DROP PROCEDURE IF EXISTS p138;

CREATE PROCEDURE p139(id INT, new_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = new_str WHERE id = id; SELECT 'p139 executed' AS status, id AS target_id, new_str AS new_value; ROLLBACK; END;
CALL p139(1, 'new_value');
DROP PROCEDURE IF EXISTS p139;

CREATE PROCEDURE p140(min_id INT, max_id INT, pattern TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT estring FROM type WHERE id = current_id) LIKE CONCAT('%', pattern, '%') THEN UPDATE type SET eint4 = eint4 + 10 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p140 executed' AS status, COUNT(*) AS matches FROM type WHERE estring LIKE CONCAT('%', pattern, '%') AND id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p140(1, 5, 'test');
DROP PROCEDURE IF EXISTS p140;

CREATE PROCEDURE p141(id INT, shift_direction TEXT, shift_amount INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; IF shift_direction = 'left' THEN UPDATE type SET eint4 = val << shift_amount WHERE id = id; ELSE UPDATE type SET eint4 = val >> shift_amount WHERE id = id; endif; SELECT 'p141 executed' AS status, id AS target_id, shift_direction AS direction, shift_amount AS amount; ROLLBACK; END;
CALL p141(1, 'right', 1);
DROP PROCEDURE IF EXISTS p141;

CREATE PROCEDURE p142(min_id INT, max_id INT, step INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 + step WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p142 executed' AS status, SUM(eint4) AS total_after_update FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p142(1, 3, 2);
DROP PROCEDURE IF EXISTS p142;

CREATE PROCEDURE p143(id INT, char_to_replace TEXT, replacement TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, char_to_replace, replacement) WHERE id = id; SELECT 'p143 executed' AS status, id AS target_id, char_to_replace AS replaced, replacement AS replacement; ROLLBACK; END;
CALL p143(1, 'o', '0');
DROP PROCEDURE IF EXISTS p143;

CREATE PROCEDURE p144(id INT, mask INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val & mask WHERE id = id; SELECT 'p144 executed' AS status, id AS target_id, mask AS bitmask_used; ROLLBACK; END;
CALL p144(1, 3);
DROP PROCEDURE IF EXISTS p144;

CREATE PROCEDURE p145(id INT, max_length INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; IF LENGTH(str_val) > max_length THEN UPDATE type SET estring = SUBSTRING(str_val, 1, max_length) WHERE id = id; endif; SELECT 'p145 executed' AS status, id AS target_id, LENGTH(estring) AS new_length FROM type WHERE id = id; ROLLBACK; END;
CALL p145(1, 8);
DROP PROCEDURE IF EXISTS p145;

CREATE PROCEDURE p146(min_id INT, max_id INT, reset_val INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) < 0 THEN UPDATE type SET eint4 = reset_val WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p146 executed' AS status, COUNT(*) AS rows_reset FROM type WHERE eint4 = reset_val AND id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p146(1, 5, 0);
DROP PROCEDURE IF EXISTS p146;

CREATE PROCEDURE p147(id INT, new_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = CONCAT(new_str, estring) WHERE id = id; SELECT 'p147 executed' AS status, id AS target_id, (SELECT estring FROM type WHERE id = id) AS updated_string; ROLLBACK; END;
CALL p147(1, 'prefix_');
DROP PROCEDURE IF EXISTS p147;

CREATE PROCEDURE p148(min_id INT, max_id INT, factor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * factor WHERE eint4 > 0; SET current_id = current_id + 1; endwhile; SELECT 'p148 executed' AS status, SUM(eint4) AS total_product FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p148(1, 3, 2);
DROP PROCEDURE IF EXISTS p148;

CREATE PROCEDURE p149(id INT, char_count INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; IF LENGTH(str_val) < char_count THEN UPDATE type SET estring = LPAD(str_val, char_count, '0') WHERE id = id; endif; SELECT 'p149 executed' AS status, id AS target_id, estring AS padded_string FROM type WHERE id = id; ROLLBACK; END;
CALL p149(1, 10);
DROP PROCEDURE IF EXISTS p149;

CREATE PROCEDURE p150(min_id INT, max_id INT, threshold INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) > threshold THEN UPDATE type SET eint4 = threshold WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p150 executed' AS status, MAX(eint4) AS new_max FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p150(1, 4, 50);
DROP PROCEDURE IF EXISTS p150;

CREATE PROCEDURE p151(id INT, search_str TEXT, replace_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, search_str, replace_str) WHERE id = id AND estring LIKE CONCAT('%', search_str, '%'); SELECT 'p151 executed' AS status, id AS target_id, (SELECT estring FROM type WHERE id = id) AS updated_str FROM type WHERE id = id; ROLLBACK; END;
CALL p151(1, 'old', 'new');
DROP PROCEDURE IF EXISTS p151;

CREATE PROCEDURE p152(min_id INT, max_id INT, add_val INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 + add_val WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p152 executed' AS status, AVG(eint4) AS new_average FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p152(1, 3, 10);
DROP PROCEDURE IF EXISTS p152;

CREATE PROCEDURE p153(id INT, pos INT, len INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; UPDATE type SET estring = SUBSTRING(str_val, pos, len) WHERE id = id; SELECT 'p153 executed' AS status, id AS target_id, estring AS substring FROM type WHERE id = id; ROLLBACK; END;
CALL p153(1, 2, 5);
DROP PROCEDURE IF EXISTS p153;

CREATE PROCEDURE p154(min_id INT, max_id INT, base INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = base WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p154 executed' AS status, COUNT(*) AS rows_updated FROM type WHERE eint4 = base AND id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p154(1, 5, 100);
DROP PROCEDURE IF EXISTS p154;

CREATE PROCEDURE p155(id INT, append_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = CONCAT(estring, append_str) WHERE id = id; SELECT 'p155 executed' AS status, id AS target_id, estring AS new_string FROM type WHERE id = id; ROLLBACK; END;
CALL p155(1, '_suffix');
DROP PROCEDURE IF EXISTS p155;

CREATE PROCEDURE p156(min_id INT, max_id INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * eint4 WHERE eint4 > 0; SET current_id = current_id + 1; endwhile; SELECT 'p156 executed' AS status, SUM(eint4) AS total_squared FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p156(1, 2);
DROP PROCEDURE IF EXISTS p156;

CREATE PROCEDURE p157(id INT, new_val INT) BEGIN START TRANSACTION; UPDATE type SET eint4 = new_val WHERE id = id; SELECT 'p157 executed' AS status, id AS target_id, new_val AS new_value; ROLLBACK; END;
CALL p157(1, 99);
DROP PROCEDURE IF EXISTS p157;

CREATE PROCEDURE p158(min_id INT, max_id INT, pattern TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT estring FROM type WHERE id = current_id) NOT LIKE CONCAT('%', pattern, '%') THEN UPDATE type SET eint4 = eint4 - 1 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p158 executed' AS status, COUNT(*) AS non_matches FROM type WHERE id BETWEEN min_id AND max_id AND estring NOT LIKE CONCAT('%', pattern, '%'); ROLLBACK; END;
CALL p158(1, 4, 'match');
DROP PROCEDURE IF EXISTS p158;

CREATE PROCEDURE p159(id INT, shift_left INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val << shift_left WHERE id = id; SELECT 'p159 executed' AS status, id AS target_id, shift_left AS shift_amount; ROLLBACK; END;
CALL p159(1, 2);
DROP PROCEDURE IF EXISTS p159;

CREATE PROCEDURE p160(min_id INT, max_id INT, step INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 + step WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p160 executed' AS status, SUM(eint4) AS total_increment FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p160(1, 3, 5);
DROP PROCEDURE IF EXISTS p160;

CREATE PROCEDURE p161(id INT, char_to_replace TEXT, replacement TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, char_to_replace, replacement) WHERE id = id; SELECT 'p161 executed' AS status, id AS target_id, char_to_replace AS replaced_char, replacement AS replacement_char; ROLLBACK; END;
CALL p161(1, 'a', '@');
DROP PROCEDURE IF EXISTS p161;

CREATE PROCEDURE p162(min_id INT, max_id INT, factor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * factor WHERE id = current_id AND eint4 > 0; SET current_id = current_id + 1; endwhile; SELECT 'p162 executed' AS status, AVG(eint4) AS average_after_multiply FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p162(1, 3, 3);
DROP PROCEDURE IF EXISTS p162;

CREATE PROCEDURE p163(id INT, insert_pos INT, insert_str TEXT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; UPDATE type SET estring = CONCAT(SUBSTRING(str_val, 1, insert_pos), insert_str, SUBSTRING(str_val, insert_pos + 1)) WHERE id = id; SELECT 'p163 executed' AS status, id AS target_id, estring AS modified_string FROM type WHERE id = id; ROLLBACK; END;
CALL p163(1, 3, 'insert');
DROP PROCEDURE IF EXISTS p163;

CREATE PROCEDURE p164(id INT, shift_val INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; UPDATE type SET eint4 = val >> shift_val WHERE id = id; SELECT 'p164 executed' AS status, id AS target_id, shift_val AS right_shift; ROLLBACK; END;
CALL p164(1, 1);
DROP PROCEDURE IF EXISTS p164;

CREATE PROCEDURE p165(min_id INT, max_id INT, prefix TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET estring = CONCAT(prefix, estring) WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p165 executed' AS status, COUNT(*) AS rows_prefixed FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p165(1, 3, 'new_');
DROP PROCEDURE IF EXISTS p165;

CREATE PROCEDURE p166(id INT, replace_from TEXT, replace_to TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, replace_from, replace_to) WHERE id = id; SELECT 'p166 executed' AS status, id AS target_id, replace_from AS original_text; ROLLBACK; END;
CALL p166(1, 'old', 'new');
DROP PROCEDURE IF EXISTS p166;

CREATE PROCEDURE p167(min_id INT, max_id INT, threshold INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT eint4 FROM type WHERE id = current_id) < threshold THEN UPDATE type SET eint4 = threshold WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p167 executed' AS status, MIN(eint4) AS min_value FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p167(1, 5, 10);
DROP PROCEDURE IF EXISTS p167;

CREATE PROCEDURE p168(id INT, append_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = CONCAT(estring, append_str) WHERE id = id; SELECT 'p168 executed' AS status, id AS target_id, estring AS updated_string FROM type WHERE id = id; ROLLBACK; END;
CALL p168(1, '_appended');
DROP PROCEDURE IF EXISTS p168;

CREATE PROCEDURE p169(min_id INT, max_id INT, multiplier INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * multiplier WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p169 executed' AS status, SUM(eint4) AS total_after_multiply FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p169(1, 3, 2);
DROP PROCEDURE IF EXISTS p169;

CREATE PROCEDURE p170(id INT, substr_start INT, substr_length INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; UPDATE type SET estring = SUBSTRING(str_val, substr_start, substr_length) WHERE id = id; SELECT 'p170 executed' AS status, id AS target_id, estring AS substring FROM type WHERE id = id; ROLLBACK; END;
CALL p170(1, 2, 4);
DROP PROCEDURE IF EXISTS p170;

CREATE PROCEDURE p171(min_id INT, max_id INT, replace_char TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET estring = REPLACE(estring, 'a', replace_char) WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p171 executed' AS status, COUNT(*) AS rows_replaced FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p171(1, 3, 'X');
DROP PROCEDURE IF EXISTS p171;

CREATE PROCEDURE p172(id INT, add_val INT) BEGIN START TRANSACTION; UPDATE type SET eint4 = eint4 + add_val WHERE id = id; SELECT 'p172 executed' AS status, id AS target_id, eint4 AS new_value FROM type WHERE id = id; ROLLBACK; END;
CALL p172(1, 5);
DROP PROCEDURE IF EXISTS p172;

CREATE PROCEDURE p173(min_id INT, max_id INT, pattern TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO IF (SELECT estring FROM type WHERE id = current_id) LIKE CONCAT('%', pattern, '%') THEN UPDATE type SET eint4 = eint4 * 2 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; SELECT 'p173 executed' AS status, COUNT(*) AS matches FROM type WHERE id BETWEEN min_id AND max_id AND estring LIKE CONCAT('%', pattern, '%'); ROLLBACK; END;
CALL p173(1, 5, 'test');
DROP PROCEDURE IF EXISTS p173;

CREATE PROCEDURE p174(id INT, left_pad_char TEXT, total_length INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; UPDATE type SET estring = LPAD(str_val, total_length, left_pad_char) WHERE id = id; SELECT 'p174 executed' AS status, id AS target_id, estring AS padded_string FROM type WHERE id = id; ROLLBACK; END;
CALL p174(1, '0', 10);
DROP PROCEDURE IF EXISTS p174;

CREATE PROCEDURE p175(min_id INT, max_id INT, base_val INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = base_val WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p175 executed' AS status, AVG(eint4) AS average_value FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p175(1, 3, 100);
DROP PROCEDURE IF EXISTS p175;

CREATE PROCEDURE p176(id INT, replace_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = replace_str WHERE id = id; SELECT 'p176 executed' AS status, id AS target_id, replace_str AS new_string; ROLLBACK; END;
CALL p176(1, 'replaced');
DROP PROCEDURE IF EXISTS p176;

CREATE PROCEDURE p177(min_id INT, max_id INT, divisor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 >> divisor WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p177 executed' AS status, SUM(eint4) AS total_after_division FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p177(1, 3, 1);
DROP PROCEDURE IF EXISTS p177;

CREATE PROCEDURE p178(id INT, pattern TEXT) BEGIN START TRANSACTION; IF (SELECT estring FROM type WHERE id = id) LIKE CONCAT('%', pattern, '%') THEN UPDATE type SET eint4 = eint4 + 5 WHERE id = id; endif; SELECT 'p178 executed' AS status, id AS target_id, eint4 AS new_value FROM type WHERE id = id; ROLLBACK; END;
CALL p178(1, 'match');
DROP PROCEDURE IF EXISTS p178;

CREATE PROCEDURE p179(min_id INT, max_id INT, char_to_replace TEXT, replacement TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET estring = REPLACE(estring, char_to_replace, replacement) WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p179 executed' AS status, COUNT(*) AS rows_affected FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p179(1, 3, 'o', '0');
DROP PROCEDURE IF EXISTS p179;

CREATE PROCEDURE p180(id INT, shift_direction TEXT, shift_amount INT) BEGIN DECLARE val INT4; START TRANSACTION; SELECT eint4 INTO val FROM type WHERE id = id; IF shift_direction = 'left' THEN UPDATE type SET eint4 = val << shift_amount WHERE id = id; ELSE UPDATE type SET eint4 = val >> shift_amount WHERE id = id; endif; SELECT 'p180 executed' AS status, id AS target_id, shift_direction AS direction, shift_amount AS amount; ROLLBACK; END;
CALL p180(1, 'right', 2);
DROP PROCEDURE IF EXISTS p180;

CREATE PROCEDURE p181(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE str_val STRING; START TRANSACTION; WHILE current_id <= max_id DO SELECT estring INTO str_val FROM type WHERE id = current_id; IF LENGTH(str_val) < 10 THEN UPDATE type SET estring = CONCAT(estring, '_append') WHERE id = current_id; elsif LENGTH(str_val) >= 10 AND LENGTH(str_val) < 20 THEN UPDATE type SET estring = SUBSTRING(estring, 1, 5) WHERE id = current_id; ELSE UPDATE type SET estring = UPPER(estring) WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p181(2);
DROP PROCEDURE IF EXISTS p181;

CREATE PROCEDURE p182(max_id INT) BEGIN DECLARE current_id INT DEFAULT 1; DECLARE total INT4 DEFAULT 0; START TRANSACTION; WHILE current_id <= max_id DO SET total = total + (SELECT eint4 FROM type WHERE id = current_id); IF total > 100 THEN UPDATE type SET eint8 = total WHERE id = current_id; SET total = 0; ELSE UPDATE type SET eint8 = total * 2 WHERE id = current_id; endif; SET current_id = current_id + 1; endwhile; ROLLBACK; END;
CALL p182(2);
DROP PROCEDURE IF EXISTS p182;

CREATE PROCEDURE p183(id INT, old_str TEXT, new_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, old_str, new_str) WHERE id = id; SELECT 'p127 executed' AS status, id AS target_id, old_str AS replaced_text; ROLLBACK; END;
CALL p183(1, 'test', 'demo');
DROP PROCEDURE IF EXISTS p183;

CREATE PROCEDURE p184(min_id INT, max_id INT, factor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 * factor WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p132 executed' AS status, SUM(eint4) AS total_sum FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p184(1, 2, 3);
DROP PROCEDURE IF EXISTS p184;

CREATE PROCEDURE p185(id INT, char_count INT) BEGIN DECLARE str_val TEXT; START TRANSACTION; SELECT estring INTO str_val FROM type WHERE id = id; IF LENGTH(str_val) < char_count THEN UPDATE type SET estring = LPAD(str_val, char_count, '0') WHERE id = id; endif; SELECT 'p149 executed' AS status, id AS target_id, estring AS padded_string FROM type WHERE id = id; ROLLBACK; END;
CALL p185(1, 10);
DROP PROCEDURE IF EXISTS p185;

CREATE PROCEDURE p186(id INT, search_str TEXT, replace_str TEXT) BEGIN START TRANSACTION; UPDATE type SET estring = REPLACE(estring, search_str, replace_str) WHERE id = id AND estring LIKE CONCAT('%', search_str, '%'); SELECT 'p151 executed' AS status, id AS target_id, (SELECT estring FROM type WHERE id = id) AS updated_str FROM type WHERE id = id; ROLLBACK; END;
CALL p186(1, 'old', 'new');
DROP PROCEDURE IF EXISTS p186;

CREATE PROCEDURE p187(min_id INT, max_id INT, prefix TEXT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET estring = CONCAT(prefix, estring) WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p165 executed' AS status, COUNT(*) AS rows_prefixed FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p187(1, 3, 'new_');
DROP PROCEDURE IF EXISTS p187;

CREATE PROCEDURE p188(min_id INT, max_id INT, divisor INT) BEGIN DECLARE current_id INT DEFAULT min_id; START TRANSACTION; WHILE current_id <= max_id DO UPDATE type SET eint4 = eint4 >> divisor WHERE id = current_id; SET current_id = current_id + 1; endwhile; SELECT 'p177 executed' AS status, SUM(eint4) AS total_after_division FROM type WHERE id BETWEEN min_id AND max_id; ROLLBACK; END;
CALL p188(1, 3, 1);
DROP PROCEDURE IF EXISTS p188;

CREATE PROCEDURE p189() BEGIN declare a int; if 1 between 0 and 2 then set a=1; else set a=2; endif;end;
CALL p189();
DROP PROCEDURE IF EXISTS p189;

CREATE PROCEDURE p190(a INT, b INT) BEGIN SELECT a - b AS difference; END;
CALL p190(10, 3);
DROP PROCEDURE IF EXISTS p190;

CREATE PROCEDURE p191(a INT, b INT) BEGIN SELECT a + b AS sum; END;
CALL p191(5, 3);
DROP PROCEDURE IF EXISTS p191;

CREATE PROCEDURE p192(str TEXT) BEGIN SELECT UPPER(str) AS result; END;
CALL p192('hello');
DROP PROCEDURE IF EXISTS p192;

CREATE PROCEDURE p193(num INT) BEGIN SELECT num * num AS square; END;
CALL p193(4);
DROP PROCEDURE IF EXISTS p193;

CREATE PROCEDURE p194(a INT, b INT) BEGIN IF a > b THEN SELECT a AS max; ELSE SELECT b AS max; ENDIF; END;
CALL p194(7, 9);
DROP PROCEDURE IF EXISTS p194;

CREATE PROCEDURE p195(str TEXT) BEGIN SELECT LENGTH(str) AS length; END;
CALL p195('test');
DROP PROCEDURE IF EXISTS p195;

CREATE PROCEDURE p196(num INT) BEGIN IF num % 2 = 0 THEN SELECT 'Even'; ELSE SELECT 'Odd'; ENDIF; END;
CALL p196(8);
DROP PROCEDURE IF EXISTS p196;

CREATE PROCEDURE p197(str1 TEXT, str2 TEXT) BEGIN SELECT CONCAT(str1, str2) AS combined; END;
CALL p197('Hello', 'World');
DROP PROCEDURE IF EXISTS p197;

CREATE PROCEDURE p198(num INT) BEGIN SELECT num / 2 AS half; END;
CALL p198(10);
DROP PROCEDURE IF EXISTS p198;

CREATE PROCEDURE p199(str TEXT) BEGIN SELECT REVERSE(str) AS reversed; END;
CALL p199('abc');
DROP PROCEDURE IF EXISTS p199;

CREATE PROCEDURE p200(p_id1 INT, p_id2 INT) BEGIN DECLARE val1 FLOAT8; DECLARE val2 FLOAT8; DECLARE orig_val1 FLOAT8; DECLARE orig_val2 FLOAT8; DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; ENDHANDLER; START TRANSACTION; SELECT efloat8 INTO orig_val1 FROM type WHERE id = p_id1 LIMIT 1; SELECT efloat8 INTO orig_val2 FROM type WHERE id = p_id2 LIMIT 1; UPDATE type SET efloat8 = orig_val1 * orig_val2 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val1 / orig_val2 WHERE id = p_id2; SELECT efloat8 AS val1 FROM type WHERE id = p_id1; SELECT efloat8 AS val2 FROM type WHERE id = p_id2; UPDATE type SET efloat8 = orig_val1 WHERE id = p_id1; UPDATE type SET efloat8 = orig_val2 WHERE id = p_id2; COMMIT; END;
CALL p200(2, 3);
DROP PROCEDURE p200;

drop database d1 cascade;