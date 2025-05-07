create ts database first_db;
use first_db;

/* normal time-series table */ 
create table t(k_timestamp timestamp not null, x float, y int, z varchar(32)) tags(a int not null) primary tags (a);
insert into t values('2023-07-29 03:11:59.688', 1.0, 1, 'a', 1);
insert into t values('2023-07-29 03:12:59.688', 2.0, 2, NULL, 1);
insert into t values('2023-07-29 03:15:59.688', 3.0, 1, 'a', 1);
insert into t values('2023-07-29 03:18:59.688', 4.0, 2, 'b', 1);
insert into t values('2023-07-29 03:25:59.688', 5.0, 5, 'e', 1);
insert into t values('2023-07-29 03:35:59.688', 6.0, 6, 'e', 1);
insert into t values('2023-07-29 03:10:59.688', 0.1, NULL, 'b', 1);
insert into t values('2023-07-29 03:26:59.688', NULL, 5, 'e', 1);

/* first */
select first(*) from t;
select firstts(*) from t;
select first_row(*) from t;
select first_row_ts(*) from t;

create table t1(k_timestamp timestamp not null, x float, y int) tags(a int not null, b varchar(10)) primary tags (a);
insert into t1 values('2023-07-29 03:11:59.688', 4.0, 2, 1, 'a');
insert into t1 values('2023-07-29 03:10:59.688', 0.1, NULL, 1, 'a');
insert into t1 values('2023-07-29 03:35:59.688', 6.0, 6, 1, 'b');
select first(x),first(y) from t1 group by b having b = 'a'; 

use defaultdb;
drop database first_db cascade;