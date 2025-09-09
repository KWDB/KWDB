DROP DATABASE IF EXISTS ts_db;
CREATE ts DATABASE ts_db;
USE ts_db;
CREATE TABLE ts_db.test(k_timestamp TIMESTAMP not null,ts INT8 ,ts1 INT8) tags (code1 INT2 not null) primary tags (code1);
select ts,ts1 from test group by ts,ts1 having max(ts)-last(ts1) = 0;
select ts,ts1 from test group by ts,ts1 having max(ts)-max(ts1) = 0;

drop table ts_db.test;
drop database ts_db cascade;