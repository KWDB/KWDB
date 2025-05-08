create ts database t1;
create table t1.d1(k_timestamp timestamptz not null ,e1 bigint  not null, e2 char(20) not null , e3 timestamp  not null , e4 int not null, e5 smallint not null, e6 float not null, e7 bigint not null, e8 smallint not null, e9 float  not null, e10 double not null )  tags(t1_d1 int not null ) primary tags(t1_d1);
INSERT INTO t1.d1  VALUES (1667590000000, 444444444, 'a', 1667597776000, 98, 1, 499.999, 111111111, 10, 10.10, 0.0001,0);
INSERT INTO t1.d1  VALUES  (1667591000000,111111111, 'b', 1667597777111, 100, 1, 99.999, 2147483648, 100, -2000.2022, -700000.707077,1);
INSERT INTO t1.d1  VALUES   (1667592000000, 222222222, 'c', 1667597778112, 99, 1, 299.999, 111111111, 6351, 10.10, 2147483646.6789,2);
INSERT INTO t1.d1  VALUES (1667592010000, 333333333, 'd', 1667597779000, 98, 1, 55.999, 2222222, 12240, 10.100003, 32766.222,3);
INSERT INTO t1.d1  VALUES (1667592600000, 333333333, 'd', 1667597779000, 98, 1, 20.999, 1234567, 210, 1435345.10, 0.0001,4);

select time_bucket_gapfill(k_timestamp,12345678) as tb,interpolate(avg(e6::int),'null') from t1.d1 group by tb order by tb;

-- error
select time_bucket_gapfill(k_timestamp,12345678) as tb,interpolate(max(e3),'null') from t1.d1 group by tb order by tb;

-- error
select time_bucket_gapfill(k_timestamp,12345678) as tb,interpolate(max(substr(e2, 0, 2)),'null') from t1.d1 group by tb order by tb;


drop database t1 cascade;
