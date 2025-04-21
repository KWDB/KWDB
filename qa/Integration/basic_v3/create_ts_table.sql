create ts database test;
create table test.sjcx01(k_timestamp timestamp not null,A INT4 null,B INT4 not null,C VARCHAR(10) null,D VARCHAR(10) not null)attributes (t1_attribute varchar not null) primary tags(t1_attribute);
insert into test.sjcx01 values (1681111110000,1,1,'A','DFFDARG', 'qqq');
insert into test.sjcx01 values (1681111110001,2,2,'B','DFFDARGD', 'qqq1');
insert into test.sjcx01 values (1681111110002,3,3,'C','A', 'A1');
insert into test.sjcx01 values (1681111110003,4,4,'D','B', 'B2');
insert into test.sjcx01 values (1681111110004,5,5,'E','C', 'C3');
insert into test.sjcx01 values (1681111110005,null,7,null,'D', 'D4');
insert into test.sjcx01 values (1681111110006,8,9,'G','E', 'E5');
insert into test.sjcx01 values (1681111110007,null,11,'H','F', 'F6');
insert into test.sjcx01 values (1681111110008,12,13,null,'G', 'G7');
select * from test.sjcx01 order by k_timestamp;
select k_timestamp, A, B from test.sjcx01 order by k_timestamp;
select A, B from test.sjcx01 order by k_timestamp;
select * from test.sjcx01 where k_timestamp > '2023-04-10 07:00:30+00:00' and k_timestamp <= '2023-04-10 08:00:30+00:00' order by k_timestamp;
select * from test.sjcx01 where k_timestamp > '2023-04-10 07:18:30.002+00:00' and k_timestamp <= '2023-04-10 07:18:30.006+00:00' order by k_timestamp;
select * from test.sjcx01
where k_timestamp >= '2023-04-10 07:18:30.001+00:00' and k_timestamp < '2023-04-10 07:18:30.003+00:00'
   or (k_timestamp > '2023-04-10 07:18:30.005+00:00' and k_timestamp <= '2023-04-10 07:18:30.009+00:00')
order by k_timestamp;
select count(*) from test.sjcx01;
select * from test.sjcx01 where t1_attribute = 'F6';
drop database test cascade;