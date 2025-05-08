create ts database last_db;
use last_db;
create table t(k_timestamp timestamp not null, x nchar(10), y char, z bool) tags(a int not null) primary tags(a);
insert into t values('2023-07-29 03:11:59.688', 'hello', 'a', true, 1);

-- all queries should throw error
select last(x),pi() from t where x = 'a' group by x order by x;
select last(x),pi() from t where x != 'a' group by x order by x; 
select last(x),pi() from t where x < 'a' group by x order by x; 
select last(x),pi() from t where x <= 'a' group by x order by x; 
select last(x),pi() from t where x > 'a' group by x order by x; 
select last(x),pi() from t where x >= 'a' group by x order by x; 

select last(y),pi() from t where y = 'a' group by y order by y;
select last(y),pi() from t where y != 'a' group by y order by y;
select last(y),pi() from t where y < 'a' group by y order by y;
select last(y),pi() from t where y <= 'a' group by y order by y;
select last(y),pi() from t where y > 'a' group by y order by y;
select last(y),pi() from t where y >= 'a' group by y order by y;
select last(y),pi() from t group by y having y = 'a' order by y;
select last(y),pi() from t group by y having y != 'a' order by y;
select last(y),pi() from t group by y having y < 'a' order by y;
select last(y),pi() from t group by y having y <= 'a' order by y;
select last(y),pi() from t group by y having y > 'a' order by y;
select last(y),pi() from t group by y having y >= 'a' order by y;


select last(z),pi() from t where z = 1.3 group by z order by z;
select last(z),pi() from t where z != 1.3 group by z order by z;
select last(z),pi() from t where z < 1.3 group by z order by z;
select last(z),pi() from t where z <= 1.3 group by z order by z;
select last(z),pi() from t where z > 1.3 group by z order by z;
select last(z),pi() from t where z >= 1.3 group by z order by z;
select last(z),pi() from t group by z having z = 1.3 order by z;
select last(z),pi() from t group by z having z != 1.3 order by z;
select last(z),pi() from t group by z having z < 1.3 order by z;
select last(z),pi() from t group by z having z <= 1.3 order by z;
select last(z),pi() from t group by z having z > 1.3 order by z;
select last(z),pi() from t group by z having z >= 1.3 order by z;


select last(z),pi() from t where z = false group by z order by z;
select last(z),pi() from t where z != false group by z order by z;
select last(z),pi() from t where z < false group by z order by z;
select last(z),pi() from t where z <= false group by z order by z;
select last(z),pi() from t where z > false group by z order by z;
select last(z),pi() from t where z >= false group by z order by z;
select last(z),pi() from t group by z having z = false order by z;
select last(z),pi() from t group by z having z != false order by z;
select last(z),pi() from t group by z having z < false order by z;
select last(z),pi() from t group by z having z <= false order by z;
select last(z),pi() from t group by z having z > false order by z;
select last(z),pi() from t group by z having z >= false order by z;

drop table t;
drop database last_db;
