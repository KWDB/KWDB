set cluster setting sql.hashrouter.partition_coefficient_num=1;

Create ts database index_tag_test_ts_1;
Create table index_tag_test_ts_1.t1(k_timestamp timestamp not null,e1 int) tags
    (a int not null, b int, c int, d int, e int )primary tags(a) with hash(5);
use index_tag_test_ts_1;
create index index_tag_1 on index_tag_test_ts_1.t1(e);
drop index index_tag_1;
select range_type, database_name, table_name from kwdb_internal.ranges_no_leases where database_name='index_tag_test_ts_1';

drop database index_tag_test_ts_1;
set cluster setting sql.hashrouter.partition_coefficient_num=DEFAULT;
show cluster setting sql.hashrouter.partition_coefficient_num;