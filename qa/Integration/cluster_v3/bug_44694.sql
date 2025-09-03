create ts database test_gc;
ALTER DATABASE test_gc CONFIGURE ZONE USING gc.ttlseconds=10;
create table test_gc.t1(k_timestamp timestamptz not null,e1 int2) tags (ptag smallint not null, attr1 varchar(32),attr2 varchar(32), attr3 varchar(32), attr4 varchar(32), attr5 varchar(32))primary tags(ptag);

alter table test_gc.t1 partition by hashpoint(
    partition  p0 values from (0) to (800),
    partition  p1 values from (800) to (1500),
    partition  p2 values from (1500) to (2000));

ALTER PARTITION p0 OF TABLE test_gc.t1 CONFIGURE ZONE USING
                                lease_preferences = '[[+region=CN-100000-001]]',
                                constraints = '{\"+region=CN-100000-001\":1}',
                                num_replicas=3;
ALTER PARTITION p1 OF TABLE test_gc.t1 CONFIGURE ZONE USING
                                lease_preferences = '[[+region=CN-100000-002]]',
                                constraints = '{\"+region=CN-100000-002\":1}',
                                num_replicas=3;
ALTER PARTITION p2 OF TABLE test_gc.t1 CONFIGURE ZONE USING
                                lease_preferences = '[[+region=CN-100000-003]]',
                                constraints = '{\"+region=CN-100000-003\":1}',
                                num_replicas=3;
insert into test_gc.t1(k_timestamp,e1,ptag, attr1,attr2, attr3, attr4,attr5)values(1672531211001, 1, 1, '-32768', '-2147483648', '-9223372036854775808', '-2.712882', '-3.14159267890796');
select * from test_gc.t1 order by k_timestamp,ptag;
select pg_sleep(20);
show zone configuration for table test_gc.t1;
drop database test_gc cascade;
-- wait-clear: 300s
-- select * from kwdb_internal.node_metrics where name='capacity.tsdb_used';
