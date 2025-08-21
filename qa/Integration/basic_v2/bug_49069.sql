create ts database test;

CREATE TABLE test.t_data(ts timestamp(6) NOT NULL, value VARCHAR(16)) TAGS (device VARCHAR(16) not NULL, item VARCHAR(16) not NULL,tag1 int, tag2 int) PRIMARY TAGS(device, item);

explain SELECT last(value) AS value, device, item FROM test.t_data WHERE device = 'Device01' GROUP BY device, item;

explain SELECT last(value) AS value, device, item FROM test.t_data WHERE device = 'Device01' and tag1=11 GROUP BY device, item;

drop database test cascade;