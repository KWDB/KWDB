DROP DATABASE IF EXISTS test_delete_ptag CASCADE;
create ts database test_delete_ptag;

CREATE TABLE test_delete_ptag.tb1
(
    k_timestamp TIMESTAMPTZ NOT NULL,
    e1          INT2,
    e2          INT,
    e3          INT8,
    e4          FLOAT4,
    e5          FLOAT,
    e6          BOOL,
    e7          TIMESTAMP,
    e8          CHAR(1023),
    e9          VARCHAR,
    e10         VARCHAR(4096),
    e11         NCHAR(255),
    e12         VARBYTES(5)
) TAGS (code1 INT8 NOT NULL,code2 CHAR(10),code3 VARCHAR(128)) PRIMARY TAGS (code1);

set cluster setting default_transaction_read_only.enabled = true;

INSERT INTO test_delete_ptag.tb1 VALUES (10001, 10001, 1000001, 100000001, 110011.110011, 110011.110011, true, '2010-10-10 10:10:11.101','《test1中文YingYu@!!!》', '《test1中文YingYu@!!!》', '《test1中文YingYu@!!!》', '《test1中文YingYu@!!!》', b'a', 0,'ATEST(1', 'ATEST(2');

INSERT INTO test_delete_ptag.tb1 VALUES (3003000000003, 30003, 3000003, 300000003, 330033.330033, 330033.33033, true, '2230-3-30 10:10:00.3','《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', b'�', -32768,'CTEST(3', 'CTEST(4');

INSERT INTO test_delete_ptag.tb1 VALUES (3003000000003, 30003, 3000003, 300000003, 330033.330033, 330033.33033, true, '2230-3-30 10:10:00.3','《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', '《test3中文YingYu@!!!》', b'�',-9223372036854775808, 'CTEST(3', 'CTEST(4');

set cluster setting default_transaction_read_only.enabled = false;

DROP DATABASE IF EXISTS test_delete_ptag CASCADE;