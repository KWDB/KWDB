create ts database ts1;
use ts1;

create table t1(ts timestamp not null, a int, b nchar, c nchar(16), d nvarchar, e nvarchar(16), f varbytes, g varbytes(16)) tags (taga int not null) primary tags (taga);
create table defaultdb.t1(a int, b nchar, c nchar(16), d nvarchar, e nvarchar(16), f bytes);

Insert into defaultdb.t1 values(1, 'h', 'hello, world!', 'hello, world!', 'hello, world!', 'hello, world!');
Insert into defaultdb.t1 values(2, '你', '你好世界！', '你好世界！', '你好世界！', '你好世界！');
Insert into defaultdb.t1 values(3, 'こ', 'こんにちは', 'こんにちは', 'こんにちは', 'こんにちは');
Insert into defaultdb.t1 values(4, 'κ', 'καλημέρα', 'καλημέρα', 'καλημέρα', 'καλημέρα');
Insert into defaultdb.t1 values(5, '~', '~!@#$%^&*(', '~!@#$%^&*(', '~!@#$%^&*(', '~!@#$%^&*(');
Insert into defaultdb.t1 values(6, '1', '123456', '123456', '123456', '123456');
Insert into defaultdb.t1 values(7, '1', '1234567891011121', '1234567891011121', '1234567891011121', '1234567891011121');
--Insert into defaultdb.t1 values(8, b'\xcc\xbb', b'\xcc\xbb\xee', b'\xcc\xbb\xee', b'\xcc\xbb\xee');
Insert into defaultdb.t1 values(9, b'\xe4\xb8\xad', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87');
Insert into defaultdb.t1 values(10, '👿', '👿', '👿', '👿', '👿');
Insert into defaultdb.t1 values(11, '\xe4\xb8\xad', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87');
Insert into defaultdb.t1 values(12, b'a', b'abc', b'abc', b'abc', b'abc');


Insert into t1 values(1, 1, 'h', 'hello, world!', 'hello, world!', 'hello, world!', 'hello, world!', 'hello, world!', 1);
Insert into t1 values(2, 2, '你', '你好世界！', '你好世界！', '你好世界！', '你好世界！', '你好世界！', 1);
Insert into t1 values(3, 3, 'こ', 'こんにちは', 'こんにちは', 'こんにちは', 'こんにちは', 'こんにちは', 1);
Insert into t1 values(4, 4, 'κ', 'καλημέρα', 'καλημέρα', 'καλημέρα', 'καλημέρα', 'καλημέρα', 1);
Insert into t1 values(5, 5, '~', '~!@#$%^&*(', '~!@#$%^&*(', '~!@#$%^&*(', '~!@#$%^&*(', '~!@#$%^&*(', 1);
Insert into t1 values(6, 6, '1', '123456', '123456', '123456', '123456', '123456', 1);
Insert into t1 values(7, 7, '1', '1234567891011121', '1234567891011121', '1234567891011121', '1234567891011121', '1234567891011121', 1);
--Insert into t1 values(now(), 8, 'a', b'\xcc\xbb\xee', b'\xcc\xbb\xee', b'\xcc\xbb\xee', 1);
Insert into t1 values(9, 9, b'\xe4\xb8\xad', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', b'\xe4\xb8\xad\xe6\x96\x87', 1);
Insert into t1 values(10, 10, '👿', '👿', '👿', '👿', '👿', '👿', 1);
Insert into t1 values(11, 11, '\xe4\xb8\xad', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', '\xe4\xb8\xad\xe6\x96\x87', 1);
Insert into t1 values(12, 12, b'a', b'abc', b'abc', b'abc', b'abc', b'abc', 1);

select a, b, c, d, e from defaultdb.t1 order by a;
select a, b::bytes,c::bytes,d::bytes,e::bytes,f::bytes from defaultdb.t1 order by a;
select a, b::string,c::string,d::string,e::string,f::string from defaultdb.t1 order by a;
select a, ascii(b), ascii(c), ascii(d), ascii(e) from defaultdb.t1 order by a;
select a, greatest(b), greatest(c), greatest(d), greatest(e), greatest(f) from defaultdb.t1 order by a;
select a, concat(b,'1'), concat(c,'1'), concat(d,'1'), concat(e,'1') from defaultdb.t1 order by a;
select a, concat(b,b), concat(c,c), concat(d,d), concat(e,e) from defaultdb.t1 order by a;
select a, last(b), last(c), last(d), last(e), last(f) from defaultdb.t1 order by a;--relational table does not support last operator
select concat_agg(b::string), concat_agg(c::string), concat_agg(d::string), concat_agg(e::string), concat_agg(f::string) from defaultdb.t1 group by a order by a;
select a, length(b), length(c), length(d), length(e), length(f) from defaultdb.t1 order by a;
select * from defaultdb.t1 where b = 'h';
select * from defaultdb.t1 where c = '中文';
select * from defaultdb.t1 where c = b'\xcc\xbb\xee';
select * from defaultdb.t1 where c = b'\xe4\xb8\xad\xe6\x96\x87';
select * from defaultdb.t1 where b like 'h%' and c like 'h%' and d like 'h%' and e like 'h%';
select * from defaultdb.t1 where c like '中%' and d like '中%' and e like '中%';

select * from t1 order by a;
select a, b::bytes,c::bytes,d::bytes,e::bytes,f::bytes,g::bytes from t1 order by a;
select a, b::string,c::string,d::string,e::string,f::string,g::string from t1 order by a;
select a, ascii(b), ascii(c), ascii(d), ascii(e) from t1 order by a;
select a, greatest(b), greatest(c), greatest(d), greatest(e), greatest(f), greatest(g) from t1 order by a;
select a, concat(b,'1'), concat(c,'1'), concat(d,'1'), concat(e,'1') from t1 order by a;
select a, concat(b,b), concat(c,c), concat(d,d), concat(e,e) from t1 order by a;
select a, last(b), last(c), last(d), last(e), last(f), last(g)from t1 order by a;--relational table does not support last operator
select concat_agg(b::string), concat_agg(c::string), concat_agg(d::string), concat_agg(e::string), concat_agg(f::string), concat_agg(g::string)from t1 group by a order by a;
select a, length(b), length(c), length(d), length(e), length(f), length(g) from t1 order by a;
select * from t1 where b = 'h';
select * from t1 where c = '中文';
select * from t1 where c = b'\xcc\xbb\xee';
select * from t1 where c = b'\xe4\xb8\xad\xe6\x96\x87';
select * from t1 where b like 'h%' and c like 'h%' and d like 'h%' and e like 'h%';
select * from t1 where c like '中%' and d like '中%' and e like '中%';

drop database ts1 cascade;