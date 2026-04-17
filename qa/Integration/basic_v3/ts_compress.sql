set cluster setting ts.compress.stage = 0;
set cluster setting ts.compress.stage = 1;
set cluster setting ts.compress.stage = 2;
set cluster setting ts.compress.stage = 3;
set cluster setting ts.compress.stage = 4;

set cluster setting ts.compress.level = 'low';
set cluster setting ts.compress.level = 'HIGH';
set cluster setting ts.compress.level = 'mediuM';
set cluster setting ts.compress.level = "l";
set cluster setting ts.compress.level = 'H';
set cluster setting ts.compress.level = 'm';
set cluster setting ts.compress.level = 'normal';
set cluster setting ts.compress.level = 'N';


create ts database test_compress;
create table test_compress.t1(ts timestamp not null encode 'simple8B' compress 'lZ4' level 'low', c1 int2 encode SIMPLE8B compress zlib level "medium", c2 int4 encode SIMPLE8b compress "lz4" level HIGH, c3 int8 encode 'disabled' compress 'lZ4' level 'm', c4 float encode chimp compress zstd level l, c5 double encode CHIMP compress snappy level h, c6 bool encode "bit-packing" compress disabled) tags(ptag int not null) primary tags(ptag);
create table test_compress.t11(ts timestamp not null encode 'simple8B' , c1 int2 compress zlib level "medium", c2 int4 encode SIMPLE8b compress "lz4", c3 int8 compress 'lZ4' level 'm' encode 'disabled', c4 float compress lz4 encode chimp , c5 double encode disabled compress disabled, c6 bool) tags(ptag int not null) primary tags(ptag);

create table test_compress.t2(ts timestamp not null encode 'delta' compress 'lZ4' level 'low', c1 int2 compress zlib level medium encode simple8b) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'xz' level 'low', c1 int2 compress zlib level medium encode simple8b) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'n', c1 int2 compress zlib level medium encode simple8b) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'low', c1 int2 compress zlib encode simple8b level medium ) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'low', c1 int2 encode simple8b level medium compress zlib) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'low', c1 int2 level medium encode simple8b compress zlib) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'low', c1 int2 encode simple8b level medium) tags (ptag int not null) primary tags(ptag);
create table test_compress.t2(ts timestamp not null encode 'simple8B' compress 'lz4' level 'low', c1 int2 level medium ) tags (ptag int not null) primary tags(ptag);

set cluster setting ts.compress.stage = 2;
set cluster setting ts.compress.level = 'medium';
drop database test_compress;