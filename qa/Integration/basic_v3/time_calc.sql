DROP DATABASE IF EXISTS ts_db CASCADE;
CREATE TS DATABASE  ts_db;

CREATE TABLE ts_db.st(k_timestamp  TIMESTAMP not null,ts TIMESTAMPTZ, e1 int) tags (code1 INT not null) primary tags(code1);
INSERT INTO ts_db.st values(100000,'2020-2-2 12:00:00.111+08:00',1, 1); -- 1970-01-01 08:01:40
INSERT INTO ts_db.st values(200000,'2020-2-2 11:01:10.222+08:00',1, 1); -- 1970-01-01 08:03:20
INSERT INTO ts_db.st values(300000,'2020-2-2 10:02:20.333+08:00',1, 1); -- 1970-01-01 08:05:00
INSERT INTO ts_db.st values(-400000,'1920-2-2 09:25:43.247+08:00',1, 1); -- 1970-01-01 08:05:00
INSERT INTO ts_db.st values(-500000,'1824-5-22 16:06:44.254+08:00',1, 1); -- 1970-01-01 08:05:00


set time zone 0;
SELECT k_timestamp+'10y',k_timestamp+'12mon',k_timestamp+'4w',k_timestamp+'32d',k_timestamp+'24h',k_timestamp+'60m',k_timestamp+'60s',k_timestamp+'1000ms',k_timestamp+'10y2mon3w5d10h20m12s600ms',k_timestamp+'10y12mon30w5d',k_timestamp+'-10y-12mon-30w-5d',k_timestamp+'10y12mon5d',k_timestamp+'10y12mon12h',k_timestamp+'10y12mon12m',k_timestamp+'10y12mon12s',k_timestamp+'10y12mon12ms',k_timestamp+'10h20m12s',k_timestamp+'10h20m12ms',k_timestamp+'-10h-20m-12ms',k_timestamp+'0m12ms',k_timestamp+'0h12ms',k_timestamp+'10y12ms',k_timestamp+'10y2mon12ms' FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp-'10y',k_timestamp-'12mon',k_timestamp-'4w',k_timestamp-'32d',k_timestamp-'24h',k_timestamp-'60m',k_timestamp-'60s',k_timestamp-'1000ms',k_timestamp-'10y2mon3w5d10h20m12s600ms',k_timestamp-'10y12mon30w5d',k_timestamp-'-10y-12mon-30w-5d',k_timestamp-'10y12mon5d',k_timestamp-'10y12mon12h',k_timestamp-'10y12mon12m',k_timestamp-'10y12mon12s',k_timestamp-'10y12mon12ms',k_timestamp-'10h20m12s',k_timestamp-'10h20m12ms',k_timestamp-'-10h-20m-12ms',k_timestamp-'0m12ms',k_timestamp-'0h12ms',k_timestamp-'10y12ms',k_timestamp-'10y2mon12ms' FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp+10y, k_timestamp+12mon,k_timestamp+4w,k_timestamp+32d,k_timestamp+24h,k_timestamp+60m,k_timestamp+60s,k_timestamp+1000ms FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp-10y, k_timestamp-12mon,k_timestamp-4w,k_timestamp-32d,k_timestamp-24h,k_timestamp-60m,k_timestamp-60s,k_timestamp-1000ms FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp-(TIMESTAMP'2020-1-1 12:12:12.000'-TIMESTAMP'2019-1-1 12:12:12.000') FROM ts_db.st order by k_timestamp;
SELECT k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'10y',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'12mon',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'4w',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'30d',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'24h',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60m',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60s',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'1000ms' FROM ts_db.st order by k_timestamp;
SELECT first(k_timestamp+10y), first(k_timestamp+12mon),first(k_timestamp+4w),first(k_timestamp+32d),first(k_timestamp+24h),first(k_timestamp+60m),first(k_timestamp+60s),first(k_timestamp+1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT max(k_timestamp+10y), max(k_timestamp+12mon),max(k_timestamp+4w),max(k_timestamp+32d),max(k_timestamp+24h),max(k_timestamp+60m),max(k_timestamp+60s),max(k_timestamp+1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT min(k_timestamp-10y), min(k_timestamp-12mon),min(k_timestamp-4w),min(k_timestamp-32d),min(k_timestamp-24h),min(k_timestamp-60m),min(k_timestamp-60s),min(k_timestamp-1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT count(k_timestamp-10y), count(k_timestamp-12mon),count(k_timestamp-4w),count(k_timestamp-32d),count(k_timestamp-24h),count(k_timestamp-60m),count(k_timestamp-60s),count(k_timestamp-1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT max(k_timestamp-TIMESTAMPTZ'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT count(k_timestamp-TIMESTAMP'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;

SELECT ts+'10y',ts+'12mon',ts+'4w',ts+'32d',ts+'24h',ts+'60m',ts+'60s',ts+'1000ms',ts+'10y2mon3w5d10h20m12s600ms',ts+'10y12mon30w5d',ts+'-10y-12mon-30w-5d',ts+'10y12mon5d',ts+'10y12mon12h',ts+'10y12mon12m',ts+'10y12mon12s',ts+'10y12mon12ms',ts+'10h20m12s',ts+'10h20m12ms',ts+'-10h-20m-12ms',ts+'0m12ms',ts+'0h12ms',ts+'10y12ms',ts+'10y2mon12ms' FROM ts_db.st ORDER BY ts;
SELECT ts-'10y',ts-'12mon',ts-'4w',ts-'32d',ts-'24h',ts-'60m',ts-'60s',ts-'1000ms',ts-'10y2mon3w5d10h20m12s600ms',ts-'10y12mon30w5d',ts-'-10y-12mon-30w-5d',ts-'10y12mon5d',ts-'10y12mon12h',ts-'10y12mon12m',ts-'10y12mon12s',ts-'10y12mon12ms',ts-'10h20m12s',ts-'10h20m12ms',ts-'-10h-20m-12ms',ts-'0m12ms',ts-'0h12ms',ts-'10y12ms',ts-'10y2mon12ms' FROM ts_db.st ORDER BY ts;
SELECT ts+10y, ts+12mon,ts+4w,ts+32d,ts+24h,ts+60m,ts+60s,ts+1000ms FROM ts_db.st ORDER BY ts;
SELECT ts-10y, ts-12mon,ts-4w,ts-32d,ts-24h,ts-60m,ts-60s,ts-1000ms FROM ts_db.st ORDER BY ts;
SELECT ts-(TIMESTAMP'2020-1-1 12:12:12.000'-TIMESTAMP'2019-1-1 12:12:12.000') FROM ts_db.st order by ts;
SELECT ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'10y',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'12mon',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'4w',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'30d',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'24h',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60m',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60s',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'1000ms' FROM ts_db.st order by ts;
SELECT first(ts+10y), first(ts+12mon),first(ts+4w),first(ts+32d),first(ts+24h),first(ts+60m),first(ts+60s),first(ts+1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT max(ts+10y), max(ts+12mon),max(ts+4w),max(ts+32d),max(ts+24h),max(ts+60m),max(ts+60s),max(ts+1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT min(ts-10y), min(ts-12mon),min(ts-4w),min(ts-32d),min(ts-24h),min(ts-60m),min(ts-60s),min(ts-1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT count(ts-10y), count(ts-12mon),count(ts-4w),count(ts-32d),count(ts-24h),count(ts-60m),count(ts-60s),count(ts-1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT max(ts-TIMESTAMPTZ'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT count(ts-TIMESTAMP'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY ts ORDER BY ts;
-- bug ZDP-33483
SELECT ts - k_timestamp FROM ts_db.st WHERE ts - k_timestamp BETWEEN '-60y' AND '60y' ORDER BY k_timestamp;
-- bug ZDP-33477, bug ZDP-33097
SELECT (TIMESTAMP'1970-1-1 12:12:12.121'+99999y) FROM ts_db.st ORDER BY k_timestamp;
SELECT (TIMESTAMP'1970-1-1 12:12:12.121'+'20y')::TIMESTAMP FROM ts_db.st ORDER BY k_timestamp;
SELECT (TIMESTAMP'1970-1-1 12:12:12.121'-INTERVAL'99999years')::TIMESTAMP FROM ts_db.st ORDER BY k_timestamp;
-- bug ZDP-33480
SELECT max(TIMESTAMP'1970-1-1 12:12:12.121'+'20y'),max(TIMESTAMP'1970-1-1 12:12:12.121'+'12mon'),max(TIMESTAMP'1970-1-1 12:12:12.121'+'8w') FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;

set time zone 8;
SELECT (k_timestamp+'10y',k_timestamp+'12mon',k_timestamp+'4w',k_timestamp+'32d',k_timestamp+'24h',k_timestamp+'60m',k_timestamp+'60s',k_timestamp+'1000ms',k_timestamp+'10y2mon3w5d10h20m12s600ms',k_timestamp+'10y12mon30w5d',k_timestamp+'-10y-12mon-30w-5d',k_timestamp+'10y12mon5d',k_timestamp+'10y12mon12h',k_timestamp+'10y12mon12m',k_timestamp+'10y12mon12s',k_timestamp+'10y12mon12ms',k_timestamp+'10h20m12s',k_timestamp+'10h20m12ms',k_timestamp+'-10h-20m-12ms',k_timestamp+'0m12ms',k_timestamp+'0h12ms',k_timestamp+'10y12ms',k_timestamp+'10y2mon12ms' FROM ts_db.st ORDER BY k_timestamp;
SELECT (k_timestamp-'10y',k_timestamp-'12mon',k_timestamp-'4w',k_timestamp-'32d',k_timestamp-'24h',k_timestamp-'60m',k_timestamp-'60s',k_timestamp-'1000ms',k_timestamp-'10y2mon3w5d10h20m12s600ms',k_timestamp-'10y12mon30w5d',k_timestamp-'-10y-12mon-30w-5d',k_timestamp-'10y12mon5d',k_timestamp-'10y12mon12h',k_timestamp-'10y12mon12m',k_timestamp-'10y12mon12s',k_timestamp-'10y12mon12ms',k_timestamp-'10h20m12s',k_timestamp-'10h20m12ms',k_timestamp-'-10h-20m-12ms',k_timestamp-'0m12ms',k_timestamp-'0h12ms',k_timestamp-'10y12ms',k_timestamp-'10y2mon12ms' FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp+10y, k_timestamp+12mon,k_timestamp+4w,k_timestamp+32d,k_timestamp+24h,k_timestamp+60m,k_timestamp+60s,k_timestamp+1000ms FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp-10y, k_timestamp-12mon,k_timestamp-4w,k_timestamp-32d,k_timestamp-24h,k_timestamp-60m,k_timestamp-60s,k_timestamp-1000ms FROM ts_db.st ORDER BY k_timestamp;
SELECT k_timestamp-(TIMESTAMP'2020-1-1 12:12:12.000'-TIMESTAMP'2019-1-1 12:12:12.000') FROM ts_db.st order by k_timestamp;
SELECT k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'10y',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'12mon',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'4w',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'30d',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'24h',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60m',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60s',k_timestamp-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'1000ms' FROM ts_db.st order by k_timestamp;
SELECT first(k_timestamp+10y), first(k_timestamp+12mon),first(k_timestamp+4w),first(k_timestamp+32d),first(k_timestamp+24h),first(k_timestamp+60m),first(k_timestamp+60s),first(k_timestamp+1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT max(k_timestamp+10y), max(k_timestamp+12mon),max(k_timestamp+4w),max(k_timestamp+32d),max(k_timestamp+24h),max(k_timestamp+60m),max(k_timestamp+60s),max(k_timestamp+1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT min(k_timestamp-10y), min(k_timestamp-12mon),min(k_timestamp-4w),min(k_timestamp-32d),min(k_timestamp-24h),min(k_timestamp-60m),min(k_timestamp-60s),min(k_timestamp-1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT count(k_timestamp-10y), count(k_timestamp-12mon),count(k_timestamp-4w),count(k_timestamp-32d),count(k_timestamp-24h),count(k_timestamp-60m),count(k_timestamp-60s),count(k_timestamp-1000ms) FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT max(k_timestamp-TIMESTAMPTZ'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;
SELECT count(k_timestamp-TIMESTAMP'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY k_timestamp ORDER BY k_timestamp;

SELECT ts+'10y',ts+'12mon',ts+'4w',ts+'32d',ts+'24h',ts+'60m',ts+'60s',ts+'1000ms',ts+'10y2mon3w5d10h20m12s600ms',ts+'10y12mon30w5d',ts+'-10y-12mon-30w-5d',ts+'10y12mon5d',ts+'10y12mon12h',ts+'10y12mon12m',ts+'10y12mon12s',ts+'10y12mon12ms',ts+'10h20m12s',ts+'10h20m12ms',ts+'-10h-20m-12ms',ts+'0m12ms',ts+'0h12ms',ts+'10y12ms',ts+'10y2mon12ms' FROM ts_db.st ORDER BY ts;
SELECT ts-'10y',ts-'12mon',ts-'4w',ts-'32d',ts-'24h',ts-'60m',ts-'60s',ts-'1000ms',ts-'10y2mon3w5d10h20m12s600ms',ts-'10y12mon30w5d',ts-'-10y-12mon-30w-5d',ts-'10y12mon5d',ts-'10y12mon12h',ts-'10y12mon12m',ts-'10y12mon12s',ts-'10y12mon12ms',ts-'10h20m12s',ts-'10h20m12ms',ts-'-10h-20m-12ms',ts-'0m12ms',ts-'0h12ms',ts-'10y12ms',ts-'10y2mon12ms' FROM ts_db.st ORDER BY ts;
SELECT ts+10y, ts+12mon,ts+4w,ts+32d,ts+24h,ts+60m,ts+60s,ts+1000ms FROM ts_db.st ORDER BY ts;
SELECT ts-10y, ts-12mon,ts-4w,ts-32d,ts-24h,ts-60m,ts-60s,ts-1000ms FROM ts_db.st ORDER BY ts;
SELECT ts-(TIMESTAMP'2020-1-1 12:12:12.000'-TIMESTAMP'2019-1-1 12:12:12.000') FROM ts_db.st order by ts;
SELECT ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'10y',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'12mon',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'4w',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'30d',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'24h',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60m',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'60s',ts-TIMESTAMP'2020-1-1 12:00:12.000'-INTERVAL'1000ms' FROM ts_db.st order by ts;
SELECT first(ts+10y), first(ts+12mon),first(ts+4w),first(ts+32d),first(ts+24h),first(ts+60m),first(ts+60s),first(ts+1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT max(ts+10y), max(ts+12mon),max(ts+4w),max(ts+32d),max(ts+24h),max(ts+60m),max(ts+60s),max(ts+1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT min(ts-10y), min(ts-12mon),min(ts-4w),min(ts-32d),min(ts-24h),min(ts-60m),min(ts-60s),min(ts-1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT count(ts-10y), count(ts-12mon),count(ts-4w),count(ts-32d),count(ts-24h),count(ts-60m),count(ts-60s),count(ts-1000ms) FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT max(ts-TIMESTAMPTZ'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY ts ORDER BY ts;
SELECT count(ts-TIMESTAMP'1970-1-1 01:00:12.000-05:00') FROM ts_db.st GROUP BY ts ORDER BY ts;

DROP DATABASE ts_db;

SELECT timestamp'2970-01-01 00:00:00' - timestamp'2000-01-01 00:00:00' ;
SELECT '0000-01-01 00:00:00+00:00'::timestamptz - '2020-01-01 12:00:00+00:00'::timestamptz;

--- ZDP-41183
set time zone -15;
set time zone 'a';
set time zone 15;
set time zone -13;
set time zone -1.5;

set time zone -12;
set time zone 14;
set time zone 0;
