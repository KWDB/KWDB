use defaultdb;
drop database if EXISTS runba cascade;
drop database if EXISTS runba_tra cascade;

create ts database runba;
use runba;
CREATE TABLE opcdata449600 (
    "time" TIMESTAMPTZ NOT NULL,
    value FLOAT8 NULL
) TAGS (
    adr VARCHAR(20),
    channel VARCHAR(20) NOT NULL,
    companyid INT4 NOT NULL,
    companyname VARCHAR(100),
    datatype VARCHAR(20),
    device VARCHAR(100),
    deviceid VARCHAR(20),
    highmorewarn VARCHAR(20),
    highwarn VARCHAR(20),
    lowmorewarn VARCHAR(20),
    lowwarn VARCHAR(20),
    name VARCHAR(20),
    region VARCHAR(20),
    slaveid VARCHAR(100),
    "tag" VARCHAR(20),
    unit VARCHAR(20) ) PRIMARY TAGS(channel, companyid);

create database runba_tra;
use runba_tra;
CREATE TABLE cd_device_point (
     id INT4 PRIMARY KEY NOT NULL,
     point_name VARCHAR(500) NULL,
     adr VARCHAR(255) NULL,
     device_name VARCHAR(500) NULL,
     device_id INT4 NOT NULL,
     index_id INT4 NULL,
     index_upper_value DECIMAL(15,6) NULL,
     index_lower_value DECIMAL(15,6) NULL,
     company_id INT4 NULL,
     create_time TIMESTAMP NULL,
     update_time TIMESTAMP NULL
);


CREATE TABLE plat_device_info (
    id INT4 PRIMARY KEY NOT NULL,
    devicenumber VARCHAR(255) NOT NULL,
    usecernum VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    areacode VARCHAR(50) NOT NULL,
    townname VARCHAR(255) NOT NULL,
    companyname VARCHAR(255) NOT NULL,
    makecomname VARCHAR(255) NOT NULL,
    usestatus VARCHAR(50) NOT NULL,
    address VARCHAR(255) NOT NULL,
    dangerflag VARCHAR(50) NOT NULL,
    jycomname VARCHAR(50) NOT NULL,
    result VARCHAR(50) NOT NULL,
    nextdate VARCHAR(50) NOT NULL,
    nextselfdate VARCHAR(50) NOT NULL,
    inspusestatus VARCHAR(50) NOT NULL,
    maintaincomname VARCHAR(255) NOT NULL,
    item_pk VARCHAR(255) NOT NULL,
    remark VARCHAR(255) NOT NULL,
    category1 VARCHAR(255) NOT NULL,
    category2 VARCHAR(255) NOT NULL,
    category3 VARCHAR(255) NOT NULL,
    company_id INT4 NOT NULL,
    qianyi_id INT4 NOT NULL,
    device_serial_number VARCHAR(255) NOT NULL,
    device_name_plate VARCHAR(255) NOT NULL,
    device_birth_number VARCHAR(255) NOT NULL,
    check_period INT4 NULL,
    check_period_unit VARCHAR(255) NULL,
    first_check_time VARCHAR(255) NULL,
    self_check_period INT4 NULL,
    self_check_period_unit VARCHAR(255) NULL,
    self_first_check_time VARCHAR(255) NULL
);


CREATE TABLE plat_tijian_item_index (
    id INT4 PRIMARY KEY NOT NULL,
    index_name VARCHAR(255) NOT NULL
);

SELECT
MIN(o.value) AS min_value,
t.index_name as index_name
FROM
runba.opcdata449600 o
JOIN
  runba_tra.cd_device_point d 
    ON 
  o.companyid = d.company_id and o.tag = 'a' 
JOIN
  runba_tra.plat_device_info p 
    ON
  d.device_id = p.id
JOIN
  runba_tra.plat_tijian_item_index t 
    ON 
  d.index_id = t.id           
GROUP BY
t.index_name
;

use defaultdb;
drop database runba cascade;
drop database runba_tra cascade;