drop database if exists test cascade;
create database test;
CREATE TABLE ent_info (
    ent_code VARCHAR(50) NULL,
    ent_name VARCHAR(300) NULL,
    legal_name VARCHAR(300) NULL,
    address VARCHAR(500) NULL,
    person_to_contact VARCHAR(50) NULL,
    email VARCHAR(100) NULL,
    company_nickname VARCHAR(300) NULL,
    company_type VARCHAR(10) NULL,
    regester_money DECIMAL(22,6) NULL,
    regester_date DATE NULL,
    phone VARCHAR(20) NULL,
    remark STRING NULL,
    update_date DATE NULL,
    ent_id INT8 NOT NULL,
    regester_money_unit VARCHAR(50) NULL,
    regester_money_unit_name VARCHAR(200) NULL,
    region_code VARCHAR(6) NULL,
    region_name VARCHAR(50) NULL,
    industry_code VARCHAR(20) NULL,
    industry_name VARCHAR(50) NULL,
    produce_address VARCHAR(500) NULL,
    zip_code VARCHAR(100) NULL,
    technology_process STRING NULL,
    city_code VARCHAR(100) NULL,
    city_name VARCHAR(100) NULL,
    org_file VARCHAR(500) NULL,
    plane_file VARCHAR(500) NULL,
    technology_process_file VARCHAR(500) NULL,
    longitude INT8 NULL,
    latitude INT8 NULL,
    license_file VARCHAR(200) NULL,
    industry_type VARCHAR(100) NULL,
    CONSTRAINT "primary" PRIMARY KEY (ent_id ASC),
    FAMILY "primary" (ent_code, ent_name, legal_name, address, person_to_contact, email, company_nickname, company_type, regester_money, regester_date, phone, remark, update_date, ent_id, regester_money_unit, regester_money_unit_name, region_code, region_name, industry_code, industry_name, produce_address, zip_code, technology_process, city_code, city_name, org_file, plane_file, technology_process_file, longitude, latitude, license_file, industry_type));

CREATE TABLE electric_indicator_day (
    create_date TIMESTAMPTZ NOT NULL,
    dimension VARCHAR(16) NULL,
    id VARCHAR(50) NOT NULL,
    ent_id VARCHAR(50) NULL,
    group_id VARCHAR(50) NULL,
    dl01 FLOAT8 NULL,
    dl02 FLOAT8 NULL,
    dl03 FLOAT8 NULL,
    dl04 FLOAT8 NULL,
    dl05 FLOAT8 NULL,
    dl06 FLOAT8 NULL,
    dl07 FLOAT8 NULL,
    dl08 FLOAT8 NULL,
    dl09 FLOAT8 NULL,
    dl10 FLOAT8 NULL,
    dl11 FLOAT8 NULL,
    dl12 FLOAT8 NULL,
    dl13 FLOAT8 NULL,
    dl14 FLOAT8 NULL,
    dl15 FLOAT8 NULL,
    dl16 FLOAT8 NULL,
    dl17 FLOAT8 NULL,
    dl18 FLOAT8 NULL,
    dl19 FLOAT8 NULL,
    dl20 FLOAT8 NULL,
    dl21 FLOAT8 NULL,
    dl22 FLOAT8 NULL,
    dl23 FLOAT8 NULL,
    dl24 FLOAT8 NULL,
    dl25 FLOAT8 NULL,
    dl26 FLOAT8 NULL,
    dl27 FLOAT8 NULL,
    dl28 FLOAT8 NULL,
    dl29 FLOAT8 NULL,
    dl30 FLOAT8 NULL,
    dl31 FLOAT8 NULL,
    dl32 FLOAT8 NULL,
    dl33 FLOAT8 NULL,
    dl34 FLOAT8 NULL,
    dl35 FLOAT8 NULL,
    dl36 FLOAT8 NULL,
    dl37 FLOAT8 NULL,
    dl38 FLOAT8 NULL,
    dl39 FLOAT8 NULL,
    dl40 FLOAT8 NULL,
    f FLOAT8 NULL,
    f1 FLOAT8 NULL,
    f2 FLOAT8 NULL,
    f3 FLOAT8 NULL,
    g FLOAT8 NULL,
    g1 FLOAT8 NULL,
    g2 FLOAT8 NULL,
    g3 FLOAT8 NULL,
    o FLOAT8 NULL,
    carbon_emission FLOAT8 NULL,
    uf FLOAT8 NULL,
    uf1 FLOAT8 NULL,
    uf2 FLOAT8 NULL,
    uf3 FLOAT8 NULL,
    ug FLOAT8 NULL,
    ug1 FLOAT8 NULL,
    ug2 FLOAT8 NULL,
    ug3 FLOAT8 NULL,
    ut FLOAT8 NULL,
    uo FLOAT8 NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC),
    FAMILY "primary" (create_date, dimension, id, ent_id, group_id, dl01, dl02, dl03, dl04, dl05, dl06, dl07, dl08, dl09, dl10, dl11, dl12, dl13, dl14, dl15, dl16, dl17, dl18, dl19, dl20, dl21, dl22, dl23, dl24, dl25, dl26, dl27, dl28, dl29, dl30, dl31, dl32, dl33, dl34, dl35, dl36, dl37, dl38, dl39, dl40, f, f1, f2, f3, g, g1, g2, g3, o, carbon_emission, uf, uf1, uf2, uf3, ug, ug1, ug2, ug3, ut, uo));

CREATE TABLE cement_indicator_e_day (
    create_date TIMESTAMPTZ NOT NULL,
    dimension VARCHAR(16) NULL,
    id VARCHAR(50) NOT NULL,
    ent_id VARCHAR(50) NOT NULL,
    group_id VARCHAR(50) NOT NULL,
    sn181 FLOAT8 NULL,
    sn182 FLOAT8 NULL,
    sn183 FLOAT8 NULL,
    sn184 FLOAT8 NULL,
    sn185 FLOAT8 NULL,
    sn186 FLOAT8 NULL,
    sn187 FLOAT8 NULL,
    sn191 FLOAT8 NULL,
    sn192 FLOAT8 NULL,
    sn193 FLOAT8 NULL,
    sn194 FLOAT8 NULL,
    sn195 FLOAT8 NULL,
    sn196 FLOAT8 NULL,
    sn197 FLOAT8 NULL,
    sn201 FLOAT8 NULL,
    sn202 FLOAT8 NULL,
    sn203 FLOAT8 NULL,
    sn204 FLOAT8 NULL,
    sn205 FLOAT8 NULL,
    sn206 FLOAT8 NULL,
    sn207 FLOAT8 NULL,
    sn211 FLOAT8 NULL,
    sn212 FLOAT8 NULL,
    sn213 FLOAT8 NULL,
    sn214 FLOAT8 NULL,
    sn215 FLOAT8 NULL,
    sn216 FLOAT8 NULL,
    sn217 FLOAT8 NULL,
    sn22 FLOAT8 NULL,
    sn23 FLOAT8 NULL,
    sn24 FLOAT8 NULL,
    sn25 FLOAT8 NULL,
    sn26 FLOAT8 NULL,
    sn27 FLOAT8 NULL,
    sn28 FLOAT8 NULL,
    sn29 FLOAT8 NULL,
    sn30 FLOAT8 NULL,
    u1 FLOAT8 NULL,
    u2 FLOAT8 NULL,
    u3 FLOAT8 NULL,
    CONSTRAINT "primary" PRIMARY KEY (id ASC),
    FAMILY "primary" (create_date, dimension, id, ent_id, group_id, sn181, sn182, sn183, sn184, sn185, sn186, sn187, sn191, sn192, sn193, sn194, sn195, sn196, sn197, sn201, sn202, sn203, sn204, sn205, sn206, sn207, sn211, sn212, sn213, sn214, sn215, sn216, sn217, sn22, sn23, sn24, sn25, sn26, sn27, sn28, sn29, sn30, u1, u2, u3));

 CREATE VIEW v_carbon_emission_day (dimension, ent_id, carbon_emission, industry, region_code) AS SELECT t1.dimension, t1.ent_id, t1.carbon_emission, t1.industry, t2.region_code FROM (SELECT dimension, ent_id, sum(sn23) AS carbon_emission, 'SN' AS industry FROM cement_indicator_e_day GROUP BY dimension, ent_id UNION ALL SELECT dimension, ent_id, sum(carbon_emission) AS carbon_emission, 'DL' AS industry FROM electric_indicator_day GROUP BY dimension, ent_id) AS t1 JOIN ent_info AS t2 ON CAST(t1.ent_id AS INT4) = t2.ent_id;

PREPARE stmt1 AS SELECT IFNULL(SUM(carbon_emission), 0) AS dataVal FROM v_carbon_emission_day WHERE dimension LIKE CONCAT('', $1, '%')AND industry = $2;

EXECUTE stmt1('20241117', 'SN');
EXECUTE stmt1('20241117', 'SN');


PREPARE st2 AS SELECT SUM(carbon_emission) AS dataVal FROM v_carbon_emission_day WHERE dimension LIKE CONCAT('', $1, '%')AND industry = $2;
EXECUTE st2('20241117', 'SN');
EXECUTE st2('20241117', 'SN');

PREPARE st14 AS SELECT b FROM (SELECT t1.industry as b
 FROM
 (
	SELECT ent_id, 'SN' AS industry
	FROM cement_indicator_e_day
	UNION ALL
	SELECT ent_id, 'DL' AS industry
	FROM
	electric_indicator_day
 ) AS t1 JOIN ent_info AS t2 ON CAST(t1.ent_id AS INT4) = t2.ent_id) WHERE b = $1;
EXECUTE st14('SN');
EXECUTE st14('SN');

use defaultdb;
drop database test cascade;
