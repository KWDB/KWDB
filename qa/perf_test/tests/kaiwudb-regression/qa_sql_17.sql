WITH point_info_data AS (
    SELECT 
        t.k_timestamp,
        t.measure_value,
        p.station_sn
    FROM 
        db_pipec.t_point t,    -- 45M
        pipec_r.point_info p   -- 150k
    WHERE 
        t.point_sn = p.point_sn -- 150k, 150k
    AND 
        t.k_timestamp BETWEEN '2024-01-04 14:30:50' AND '2024-01-04 14:30:59' -- 2M/45M
),
station_info_data AS (
    SELECT 
        s.station_sn,
        s.work_area_sn,
        s.sub_company_sn,
        wa.work_area_name,
        c.sub_company_name
    FROM 
        pipec_r.station_info s,    -- 436
        pipec_r.workarea_info wa,  -- 41
        pipec_r.company_info c     -- 8
    WHERE 
        s.work_area_sn = wa.work_area_sn    -- 41, 41
    AND 
        s.sub_company_sn = c.sub_company_sn    -- 8, 8
),
filtered_station_data AS (
    SELECT
        si.station_sn,
        si.work_area_sn,
        si.work_area_name,
        si.sub_company_name
    FROM
        station_info_data si    -- 436
    WHERE
        si.station_sn IN (SELECT p.station_sn FROM point_info_data p) -- 436, 436, 150k
)
SELECT
    time_bucket(p.k_timestamp, '30m') AS timebucket,
    fs.work_area_sn,
    fs.work_area_name,
    fs.sub_company_name,
    COUNT(p.k_timestamp) AS measurement_count,
    ROUND(SUM(p.measure_value), 2) AS total_measure_value,
    ROUND(AVG(p.measure_value), 2) AS avg_measure_value
FROM
    point_info_data p,          -- 150k
    filtered_station_data fs    -- 436
WHERE
    p.station_sn = fs.station_sn    -- 436,436
GROUP BY
    timebucket, fs.work_area_sn, fs.work_area_name, fs.sub_company_name
ORDER BY
    timebucket, fs.work_area_sn, fs.work_area_name, fs.sub_company_name;
