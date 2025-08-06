set hash_scan_mode=3;
select
    ts_t.station_sn,
    ts_t.work_area_sn,
    ts_t.sub_com_sn,
    ts_t.pipeline_sn,
    ts_t.point_sn,
    ts_t.measure_type,
    ts_t.measure_location,
    sum(ts_t.measure_value),
    max(ts_t.measure_value)
from
    db_pipec.t_point ts_t,
    (select
        si.station_sn as "station_sn",
        wi.work_area_sn as "work_area_sn",
        ci.sub_company_sn as "sub_company_sn",
        pi.pipeline_sn as "pipeline_sn",
        p_i.point_sn as "point_sn",
        6 as "measure_type",
        'measure_location_15000' as "measure_location",
        95.86477 as "measure_value"
    from
        pipec_r.station_info si,
        pipec_r.workarea_info wi,
        pipec_r.company_info ci,
        pipec_r.pipeline_info pi,
        pipec_r.point_info p_i
    where
        substring(p_i.point_sn, 10, 5) = '15000'
    ) as rel_t
where
    ts_t.station_sn = rel_t.station_sn
    and ts_t.work_area_sn = rel_t.work_area_sn
    and ts_t.sub_com_sn = rel_t.sub_company_sn
    and ts_t.pipeline_sn = rel_t.pipeline_sn
    and ts_t.point_sn = rel_t.point_sn
    and ts_t.measure_type = rel_t.measure_type
    and ts_t.measure_location = rel_t.measure_location
    and ts_t.measure_value = rel_t.measure_value
group by
    ts_t.station_sn,
    ts_t.work_area_sn,
    ts_t.sub_com_sn,
    ts_t.pipeline_sn,
    ts_t.point_sn,
    ts_t.measure_type,
    ts_t.measure_location;