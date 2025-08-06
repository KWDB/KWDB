set hash_scan_mode=3;
select
    ts_t.station_sn,
    ts_t.work_area_sn,
    ts_t.sub_com_sn,
    ts_t.pipeline_sn,
    ts_t.point_sn,
    sum(ts_t.measure_value),
    max(ts_t.measure_value)
from
    db_pipec.t_point ts_t,
    (select
        si.station_sn as "station_sn",
        wi.work_area_sn as "work_area_sn",
        ci.sub_company_sn as "sub_company_sn",
        pi.pipeline_sn as "pipeline_sn",
        p_i.point_sn as "point_sn"
    from
        pipec_r.station_info si,
        pipec_r.workarea_info wi,
        pipec_r.company_info ci,
        pipec_r.pipeline_info pi,
        pipec_r.point_info p_i
    where
        p_i.point_sn = 'point_sn_1'
    ) as rel_t
where
    ts_t.station_sn = rel_t.station_sn
    and ts_t.work_area_sn = rel_t.work_area_sn
    and ts_t.sub_com_sn = rel_t.sub_company_sn
    and ts_t.pipeline_sn = rel_t.pipeline_sn
    and ts_t.point_sn = rel_t.point_sn
group by
    ts_t.station_sn,
    ts_t.work_area_sn,
    ts_t.sub_com_sn,
    ts_t.pipeline_sn,
    ts_t.point_sn;