SELECT li.pipeline_name,
       li.pipe_start,
       li.pipe_end,
       COUNT(mt.measure_value),
       AVG(mt.measure_value)
FROM (select  pipe_start,
            pipe_end,
            pipeline_name,
            pipeline_sn,
            pipe_properties,
            (CASE WHEN substr(pipe_properties, 16, 1) = '1' THEN 2 ELSE 1 END) as ltype,
            (CASE WHEN substr(pipeline_sn, 13, 1) = '1' THEN 3 ELSE 2 END) as lstyle
      from pipec_r.pipeline_info) as li,              
     mtagdb.measurepoints mt                    
WHERE mt.measure_tag = li.pipeline_name
  AND mt.measure_position = li.pipeline_sn                   
  AND mt.measure_type = cast(li.ltype as int2)
  AND mt.measure_style = cast(li.lstyle as int)
  AND mt.measure_value >= 2.7
GROUP BY li.pipeline_name, li.pipe_start, li.pipe_end;
