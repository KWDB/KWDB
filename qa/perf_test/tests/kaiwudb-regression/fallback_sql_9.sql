SELECT li.pipeline_name,
       t.measure_value,
       o.value
FROM pipec_r.pipeline_info li
JOIN db_pipec.t_point t ON t.measure_value BETWEEN li.pipeline_sn::NUMERIC - 10 AND li.pipeline_sn::NUMERIC + 10
JOIN runba.opcdata449600 o ON o.adr = t.measure_location
WHERE t.measure_value > 100 AND o.value < 50;