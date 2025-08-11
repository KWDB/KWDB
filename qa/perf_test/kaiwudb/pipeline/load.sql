IMPORT INTO pipec_r.station_info CSV DATA ("nodelocal://1/pipec_r_station_info/n1.0.csv");

IMPORT INTO pipec_r.workarea_info CSV DATA ("nodelocal://1/pipec_r_workarea_info/n1.0.csv");

IMPORT INTO pipec_r.company_info CSV DATA ("nodelocal://1/pipec_r_company_info/n1.0.csv");

IMPORT INTO pipec_r.pipeline_info CSV DATA ("nodelocal://1/pipec_r_pipeline_info/n1.0.csv");

IMPORT INTO pipec_r.point_info CSV DATA ("nodelocal://1/pipec_r_point_info/n1.0.csv");

IMPORT INTO db_pipec.t_point CSV DATA ("nodelocal://1/db_pipec_t_point/");