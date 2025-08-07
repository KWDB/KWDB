-- create stats to avoid inconsistent query results
CREATE STATISTICS _stats_ FROM db_pipec.t_point;
CREATE STATISTICS _stats_ FROM pipec_r.station_info;
CREATE STATISTICS _stats_ FROM pipec_r.pipeline_info;
CREATE STATISTICS _stats_ FROM pipec_r.point_info;
CREATE STATISTICS _stats_ FROM pipec_r.workarea_info;

CREATE STATISTICS _stats_ FROM runba.opcdata449600;
CREATE STATISTICS _stats_ FROM runba_tra.cd_behavior_area;
CREATE STATISTICS _stats_ FROM runba_tra.cd_device_point;
CREATE STATISTICS _stats_ FROM runba_tra.cd_security_device;
CREATE STATISTICS _stats_ FROM runba_tra.cd_tijian_item_index;
CREATE STATISTICS _stats_ FROM runba_tra.plat_device_info;
CREATE STATISTICS _stats_ FROM runba_tra.plat_risk_analyse_objects;
CREATE STATISTICS _stats_ FROM runba_tra.plat_risk_area;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijian_item;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijian_item_index;
CREATE STATISTICS _stats_ FROM runba_tra.plat_tijiancard_item_relation;
CREATE STATISTICS _stats_ FROM runba_tra.plat_zone_member;

show statistics for table db_pipec.t_point;
show statistics for table pipec_r.station_info;
show statistics for table pipec_r.pipeline_info;
show statistics for table pipec_r.point_info;
show statistics for table pipec_r.workarea_info;

show statistics for table runba.opcdata449600;
show statistics for table runba_tra.cd_behavior_area;
show statistics for table runba_tra.cd_device_point;
show statistics for table runba_tra.cd_security_device;
show statistics for table runba_tra.cd_tijian_item_index;
show statistics for table runba_tra.plat_risk_analyse_objects;
show statistics for table runba_tra.plat_risk_area;
show statistics for table runba_tra.plat_tijian_item;
show statistics for table runba_tra.plat_tijian_item_index;
show statistics for table runba_tra.plat_tijiancard_item_relation;
show statistics for table runba_tra.plat_zone_member;
