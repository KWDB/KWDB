show timezone;

set cluster setting cluster.connection.timezone='Asia/Shanghai';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='3';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='+7';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='-4:15';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='1.5';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='America/St_Johns';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone='';
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';

set cluster setting cluster.connection.timezone=default;
show cluster setting cluster.connection.timezone;
SELECT * FROM kwdb_internal.cluster_settings WHERE variable = 'cluster.connection.timezone';
