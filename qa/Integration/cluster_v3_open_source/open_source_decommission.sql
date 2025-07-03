ALTER RANGE default CONFIGURE ZONE USING range_min_bytes = 134217728;
ALTER RANGE default CONFIGURE ZONE USING range_max_bytes = 536870912;
ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 80000;
ALTER RANGE default CONFIGURE ZONE USING num_replicas = 3;
ALTER RANGE default CONFIGURE ZONE USING ts_merge.days = 24h;
ALTER RANGE default CONFIGURE ZONE USING constraints = '[]';
ALTER RANGE default CONFIGURE ZONE USING lease_preferences = '[]';
-- background-decommission: c4
-- join: c6

