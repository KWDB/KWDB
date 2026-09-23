// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	gosql "database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// PubTest is publication unit test.
func PubTest(t *testing.T, db *gosql.DB) {
	// create a test database and tables
	_, err := db.Exec(`CREATE DATABASE IF NOT EXISTS test_pub_rdb`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TS DATABASE IF NOT EXISTS test_pub_db`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_pub_db.sensor_data1 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id);`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE test_pub_rdb.sensor_data1 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
);`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE test_pub_db.sensor_data2 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id);`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE test_pub_db.sensor_data3 (
  k_timestamp TIMESTAMP NOT NULL,
  temperature FLOAT NOT NULL,
  humidity FLOAT,
  pressure FLOAT
) TAGS (
  sensor_id INT NOT NULL,
  sensor_type VARCHAR(30) NOT NULL
) PRIMARY TAGS (sensor_id);`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUB pub_sensor_data FOR TABLE test_pub_db.sensor_data1;`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_rdb FOR TABLE test_pub_rdb.sensor_data1;`)
	require.Error(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_filter FOR TABLE test_pub_db.sensor_data1 WHERE temperature > 36.8;`)
	require.Error(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_option_publish FOR TABLE test_pub_db.sensor_data1 WITH OPTIONS(publish='insert, update');`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUBLICATION pub_table_option_buffer_size FOR TABLE test_pub_db.sensor_data1 WITH OPTIONS(buffer_size='2');`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_option_sub_timeout FOR TABLE test_pub_db.sensor_data1 WITH OPTIONS(sub_timeout='5'); `)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_option_publish_error FOR TABLE test_pub_db.sensor_data1 WITH OPTIONS(publish='insert, update, select ');`)
	require.Error(t, err)

	rows, err := db.Query(`SELECT name, pub_objects FROM [SHOW PUB pub_sensor_data];`)
	require.NoError(t, err)
	count := 0
	for rows.Next() {
		count++
		var c1, c2 string
		err = rows.Scan(&c1, &c2)
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, count)

	_, err = db.Exec(`ALTER PUB pub_sensor_data SET OPTIONS(publish='insert, update');`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER PUB pub_sensor_data SET OPTIONS(publish='insert, update, select ');`)
	require.Error(t, err)

	_, err = db.Exec(`ALTER PUBLICATION pub_sensor_data SET OPTIONS(publish='insert',buffer_size='1',sub_timeout='3',retrieve_tags='on');`)
	require.NoError(t, err)

	_, err = db.Exec(`CREATE PUB pub_table_multi FOR TABLE test_pub_db.sensor_data1, test_pub_db.sensor_data2;`)
	require.NoError(t, err)

	_, err = db.Exec(`ALTER PUB pub_table_multi SET TABLE test_pub_db.sensor_data2;`)
	require.Error(t, err)

	_, err = db.Exec(`CREATE PUB pub_db FOR DATABASE test_pub_db;`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE test_pub_db.sensor_data2 ADD COLUMN c2 FLOAT;`)
	require.Error(t, err)
	_, err = db.Exec(`DROP TABLE test_pub_db.sensor_data3;`)
	require.Error(t, err)
	row := db.QueryRow(`SELECT count(*) FROM [SHOW PUBS];`)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 6, count)

	_, err = db.Exec(`DROP PUB IF EXISTS pub_dba;`)
	require.NoError(t, err)
	_, err = db.Exec(`DROP PUB pub_dba;`)
	require.Error(t, err)

	_, err = db.Exec(`DROP PUBLICATION IF EXISTS pub_db;`)
	require.NoError(t, err)
	_, err = db.Exec(`DROP PUB IF EXISTS pub_sensor_data;`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP database test_pub_db cascade;`)
	require.Error(t, err)

	_, err = db.Exec(`DROP PUB pub_table_multi;`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP PUB pub_table_option_publish;`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP PUB IF EXISTS pub_table_option_buffer_size;`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP PUB IF EXISTS pub_table_option_sub_timeout;`)
	require.NoError(t, err)

	row = db.QueryRow(`SELECT count(*) FROM [SHOW PUBS];`)
	err = row.Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 0, count)

	_, err = db.Exec(`DROP database test_pub_rdb`)
	require.NoError(t, err)
}
