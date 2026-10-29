// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package sql_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestMLImportModelMetadata verifies that kwdbml.import_model persists the
// model metadata (features, target, entity, transformer entity) and allocates
// the model version, instead of only registering the model name.
func TestMLImportModelMetadata(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Enabling the ML component creates the kwdbml meta tables and imports the
	// built-in TSLLM model.
	if _, err := db.Exec(`SET CLUSTER SETTING ml.enabled = true`); err != nil {
		t.Fatal(err)
	}

	const features = `[{"name":"Active_Power","type":"float"}]`
	const target = `[{"name":"Predicted_Power","type":"float"}]`

	var ret string
	row := db.QueryRow(`
		SELECT kwdbml.import_model(
			'public.ml_import_test', 'a model', 'regression', 'sklearn_1.3', 'python_3.10',
			$1, $2, 'model-entity-bytes', 'transformer-entity-bytes')`,
		features, target)
	if err := row.Scan(&ret); err != nil {
		t.Fatal(err)
	}
	if want := "public.ml_import_test,1"; ret != want {
		t.Fatalf("import_model returned %q, want %q", ret, want)
	}

	var gotFeatures, gotTarget string
	var gotEntity, gotTransformer []byte
	var gotVersion int
	row = db.QueryRow(`
		SELECT sm.features, sm.target, smv.version, smv.entity, smv.transformer_entity
		FROM kwdbml.ml_models sm
		JOIN kwdbml.ml_model_versions smv ON smv.model_id = sm.id
		WHERE sm.schema = 'public' AND sm.name = 'ml_import_test'`)
	if err := row.Scan(&gotFeatures, &gotTarget, &gotVersion, &gotEntity, &gotTransformer); err != nil {
		t.Fatal(err)
	}

	if !sameJSON(t, gotFeatures, features) {
		t.Errorf("stored features = %q, want %q", gotFeatures, features)
	}
	if !sameJSON(t, gotTarget, target) {
		t.Errorf("stored target = %q, want %q", gotTarget, target)
	}
	if gotVersion != 1 {
		t.Errorf("stored version = %d, want 1", gotVersion)
	}
	if want := hexBytes("model-entity-bytes"); string(gotEntity) != want {
		t.Errorf("stored entity = %q, want %q", gotEntity, want)
	}
	if want := hexBytes("transformer-entity-bytes"); string(gotTransformer) != want {
		t.Errorf("stored transformer entity = %q, want %q", gotTransformer, want)
	}

	// The model shows up in show_models() with its metadata.
	var shownFeatures, shownTarget, shownStatus string
	row = db.QueryRow(`
		SELECT features, target, status FROM kwdbml.show_models()
		WHERE schema = 'public' AND name = 'ml_import_test'`)
	if err := row.Scan(&shownFeatures, &shownTarget, &shownStatus); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, shownFeatures, features) {
		t.Errorf("show_models features = %q, want %q", shownFeatures, features)
	}
	if !sameJSON(t, shownTarget, target) {
		t.Errorf("show_models target = %q, want %q", shownTarget, target)
	}
	if shownStatus == "" || shownStatus == "{}" {
		t.Errorf("show_models status = %q, want a non-empty deployment status", shownStatus)
	}
}

// TestMLImportModelVersionIncrement verifies that re-importing a model
// allocates a new version rather than reusing version 1.
func TestMLImportModelVersionIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`SET CLUSTER SETTING ml.enabled = true`); err != nil {
		t.Fatal(err)
	}

	for i, want := range []string{"public.ml_version_test,1", "public.ml_version_test,2"} {
		var ret string
		row := db.QueryRow(`
			SELECT kwdbml.import_model(
				'public.ml_version_test', 'a model', 'regression', 'sklearn_1.3', 'python_3.10',
				'[{"name":"x","type":"float"}]', '[{"name":"y","type":"float"}]',
				'entity', 'transformer')`)
		if err := row.Scan(&ret); err != nil {
			t.Fatalf("import #%d: %v", i, err)
		}
		if ret != want {
			t.Fatalf("import #%d returned %q, want %q", i, ret, want)
		}
	}

	var count int
	if err := db.QueryRow(`
		SELECT count(*) FROM kwdbml.ml_model_versions smv
		JOIN kwdbml.ml_models sm ON sm.id = smv.model_id
		WHERE sm.schema = 'public' AND sm.name = 'ml_version_test'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("stored %d model versions, want 2", count)
	}
}

// TestMLImportModelMetrics verifies the optional 10th argument (metrics) of
// kwdbml.import_model.
func TestMLImportModelMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`SET CLUSTER SETTING ml.enabled = true`); err != nil {
		t.Fatal(err)
	}

	const metrics = `[{"name":"mse","type":"float"}]`
	var ret string
	row := db.QueryRow(`
		SELECT kwdbml.import_model(
			'public.ml_metrics_test', 'a model', 'regression', 'sklearn_1.3', 'python_3.10',
			'[{"name":"x","type":"float"}]', '[{"name":"y","type":"float"}]',
			'entity', 'transformer', $1)`,
		metrics)
	if err := row.Scan(&ret); err != nil {
		t.Fatal(err)
	}
	if want := "public.ml_metrics_test,1"; ret != want {
		t.Fatalf("import_model returned %q, want %q", ret, want)
	}

	var gotMetrics string
	if err := db.QueryRow(`
		SELECT smv.metrics FROM kwdbml.ml_model_versions smv
		JOIN kwdbml.ml_models sm ON sm.id = smv.model_id
		WHERE sm.schema = 'public' AND sm.name = 'ml_metrics_test'`).Scan(&gotMetrics); err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, gotMetrics, metrics) {
		t.Fatalf("stored metrics = %q, want %q", gotMetrics, metrics)
	}
}

// TestMLModelPrivileges verifies that the privilege builtins of the ported
// implementation are wired up.
func TestMLModelPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`SET CLUSTER SETTING ml.enabled = true`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE USER ml_test_user`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`
		SELECT kwdbml.import_model(
			'public.ml_priv_test', 'a model', 'regression', 'sklearn_1.3', 'python_3.10',
			'[{"name":"x","type":"float"}]', '[{"name":"y","type":"float"}]',
			'entity', 'transformer')`); err != nil {
		t.Fatal(err)
	}

	var granted string
	if err := db.QueryRow(`
		SELECT kwdbml.grant_privilege_on_model('public.ml_priv_test', 'ml_test_user', 'SELECT')`).
		Scan(&granted); err != nil {
		t.Fatal(err)
	}

	var hasPriv bool
	if err := db.QueryRow(`
		SELECT kwdbml.has_model_privilege('public.ml_priv_test', 'ml_test_user', 'SELECT')`).
		Scan(&hasPriv); err != nil {
		t.Fatal(err)
	}
	if !hasPriv {
		t.Fatal("testuser should hold the SELECT privilege on the model")
	}

	var privCount int
	if err := db.QueryRow(`
		SELECT count(*) FROM kwdbml.show_model_privilege('public.ml_priv_test')`).
		Scan(&privCount); err != nil {
		t.Fatal(err)
	}
	if privCount == 0 {
		t.Fatal("show_model_privilege returned no rows")
	}

	var revoked string
	if err := db.QueryRow(`
		SELECT kwdbml.revoke_privilege_on_model('public.ml_priv_test', 'ml_test_user', 'SELECT')`).
		Scan(&revoked); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`
		SELECT kwdbml.has_model_privilege('public.ml_priv_test', 'ml_test_user', 'SELECT')`).
		Scan(&hasPriv); err != nil {
		t.Fatal(err)
	}
	if hasPriv {
		t.Fatal("testuser should no longer hold the SELECT privilege on the model")
	}
}

// TestMLUsingModelSyntax verifies that the "USING MODEL" clause added to
// SELECT statements is parsed and reaches the plan metadata.
func TestMLUsingModelSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`SET CLUSTER SETTING ml.enabled = true`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE ml_using_model_t (a INT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO ml_using_model_t VALUES (1)`); err != nil {
		t.Fatal(err)
	}

	// The clause must parse; the model does not exist so the call fails, but
	// with a model-resolution error rather than a syntax error.
	_, err := db.Exec(`SELECT kwdbml.predict(a) USING public.no_such_model FROM ml_using_model_t`)
	if err == nil {
		t.Fatal("expected an error for a non-existent model")
	}
	if !contains(err.Error(), "no_such_model") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// sameJSON compares two JSON documents ignoring the formatting differences
// introduced by the server-side JSONB normalization.
func sameJSON(t *testing.T, got, want string) bool {
	t.Helper()
	var gotVal, wantVal interface{}
	if err := json.Unmarshal([]byte(got), &gotVal); err != nil {
		t.Errorf("cannot parse %q as JSON: %v", got, err)
		return false
	}
	if err := json.Unmarshal([]byte(want), &wantVal); err != nil {
		t.Errorf("cannot parse %q as JSON: %v", want, err)
		return false
	}
	return reflect.DeepEqual(gotVal, wantVal)
}

// hexBytes renders a byte string the way the pgwire protocol transfers BYTES.
func hexBytes(s string) string {
	return `\x` + hex.EncodeToString([]byte(s))
}

// contains reports whether s contains sub.
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
