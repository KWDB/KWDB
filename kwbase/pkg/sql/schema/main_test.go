package schema_test

import (
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
)

func TestMain(m *testing.M) {
func TestMain(m *testing.M) { 
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}
