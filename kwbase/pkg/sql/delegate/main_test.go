package delegate_test

import (
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/security/securitytest"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
)

func TestMain(m *testing.M) {
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}
