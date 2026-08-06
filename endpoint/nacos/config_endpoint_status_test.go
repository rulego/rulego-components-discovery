package nacos

import (
	"testing"

	"github.com/rulego/rulego/api/types"
)

// 聚合逻辑靠运行时断言该接口,编译期守住签名防止静默失效。
var _ types.ConnectionStatusGetter = (*ConfigEndpoint)(nil)

// TestConfigEndpointInitSoftFail verifies Init tolerates an unreachable nacos:
// Init returns nil and ConnectionStatus reports reconnecting (not connected).
func TestConfigEndpointInitSoftFail(t *testing.T) {
	ep := &ConfigEndpoint{}
	configuration := types.Configuration{"server": "127.0.0.1:59999", "group": "DEFAULT_GROUP"}
	if err := ep.Init(types.Config{}, configuration); err != nil {
		t.Fatalf("Init should soft-fail, got error: %v", err)
	}
	defer func() { _ = ep.Close() }()
	st := ep.ConnectionStatus()
	t.Logf("status=%s msg=%q", st.Status, st.Message)
	if st.Status == types.StatusConnected {
		t.Fatalf("want non-connected status against dead nacos, got %s", st.Status)
	}
	if st.Status == types.StatusNone {
		t.Fatalf("want reconnecting status, got none")
	}
}
