//go:build integration

package nacos

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
)

var epNacosAddr = envAddr()

func envAddr() string {
	if a := os.Getenv("NACOS_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:8848"
}

// waitUntil polls cond until it returns true or the timeout elapses; fatals on timeout.
func waitUntil(t *testing.T, desc string, timeout time.Duration, cond func() bool) {
	t.Helper()
	stop := time.Now().Add(timeout)
	for time.Now().Before(stop) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// getConfig reads config content via the nacos open API (returns "" on error for polling).
func getConfig(t *testing.T, dataId, group string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/nacos/v1/cs/configs?dataId=%s&group=%s", epNacosAddr, dataId, group))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// epPublishConfig publishes a config via the nacos open API.
func epPublishConfig(t *testing.T, dataId, group, content string) {
	t.Helper()
	form := url.Values{}
	form.Set("dataId", dataId)
	form.Set("group", group)
	form.Set("content", content)
	form.Set("type", "json")
	resp, err := http.PostForm("http://"+epNacosAddr+"/nacos/v1/cs/configs", form)
	if err != nil {
		t.Fatalf("publish config: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "true") {
		t.Fatalf("publish config resp not true: %s", b)
	}
}

// TestConfigEndpointChainE2E verifies endpoint/nacos triggers a full rule chain (key end-to-end check):
// the chain metadata.endpoints defines endpoint/nacos (from.path=dataId, to.path=chain id);
// the endpoint subscribes to nacos config; a config change -> endpoint callback -> DoProcess -> rule chain runs (jsTransform).
// The final msg of the endpoint-triggered chain is captured via the global Config.OnEnd and verified.
func TestConfigEndpointChainE2E(t *testing.T) {
	dataId := "e2e-ep-chain-config"
	group := "DEFAULT_GROUP"

	// publish the initial config first (ensure dataId exists)
	epPublishConfig(t, dataId, group, `{"v":1}`)
	waitUntil(t, "initial config published", 30*time.Second, func() bool { return getConfig(t, dataId, group) == `{"v":1}` })

	// global OnEnd captures the endpoint-triggered chain's final msg (endpoint chains are driven by ChainExecutor.OnMsg, so WithEndFunc is unavailable)
	resultCh := make(chan types.RuleMsg, 4)
	config := rulego.NewConfig(
		types.WithEndpointEnabled(true),
		types.WithOnEndGlobal(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
			if err != nil {
				return
			}
			select {
			case resultCh <- m:
			default:
			}
		}),
	)

	chainId := "e2e_ep_chain"
	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "%s", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id":"ep_cfg","type":"endpoint/nacos","configuration":{
	         "server":"%s","group":"%s"},
	       "routers":[{"from":{"path":"%s"},"to":{"path":"%s"}}]}
	    ],
	    "nodes": [
	      {"id":"t1","type":"jsTransform","configuration":{
	         "jsScript":"return {msg:'EP['+JSON.stringify(msg)+']',metadata:metadata,msgType:msgType};"}}
	    ]
	  }
	}`, chainId, epNacosAddr, group, dataId, chainId)

	eng, err := rulego.New(chainId, []byte(chain), engine.WithConfig(config))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	// rulego.Del internally calls engine.Stop -> EndpointAspect.OnDestroy to release the endpoint (cancel listener)
	defer rulego.Del(chainId)

	// verify the endpoint is loaded via EndpointAspect and registered to the chain-scoped resource registry (ListenConfig starts at AddRouter)
	ruleEng, ok := eng.(*engine.RuleEngine)
	if !ok {
		t.Fatalf("engine type %T not *engine.RuleEngine", eng)
	}
	if _, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep_cfg"); !found {
		t.Fatal("endpoint ep_cfg not registered to chain-scoped resource registry, EndpointAspect not effective")
	}

	// wait for the endpoint's ListenConfig to register on the nacos server (the SDK exposes no readiness signal; the 40s callback wait below is the real guard)
	time.Sleep(3 * time.Second)

	// change config to trigger endpoint -> rule chain
	newContent := `{"v":2}`
	epPublishConfig(t, dataId, group, newContent)

	want := "EP[" + newContent + "]"
	select {
	case m := <-resultCh:
		if m.GetData() != want {
			t.Errorf("want %q, got %q", want, m.GetData())
		} else {
			t.Logf("PASS: endpoint/nacos triggered full chain jsTransform output=%s", m.GetData())
		}
	case <-time.After(40 * time.Second):
		t.Fatal("timed out waiting for endpoint-triggered chain result")
	}
}
