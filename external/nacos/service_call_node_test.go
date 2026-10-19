//go:build integration

package nacos

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/engine"
)

var nacosAddr = envAddr()

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

// instanceExists queries whether an instance is registered via the nacos open API.
func instanceExists(t *testing.T, serviceName, ip string) bool {
	t.Helper()
	u := fmt.Sprintf("http://%s/nacos/v1/ns/instance/list?serviceName=%s&groupName=DEFAULT_GROUP", nacosAddr, serviceName)
	resp, err := http.Get(u)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return strings.Contains(string(b), fmt.Sprintf(`"ip":"%s"`, ip))
}

// ==================== nacos open API helpers ====================

// registerInstance registers a service instance via the nacos open API.
func registerInstance(t *testing.T, serviceName, ip string, port int) {
	t.Helper()
	form := url.Values{}
	form.Set("serviceName", serviceName)
	form.Set("ip", ip)
	form.Set("port", strconv.Itoa(port))
	form.Set("groupName", "DEFAULT_GROUP")
	form.Set("healthy", "true")
	resp, err := http.PostForm("http://"+nacosAddr+"/nacos/v1/ns/instance", form)
	if err != nil {
		t.Fatalf("register instance: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "ok") {
		t.Fatalf("register instance resp not ok: %s", b)
	}
}

// publishConfig publishes a config via the nacos open API.
func publishConfig(t *testing.T, dataId, group, content string) {
	t.Helper()
	form := url.Values{}
	form.Set("dataId", dataId)
	form.Set("group", group)
	form.Set("content", content)
	form.Set("type", "json")
	resp, err := http.PostForm("http://"+nacosAddr+"/nacos/v1/cs/configs", form)
	if err != nil {
		t.Fatalf("publish config: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "true") {
		t.Fatalf("publish config resp not true: %s", b)
	}
}

// getConfig reads config content via the nacos open API.
func getConfig(t *testing.T, dataId, group string) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/nacos/v1/cs/configs?dataId=%s&group=%s", nacosAddr, dataId, group))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

// getCallNode returns the *ServiceCallNode with the given id from the rule chain.
func getCallNode(t *testing.T, eng *engine.RuleEngine, id string) *ServiceCallNode {
	t.Helper()
	nc, ok := eng.RootRuleChainCtx().GetNodeById(types.RuleNodeId{Id: id})
	if !ok {
		t.Fatalf("node %s not found", id)
	}
	rnc, ok := nc.(*engine.RuleNodeCtx)
	if !ok {
		t.Fatalf("node %s ctx type %T", id, nc)
	}
	node, ok := rnc.Node.(*ServiceCallNode)
	if !ok {
		t.Fatalf("node %s type %T not *ServiceCallNode", id, rnc.Node)
	}
	return node
}

// ==================== service call ====================

// TestServiceCallE2E is a real end-to-end test: a mock backend registers to nacos and the nacosServiceCall chain calls it.
func TestServiceCallE2E(t *testing.T) {
	wantBody := `{"msg":"hello-from-backend"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, wantBody)
	}))
	defer backend.Close()
	_, portStr, _ := net.SplitHostPort(backend.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	svc := "e2e-test-svc"
	registerInstance(t, svc, "127.0.0.1", port)
	waitUntil(t, "service instance registered", 30*time.Second, func() bool { return instanceExists(t, svc, "127.0.0.1") })

	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "e2e_call", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"n1","type":"x/nacosServiceCall","configuration":{
	         "server":"%s","serviceName":"%s","groupName":"DEFAULT_GROUP",
	         "method":"GET","path":"/ping"}}
	    ]
	  }
	}`, nacosAddr, svc)

	eng, err := rulego.New("e2e_call", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("e2e_call")

	resultCh := make(chan types.RuleMsg, 1)
	errCh := make(chan error, 1)
	msg := types.NewMsg(0, "e2e", types.JSON, types.NewMetadata(), "")
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- m
	}))

	select {
	case m := <-resultCh:
		if m.GetData() != wantBody {
			t.Errorf("response want %q, got %q", wantBody, m.GetData())
		} else {
			t.Logf("PASS: nacosServiceCall discovered and called backend via nacos, resp=%s", m.GetData())
		}
	case err := <-errCh:
		t.Fatalf("rule chain failed: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

// TestSharedNodeReuse verifies that multiple nacosServiceCall nodes sharing the same server config reuse one client (base.SharedNode reuse).
func TestSharedNodeReuse(t *testing.T) {
	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "reuse", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"a","type":"x/nacosServiceCall","configuration":{"server":"%s","serviceName":"svc-a","method":"GET","path":"/"}},
	      {"id":"b","type":"x/nacosServiceCall","configuration":{"server":"ref://a","serviceName":"svc-b","method":"GET","path":"/"}}
	    ]
	  }
	}`, nacosAddr)

	eng, err := rulego.New("reuse", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("reuse")
	re := eng.(*engine.RuleEngine)

	a := getCallNode(t, re, "a")
	b := getCallNode(t, re, "b")
	ca, err := a.SharedNode.GetSafely()
	if err != nil {
		t.Fatalf("a GetSafely: %v", err)
	}
	cb, err := b.SharedNode.GetSafely()
	if err != nil {
		t.Fatalf("b GetSafely: %v", err)
	}
	if ca != cb {
		t.Fatalf("nodes with same server config should reuse one client: a=%p b=%p", ca, cb)
	}
	t.Logf("PASS: multiple nacosServiceCall with same server config reuse one client (a=%p b=%p)", ca, cb)
}

// TestServiceCallChainE2E is a full multi-node chain: nacosServiceCall discovers and calls the backend via nacos,
// then jsTransform wraps the response, verifying the whole chain runs and the final msg is the transformed result.
// Unlike TestServiceCallE2E (single node), this covers the "service-call node -> subsequent built-in node" flow.
func TestServiceCallChainE2E(t *testing.T) {
	wantBody := `{"msg":"chain-backend"}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, wantBody)
	}))
	defer backend.Close()
	_, portStr, _ := net.SplitHostPort(backend.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	// register the mock backend to nacos
	svc := "e2e-chain-svc"
	registerInstance(t, svc, "127.0.0.1", port)
	waitUntil(t, "service instance registered", 30*time.Second, func() bool { return instanceExists(t, svc, "127.0.0.1") })

	// n1 calls the service and emits the response; n2 wraps it; connections route n1 Success to n2
	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "e2e_call_chain", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"n1","type":"x/nacosServiceCall","configuration":{
	         "server":"%s","serviceName":"%s","groupName":"DEFAULT_GROUP",
	         "method":"GET","path":"/ping"}},
	      {"id":"n2","type":"jsTransform","configuration":{
	         "jsScript":"return {msg:'WRAP['+JSON.stringify(msg)+']',metadata:metadata,msgType:msgType};"}}
	    ],
	    "connections":[{"fromId":"n1","toId":"n2","type":"Success"}]
	  }
	}`, nacosAddr, svc)

	eng, err := rulego.New("e2e_call_chain", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("e2e_call_chain")

	resultCh := make(chan types.RuleMsg, 1)
	errCh := make(chan error, 1)
	msg := types.NewMsg(0, "chain", types.JSON, types.NewMetadata(), "")
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- m
	}))

	want := "WRAP[" + wantBody + "]"
	select {
	case m := <-resultCh:
		if m.GetData() != want {
			t.Errorf("want %q, got %q", want, m.GetData())
		} else {
			t.Logf("PASS: full chain nacosServiceCall->jsTransform output=%s", m.GetData())
		}
	case err := <-errCh:
		t.Fatalf("rule chain failed: %v", err)
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

// TestServiceCallHttpError verifies that a 5xx backend response makes nacosServiceCall call TellFailure.
func TestServiceCallHttpError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"err":"boom"}`, http.StatusInternalServerError)
	}))
	defer backend.Close()
	_, portStr, _ := net.SplitHostPort(backend.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)

	svc := "e2e-err-svc"
	registerInstance(t, svc, "127.0.0.1", port)
	waitUntil(t, "service instance registered", 30*time.Second, func() bool { return instanceExists(t, svc, "127.0.0.1") })

	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "err_call", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"n1","type":"x/nacosServiceCall","configuration":{
	         "server":"%s","serviceName":"%s","groupName":"DEFAULT_GROUP",
	         "method":"GET","path":"/broken"}}
	    ]
	  }
	}`, nacosAddr, svc)

	eng, err := rulego.New("err_call", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("err_call")

	endCh := make(chan error, 1)
	msg := types.NewMsg(0, "err", types.JSON, types.NewMetadata(), "")
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		endCh <- err
	}))

	select {
	case err := <-endCh:
		if err == nil {
			t.Fatal("5xx should trigger TellFailure but succeeded")
		}
		if !strings.Contains(err.Error(), "HTTP 500") {
			t.Errorf("error should contain HTTP 500, got: %v", err)
		} else {
			t.Logf("PASS: backend 500 correctly triggered TellFailure: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

// ==================== config read ====================

// TestConfigGetE2E is a real end-to-end test: publish a config to nacos and read it back via x/nacosConfigGet.
func TestConfigGetE2E(t *testing.T) {
	dataId := "e2e-config-get"
	group := "DEFAULT_GROUP"
	content := `{"key":"value123"}`
	publishConfig(t, dataId, group, content)
	waitUntil(t, "config published", 30*time.Second, func() bool { return getConfig(t, dataId, group) == content })

	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "cfg_get", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"g1","type":"x/nacosConfigGet","configuration":{
	         "server":"%s","dataId":"%s","group":"%s"}}
	    ]
	  }
	}`, nacosAddr, dataId, group)

	eng, err := rulego.New("cfg_get", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("cfg_get")

	resultCh := make(chan types.RuleMsg, 1)
	errCh := make(chan error, 1)
	msg := types.NewMsg(0, "cfg", types.JSON, types.NewMetadata(), "")
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- m
	}))

	select {
	case m := <-resultCh:
		if m.GetData() != content {
			t.Errorf("config want %q, got %q", content, m.GetData())
		} else {
			t.Logf("PASS: nacosConfigGet read config=%s", m.GetData())
		}
	case err := <-errCh:
		t.Fatalf("rule chain failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

// ==================== config write ====================

// TestConfigSetE2E is a real end-to-end test: x/nacosConfigSet writes a config, then reads it back via the open API to verify full overwrite.
func TestConfigSetE2E(t *testing.T) {
	dataId := "e2e-config-set"
	group := "DEFAULT_GROUP"
	content := `{"k":"v-set"}`

	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "cfg_set", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"s1","type":"x/nacosConfigSet","configuration":{
	         "server":"%s","dataId":"%s","group":"%s","content":%q,"format":"json"}}
	    ]
	  }
	}`, nacosAddr, dataId, group, content)

	eng, err := rulego.New("cfg_set", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("cfg_set")

	endCh := make(chan error, 1)
	msg := types.NewMsg(0, "set", types.JSON, types.NewMetadata(), "")
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		endCh <- err
	}))

	select {
	case err := <-endCh:
		if err != nil {
			t.Fatalf("nacosConfigSet write failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	// read back and verify (via nacos open API)
	waitUntil(t, "config set written", 30*time.Second, func() bool { return getConfig(t, dataId, group) == content })
	t.Logf("PASS: nacosConfigSet read back after write=%s", content)
}

// TestConfigSetTemplateE2E verifies content supports the ${msg.xx} template: writes the message body as the config content.
func TestConfigSetTemplateE2E(t *testing.T) {
	dataId := "e2e-config-set-tmpl"
	group := "DEFAULT_GROUP"
	wantContent := `{"k":"from-msg"}`

	chain := fmt.Sprintf(`{
	  "ruleChain": {"id": "cfg_set_tmpl", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "nodes": [
	      {"id":"s1","type":"x/nacosConfigSet","configuration":{
	         "server":"%s","dataId":"%s","group":"%s","content":"${msg}","format":"json"}}
	    ]
	  }
	}`, nacosAddr, dataId, group)

	eng, err := rulego.New("cfg_set_tmpl", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("cfg_set_tmpl")

	endCh := make(chan error, 1)
	msg := types.NewMsg(0, "set", types.JSON, types.NewMetadata(), wantContent)
	eng.OnMsg(msg, types.WithOnEnd(func(ctx types.RuleContext, m types.RuleMsg, err error, relationType string) {
		endCh <- err
	}))

	select {
	case err := <-endCh:
		if err != nil {
			t.Fatalf("nacosConfigSet template write failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	waitUntil(t, "config set written", 30*time.Second, func() bool { return getConfig(t, dataId, group) == wantContent })
	t.Logf("PASS: nacosConfigSet template content=${msg} read back after write=%s", wantContent)
}
