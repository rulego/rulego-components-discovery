//go:build integration

package discovery

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

var testNacosAddr = envAddr()

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
	resp, err := http.Get(fmt.Sprintf("http://%s/nacos/v1/cs/configs?dataId=%s&group=%s", testNacosAddr, dataId, group))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func publishConfig(t *testing.T, dataId, group, content string) {
	t.Helper()
	form := url.Values{}
	form.Set("dataId", dataId)
	form.Set("group", group)
	form.Set("content", content)
	form.Set("type", "json")
	resp, err := http.PostForm("http://"+testNacosAddr+"/nacos/v1/cs/configs", form)
	if err != nil {
		t.Fatalf("publish config: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "true") {
		t.Fatalf("publish config resp not true: %s", b)
	}
}

// TestListenConfig verifies client.ListenConfig: changing a published config triggers the callback (core of endpoint/nacos).
func TestListenConfig(t *testing.T) {
	dataId := "e2e-listen-config"
	group := "DEFAULT_GROUP"
	publishConfig(t, dataId, group, `{"v":1}`)
	waitUntil(t, "initial config published", 30*time.Second, func() bool { return getConfig(t, dataId, group) == `{"v":1}` })

	client, err := NewClient(ConnConfig{ServerAddrs: testNacosAddr})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	changed := make(chan string, 4)
	if err := client.ListenConfig(dataId, group, func(data string) {
		changed <- data
	}); err != nil {
		t.Fatalf("ListenConfig: %v", err)
	}

	// change config to trigger the callback (the SDK exposes no listener-readiness signal; the 40s wait below is the real guard)
	newContent := `{"v":2}`
	time.Sleep(2 * time.Second)
	publishConfig(t, dataId, group, newContent)

	select {
	case data := <-changed:
		if data != newContent {
			t.Errorf("callback want %q, got %q", newContent, data)
		} else {
			t.Logf("PASS: config change triggers ListenConfig callback, data=%s", data)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("timed out waiting for config-change callback")
	}
}
