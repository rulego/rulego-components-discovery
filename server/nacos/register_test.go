//go:build integration

package nacos

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	discovery "github.com/rulego/rulego-components-discovery/pkg"
)

var regNacosAddr = envAddr()

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

// instanceExists queries whether an instance exists via the nacos open API.
func instanceExists(t *testing.T, serviceName, ip string) bool {
	t.Helper()
	url := fmt.Sprintf("http://%s/nacos/v1/ns/instance/list?serviceName=%s&groupName=DEFAULT_GROUP", regNacosAddr, serviceName)
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return strings.Contains(string(b), fmt.Sprintf(`"ip":"%s"`, ip))
}

// TestRegisterE2E verifies the instance appears after register and disappears after deregister.
func TestRegisterE2E(t *testing.T) {
	cfg := RegisterConfig{
		ConnConfig:  discovery.ConnConfig{ServerAddrs: regNacosAddr},
		ServiceName: "e2e-reg-svc",
		Ip:          "127.0.0.1",
		Port:        9999,
	}
	_, cleanup, err := Register(cfg)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	waitUntil(t, "instance registered", 30*time.Second, func() bool { return instanceExists(t, "e2e-reg-svc", "127.0.0.1") })

	if !instanceExists(t, "e2e-reg-svc", "127.0.0.1") {
		t.Error("instance should exist after register")
	} else {
		t.Log("PASS: instance appears in nacos after register")
	}

	cleanup()
	waitUntil(t, "instance deregistered", 30*time.Second, func() bool { return !instanceExists(t, "e2e-reg-svc", "127.0.0.1") })

	if instanceExists(t, "e2e-reg-svc", "127.0.0.1") {
		t.Error("instance should disappear after deregister")
	} else {
		t.Log("PASS: instance disappears from nacos after deregister")
	}
}
