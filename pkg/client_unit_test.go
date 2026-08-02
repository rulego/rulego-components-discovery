package discovery

import (
	"sync"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// mockNaming embeds INamingClient so only the methods exercised by Select/Subscribe need implementing;
// unused methods stay nil and are never called by these tests.
type mockNaming struct {
	naming_client.INamingClient
	mu        sync.Mutex
	instances []model.Instance
	err       error
	lastGroup string
	cb        func(services []model.Instance, err error)
}

func (m *mockNaming) SelectInstances(p vo.SelectInstancesParam) ([]model.Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastGroup = p.GroupName
	return m.instances, m.err
}

func (m *mockNaming) Subscribe(p *vo.SubscribeParam) error {
	m.mu.Lock()
	m.cb = p.SubscribeCallback
	m.mu.Unlock()
	return nil
}

func newTestClient(naming naming_client.INamingClient) *Client {
	return &Client{
		naming:    naming,
		instances: map[string][]model.Instance{},
		counters:  map[string]*uint64{},
		listeners: map[string]func(string, string, string, string){},
	}
}

func healthyInstances(ips ...string) []model.Instance {
	out := make([]model.Instance, len(ips))
	for i, ip := range ips {
		out[i] = model.Instance{Ip: ip, Port: 80, Healthy: true, Enable: true, Weight: 1}
	}
	return out
}

// TestSelectRoundRobin verifies the round-robin counter cycles through instances.
func TestSelectRoundRobin(t *testing.T) {
	c := newTestClient(&mockNaming{instances: healthyInstances("1.1.1.1", "2.2.2.2")})

	var got []string
	for i := 0; i < 5; i++ {
		host, _, err := c.Select("svc", "")
		if err != nil {
			t.Fatalf("Select %d: %v", i, err)
		}
		got = append(got, host)
	}
	want := []string{"1.1.1.1", "2.2.2.2", "1.1.1.1", "2.2.2.2", "1.1.1.1"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("round %d: want %s, got %s (all=%v)", i, want[i], got[i], got)
		}
	}
}

// TestSelectEffectiveGroup verifies an empty group is normalized to DEFAULT_GROUP before querying.
func TestSelectEffectiveGroup(t *testing.T) {
	mn := &mockNaming{instances: healthyInstances("1.1.1.1")}
	c := newTestClient(mn)
	if _, _, err := c.Select("svc", ""); err != nil {
		t.Fatalf("Select: %v", err)
	}
	mn.mu.Lock()
	g := mn.lastGroup
	mn.mu.Unlock()
	if g != DefaultGroup {
		t.Errorf("empty group should normalize to %s, got %s", DefaultGroup, g)
	}
}

// TestSelectNoInstance verifies the error when no instance is available.
func TestSelectNoInstance(t *testing.T) {
	c := newTestClient(&mockNaming{}) // no instances
	if _, _, err := c.Select("svc", ""); err == nil {
		t.Fatal("want error for no available instance")
	}
}

// TestSubscribeFiltersUnhealthy verifies the subscribe callback caches only healthy, enabled, weighted instances.
func TestSubscribeFiltersUnhealthy(t *testing.T) {
	mn := &mockNaming{}
	c := newTestClient(mn)
	if err := c.Subscribe("svc", ""); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	mn.mu.Lock()
	cb := mn.cb
	mn.mu.Unlock()
	if cb == nil {
		t.Fatal("Subscribe did not register a callback")
	}
	cb([]model.Instance{
		{Ip: "ok", Port: 80, Healthy: true, Enable: true, Weight: 1},
		{Ip: "down", Port: 80, Healthy: false, Enable: true, Weight: 1},
		{Ip: "off", Port: 80, Healthy: true, Enable: false, Weight: 1},
		{Ip: "zero", Port: 80, Healthy: true, Enable: true, Weight: 0},
	}, nil)

	k := keyOf("svc", EffectiveGroup(""))
	c.mu.RLock()
	cached := c.instances[k]
	c.mu.RUnlock()
	if len(cached) != 1 || cached[0].Ip != "ok" {
		t.Fatalf("want only healthy [ok], got %+v", cached)
	}
}
