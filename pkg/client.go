package discovery

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Connection probe states (atomic connStatus).
const (
	connStateUnknown      int32 = 0
	connStateConnected    int32 = 1
	connStateReconnecting int32 = 2
)

// HealthProbeInterval is how often the background probe polls naming.ServerHealthy().
var HealthProbeInterval = 10 * time.Second

// Client wraps the nacos naming+config clients, providing instance selection and config read/write.
type Client struct {
	naming naming_client.INamingClient
	config config_client.IConfigClient

	mu        sync.RWMutex
	instances map[string][]model.Instance
	counters  map[string]*uint64
	subs      []subKey

	listenMu  sync.Mutex
	listeners map[string]func(namespace, group, dataId, data string)

	// Live connection health, driven by a background probe calling
	// naming.ServerHealthy(). The SDK owns its own gRPC reconnect loop; this
	// surfaces the resulting connectivity so components can report it via
	// ConnectionStatus() instead of only the SharedNode wrapper lifecycle.
	connStatus int32
	connMsgMu  sync.RWMutex
	connMsg    string
	probeStop  chan struct{}
	probeDone  chan struct{}
	probeOnce  sync.Once
}

type subKey struct {
	serviceName string
	group       string
}

// NewClient creates a nacos client from ConnConfig.
func NewClient(cfg ConnConfig) (*Client, error) {
	sc := cfg.toServerConfigs()
	if len(sc) == 0 {
		return nil, fmt.Errorf("server addrs is empty")
	}
	cc := cfg.toClientConfig()
	naming, err := clients.CreateNamingClient(map[string]any{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return nil, fmt.Errorf("create naming client: %w", err)
	}
	config, err := clients.CreateConfigClient(map[string]any{
		"serverConfigs": sc,
		"clientConfig":  cc,
	})
	if err != nil {
		return nil, fmt.Errorf("create config client: %w", err)
	}
	c := &Client{
		naming:    naming,
		config:    config,
		instances: map[string][]model.Instance{},
		counters:  map[string]*uint64{},
		listeners: map[string]func(string, string, string, string){},
		probeStop: make(chan struct{}),
		probeDone: make(chan struct{}),
	}
	// Seed status from the first probe, then poll periodically.
	c.applyProbe(naming.ServerHealthy())
	go c.probeLoop()
	return c, nil
}

// probeLoop polls naming.ServerHealthy() and updates connStatus. The SDK keeps
// its own gRPC reconnect loop; this only reflects the outcome so components can
// report live health rather than just the SharedNode wrapper lifecycle.
func (c *Client) probeLoop() {
	defer close(c.probeDone)
	t := time.NewTicker(HealthProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-c.probeStop:
			return
		case <-t.C:
			c.applyProbe(c.naming.ServerHealthy())
		}
	}
}

func (c *Client) applyProbe(healthy bool) {
	if healthy {
		atomic.StoreInt32(&c.connStatus, connStateConnected)
		c.setConnMsg("")
	} else {
		atomic.StoreInt32(&c.connStatus, connStateReconnecting)
		c.setConnMsg("nacos server unhealthy")
	}
}

func (c *Client) setConnMsg(msg string) {
	c.connMsgMu.Lock()
	c.connMsg = msg
	c.connMsgMu.Unlock()
}

// ConnState reports the live connection health as (connected, lastMessage).
// Components map this to types.StatusInfo in their ConnectionStatus().
func (c *Client) ConnState() (connected bool, msg string) {
	c.connMsgMu.RLock()
	msg = c.connMsg
	c.connMsgMu.RUnlock()
	return atomic.LoadInt32(&c.connStatus) == connStateConnected, msg
}

func keyOf(serviceName, group string) string {
	return group + "@@" + serviceName
}

// Select round-robins a healthy instance: uses the subscription cache first, queries on miss.
func (c *Client) Select(serviceName, group string) (host string, port uint64, err error) {
	group = EffectiveGroup(group)
	k := keyOf(serviceName, group)
	c.mu.RLock()
	insts := c.instances[k]
	c.mu.RUnlock()

	if len(insts) == 0 {
		insts, err = c.naming.SelectInstances(vo.SelectInstancesParam{
			ServiceName: serviceName,
			GroupName:   group,
			HealthyOnly: true,
		})
		if err != nil {
			return "", 0, fmt.Errorf("select instances: %w", err)
		}
		if len(insts) == 0 {
			return "", 0, fmt.Errorf("no available instance: %s", serviceName)
		}
	}
	idx := atomic.AddUint64(c.counter(k), 1)
	inst := insts[int(idx-1)%len(insts)]
	return inst.Ip, inst.Port, nil
}

func (c *Client) counter(k string) *uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.counters[k]
	if !ok {
		p = new(uint64)
		c.counters[k] = p
	}
	return p
}

// Subscribe subscribes to service instance changes and caches healthy instances.
func (c *Client) Subscribe(serviceName, group string) error {
	group = EffectiveGroup(group)
	err := c.naming.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   group,
		SubscribeCallback: func(ins []model.Instance, err error) {
			if err != nil {
				return
			}
			// cache only healthy, enabled instances with weight>0, matching SelectInstances semantics
			healthy := make([]model.Instance, 0, len(ins))
			for _, in := range ins {
				if in.Healthy && in.Enable && in.Weight > 0 {
					healthy = append(healthy, in)
				}
			}
			k := keyOf(serviceName, group)
			c.mu.Lock()
			if len(healthy) > 0 {
				c.instances[k] = healthy
			} else {
				delete(c.instances, k)
			}
			c.mu.Unlock()
		},
	})
	if err == nil {
		c.mu.Lock()
		c.subs = append(c.subs, subKey{serviceName, group})
		c.mu.Unlock()
	}
	return err
}

// GetConfig reads a config.
func (c *Client) GetConfig(dataId, group string) (string, error) {
	return c.config.GetConfig(vo.ConfigParam{DataId: dataId, Group: EffectiveGroup(group)})
}

// PublishConfig publishes a config (full overwrite: create if absent, overwrite if present).
func (c *Client) PublishConfig(dataId, group, content, contentType string) error {
	ok, err := c.config.PublishConfig(vo.ConfigParam{
		DataId:  dataId,
		Group:   EffectiveGroup(group),
		Content: content,
		Type:    contentType,
	})
	if err != nil {
		return fmt.Errorf("publish config: %w", err)
	}
	if !ok {
		return fmt.Errorf("publish config returned false")
	}
	return nil
}

// ListenConfig listens for config changes.
func (c *Client) ListenConfig(dataId, group string, onChange func(data string)) error {
	group = EffectiveGroup(group)
	raw := func(namespace, grp, did, data string) { onChange(data) }
	c.listenMu.Lock()
	c.listeners[keyOf(dataId, group)] = raw
	c.listenMu.Unlock()
	return c.config.ListenConfig(vo.ConfigParam{DataId: dataId, Group: group, OnChange: raw})
}

// CancelListenConfig cancels a config listener.
func (c *Client) CancelListenConfig(dataId, group string) error {
	group = EffectiveGroup(group)
	c.listenMu.Lock()
	raw, ok := c.listeners[keyOf(dataId, group)]
	if ok {
		delete(c.listeners, keyOf(dataId, group))
	}
	c.listenMu.Unlock()
	if !ok {
		return nil
	}
	return c.config.CancelListenConfig(vo.ConfigParam{DataId: dataId, Group: group, OnChange: raw})
}

// RegisterInstance registers a service instance (ephemeral + heartbeat).
func (c *Client) RegisterInstance(serviceName, ip string, port uint64, group string) error {
	group = EffectiveGroup(group)
	_, err := c.naming.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		GroupName:   group,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
	})
	return err
}

// DeregisterInstance deregisters a service instance.
func (c *Client) DeregisterInstance(serviceName, ip string, port uint64, group string) error {
	group = EffectiveGroup(group)
	_, err := c.naming.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        port,
		ServiceName: serviceName,
		GroupName:   group,
		Ephemeral:   true,
	})
	return err
}

// Close unsubscribes all services and closes the naming/config clients.
func (c *Client) Close() error {
	// Stop the health probe goroutine and wait for it to exit, so no probe
	// runs against the naming client after CloseClient below.
	if c.probeStop != nil {
		c.probeOnce.Do(func() { close(c.probeStop) })
		<-c.probeDone
	}
	atomic.StoreInt32(&c.connStatus, connStateUnknown)
	c.mu.Lock()
	subs := c.subs
	c.subs = nil
	c.mu.Unlock()
	for _, s := range subs {
		_ = c.naming.Unsubscribe(&vo.SubscribeParam{
			ServiceName: s.serviceName,
			GroupName:   s.group,
		})
	}
	c.listenMu.Lock()
	c.listeners = nil
	c.listenMu.Unlock()
	c.naming.CloseClient()
	c.config.CloseClient()
	return nil
}
