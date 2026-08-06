package nacos

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rulego/rulego-components-discovery/pkg"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/utils/maps"
	"github.com/rulego/rulego/utils/runtime"
)

func init() { _ = endpoint.Registry.Register(&Endpoint{}) }

// Type is the component type.
const Type = types.EndpointTypePrefix + "nacos"

// Endpoint is an alias for ConfigEndpoint.
type Endpoint = ConfigEndpoint

// Config holds the config for the config-listen endpoint.
type Config struct {
	discovery.ConnConfig `json:",squash"`
	Group                string `json:"group" label:"Group" desc:"Config group, default DEFAULT_GROUP"`
}

// ConfigEndpoint subscribes to nacos config; config changes trigger the rule chain.
type ConfigEndpoint struct {
	impl.BaseEndpoint
	base.SharedNode[*discovery.Client]
	base.GracefulShutdown
	RuleConfig      types.Config
	Config          Config
	mu              sync.RWMutex
	dataIdRouterMap map[string][]endpointApi.Router
}

func (x *ConfigEndpoint) Type() string { return Type }
func (x *ConfigEndpoint) Id() string   { return x.Config.ServerAddrs }

func (x *ConfigEndpoint) New() types.Node {
	return &ConfigEndpoint{Config: Config{ConnConfig: discovery.ConnConfig{ServerAddrs: discovery.DefaultServerAddrs}, Group: discovery.DefaultGroup}}
}

func (x *ConfigEndpoint) Def() types.ComponentForm {
	return types.ComponentForm{
		Desc: "nacos config listen endpoint, auto triggers rule chain on config change",
		RouterForm: &types.RouterForm{
			From: &types.RouterFormField{
				Path: types.ComponentFormField{
					Name: "path", Type: "string", Label: "Data ID",
					Desc: "Config dataId to listen, comma-separated for multiple", Required: true,
				},
			},
		},
	}
}

func (x *ConfigEndpoint) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := maps.Map2Struct(configuration, &x.Config); err != nil {
		return err
	}
	x.RuleConfig = ruleConfig
	x.GracefulShutdown.InitGracefulShutdown(ruleConfig.Logger, 0)
	x.dataIdRouterMap = map[string][]endpointApi.Router{}
	// Soft-fail init: nacos not ready at startup must not block; GetSafely retries with cooldown.
	_ = x.SharedNode.InitWithCloseSoftFail(ruleConfig, x.Type(), x.Config.ServerAddrs, true,
		func() (*discovery.Client, error) { return discovery.NewClient(x.Config.ConnConfig) },
		func(c *discovery.Client) error { return c.Close() })
	x.SharedNode.BindChain(configuration)
	return nil
}

func (x *ConfigEndpoint) AddRouter(router endpointApi.Router, params ...interface{}) (string, error) {
	if router == nil || router.GetFrom() == nil {
		return "", errors.New("router or from can not nil")
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return "", err
	}
	routerId := x.CheckAndSetRouterId(router)
	group := x.Config.Group
	dataIds := splitDataIds(router.GetFrom().ToString())
	x.mu.Lock()
	if x.routerExistsLocked(routerId) {
		x.mu.Unlock()
		return routerId, fmt.Errorf("routerId %s already exists", routerId)
	}
	for _, dataId := range dataIds {
		x.dataIdRouterMap[dataId] = append(x.dataIdRouterMap[dataId], router)
	}
	x.mu.Unlock()
	// subscribe one by one; on failure cancel already-subscribed dataIds and roll back the registry
	for i, dataId := range dataIds {
		if err := client.ListenConfig(dataId, group, func(data string) {
			x.handlerMsg(dataId, []byte(data))
		}); err != nil {
			for j := 0; j < i; j++ {
				_ = client.CancelListenConfig(dataIds[j], group)
			}
			x.mu.Lock()
			x.removeRouterLocked(routerId)
			x.mu.Unlock()
			return routerId, err
		}
	}
	return routerId, nil
}

func (x *ConfigEndpoint) RemoveRouter(routerId string, params ...interface{}) error {
	x.mu.Lock()
	emptied := x.removeRouterLocked(routerId)
	group := x.Config.Group
	x.mu.Unlock()
	if len(emptied) == 0 {
		return nil
	}
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		return err
	}
	for _, dataId := range emptied {
		_ = client.CancelListenConfig(dataId, group)
	}
	return nil
}

// routerExistsLocked reports whether routerId is registered; caller must hold x.mu.
func (x *ConfigEndpoint) routerExistsLocked(routerId string) bool {
	for _, routers := range x.dataIdRouterMap {
		for _, r := range routers {
			if r.GetId() == routerId {
				return true
			}
		}
	}
	return false
}

// removeRouterLocked removes the routerId across all dataIds, returning dataIds that became empty.
func (x *ConfigEndpoint) removeRouterLocked(routerId string) []string {
	var emptied []string
	for dataId, routers := range x.dataIdRouterMap {
		kept := routers[:0]
		for _, r := range routers {
			if r.GetId() != routerId {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			delete(x.dataIdRouterMap, dataId)
			emptied = append(emptied, dataId)
		} else {
			x.dataIdRouterMap[dataId] = kept
		}
	}
	return emptied
}

func (x *ConfigEndpoint) Start() error {
	if !x.SharedNode.IsInit() {
		_, err := x.SharedNode.GetSafely()
		return err
	}
	return nil
}

func (x *ConfigEndpoint) Destroy() {
	x.GracefulShutdown.GracefulStop(func() { _ = x.Close() })
}

func (x *ConfigEndpoint) Close() error {
	x.BaseEndpoint.Destroy()
	_ = x.SharedNode.Close()
	x.mu.Lock()
	x.dataIdRouterMap = nil
	x.mu.Unlock()
	return nil
}

// ConnectionStatus reports the live Nacos connection health, overriding the
// SharedNode default (which only reflects the wrapper lifecycle).
func (x *ConfigEndpoint) ConnectionStatus() types.StatusInfo {
	if c, ok := x.SharedNode.Instance(); ok {
		return clientConnStatus(c)
	}
	return x.SharedNode.ConnectionStatus()
}

func (x *ConfigEndpoint) Printf(format string, v ...interface{}) {
	if x.RuleConfig.Logger != nil {
		x.RuleConfig.Logger.Printf(format, v...)
	}
}

// handlerMsg is the config-change callback: finds routers by dataId and triggers the rule chain.
func (x *ConfigEndpoint) handlerMsg(dataId string, body []byte) {
	defer func() {
		if e := recover(); e != nil {
			x.Printf("nacos config endpoint handler err:\n%v", runtime.Stack())
		}
	}()
	x.mu.RLock()
	routers := x.dataIdRouterMap[dataId]
	x.mu.RUnlock()
	for _, router := range routers {
		exchange := &endpointApi.Exchange{
			In:  &RequestMessage{dataId: dataId, body: body},
			Out: &ResponseMessage{},
		}
		x.DoProcess(context.Background(), router, exchange)
	}
}

// splitDataIds splits comma-separated dataIds.
func splitDataIds(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			d := s[start:i]
			if d != "" {
				out = append(out, d)
			}
			start = i + 1
		}
	}
	return out
}
