package nacos

import (
	"github.com/rulego/rulego"
	"github.com/rulego/rulego-components-discovery/pkg"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/maps"
)

func init() { _ = rulego.Registry.Register(&ConfigGetNode{}) }

// ConfigGetConfig holds the config for reading nacos config.
type ConfigGetConfig struct {
	discovery.ConnConfig `json:",squash"`
	DataId               string `json:"dataId" label:"Data ID" desc:"Config Data ID" required:"true"`
	Group                string `json:"group" label:"Group" desc:"Config group, default DEFAULT_GROUP"`
}

// ConfigGetNode reads config content from nacos into msg.Data.
type ConfigGetNode struct {
	base.SharedNode[*discovery.Client]
	Config ConfigGetConfig
}

func (x *ConfigGetNode) New() types.Node {
	return &ConfigGetNode{Config: ConfigGetConfig{ConnConfig: discovery.ConnConfig{ServerAddrs: discovery.DefaultServerAddrs}, Group: discovery.DefaultGroup}}
}

func (x *ConfigGetNode) Type() string { return "x/nacosConfigGet" }

func (x *ConfigGetNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := maps.Map2Struct(configuration, &x.Config); err != nil {
		return err
	}
	if err := x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.ServerAddrs, ruleConfig.NodeClientInitNow,
		func() (*discovery.Client, error) { return discovery.NewClient(x.Config.ConnConfig) },
		func(c *discovery.Client) error { return c.Close() }); err != nil {
		return err
	}
	x.SharedNode.BindChain(configuration)
	return nil
}

func (x *ConfigGetNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	content, err := client.GetConfig(x.Config.DataId, x.Config.Group)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	msg.SetData(content)
	ctx.TellSuccess(msg)
}

func (x *ConfigGetNode) Destroy() { _ = x.SharedNode.Close() }

// ConnectionStatus reports the live Nacos connection health, overriding the
// SharedNode default (which only reflects the wrapper lifecycle).
func (x *ConfigGetNode) ConnectionStatus() types.StatusInfo {
	if c, ok := x.SharedNode.Instance(); ok {
		return clientConnStatus(c)
	}
	return x.SharedNode.ConnectionStatus()
}

func (x *ConfigGetNode) Desc() string { return "Read config content from nacos" }
