package nacos

import (
	"github.com/rulego/rulego"
	"github.com/rulego/rulego-components-discovery/pkg"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
)

func init() { _ = rulego.Registry.Register(&ConfigSetNode{}) }

// ConfigSetConfig holds the config for writing nacos config.
type ConfigSetConfig struct {
	discovery.ConnConfig `json:",squash"`
	DataId               string `json:"dataId" label:"Data ID" desc:"Config Data ID" required:"true"`
	Group                string `json:"group" label:"Group" desc:"Config group, default DEFAULT_GROUP"`
	Content              string `json:"content" label:"Content" desc:"Config content, supports ${msg.xx}"`
	Type                 string `json:"format" label:"Format" desc:"Config format: json/yaml/properties/text"`
}

// ConfigSetNode writes (publishes) config content to nacos.
type ConfigSetNode struct {
	base.SharedNode[*discovery.Client]
	Config      ConfigSetConfig
	contentTmpl el.Template
}

func (x *ConfigSetNode) New() types.Node {
	return &ConfigSetNode{Config: ConfigSetConfig{ConnConfig: discovery.ConnConfig{ServerAddrs: discovery.DefaultServerAddrs}, Group: discovery.DefaultGroup, Type: "json"}}
}

func (x *ConfigSetNode) Type() string { return "x/nacosConfigSet" }

func (x *ConfigSetNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := maps.Map2Struct(configuration, &x.Config); err != nil {
		return err
	}
	x.contentTmpl, _ = el.NewTemplate(x.Config.Content)
	if err := x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.ServerAddrs, ruleConfig.NodeClientInitNow,
		func() (*discovery.Client, error) {
			return discovery.NewClient(x.Config.ConnConfig, discovery.WithLogger(ruleConfig.Logger))
		},
		func(c *discovery.Client) error { return c.Close() }); err != nil {
		return err
	}
	x.SharedNode.BindChain(configuration)
	return nil
}

func (x *ConfigSetNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	// content supports the ${msg.xx} template; used as-is when no variable is present
	content := x.Config.Content
	if x.contentTmpl.HasVar() {
		env := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
		content = x.contentTmpl.ExecuteAsString(env)
	}
	if err := client.PublishConfig(x.Config.DataId, x.Config.Group, content, x.Config.Type); err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	ctx.TellSuccess(msg)
}

func (x *ConfigSetNode) Destroy() { _ = x.SharedNode.Close() }

// ConnectionStatus reports the live Nacos connection health, overriding the
// SharedNode default (which only reflects the wrapper lifecycle).
func (x *ConfigSetNode) ConnectionStatus() types.StatusInfo {
	if c, ok := x.SharedNode.Instance(); ok {
		return clientConnStatus(c)
	}
	return x.SharedNode.ConnectionStatus()
}

func (x *ConfigSetNode) Desc() string { return "Set (publish) config content to nacos" }
