package nacos

import (
	"fmt"
	"time"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego-components-discovery/pkg"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/base"
	"github.com/rulego/rulego/utils/el"
	"github.com/rulego/rulego/utils/maps"
	"github.com/valyala/fasthttp"
)

func init() { _ = rulego.Registry.Register(&ServiceCallNode{}) }

// ServiceCallConfig holds the config for calling a service via nacos discovery.
type ServiceCallConfig struct {
	discovery.ConnConfig `json:",squash"`
	ServiceName          string `json:"serviceName" label:"Service Name" desc:"Target service name" required:"true"`
	GroupName            string `json:"groupName" label:"Group" desc:"Service group, default DEFAULT_GROUP"`
	Method               string `json:"method" label:"Method" desc:"HTTP method"`
	Path                 string `json:"path" label:"Path" desc:"Request path, supports ${msg.xx}"`
	Body                 string `json:"body" label:"Body" desc:"Request body, supports ${msg.xx}; empty uses msg data"`
}

// ServiceCallNode calls a cluster microservice by service name via nacos discovery.
type ServiceCallNode struct {
	base.SharedNode[*discovery.Client]
	Config   ServiceCallConfig
	pathTmpl el.Template
	bodyTmpl el.Template
}

func (x *ServiceCallNode) New() types.Node {
	return &ServiceCallNode{Config: ServiceCallConfig{ConnConfig: discovery.ConnConfig{ServerAddrs: discovery.DefaultServerAddrs}, Method: "GET"}}
}

func (x *ServiceCallNode) Type() string { return "x/nacosServiceCall" }

func (x *ServiceCallNode) Init(ruleConfig types.Config, configuration types.Configuration) error {
	if err := maps.Map2Struct(configuration, &x.Config); err != nil {
		return err
	}
	x.pathTmpl, _ = el.NewTemplate(x.Config.Path)
	x.bodyTmpl, _ = el.NewTemplate(x.Config.Body)
	if err := x.SharedNode.InitWithClose(ruleConfig, x.Type(), x.Config.ServerAddrs, ruleConfig.NodeClientInitNow,
		func() (*discovery.Client, error) { return discovery.NewClient(x.Config.ConnConfig) },
		func(c *discovery.Client) error { return c.Close() }); err != nil {
		return err
	}
	x.SharedNode.BindChain(configuration)
	return nil
}

func (x *ServiceCallNode) OnMsg(ctx types.RuleContext, msg types.RuleMsg) {
	client, err := x.SharedNode.GetSafely()
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	host, port, err := client.Select(x.Config.ServiceName, x.Config.GroupName)
	if err != nil {
		ctx.TellFailure(msg, err)
		return
	}
	env := base.NodeUtils.GetEvnAndMetadata(ctx, msg)
	path := x.Config.Path
	if x.pathTmpl.HasVar() {
		path = x.pathTmpl.ExecuteAsString(env)
	}
	// default request body is the message data; overridden when Body (template or literal) is set
	body := msg.GetData()
	if x.Config.Body != "" {
		if x.bodyTmpl.HasVar() {
			body = x.bodyTmpl.ExecuteAsString(env)
		} else {
			body = x.Config.Body
		}
	}

	url := fmt.Sprintf("http://%s:%d%s", host, port, path)
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod(x.Config.Method)
	if body != "" {
		req.SetBody([]byte(body))
	}
	timeout := time.Duration(x.Config.Timeout) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(discovery.DefaultTimeoutSec) * time.Second
	}
	if err := fasthttp.DoTimeout(req, resp, timeout); err != nil {
		ctx.TellFailure(msg, fmt.Errorf("call %s: %w", x.Config.ServiceName, err))
		return
	}
	msg.SetData(string(resp.Body()))
	if code := resp.StatusCode(); code >= 400 {
		ctx.TellFailure(msg, fmt.Errorf("call %s returned HTTP %d", x.Config.ServiceName, code))
		return
	}
	ctx.TellSuccess(msg)
}

func (x *ServiceCallNode) Destroy() {
	_ = x.SharedNode.Close()
}

func (x *ServiceCallNode) Desc() string {
	return "Call cluster microservice via nacos service discovery"
}
