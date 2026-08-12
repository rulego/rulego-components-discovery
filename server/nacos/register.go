package nacos

import (
	"net"

	discovery "github.com/rulego/rulego-components-discovery/pkg"
)

// RegisterConfig holds the config for service registration.
type RegisterConfig struct {
	discovery.ConnConfig `json:",squash"`
	ServiceName          string `json:"serviceName" label:"Service Name" desc:"Service name to register" required:"true"`
	GroupName            string `json:"groupName" label:"Group" desc:"Service group, default DEFAULT_GROUP"`
	Ip                   string `json:"ip" label:"IP" desc:"Local IP, auto-detected if empty"`
	Port                 uint64 `json:"port" label:"Port" desc:"Service port"`
}

// Register registers an instance to nacos, returning the client and a deregister cleanup.
func Register(cfg RegisterConfig, opts ...discovery.Option) (*discovery.Client, func(), error) {
	client, err := discovery.NewClient(cfg.ConnConfig, opts...)
	if err != nil {
		return nil, nil, err
	}
	ip := cfg.Ip
	if ip == "" {
		ip = localIP()
	}
	if err := client.RegisterInstance(cfg.ServiceName, ip, cfg.Port, cfg.GroupName); err != nil {
		client.Close()
		return nil, nil, err
	}
	cleanup := func() {
		_ = client.DeregisterInstance(cfg.ServiceName, ip, cfg.Port, cfg.GroupName)
		client.Close()
	}
	return client, cleanup, nil
}

// localIP detects the local non-loopback IPv4, falling back to 127.0.0.1.
func localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ip4 := ipNet.IP.To4(); ip4 != nil {
				return ip4.String()
			}
		}
	}
	return "127.0.0.1"
}
