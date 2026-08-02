package discovery

import (
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
)

// defaults
const (
	DefaultTimeoutSec = 5
	DefaultContext    = "/nacos"
	DefaultGroup      = "DEFAULT_GROUP"
	DefaultPort       = 8848
	// DefaultServerAddrs is the default nacos dev address, used as the component server default.
	DefaultServerAddrs = "127.0.0.1:8848"

	grpcPortOffset = 1000

	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// ConnConfig holds the shared connection config.
type ConnConfig struct {
	ServerAddrs string     `json:"server" label:"Server" desc:"Comma-separated address list, e.g. 127.0.0.1:8848" required:"true" ref:"primary"`
	Namespace   string     `json:"namespace" label:"Namespace" desc:"Namespace ID" ref:"shared"`
	Timeout     int64      `json:"timeout" label:"Timeout" desc:"Request timeout in seconds, default 5"`
	Auth        AuthConfig `json:"auth" label:"Auth"`
	TLS         TlsConfig  `json:"tls" label:"TLS"`
}

// AuthConfig holds the auth config.
type AuthConfig struct {
	Username  string `json:"username" label:"Username" desc:"Username (nacos open auth)" ref:"shared"`
	Password  string `json:"password" label:"Password" desc:"Password" ref:"shared"`
	AccessKey string `json:"accessKey" label:"AccessKey" desc:"Alibaba Cloud AccessKey" ref:"shared"`
	SecretKey string `json:"secretKey" label:"SecretKey" desc:"Alibaba Cloud SecretKey" ref:"shared"`
}

// TlsConfig holds the TLS config.
type TlsConfig struct {
	CertFile           string `json:"certFile" label:"Cert File" desc:"Client certificate"`
	CertKeyFile        string `json:"certKeyFile" label:"Cert Key File" desc:"Client private key"`
	CAFile             string `json:"caFile" label:"CA File" desc:"CA certificate"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify" label:"Skip Verify" desc:"Skip certificate verification"`
}

// EffectiveGroup returns the effective group, falling back to DEFAULT_GROUP when empty.
func EffectiveGroup(group string) string {
	if group == "" {
		return DefaultGroup
	}
	return group
}

// toClientConfig builds the nacos client config.
func (c ConnConfig) toClientConfig() constant.ClientConfig {
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeoutSec
	}
	return constant.ClientConfig{
		NamespaceId:         c.Namespace,
		TimeoutMs:           uint64(timeout) * 1000,
		NotLoadCacheAtStart: true,
		Username:            c.Auth.Username,
		Password:            c.Auth.Password,
		AccessKey:           c.Auth.AccessKey,
		SecretKey:           c.Auth.SecretKey,
		LogLevel:            "warn",
		TLSCfg:              c.TLS.toNacosTLS(),
	}
}

// toServerConfigs parses the comma-separated address list into nacos server configs.
func (c ConnConfig) toServerConfigs() []constant.ServerConfig {
	scheme := schemeHTTP
	if c.TLS.CAFile != "" || c.TLS.CertFile != "" {
		scheme = schemeHTTPS
	}
	var sc []constant.ServerConfig
	for addr := range strings.SplitSeq(c.ServerAddrs, ",") {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		host, port := splitHostPort(addr, DefaultPort)
		sc = append(sc, constant.ServerConfig{
			IpAddr:      host,
			Port:        port,
			Scheme:      scheme,
			ContextPath: DefaultContext,
			GrpcPort:    port + grpcPortOffset,
		})
	}
	return sc
}

// splitHostPort parses host:port, using the default port when missing.
func splitHostPort(addr string, defaultPort uint64) (string, uint64) {
	host, portStr, found := strings.Cut(addr, ":")
	if !found || portStr == "" {
		return addr, defaultPort
	}
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return host, defaultPort
	}
	return host, port
}

// toNacosTLS builds the nacos TLSConfig (passes file paths; the SDK loads them).
func (t TlsConfig) toNacosTLS() constant.TLSConfig {
	enable := t.CertFile != "" || t.CertKeyFile != "" || t.CAFile != "" || t.InsecureSkipVerify
	return constant.TLSConfig{
		Enable:   enable,
		TrustAll: t.InsecureSkipVerify,
		CaFile:   t.CAFile,
		CertFile: t.CertFile,
		KeyFile:  t.CertKeyFile,
	}
}
