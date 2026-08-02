package discovery

import "testing"

func TestToServerConfigs(t *testing.T) {
	// multiple addresses + explicit port
	cfg := ConnConfig{ServerAddrs: "127.0.0.1:8848, 192.168.1.1:8848"}
	sc := cfg.toServerConfigs()
	if len(sc) != 2 {
		t.Fatalf("want 2 servers, got %d", len(sc))
	}
	if sc[0].IpAddr != "127.0.0.1" || sc[0].Port != 8848 {
		t.Errorf("bad sc[0]: %+v", sc[0])
	}
	if sc[0].GrpcPort != 9848 {
		t.Errorf("grpc port want 9848 (port+1000), got %d", sc[0].GrpcPort)
	}
	if sc[0].Scheme != "http" {
		t.Errorf("scheme want http, got %s", sc[0].Scheme)
	}

	// default port
	sc2 := ConnConfig{ServerAddrs: "nacos.host"}.toServerConfigs()
	if sc2[0].Port != 8848 {
		t.Errorf("default port want 8848, got %d", sc2[0].Port)
	}

	// empty address
	if len(ConnConfig{}.toServerConfigs()) != 0 {
		t.Errorf("empty addrs want 0 servers")
	}

	// with TLS uses https
	sc4 := ConnConfig{ServerAddrs: "h:8848", TLS: TlsConfig{CAFile: "ca.pem"}}.toServerConfigs()
	if sc4[0].Scheme != "https" {
		t.Errorf("tls scheme want https, got %s", sc4[0].Scheme)
	}
}

func TestEffectiveGroup(t *testing.T) {
	if g := EffectiveGroup(""); g != DefaultGroup {
		t.Errorf("empty want %s, got %s", DefaultGroup, g)
	}
	if g := EffectiveGroup("G1"); g != "G1" {
		t.Errorf("non-empty want G1, got %s", g)
	}
}

func TestSplitHostPort(t *testing.T) {
	tests := []struct {
		addr     string
		wantHost string
		wantPort uint64
	}{
		{"127.0.0.1:8848", "127.0.0.1", 8848},
		{"nacos.host", "nacos.host", DefaultPort},
		{"host:abc", "host", DefaultPort},
	}
	for _, tt := range tests {
		host, port := splitHostPort(tt.addr, DefaultPort)
		if host != tt.wantHost || port != tt.wantPort {
			t.Errorf("splitHostPort(%q) = (%s,%d), want (%s,%d)", tt.addr, host, port, tt.wantHost, tt.wantPort)
		}
	}
}
