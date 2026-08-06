package nacos

import (
	"github.com/rulego/rulego-components-discovery/pkg"
	"github.com/rulego/rulego/api/types"
)

// clientConnStatus maps the live Nacos connection health exposed by
// discovery.Client to a types.StatusInfo. Falls back to StatusNone when the
// probe has not yet produced a result (e.g. right after init).
func clientConnStatus(c *discovery.Client) types.StatusInfo {
	if c == nil {
		return types.StatusInfo{}
	}
	connected, msg := c.ConnState()
	if connected {
		return types.StatusInfo{Status: types.StatusConnected}
	}
	// Reconnecting covers both initial connect failure and mid-run gRPC loss;
	// the SDK drives its own reconnect loop, so we never report a hard error.
	return types.StatusInfo{Status: types.StatusReconnecting, Message: msg}
}
