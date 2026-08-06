package nacos

import "github.com/rulego/rulego/api/types"

var (
	_ types.ConnectionStatusGetter = (*ConfigGetNode)(nil)
	_ types.ConnectionStatusGetter = (*ConfigSetNode)(nil)
	_ types.ConnectionStatusGetter = (*ServiceCallNode)(nil)
)
