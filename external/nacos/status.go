/*
 * Copyright 2024 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
