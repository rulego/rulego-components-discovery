# rulego-components-discovery

[RuleGo](https://github.com/rulego/rulego) 的服务发现与配置中心组件库 — 服务调用、配置读写、配置变更端点、服务注册。

[文档](https://rulego.cc/pages/discovery-overview/)

## 组件一览

### 节点（`external/nacos/`）

| 类型 | 说明 |
|------|------|
| `x/nacosServiceCall` | 服务发现 + HTTP 调用。轮询选取健康实例并转发请求；path 和 body 支持 `${msg.xx}` 模板 |
| `x/nacosConfigGet` | 按 Data ID + Group 从 Nacos 读取配置内容，输出到 `msg.Data` |
| `x/nacosConfigSet` | 全量写入（发布）配置到 Nacos；content 支持 `${msg.xx}` 模板 |

### 端点（`endpoint/nacos/`）

| 类型 | 说明 |
|------|------|
| `endpoint/nacos` | 订阅 Nacos 配置变更。路由 `from.path` = Data ID（逗号分隔可监听多个），配置变更自动触发规则链 |

### Server Hook（`server/nacos/`）

| 函数 | 说明 |
|------|------|
| `Register()` | 将当前进程注册为 Nacos 临时实例（含心跳），返回 cleanup 函数用于优雅反注册 |

## 快速开始

```go
import (
    _ "github.com/rulego/rulego-components-discovery/external/nacos" // x/nacosServiceCall + x/nacosConfigGet + x/nacosConfigSet
    _ "github.com/rulego/rulego-components-discovery/endpoint/nacos"  // endpoint/nacos
)
```

## 连接复用

所有节点内嵌 `base.SharedNode`，相同 `server` 地址的多个节点自动共享同一个 Nacos 客户端连接。也可通过 `ref://ownerNodeId` 显式复用其他节点的连接。

## 规则链 DSL 示例

### 服务调用

```json
{
  "ruleChain": {"name": "service-call-demo", "root": true},
  "metadata": {
    "nodes": [
      {"id": "n1", "type": "x/nacosServiceCall", "configuration": {
        "server": "127.0.0.1:8848",
        "serviceName": "order-service",
        "method": "POST",
        "path": "/api/orders/${msg.orderId}",
        "body": "${msg.data}"
      }},
      {"id": "n2", "type": "log", "configuration": {"jsScript": "return msg;"}}
    ],
    "connections": [
      {"fromId": "n1", "toId": "n2", "type": "Success"}
    ]
  }
}
```

### 配置读写

```json
{
  "ruleChain": {"name": "config-demo", "root": true},
  "metadata": {
    "nodes": [
      {"id": "r1", "type": "x/nacosConfigGet", "configuration": {
        "server": "127.0.0.1:8848",
        "dataId": "app-config.json",
        "group": "DEFAULT_GROUP"
      }},
      {"id": "w1", "type": "x/nacosConfigSet", "configuration": {
        "server": "127.0.0.1:8848",
        "dataId": "dynamic-flags.json",
        "content": "{\"enabled\": ${msg.enabled}}",
        "format": "json"
      }}
    ],
    "connections": [
      {"fromId": "r1", "toId": "w1", "type": "Success"}
    ]
  }
}
```

### 配置变更端点

```json
{
  "ruleChain": {"name": "config-watch", "root": true},
  "metadata": {
    "nodes": [
      {"id": "e1", "type": "endpoint/nacos", "configuration": {
        "server": "127.0.0.1:8848",
        "group": "DEFAULT_GROUP"
      }},
      {"id": "p1", "type": "js", "configuration": {
        "jsScript": "return {'config': msg, 'dataId': metadata.dataId};"
      }},
      {"id": "n1", "type": "log", "configuration": {"jsScript": "return msg;"}}
    ],
    "connections": [
      {"fromId": "e1", "toId": "p1", "type": "ip"},
      {"fromId": "p1", "toId": "n1", "type": "Success"}
    ]
  }
}
```

> 路由 `from.path` 填写要监听的 Data ID（逗号分隔可监听多个）。配置变更时自动触发规则链，新内容写入 `msg.Data`，Data ID 写入 `metadata.dataId`。

## 连接配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `server` | string | `127.0.0.1:8848` | Nacos 服务器地址，逗号分隔多个 |
| `namespace` | string | — | 命名空间 ID |
| `timeout` | int | `5` | 请求超时（秒） |
| `username` | string | — | 用户名（Nacos 开放鉴权） |
| `password` | string | — | 密码 |
| `accessKey` | string | — | 阿里云 AccessKey |
| `secretKey` | string | — | 阿里云 SecretKey |
| `certFile` | string | — | 客户端证书（启用 TLS） |
| `certKeyFile` | string | — | 客户端私钥 |
| `caFile` | string | — | CA 证书 |
| `insecureSkipVerify` | bool | `false` | 跳过证书验证 |

## 许可证

Apache 2.0
