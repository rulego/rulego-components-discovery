# rulego-components-discovery

Service discovery & configuration center components for [RuleGo](https://github.com/rulego/rulego) — service call, config read/write, config-change endpoint, and service registration.

[Doc](https://rulego.cc/en/pages/discovery-overview/)

## Components

### Nodes (`external/nacos/`)

| Type | Description |
|------|-------------|
| `x/nacosServiceCall` | Service discovery + HTTP call. Selects a healthy instance via round-robin and forwards the request; supports `${msg.xx}` in path and body |
| `x/nacosConfigGet` | Read config content from Nacos by Data ID + Group, output to `msg.Data` |
| `x/nacosConfigSet` | Publish (full overwrite) config to Nacos; content supports `${msg.xx}` template |

### Endpoint (`endpoint/nacos/`)

| Type | Description |
|------|-------------|
| `endpoint/nacos` | Subscribe to Nacos config changes. Router `from.path` = Data ID (comma-separated for multiple); config change automatically triggers the rule chain |

### Server Hook (`server/nacos/`)

| Function | Description |
|----------|-------------|
| `Register()` | Register the current process as an ephemeral instance to Nacos (with heartbeat); returns a cleanup function for graceful deregistration |

## Quick Start

```go
import (
    _ "github.com/rulego/rulego-components-discovery/external/nacos" // x/nacosServiceCall + x/nacosConfigGet + x/nacosConfigSet
    _ "github.com/rulego/rulego-components-discovery/endpoint/nacos"  // endpoint/nacos
)
```

## Connection Reuse

All nodes embed `base.SharedNode` — nodes with the same `server` address automatically share a single Nacos client connection. Use `ref://ownerNodeId` to explicitly reuse another node's connection.

## Rule Chain DSL Examples

### Service Call

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

### Config Read / Write

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

### Config Change Endpoint

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

> Set router `from.path` to the Data ID to listen (comma-separated for multiple). When the config changes, the rule chain is triggered with the new content as `msg.Data` and the Data ID in `metadata.dataId`.

## Connection Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `server` | string | `127.0.0.1:8848` | Comma-separated Nacos server addresses |
| `namespace` | string | — | Namespace ID |
| `timeout` | int | `5` | Request timeout in seconds |
| `username` | string | — | Username (Nacos open auth) |
| `password` | string | — | Password |
| `accessKey` | string | — | Alibaba Cloud AccessKey |
| `secretKey` | string | — | Alibaba Cloud SecretKey |
| `certFile` | string | — | Client certificate (enables TLS) |
| `certKeyFile` | string | — | Client private key |
| `caFile` | string | — | CA certificate |
| `insecureSkipVerify` | bool | `false` | Skip certificate verification |

## License

Apache 2.0
