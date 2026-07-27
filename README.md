# Bridge Main

WeChat 消息桥接主平台，负责上下游插件的连接、消息路由和转发。

## 架构

```
┌─────────────────┐     ┌─────────────────┐
│  ilink_plugin   │◄───►│                 │
│   (上游插件)     │     │   Bridge Main   │
└─────────────────┘     │    (主平台)      │
                        │                 │
┌─────────────────┐     │  - 消息路由      │
│  opencode_plugin│◄───►│  - 会话管理      │
│   (下游插件)     │     │  - 权限控制      │
└─────────────────┘     └─────────────────┘
```

## 目录结构

```
bridge-main/
├── main.py              # 入口文件
├── core/                # 核心模块
│   ├── platform.py      # 平台管理
│   ├── router.py        # 消息路由
│   ├── session.py       # 会话管理
│   ├── database.py      # 数据库
│   └── models.py        # 数据模型
├── upstream/            # 上游管理
├── downstream/          # 下游管理 (WebSocket/HTTP)
├── commands/            # 命令处理
├── utils/               # 工具函数
├── security/            # 安全日志
├── config.yaml          # 主配置
├── ilink_config.yaml    # ilink 插件配置
├── Dockerfile           # Docker 镜像
├── build.sh             # 构建脚本
└── run.sh               # 运行脚本
```

## 配置

### config.yaml

```yaml
plugins:
  ilink_main:
    type: upstream
    secret: "your-secret"
  opencode_main:
    type: downstream
    secret: "your-secret"
```

### ilink_config.yaml

```yaml
ws_url: "ws://127.0.0.1:8765/ws/upstream"
app_id: "ilink_main"
app_secret: "your-secret"
```

## 使用

### 构建

```bash
bash build.sh v2.1.0
```

### 运行

```bash
bash run.sh v2.1.0
```

### Docker

```bash
docker run -d \
  --name ilink-bridge \
  --restart unless-stopped \
  -p 8765:8765 \
  -v "./config.yaml:/app/config.yaml:ro" \
  -v "./ilink_config.yaml:/app/ilink_config.yaml:ro" \
  -v "./.ilink-bridge:/app/.ilink-bridge" \
  ilink-bridge:v2.1.0
```

## 依赖

- Python 3.12+
- aiohttp
- aiosqlite
- pydantic
- pyyaml
- qrcode
- watchdog
- cryptography

## API

### WebSocket 端点

- 上游: `ws://host:8765/ws/upstream`
- 下游: `ws://host:8765/ws/downstream`

### HTTP API

- `POST /api/messages` - 发送消息
- `GET /api/files/{filename}` - 获取文件

## 插件开发

插件通过 WebSocket 连接到主平台：

1. **上游插件** (如 ilink): 连接到 `/ws/upstream`，负责与外部平台通信
2. **下游插件** (如 opencode): 连接到 `/ws/downstream`，负责与内部系统通信

消息格式:

```json
{
  "type": "message",
  "message_id": "xxx",
  "from": "user@im.wechat",
  "to": "plugin_id",
  "content": "Hello"
}
```
