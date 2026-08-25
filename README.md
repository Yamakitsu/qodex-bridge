# QQ ↔ Codex 桥接器 MVP

一个 Python 3.13 + asyncio 的多会话路由桥接器：

```
QQ 用户 ── NapCat (OneBot 11 正向 WS) ──▶ 桥接器 ──▶ codex app-server --stdio
                                                  │
                                                  ▼
                                              WebUI (aiohttp + WebSocket)
```

桥接器支持好友私聊与群聊 @机器人。每个好友、每个群拥有独立 project/thread 状态；只有管理员私聊可以使用 full，管理员在群聊中也强制处于当前 project 沙箱，但仍可使用 `/project` 等管理命令。

## 目录结构

```
.
├── config.toml                 # 本地配置（已加 .gitignore）
├── config.example.toml         # 配置模板
├── config.test.example.toml    # 测试配置模板
├── start.bat                   # Windows 一键启动脚本
├── playground/                 # 默认测试 project
│   └── AGENTS.md
├── src/qq_codex_bridge/        # 桥接器源码
│   ├── __main__.py             # python -m qq_codex_bridge
│   ├── config.py
│   ├── appserver.py            # Codex stdio JSON-RPC 客户端
│   ├── onebot.py               # OneBot 11 正向 WS 客户端
│   ├── state.py                # data/state.json 持久化
│   ├── projects.py             # WebUI 新建 project 的 overlay（data/projects.json）
│   ├── commands.py             # 命令文本
│   ├── orchestrator.py         # 路由、队列、审批、事件处理
│   ├── webui.py                # aiohttp REST + WebSocket 服务端
│   └── webui.html              # 深/浅色单页前端（会话树、流式回复、审批卡片、队列管理）
└── tests/
    ├── smoke_appserver.py      # Codex app-server 冒烟测试
    ├── fake_napcat.py          # 交互式 OneBot 模拟器
    ├── e2e.py                  # fake_napcat + 真实 Codex 端到端
    ├── e2e_batch2.py           # 队列、持久化、interrupt 端到端
    ├── test_webui.py           # WebUI HTTP / WS 端到端
    └── approval_e2e.py         # 审批路径端到端
```

## 安装

```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e .
```

依赖：`websockets`、`aiohttp`。

## 配置

首次运行时，如果不存在 `config.toml`，程序会从 `config.example.toml` 自动创建一份，并提示编辑后重新启动。也可以手动创建：

```bash
cp config.example.toml config.toml
```

Windows 用户双击 `start.bat` 时，新生成的配置会自动用记事本打开。程序不会覆盖已有的 `config.toml`。

关键字段：

```toml
[napcat]
ws_url = "ws://127.0.0.1:3001"
access_token = ""

whitelist = ["123456789"]   # 允许使用机器人的 QQ 号（字符串）

[access]
admins = ["123456789"]      # 只有这些 QQ 的私聊可以进入 full
group_whitelist = ["987654321"] # 允许响应的群；群内必须 @机器人

[projects]
playground = ".\\playground"

[bridge]
default_project = "playground"
default_model = "gpt-5.6-luna"  # 测试时统一使用 gpt-5.6-luna；空字符串表示使用 Codex 默认模型
approval_timeout_sec = 60
codex_path = "codex"
extra_writable_roots = []       # safe 模式审批时额外允许写入的目录

[routing]
project_root = "D:\\文档\\QQ-Codex-bot"
auto_create_projects = true
public_model = "gpt-5.6-luna"    # 沙箱会话初始模型；普通用户不能自行切换
default_agents_file = ""        # 留空使用 project_root/templates/default/AGENTS.md

[webui]
enabled = true
host = "127.0.0.1"
port = 8765
```

- 私聊只接受 `whitelist` 或 `access.admins` 中的 QQ。
- 群聊只接受 `access.group_whitelist` 中的群，并且必须 @机器人。
- 自动 project 使用 `projects/private/u_<QQ>` 与 `projects/groups/g_<群号>`；首次创建时复制默认 `AGENTS.md`。
- 人格跟 project 走；权限根据每条消息的发送者和私聊/群聊来源决定。
- `default_model` 非空时，每次 `turn/start` 都会带上该模型。
- 如果 `codex` 已加入 `PATH`，`codex_path` 保持默认值即可；否则填写本机 `codex.exe` 的绝对路径。
- WebUI 默认监听 `127.0.0.1:8765`；若端口被占用会自动尝试后续端口。

### 公开仓库安全

- 只提交 `config.example.toml` 和 `config.test.example.toml`；`config.toml`、`config.test.toml`、`.env*` 均为本地文件。
- `data/` 会保存 WebUI token、项目绝对路径和运行状态，默认只提交空目录占位文件。
- 日志、测试结果、虚拟环境、缓存和构建产物都已写入 `.gitignore`。
- 如果密钥曾进入 Git 历史，仅删除文件并不安全；应立即轮换密钥，并清理 Git 历史后再公开。

## NapCat 侧设置

1. 启动 NapCat，开启 **OneBot 11 正向 WebSocket**，端口保持 `3001`：
   - WS 地址：`ws://127.0.0.1:3001`
   - 如需鉴权，填写 `access_token`，桥接器会带 `Authorization: Bearer <token>` 连接。
2. 配置 `whitelist`、`access.admins` 和 `access.group_whitelist`。

## 启动

### 一键启动（推荐 Windows）

双击 `start.bat`，脚本会：

1. 进入项目目录
2. 检查 `.venv` 是否存在
3. 启动桥接器
4. 自动用默认浏览器打开 WebUI

### 命令行启动

```bash
.venv\Scripts\python.exe -m qq_codex_bridge --config config.toml
```

按 `Ctrl+C` 优雅退出。

## Windows 便携版

[下载最新 Windows 便携版](https://github.com/Yamakitsu/qodex-bridge/releases/latest)。GitHub Releases 提供 `QodexBridge-<版本>-windows-x64.zip` 及对应 SHA256 校验文件。解压后双击 `启动 Qodex Bridge.bat`：

1. 首次启动自动创建并打开 `config.toml`，填写 QQ 白名单与 NapCat 连接信息；
2. 再次双击即可启动桥接器并打开本地 WebUI；
3. 程序优先使用 PATH 中的 `codex`，也会自动发现 Codex Desktop 自带的 `codex.exe`。

便携包无需安装 Python或创建虚拟环境，但仍需在本机安装并登录 Codex，且需要可用的 NapCat OneBot 11 正向 WebSocket 服务。

维护者可在 Windows PowerShell 中运行以下命令重新构建：

```powershell
.\scripts\build_portable.ps1 -Version 0.2.0
```

## 命令

按身份处理以 `/` 开头的消息。普通用户仅有 `/list`、`/status`、`/new`、`/stop`、`/interrupt`、`/effort`；管理员在私聊和群聊中还可使用 project/thread/model/queue 管理命令：

| 命令 | 说明 |
|------|------|
| `/list` | 命令清单 + 当前 project/thread/model/effort/mode/队列长度 |
| `/new` | 当前 project 下新建 thread |
| `/stop` | `turn/interrupt` 当前 turn |
| `/interrupt <消息>` | 中断当前 turn，结束后立即处理指定消息；无消息时等同 `/stop` |
| `/status` | 空闲/执行中/待审批 + 当前设置 |
| `/project` | 列出 projects；`/project <名>` 切换 |
| `/thread` | 列出当前 project 的 thread；`/thread <序号>` 切换 |
| `/model` | 列出模型；`/model <名>` 切换 |
| `/effort` | 列出当前模型的 reasoning effort 档位；`/effort <档位>` 切换 |
| `/mode` | 仅管理员私聊/WebUI可用；群聊永远不能切到 full |
| `/queue` | 列出排队消息 |
| `/queue jump <消息>` | 插队到队首；若当前空闲则直接处理 |
| `/queue pop <序号>` | 删除指定排队消息 |
| `/queue clear` | 清空队列 |
| `/yes` `/no` | 审批应答 |

普通文本消息作为 user input 发给当前 thread。

### QQ 图片、附件与回复格式

- 单独发送图片或附件时，桥接器只归档，不启动 Codex，也不向 QQ 回复。
- 给图片附带文字，或引用此前的图片再发送文字时，桥接器会把归档后的绝对路径连同文字交给 Codex。
- QQ 上传内容统一归档到当前 project 的 `attachments/YYYY-MM-DD/HHMMSS-mmm_原文件名`，不再散落在 project 根目录。
- QQ 出站消息固定使用 OneBot `text` segment，并把常见 Markdown 标记降级为纯文本；WebUI 仍保留 Markdown 渲染。
- Codex 命令执行开始与完成信息只显示在 WebUI，不再发送到 QQ；需要用户决定的审批请求仍会发到 QQ。

### 队列说明

- 当桥接器正在处理一条 Codex turn 时，新的普通消息会自动进入队列，并收到“已排队（第 N 位）”的反馈。
- 当前 turn 结束后，桥接器会自动按顺序处理队列中的消息。
- 队列内容会持久化到 `data/state.json`，重启后保留。

## 权限与路由

- 管理员私聊/WebUI的 `full`：`dangerFullAccess + approvalPolicy=never`。
- 其他所有 QQ 场景，包括管理员在群聊内：`workspaceWrite`，唯一可写根为当前 project，`networkAccess=true`，`approvalPolicy=never`。任何越过 project 的审批请求由桥接器自动拒绝。
- 管理员在群聊中仍可执行 `/project <名>` 等管理命令；这只改变该群的路由状态，不会提升该群 turn 的沙箱权限。
- 全局 app-server 当前串行执行 turn，但队列项保存自己的作用域，出队时会恢复正确的 project/thread，避免跨好友或群串线。

## WebUI

启动后控制台会打印：

```
WebUI: http://127.0.0.1:8765/#token=<token>
```

浏览器打开该地址即可进入管理界面（Claude 风格暖色主题、思源宋体，深/浅色可切换，支持移动端窄屏），功能：

- 状态栏实时显示 project / thread / model / effort / mode / busy，NapCat、Codex、WebUI 三个连接指示灯
- 侧栏以「project → thread」树形结构组织会话：点击 project 切换并展开其 thread 列表（懒加载），点击 thread 切换并加载该会话的历史消息；一键新建会话（New Chat）、搜索过滤会话标题
- 侧栏可新建 project（New Project，输入名称 + 目录路径）；新建的 project 保存在 `data/projects.json` overlay 中（不改动 `config.toml`），重启后自动合并，QQ 侧 `/project` 同样可见
- model / effort / mode（Full 模式弹窗二次确认）、消息队列查看/清空收纳在侧栏底部的 Settings 折叠区
- 聊天流：QQ 与 WebUI 消息同屏；Codex 回复**流式打字**（WebSocket delta），思考过程折叠展示，Markdown 渲染（代码块/列表/链接）
- 命令执行、文件变更以工具卡片内联展示
- 审批卡片：待审批操作居中弹出，附倒计时，一键批准/拒绝；QQ 侧 `/yes` `/no` 与 WebUI 按钮互通，哪边先答都算数
- 底部输入框直接从 WebUI 发消息（与 QQ 发消息走同一条管线，支持 `/` 命令）

REST API（均需 `Authorization: Bearer <token>` 或 URL 参数 `?token=<token>`）：

- `GET /api/v1/status`
- `GET /api/v1/messages`
- `POST /api/v1/prompt`  body: `{"text": "..."}`
- `POST /api/v1/project` body: `{"name": "..."}`；`POST /api/v1/projects` body: `{"name": "...", "path": "..."}`（新建 project，写入 `data/projects.json`）
- `GET /api/v1/threads`（可加 `?project=<名>` 列指定 project 的 thread）；`POST /api/v1/thread`  body: `{"id": "..."}` 或 `{"index": 1}`，按 id 切换时响应带 `history` 字段（该 thread 的用户/助手消息历史）
- `GET /api/v1/models`；`POST /api/v1/model`   body: `{"name": "..."}`（空字符串恢复默认）
- `POST /api/v1/effort`  body: `{"effort": "high"}`（空字符串恢复默认）
- `POST /api/v1/mode`    body: `{"mode": "safe"}`（WebUI 直连，前端自行二次确认）
- `POST /api/v1/new`（新 thread）；`POST /api/v1/stop`（中断当前 turn）
- `POST /api/v1/interrupt` body: `{"text": "..."}`（中断并立即处理）
- `GET /api/v1/queue`；`POST /api/v1/queue/pop` body: `{"index": 0}`；`POST /api/v1/queue/clear`
- `POST /api/v1/approve`；`POST /api/v1/deny`
- `GET /api/v1/ws?token=<token>` WebSocket，事件类型：`messages`（历史回放）、`message`、`status`、`turn`、`delta`（回复流式增量）、`reasoning_delta`（思考流式增量）、`approval`

Token 首次启动时自动生成并保存到 `data/server.token`。

## 人设 / 知识库

在每个 project 目录下放：

- `AGENTS.md` —— Codex 会读取作为人设/系统提示。
- `kb/` —— 知识库目录。

这是 Codex 原生行为，桥接器无需额外处理。

## 测试

### 1. 冒烟测试（直接对 Codex app-server）

```bash
.venv\Scripts\python.exe tests/smoke_appserver.py
```

会新建临时 thread，发送“只回复两个字：你好”，断言回复包含“你好”。

### 2. 假 NapCat（交互式）

终端 1：

```bash
.venv\Scripts\python.exe tests/fake_napcat.py --config config.toml
```

终端 2：

```bash
.venv\Scripts\python.exe -m qq_codex_bridge --config config.toml
```

在 `fake_napcat` 终端输入消息，即可看到桥接器通过 `send_private_msg` 发回的回复。

### 3. 端到端自动化

```bash
.venv\Scripts\python.exe tests/e2e.py --config config.toml
```

自动完成普通消息、`/list`、`/model`、`/project`、`/new`、排队、审批触发尝试，并输出 `e2e_results.json`。

### 4. 第二批功能端到端

```bash
.venv\Scripts\python.exe tests/e2e_batch2.py --config config.toml
```

覆盖队列命令（`/queue list/jump/pop/clear`）、队列持久化、跨重启恢复、`/interrupt`。

### 5. WebUI 端到端

```bash
.venv\Scripts\python.exe tests/test_webui.py --config config.toml
```

覆盖 HTTP 鉴权、状态接口、prompt 注入、WebSocket 实时消息、审批批准。

### 6. 审批路径端到端

```bash
.venv\Scripts\python.exe tests/approval_e2e.py --config config.toml
```

通过让 Codex 在 `playground` 之外（`%USERPROFILE%\approval_test_marker.txt`）写文件，逼出 `item/commandExecution/requestApproval` / `item/fileChange/requestApproval`：

1. safe 模式下对第一个弹出的审批回复 `/no`，marker 文件未出现。
2. 再次触发，对弹出的所有审批提示连续回复 `/yes`，marker 文件出现。
3. `/mode full`（含二次确认 `确认`）后同样操作不再询问，marker 文件直接出现。

> 说明：`item/permissions/requestApproval` 在代码层面也已纳入同样的 FIFO 审批队列，但当前测试连接的 Codex build 在 `on-request` 策略下不会触发该请求，因此端到端测试未单独覆盖该分支。

跑完会保留 `approval_e2e_results.json`。

## 已知环境发现

- 该用户的 `~/.codex/config.toml` 中默认 `approval_policy = "never"`、`sandbox_mode = "danger-full-access"`。
- 对于**当前 project 范围内**的写操作/命令，Codex 不会发送审批请求（项目已被标记为 trusted）。
- 测试使用的 marker 路径在 `playground` 之外，以稳定逼出 `item/fileChange/requestApproval` 与 `item/commandExecution/requestApproval`。`item/permissions/requestApproval` 也已进入同样的 FIFO 审批队列，需要用户在 QQ 或 WebUI 批准后才能获得额外权限；但当前 Codex build 在该策略下不会触发该请求。
- 协议 schema 中 `approvalPolicy` 的字符串枚举为 `"untrusted"`、`"on-request"`、`"never"`（以及 experimental 的 granular 对象）。
