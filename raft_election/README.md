# Python Raft Leader 选举

改为了 **in-memory（纯内存）**：节点状态只保存在 Python 对象中，不使用 SQLite 或其他持久化存储。重启后任期为 0、投票为空，以 Follower 身份重新运行。因此这是演示选举与线程同步的教学实现，不提供完整 Raft 的崩溃恢复安全性。

第一次阅读建议先看 [代码详细讲解与答辩说明](CODE_GUIDE.md)，其中包含选举实例、函数调用顺序、线程同步分析和答辩问答。

## 启动三个节点

打开三个 PowerShell 终端，分别执行一组命令。所有节点使用同一份成员配置，以不同节点 ID 启动，不需要数据目录。

终端 1：

```powershell
Set-Location F:\7\PA\raft_election
python -m raft_election --node-id n1 --config configs/cluster.json
```

终端 2：

```powershell
Set-Location F:\7\PA\raft_election
python -m raft_election --node-id n2 --config configs/cluster.json
```

终端 3：

```powershell
Set-Location F:\7\PA\raft_election
python -m raft_election --node-id n3 --config configs/cluster.json
```

节点初始为 Follower，通常约一秒内产生 Leader。各终端输出 JSON 日志，包括选举开始、授票、当选、降级和停止事件；日志时间使用 UTC。启动日志包含 `"storage":"memory"`。

## 查看状态与故障重选

在第四个 PowerShell 终端执行：

```powershell
Invoke-RestMethod http://127.0.0.1:8001/health

$states = 8001..8003 | ForEach-Object {
    Invoke-RestMethod "http://127.0.0.1:$_/status"
}
$states | Format-Table nodeId, role, currentTerm, votedFor, leaderId, leaderUrl
$states | Where-Object role -eq 'leader'
```

在对应 Leader 的终端按 **Ctrl+C**。另外两个节点会在随机超时后选举出新 Leader。查询仍运行节点的状态：

```powershell
8001..8003 | ForEach-Object {
    try {
        Invoke-RestMethod "http://127.0.0.1:$_/status" -TimeoutSec 2
    } catch {
        Write-Host "Port $_ unavailable"
    }
} | Format-Table nodeId, role, currentTerm, leaderId
```

在被停止节点的终端重新运行原启动命令，即可恢复该节点。它的内存状态重新初始化为任期 0、未投票、Follower；收到现任 Leader 的心跳后会更新任期。在正常网络环境下可以观察它重新加入，但这不等于支持安全的崩溃恢复。

集群需要完整成员数的多数票：三节点需要两票。只启动三节点配置中的一个节点不会产生新 Leader。测试单节点选举时，可另建仅包含一个成员的配置。

## 配置

`configs/cluster.json` 中 `nodes` 将节点 ID 映射到 `http://host:port`。该地址同时用于本节点绑定监听与其他节点访问；跨机器运行应填写各机器实际可达的地址。配置不支持 HTTPS、URL 用户信息或路径前缀。允许 IPv4、主机名和带方括号的 IPv6 地址。

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `heartbeat_interval_ms` | 100 | Leader 心跳间隔 |
| `election_timeout_min_ms` | 500 | 随机选举超时下限 |
| `election_timeout_max_ms` | 1000 | 随机选举超时上限 |
| `rpc_timeout_ms` | 200 | 每个出站 HTTP RPC 的 socket 超时 |

时间参数必须是有限正数，满足 `heartbeat_interval_ms < election_timeout_min_ms < election_timeout_max_ms`，以及 `rpc_timeout_ms < election_timeout_min_ms`。仅填写 `nodes` 时使用全部默认时间参数。成员 ID、地址不可重复，本节点必须存在于成员配置中。

## HTTP API

请求和响应均使用 JSON。`role` 为 `follower`、`candidate` 或 `leader`。协议中的拒绝使用 HTTP 200 和布尔结果表达；非法请求返回 HTTP 400，超过 64 KiB 返回 413，停止时返回 503。

### GET /status

示例响应：

```json
{
  "nodeId": "n2",
  "role": "follower",
  "currentTerm": 1,
  "votedFor": "n1",
  "leaderId": "n1",
  "leaderUrl": "http://127.0.0.1:8001",
  "peers": {
    "n1": "http://127.0.0.1:8001",
    "n3": "http://127.0.0.1:8003"
  },
  "lastLogIndex": 0,
  "lastLogTerm": 0,
  "commitIndex": 0,
  "lastApplied": 0
}
```

未知 Leader、未投票时对应字段为 `null`；Leader 的 `leaderId` 为自身 ID。返回值是在状态锁内读取的一致快照，表示该节点的本地认知。

### GET /health

运行时返回 `{"status":"ok","nodeId":"n1"}`，表示进程可用，不代表已获得多数派或知道 Leader。

### POST /raft/request-vote

```json
{"term":1,"candidateId":"n1","lastLogIndex":0,"lastLogTerm":0}
```

响应为 `{"term":1,"voteGranted":true}`。同一任期最多投给一个候选人，允许该候选人重试；比较日志时先比较末尾任期，再比较末尾索引。

### POST /raft/append-entries

```json
{"term":1,"leaderId":"n1","prevLogIndex":0,"prevLogTerm":0,"entries":[],"leaderCommit":0}
```

响应为 `{"term":1,"success":true}`。仅接受空 `entries`；非空返回 400。前置日志索引或任期不为 0 时返回 `success:false`，不会虚报复制成功，也不会推进提交位置。

RPC 所有字段均必填，数字必须是非负整数（不能是布尔值），协议上限为 `2**63 - 1`；候选人和 Leader 必须是配置中的成员。旧任期请求被拒绝，并返回本地当前任期。

## 实现说明

- `config.py`：读取静态成员与时间配置。
- `protocol.py`：RPC 数据模型、输入校验与日志新旧比较。
- `node.py`：内存状态、角色状态机、随机选举、投票计数、心跳调度与线程同步。
- `transport.py` / `server.py`：出站 HTTP RPC、状态接口及 RPC 接收端。
- `__main__.py`：命令行入口与退出处理；`events.py` 输出 JSON 日志。

状态转换在同一把锁内完成，网络 I/O 在工作线程中执行，不持有状态锁。每个对端最多一个在途 RPC；不可达节点不会导致请求队列持续增长。按成员 ID 去重计票，旧轮次响应不能让旧 Candidate 当选，但其携带的更高任期仍会触发降级。

候选人收到同任期有效心跳时退回 Follower，并保留当前任期的投票记录。认可的 Leader 心跳和授票会重新随机选举超时；拒绝投票或旧任期心跳不会推迟选举。没有多数派时无法选出新 Leader；基础 Raft 不要求隔离的旧 Leader 主动降级，因此它在获知更高任期前仍可能报告自身为 Leader。

节点之间通过 HTTP 协议协调，每个节点进程内部通过 `Condition(RLock)` 同步。角色、任期、投票、已知 Leader、计票集合、在途 RPC 集合及计时器均由同一把锁保护；`Condition.wait()` 等待期间会释放锁，其他线程才能接收并处理心跳和投票。不要依靠 GIL 保护跨多条语句的状态转换。

内存版仅在单次进程生命周期中保留投票记录；重启丢失记录后可能在同一任期再次授票，不能保证包含崩溃重启场景的选举安全性。完整 Raft 必须在回复 RPC 前持久化任期和投票。旧版本生成的 `data/` 文件不会被新版本读取或修改。成员配置为静态配置，运行中修改文件不生效。

用于本地学习和调试，HTTP 接口没有认证或 TLS，请在可信网络内运行。源码不包含 `tests` 目录。
