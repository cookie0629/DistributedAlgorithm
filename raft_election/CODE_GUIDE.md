# 从零理解这个 Raft 项目：代码讲解与答辩说明

这份说明对应当前的纯内存实现。建议先读第 1～5 部分理解选举，再结合 `node.py` 阅读线程同步部分，最后用答辩问答自查。

## 1. 这个项目到底解决什么问题？

假设同时运行 n1、n2、n3 三个服务进程。它们需要选出一个协调者，也就是 Leader。如果 Leader 停止，剩下的节点应该自行选出新的 Leader。

项目实现的是 Raft 的 Leader 选举这一部分。它目前不保存业务数据，也没有实现日志复制。所以“选出了 Leader”不能等同于“已经完成分布式数据库”。

程序通过两种请求完成选举：

- `RequestVote`：候选人询问其他节点：“在这个任期，你愿意投给我吗？”
- 空 `AppendEntries`：Leader 通知其他节点：“我是当前任期的 Leader，我还在运行。”这就是心跳。

`GET /status` 供人查看节点状态，它不参与投票。你不查询状态，集群也会自动选举。

## 2. 内存版意味着什么？

`RaftNode.__init__()` 创建一个节点对象，初始状态是：

```python
self.current_term = 0
self.voted_for = None
self.role = "follower"
self.leader_id = None
```

这些字段是对象属性，保存在当前进程的内存里，没有写入数据库或文件。每个节点是独立进程，各自有一份状态；三个节点并没有共享同一个 Python 对象。

停止进程后，这些记录就丢失了。重新启动时又从任期 0 开始。JSON 配置文件只描述成员和时间参数，不保存运行中的任期、角色或投票。日志输出也不用于恢复节点状态。

这一简化适合演示，但必须说清限制：完整 Raft 要求任期和投票在回复 RPC 前持久化。否则一个节点重启后可能忘记自己已经投过票，再给同一任期的另一个候选人投票。**锁只能保护当前进程里的并发访问，不能恢复重启后丢失的信息。**

## 3. 三个角色和一个任期

| 概念 | 直观含义 | 本项目里的动作 |
| --- | --- | --- |
| Follower | 跟随者 | 等待心跳，按规则响应投票 |
| Candidate | 候选人 | 开始新一轮选举，投自己一票，请求他人投票 |
| Leader | 领导者 | 定期给其他节点发送心跳 |
| term | 任期编号 | 区分新旧选举，数字更大的任期更新 |

角色转换的主要路径是：

```text
Follower --选举超时--> Candidate --获得多数票--> Leader
                          |
                          +--再次超时--> 新任期的 Candidate

Candidate --收到同任期 Leader 心跳--> Follower
Candidate / Leader --发现更高任期--> Follower
```

任期不是运行时长，也不是某台机器的编号。一次选举失败也会消耗一个任期，所以并非每个任期都能产生 Leader。

本进程运行期间，任期只会增加。内存版重新启动后，任期初始化为 0，这是简化实现的边界。

## 4. 先认识节点的状态字段

在 `node.py` 的 `RaftNode` 中：

| 字段 | 用途 |
| --- | --- |
| `node_id` | 当前节点的固定身份，例如 n1 |
| `config` | 启动时读取的静态配置，运行中不修改 |
| `current_term` | 本节点知道的当前任期 |
| `voted_for` | 本任期投给了谁；`None` 表示尚未投票 |
| `role` | 当前角色 |
| `leader_id` | 本节点目前知道的 Leader；不确定时为 `None` |
| `peers` | 其他节点 ID 到地址的映射，不包含自己 |
| `majority` | 当选需要的票数，完整成员数 `// 2 + 1` |
| `_votes` | 本轮投给自己的节点 ID 集合，集合自动去重 |
| `_busy` | 当前有出站 RPC 尚未结束的对端 ID 集合 |
| `_deadline` | 当前选举等待的截止时间 |
| `_next_heartbeat` | 下次应该发送心跳的时间 |
| `_running` / `_closed` | 控制节点运行和退出 |

例如三个节点的 `majority` 是 `3 // 2 + 1 = 2`。候选人给自己一票，再获得任意一个其他节点的支持就能当选。多数派按配置里的全部成员计算，不按“当前能连上的节点数”计算。

`voted_for` 与 `_votes` 容易混淆：前者是“我投给了谁”，后者是“有哪些节点投给我”。`leader_id` 也不一定等于 `voted_for`：投过的候选人不一定当选。

## 5. 用一次完整选举串起代码

假设三个节点正常通信，初始任期都是 0。

### 5.1 从命令行进入程序

执行：

```powershell
python -m raft_election --node-id n1 --config configs/cluster.json
```

Python 执行包里的 `__main__.py`，`main()` 依次完成：

1. 解析参数，读取 `Config.load()`。
2. 创建 `RaftNode`，此时只是内存对象，还没有选举。
3. `make_server()` 绑定本节点 HTTP 地址。
4. `node.start()` 启动计时线程，设置第一次随机选举截止时间。
5. 启动 HTTP 服务线程，接收状态查询、心跳和投票请求。
6. 主线程等待 Ctrl+C 或停止信号。

因此“启动一个节点”是在启动一个有多个线程的进程；启动三个节点，就是三个这样的独立进程。

### 5.2 n1 最先超时

假设 n1 随机等待 600 ms，n2 等待 850 ms，n3 等待 930 ms。没有收到 Leader 心跳时，n1 的 `_run()` 最先发现截止时间已到，调用 `_start_election()`。

该函数在状态锁内完成：

```text
current_term: 0 -> 1
voted_for: None -> n1
role: follower -> candidate
leader_id: None
_votes: {n1}
重新设置随机选举截止时间
```

然后构造任期为 1 的 `RequestVote`，通过 `_broadcast()` 向 n2 和 n3 提交异步 HTTP 请求。

### 5.3 n2 处理投票请求

请求先由 `server.py` 解析、校验，再进入 `node.request_vote()`：

1. 取得状态锁。
2. 请求任期 1 高于本地 0，调用 `_follow(1)`，更新任期、清空旧投票。
3. 检查本任期是否尚未投票，或已经投给同一候选人。
4. 检查候选人的日志是否至少与自己一样新。
5. 条件满足，将 `voted_for` 设为 n1，重新设置选举超时。
6. 返回 `{"term": 1, "voteGranted": true}`。

当前所有日志都为空，因此正常节点最后日志任期和索引都为 0，新旧比较会通过。代码保留比较函数，方便后续实现日志。

### 5.4 n1 收到足够票数

通常由网络工作线程在请求完成后调用 `_receive_response()`；如果注册回调时 Future 已经完成，也可能由注册回调的线程立即调用，详见 6.11。它获取状态锁并检查：自己仍是 Candidate 吗？发出请求时、当前、响应中的任期是否一致？

如果成立，就把 n2 加入 `_votes`。此时 `{n1, n2}` 有两票，达到多数派，调用 `_become_leader()`。

该函数将角色改为 Leader、`leader_id` 改为自己，并将 `_next_heartbeat` 设为当前时间，再唤醒计时线程。计时线程会尽快提交第一轮心跳，无需再等一个完整心跳周期；正在处理的对端请求不会被重复提交。

### 5.5 n2 和 n3 收到心跳

心跳由 `append_entries()` 处理：旧任期心跳被拒绝；当前或更高任期的 Leader 心跳会更新已知 Leader，重设选举超时。Candidate 收到同任期 Leader 心跳也会退回 Follower。

在正常通信情况下，心跳每 100 ms 到达，而最早的选举超时是 500 ms。Follower 的截止时间不断向后移动，因此不会发起新选举。

### 5.6 Leader 停止

n1 停止后，n2、n3 收不到心跳。它们中先超时的节点会递增任期并申请选举，获得两票后成为新 Leader。

如果只剩一个节点，它在三成员配置里只能得到一票，不能当选。若配置本身只有一个成员，则多数派为一，它投自己就可以当选。

## 6. 老师问的重点：状态改变时怎么同步线程？

### 6.1 哪些线程会访问同一个节点对象？

| 线程 | 主要工作 | 会触碰什么状态 |
| --- | --- | --- |
| 主线程 | 启动、接收退出信号、停止节点 | 运行标志及启动状态快照 |
| `raft-timer` | 判断选举超时、调度心跳 | 角色、任期、票数、计时器 |
| `raft-http` | 接受 HTTP 连接并分配处理线程 | 主要负责服务调度 |
| HTTP 请求处理线程 | 处理投票、心跳和状态查询 | 任期、投票、Leader、状态快照 |
| `raft-rpc_*` 工作线程 | 向其他节点发送请求并处理回调 | 回调会更新任期、票数、角色及 `_busy` |

这些线程共享同一进程里的 `RaftNode`，所以需要同步。另一个节点进程的锁不能锁住本节点；跨节点协调依靠 RPC 和 Raft 规则。

### 6.2 不加锁会发生什么？

假设 n1 在任期 3 还没投票，n2 和 n3 同时向它请求投票。两个 HTTP 请求由不同线程处理。

没有锁时，可能出现下面的交错：

```text
线程 A：读取 voted_for，发现是 None
线程 B：读取 voted_for，也发现是 None
线程 A：把 voted_for 改为 n2，返回同意
线程 B：把 voted_for 改为 n3，也返回同意
```

结果是同一节点在同一任期给两名候选人都投了票，破坏选举安全性。最终变量只保存 n3，不能抹掉已经发给 n2 的同意响应。

所以需要保护的是“检查条件 → 修改状态 → 决定响应”这一整段操作，而不只是给单次变量赋值加锁。

### 6.3 本项目用什么锁？

```python
self._condition = threading.Condition(threading.RLock())
```

这里包含两个机制：

- `RLock` 是可重入互斥锁，同一时刻只允许一个线程进入受保护的状态操作；同一线程可再次取得它。
- `Condition` 使用这把锁，并提供等待、通知功能，让计时线程在超时或状态变化后醒来。

进入下面的代码块就是取得同一把状态锁，离开代码块自动释放：

```python
with self._condition:
    # 检查与修改节点状态
```

`request_vote()`、`append_entries()`、`status()`、`_receive_response()`、`_run()` 以及启动、停止逻辑都使用它。内部的 `_follow()`、`_set_term_vote()`、`_start_election()` 等辅助函数由已经持锁的调用者调用，不应该在外部随意直接调用。

多个字段的更新可能有多条 Python 语句，但其他遵守同一把锁的线程不能插入中间，所以它们会看到一整次转换完成后的状态。

使用 `RLock` 也允许一个已经完成的 Future 在注册回调时，在当前线程立即执行回调并再次进入同一把锁。普通不可重入锁在这种情况下可能锁住自己。

### 6.4 有了 Python 的 GIL，为什么还要锁？

GIL 不会自动把“判断投票是否为空，然后更新投票和角色”这样的多步逻辑变成一次不可分割的操作。I/O 会发生线程切换，解释器也不应作为这个协议正确性的依据。代码应显式锁住共享状态。

### 6.5 计时线程持锁，别人怎么处理心跳？

注意 `_run()` 最后的代码：

```python
self._condition.wait(timeout=max(0, wake_at - self.clock()))
```

`wait()` 的关键行为是：**等待时释放锁，被唤醒后重新取得锁。**

因此计时线程并不是睡觉时还霸占锁。HTTP 处理线程可以在这期间取得锁，处理心跳、修改截止时间，再用 `notify_all()` 通知计时线程重新检查。

`notify_all()` 不会立刻把锁交给其他线程，通知方仍要先离开临界区。被唤醒线程必须在获取锁后重新判断角色与时间，不能把“被唤醒”直接当成“选举超时”。这也是 `_run()` 使用循环的原因。

这里使用 `time.monotonic()` 比较间隔，避免用户修改系统日期影响选举计时。日志里的 UTC 时间仅供观察，不参与超时计算。

### 6.6 网络慢，会不会一直占着状态锁？

不会在持锁的线程里等待出站 HTTP 响应。`_broadcast()` 只把请求提交给线程池；真正的 `transport.call()` 在工作线程里运行，没有取得节点状态锁。

调用链是：

```text
状态锁内：确定任期、构造请求、登记 _busy、提交任务
    ↓
工作线程：HTTP 请求、等待远端响应（不持状态锁）
    ↓
响应回调：重新获取状态锁，核对任期、更新状态
```

即使某个工作线程在计时线程释放锁之前就开始网络请求，它本身也不持有状态锁；回调修改状态仍需等锁。

`_busy` 限制每个对端最多一个尚未完成回调清理的出站请求。当前源码的线程池上限是 `max(10, len(self.peers))`：至少允许 10 个工作线程，但并不意味着启动时就创建 10 个线程，也不意味着每轮发送 10 个请求。实际请求仍按对端遍历，并受 `_busy` 限制。对端不响应时，不会每 100 ms 无限堆积新任务。出站连接设置了 socket 超时；它是单次阻塞 I/O 的超时，不是对任意异常网络行为的严格总耗时保证。

### 6.7 加锁以后，还需要检查旧响应吗？

需要。锁解决“同时修改”，不解决“消息过时”。

例如 n1 在任期 2 发起投票请求，等待时收到任期 3 的 Leader 心跳并退回 Follower。之后任期 2 的同意票才到达。即使安全地拿到了锁，也不能用这张票让 n1 当选。

所以计票必须同时满足：

```python
self.role == "candidate"
sent_term == self.current_term == response_term
```

此外 `_votes` 是集合，同一成员重复响应不会增加票数。任何有效响应携带更高任期时，都先更新任期并降级，再判断能否计票，包括旧请求的迟到响应。

### 6.8 为什么状态查询也要加锁？

`GET /status` 要读取多个字段。如果在没有锁的情况下分次读取，可能先读到旧的 `role`，随后另一个线程改变角色和 Leader，查询再读到新的 `leader_id`，得到拼接出来的不一致结果。

`status()` 在锁内复制完整字典，HTTP 层在锁外编码 JSON、写回响应。这保证一次查询是该节点某个时刻的一致快照，不保证三个节点在同一瞬间知道完全相同的信息。

### 6.9 把 `self._condition` 本身拆开理解

`self` 是当前这个 `RaftNode` 对象。`self._condition` 是它的一个属性，存放一个 `threading.Condition` 对象。前面的下划线表示“项目内部使用”，不会产生自动加锁效果。

构造语句可以拆成下面两步理解，作用与源码一致：

```python
state_lock = threading.RLock()
self._condition = threading.Condition(state_lock)
```

`Condition` 是“条件变量”，这里的“条件”并不是自动绑定到 `role == "leader"` 这样的判断。我们没有给它传入任何角色判断函数。它只提供“配合一把锁进行等待与通知”的机制；到底是否超时、是否当选，仍由我们的 `if` 和 `while` 判断。

它也不会自动监听对象属性。例如执行 `self.role = "leader"`，条件变量不会自己发现并通知别人，需要代码显式执行 `notify_all()`。

本项目对这个对象使用三种操作：

| 写法 | 实际作用 | 是否释放状态锁 |
| --- | --- | --- |
| `with self._condition:` | 进入时获取它使用的 `RLock`；其他线程持锁时会等待 | 离开代码块时释放本次获取的锁 |
| `self._condition.wait(timeout=...)` | 持锁线程登记等待，暂时放开锁，等待通知或超时，然后重新取得锁再返回 | 等待期间释放；返回调用者前重新取得 |
| `self._condition.notify_all()` | 通知当前等待在这个条件变量上的线程，让它们有机会重新检查状态 | 不释放锁，也不等待被通知线程执行完 |

调用 `wait()` 或 `notify_all()` 时必须已经持有对应的锁，否则会抛出 `RuntimeError`。`notify_all()` 只通知此刻已经在等待的线程，不会像信箱一样保存一条通知给未来的等待者。程序的事实保存在字段中，所以计时线程每次都重新读取字段，不能靠“收到过几次通知”判断状态。

`with` 的锁管理可以近似理解成：

```python
self._condition.acquire()
try:
    # 原来 with 里面的代码
    ...
finally:
    self._condition.release()
```

因此，你贴出的代码里即使执行了 `return`，也会先退出 `with` 并释放本次取得的锁；抛出异常也会释放。它不会因为提前返回而永久占着锁。如果同一线程通过 `RLock` 嵌套获取了锁，退出内层只减少一次获取计数，必须全部退出后，其他线程才能获取。

锁只对**主动使用同一把锁的线程**起作用。它不会让对象的属性自动变成私有，也不会阻止某人绕过锁直接写 `node.role`。所以本项目要遵守统一约定：初始化完成、线程启动之后，共享可变状态的检查与修改都放在同一个状态锁的保护下。

### 6.10 源码中每个使用条件变量的代码块

下面按实际函数解释。先分清两种写法：有些函数自己写 `with self._condition` 获取锁；另一些内部辅助函数没有写 `with`，但调用时上层已经持有锁，仍处于同一个临界区。**调用辅助函数不会自动释放调用者的锁。**

#### A. `start()`：启动标志与计时器一起准备好

```python
with self._condition:
    if self._closed or self._running:
        raise RuntimeError("Node cannot be started twice")
    self._running = True
    self._reset_deadline()
    self._thread = threading.Thread(target=self._run, name="raft-timer")
    self._thread.start()
```

这一块先检查是否能启动，再设置运行标志、第一次截止时间和计时线程。`_reset_deadline()` 里的 `notify_all()` 合法，是因为上层已经取得锁。首次启动还没有计时线程在等待，这次通知没有接收者也不影响正确性，截止时间已经存在字段里了。

`self._thread.start()` 启动了另一个线程，不是让当前主线程直接执行 `_run()`。新计时线程进入 `_run()` 时也要取得同一把锁，所以必须等 `start()` 的这个 `with` 退出后，才能读取已经准备好的状态。

#### B. `stop()`：先通知停止，再在锁外等待线程结束

```python
with self._condition:
    if self._closed:
        return
    self._running = False
    self._condition.notify_all()

if self._thread is not None:
    self._thread.join()
self._executor.shutdown(wait=True, cancel_futures=True)

with self._condition:
    self._closed = True
```

第一段锁内把 `_running` 置为 `False`，并唤醒可能正在 `wait()` 的计时线程。它醒来后拿到锁，重新检查 `while self._running`，然后退出循环。没有通知时，它也可能等到定时超时才发现停止，退出会变慢。

`join()` 表示“等待计时线程真正结束”，线程池 `shutdown(wait=True, ...)` 表示“不再接收任务，取消还没开始的任务，并等待正在执行的任务结束”。取消不了已经在执行的 HTTP 请求，它们依靠正常完成或网络超时结束。

这两种等待都必须放在状态锁外。否则可能变成：主线程拿着锁等工作线程结束，工作线程的回调却在等主线程释放这把锁，两边永远互等。

停止阶段仍可能收到已完成任务的回调。回调先清理 `_busy`，随后看到 `_running == False` 就返回，不再改变任期或计票。最后一段锁内才将 `_closed` 设为 `True`。

#### C. `status()`：整个返回值来自同一次状态读取

```python
with self._condition:
    self._require_running()
    log_term, log_index = self.last_log()
    return {
        "role": self.role,
        "currentTerm": self.current_term,
        # 其余字段见源码
    }
```

这里的字典是在持锁时构造好的，随后才返回并释放锁。HTTP 服务拿到这个独立字典后再写网络响应，不会为了等客户端接收数据而一直持有节点状态锁。`peers` 还会复制成新字典，避免直接把内部成员映射暴露给调用者修改。

#### D. `request_vote()`：不能拆开的“检查并投票”

它在一个 `with` 内完成：检查运行标志 → 比较任期 → 必要时退回 Follower → 检查已投对象和日志新旧 → 记录投票 → 重设截止时间 → 构造响应。

必须把“判断还能不能投票”和“写入投给谁”放在同一临界区。只锁住赋值语句仍然不够，因为两个线程可能在加锁前就都判断自己可以投票。

`_follow()`、`_set_term_vote()`、`_reset_deadline()` 都沿用 `request_vote()` 已经取得的锁。响应里的 `term` 和 `voteGranted` 也是在该临界区里确定的；发送响应之前即使本地后来进入更高任期，这份响应仍表示处理请求时的结果，接收方还会检查任期。

#### E. `append_entries()`：收到心跳和触发选举互斥

这个函数在锁内比较任期、必要时降级、记录 Leader、重设选举截止时间，并构造 `success`。

它与 `_run()` 共用锁，因此当“心跳处理”和“选举截止”几乎同时发生时，只会有一个先进入临界区：

- 心跳先取得锁并刷新时间：计时线程随后读取新截止时间，不会按旧时间发起选举。
- 计时线程先取得锁并开始新任期：心跳随后按新的本地任期重新判断，旧任期心跳可能被拒绝。

锁不规定心跳必须先被处理，也不保证接收网络数据的瞬间就能更新状态；它保证两条路径不会读写交错出一个不完整的转换。

#### F. `_run()`：锁保护循环判断，`wait()` 让别人进来

```python
with self._condition:
    while self._running:
        now = self.clock()
        if self.role == "leader":
            # 到发送时间就构造心跳并提交任务，计算下一次心跳时间。
            ...
        else:
            if now >= self._deadline:
                self._start_election()
                continue
            wake_at = self._deadline
        self._condition.wait(timeout=max(0, wake_at - self.clock()))
```

这不是“计时线程永久持锁”的循环，因为每轮末尾的 `wait()` 都会在等待期间释放锁。它与 `time.sleep()` 的区别很关键：如果把这里直接改成持锁 `sleep()`，计时线程睡眠时其他请求就会拿不到状态锁。

`wake_at` 是这一轮算出的时间点，`wake_at - self.clock()` 是还需等多少秒。`max(0, ...)` 避免算出来是负数。`wait()` 可能因通知或等待时间到期而返回；代码不根据返回值直接行动，而是回到循环重新读取 `_running`、`role`、截止时间。

若开始选举，`_start_election()` 已经重新设置了随机截止时间，因此 `continue` 只是立即回到循环读取新状态，通常下一轮就进入等待；不是每次循环都无间隔地递增任期。单节点当选后，下一轮会进入 Leader 分支。

#### G. `_receive_response()`：回调拿锁后才处理共享状态

这是你贴出的代码块，下一节会逐行展开。它与上面的投票处理、心跳处理、计时循环共享同一把锁。多个网络请求可以同时等待响应，但它们的回调不能同时进入状态临界区。

#### H. 三处 `notify_all()` 各自通知了什么？

除了 `stop()`，源码还有下面三个辅助函数会通知。它们均由持锁调用路径进入：

| 函数 | 先更新的状态 | 通知后希望计时线程重新检查什么 |
| --- | --- | --- |
| `_reset_deadline()` | `_deadline` | 原来计划的选举时间已经变了，按新截止时间等 |
| `_follow()` | 任期、角色、Leader、计票集合 | 原来可能是 Leader，现在应改用 Follower 的选举计时逻辑 |
| `_become_leader()` | `role`、`leader_id`、`_next_heartbeat` | 不再等选举截止时间，尽快发送首轮心跳 |

`_follow()` 本身不重设 `_deadline`。收到有效心跳时，外层 `append_entries()` 随后调用 `_reset_deadline()`；授票时，`request_vote()` 也会重设。但更高任期的拒绝投票或响应，不会仅因任期更高就延后选举。

一次心跳可能先后调用 `_follow()` 和 `_reset_deadline()`，产生两次通知。不过外层仍持有同一把锁，计时线程不会在二者之间拿到锁看到半成品状态，必须等外层释放后才能继续。

### 6.11 `_broadcast()` 的 `submit()`、Future 和回调到底怎样执行？

先看完整代码，下面按实际执行顺序解释：

```python
def _broadcast(self, path: str, body: dict) -> None:
    for peer_id, address in self.peers.items():
        if peer_id in self._busy:
            continue
        self._busy.add(peer_id)
        future = self._executor.submit(self.transport.call, address, path, body)
        future.add_done_callback(
            lambda result, peer=peer_id, rpc=path, term=body["term"]:
                self._receive_response(peer, rpc, term, result))
```

#### A. 谁调用它？它为什么没有自己写 `with`？

通常是计时线程调用它。有两条路径：

```text
_run() 已经取得状态锁
  ├─ 选举超时 → _start_election() → _broadcast(投票请求)
  └─ Leader 到达心跳发送时间 → _broadcast(心跳)
```

因此 `_broadcast()` 检查和修改 `_busy` 时已经处于锁内。它不是可以任意从外部调用的线程安全入口；离开这两条持锁路径使用它时，调用方也必须遵守同一锁约定。

#### B. 为什么先登记 `_busy`？

例如 n1 的 `peers` 是 `{"n2": "http://127.0.0.1:8002", "n3": "http://127.0.0.1:8003"}`。

遍历到 n2 时：如果 n2 已在 `_busy`，就跳过；不在则先把 n2 放进去，再提交任务。这样下一轮心跳即使很快开始，也知道 n2 已经有请求在处理。

它是本节点的“对端任务占用登记”，不是对端的锁，也不代表对端真的在忙。n2 仍可同时处理来自其他节点的请求。这里限制的是 n1 自己向 n2 发出的所有投票或心跳 RPC，不是分别限制两种请求。

检查、登记与回调里的清理都用同一把锁，所以不会一边决定发第二个请求，一边清理第一个请求的登记。

#### C. `self._executor` 是什么？

它在构造函数中创建：

```python
self._executor = ThreadPoolExecutor(
    max_workers=max(10, len(self.peers)),
    thread_name_prefix="raft-rpc",
)
```

线程池接收待执行函数，按需创建、复用工作线程。`max_workers` 是工作线程数量上限，不是每次新建线程的数量。三节点中，对端只有两个；虽然上限至少为 10，但 `_busy` 仍限制为每个对端一个请求。

#### D. `submit(self.transport.call, address, path, body)` 的每个参数是什么？

| 部分 | 含义 |
| --- | --- |
| `self.transport.call` | 要交给工作线程执行的函数对象；这是已绑定到 transport 实例的方法 |
| `address` | 此次请求的目标地址，例如 n2 的 `http://127.0.0.1:8002` |
| `path` | API 路径，例如 `/raft/request-vote` |
| `body` | JSON 编码前的请求字典，包含发出时的任期等字段 |

工作线程真正要执行的调用等价于：

```python
self.transport.call(address, path, body)
```

但 `submit()` 让工作线程执行它，当前提交任务的线程通常很快拿到一个 Future，就可以继续遍历 n3，不等待 n2 的网络结果。异步是相对提交线程而言，底层 HTTP 仍是工作线程中的阻塞调用，没有使用 `async` / `await`，也没有为每次请求启动新进程。

这里不要写成 `submit(self.transport.call(address, path, body))`。那会先在当前线程直接执行 HTTP 请求，等返回之后才调用 `submit()`，既会阻塞持锁的计时线程，也会把返回的字典误当成待执行函数。

`submit()` 不会自动深拷贝参数，`body` 是传入的对象引用。当前程序每次构造请求字典后不再修改它，工作线程也只读取、编码它，因此可以共用。如果后续改为复用并修改同一字典，就需要额外的不可变设计或拷贝，锁不会自动保护工作线程在锁外读取的请求对象。

#### E. `future` 里装的是什么？

`future` 是一个 `concurrent.futures.Future` 对象，可以理解成“这一个任务的结果凭证”。它不是 HTTP 响应字典，也不是工作线程本身。

它随执行过程变化：

```text
等待执行 → 正在执行 → 已完成（保存返回值或异常）
    └─ 还没开始时也可能被取消
```

`transport.call()` 会创建 HTTP 连接、编码并发送请求、读取响应，再返回一个 Python 字典；线程池把这个字典存入对应的 Future。如果连接失败、超时或解析出错，线程池保存异常，Future 同样会进入“已完成”状态。

“已完成”不等于“网络成功”，更不等于“获得同意票”。例如正常返回 `{"term": 3, "voteGranted": false}` 是任务成功完成，但没有获得投票。

循环中下一次给局部变量 `future` 赋值，不会取消上一项任务。每次 `submit()` 都产生独立的 Future，任务、线程池和已注册回调会让它按自己的生命周期运行。

#### F. `add_done_callback()` 为什么能接上后面的状态处理？

```python
future.add_done_callback(...)
```

它给这个 Future 注册“结束时要调用的函数”。正常返回、任务异常、取消都会使回调有机会被调用；不是只有授票成功才调用。

回调参数由 Future 机制提供，传进来的是**已经结束的 Future 本身**。所以源码里的 `lambda result` 虽然叫 `result`，它实际上仍是 Future，不是 JSON 响应。这个命名容易误解，阅读时可以把它想成 `completed_future`。

回调通常在执行任务并标记 Future 完成的工作线程中运行，不会自动安排到计时线程，也不是必定创建一个新回调线程。如果注册时 Future 已结束，`add_done_callback()` 会立即在注册线程中调用回调。停止线程池取消尚未执行的任务时，回调也可能由执行取消的线程触发。因此回调必须自己按统一约定处理锁。

#### G. `lambda` 里的默认参数为什么这么写？

```python
lambda result, peer=peer_id, rpc=path, term=body["term"]:
    self._receive_response(peer, rpc, term, result)
```

这里只收到一个外部实参，也就是结束的 Future；`peer`、`rpc`、`term` 使用创建这个 lambda 当时算出的默认值。可以近似展开成：

```python
def on_done(completed_future, peer=peer_id, rpc=path, term=body["term"]):
    self._receive_response(peer, rpc, term, completed_future)

future.add_done_callback(on_done)
```

每次循环创建一份回调，把“发给谁、哪种 RPC、发出时的任期”带回来。尤其 `peer=peer_id` 固定了本次循环的节点 ID。若直接在延后执行的 lambda 里读取循环变量 `peer_id`，多个回调可能都读到循环结束时的最后一个 ID，导致清理错 `_busy` 或把票算错人。

`term=body["term"]` 记录的是请求发出时的任期。响应回来的时候本节点可能已经进入另一任期，这个旧值正是 `_receive_response()` 判断消息是否过时所需的信息。

#### H. 为什么 `future.result()` 在这里不会继续等网络？

响应处理函数开头是：

```python
flag = "voteGranted" if path == "/raft/request-vote" else "success"
try:
    response_term, accepted = parse_response(future.result(), flag)
except Exception as exc:
    response_term, accepted = None, False
    emit("rpc_failed", level=logging.DEBUG, ...)
```

一般情况下，对一个尚未完成的 Future 调用 `.result()` 会等待它完成。但这里是由 **done callback** 进入的，Future 已结束，因此 `.result()` 会直接返回保存的字典，或重新抛出任务保存的异常；如果任务取消，则抛出取消异常。`try` 也覆盖响应字段校验失败。

这部分解析发生在进入节点状态锁之前。它主要使用这一个任务的结果，不需要先锁住共享节点状态。失败时令 `response_term = None`，稍后仍要取得状态锁清理 `_busy`，避免这个对端从此永远不能重试。当前 `rpc_failed` 使用 DEBUG 级别，默认 INFO 日志下通常看不到。

### 6.12 逐行解释你贴出的 `_receive_response()` 临界区

先固定一个例子：当前对象是 n1；它曾向 n2 发出任期 3 的投票请求。于是 `peer_id == "n2"`，`sent_term == 3`。请求完成后，`response_term` 和 `accepted` 来自响应解析；本地 `current_term`、`role` 则要在锁内读取，因为等待期间可能已经变化。

```python
with self._condition:
    self._busy.discard(peer_id)
    if not self._running or response_term is None:
        return
    # 锁只能防止同时修改；任期检查还要防止网络迟到的旧消息改变新状态。
    if response_term > self.current_term:
        self._follow(response_term)
    if (flag == "voteGranted" and accepted and self.role == "candidate"
            and sent_term == self.current_term == response_term):
        self._votes.add(peer_id)
        if len(self._votes) >= self.majority:
            self._become_leader()
```

**第 1 行：`with self._condition:`**

取得这一个 n1 对象的状态锁。其他线程持锁时，回调在此等候；拿到锁后，才读取当前状态，不能沿用发请求时的角色判断。这把锁不锁 n2，也不阻止 n2 的线程运行。

**第 2 行：`self._busy.discard(peer_id)`**

把 n2 的任务登记清掉，因为这次网络操作已经结束，允许后续广播再次给 n2 提交请求。使用集合的 `discard()`，元素不存在时也不报错；`remove()` 在元素不存在时会报错。

这里的“空闲”指 n1 可以再向 n2 发出任务，不表示 n2 本身空闲或健康。必须在失败判断之前清理：连接失败也需要允许重试。

**第 3～4 行：检查运行标志和解析结果，必要时 `return`**

`not self._running` 表示节点正在停止或已经停止，此时不再计票、当选或更新任期。`response_term is None` 表示任务失败或响应无法按协议解析，不能把这次结果当成有效投票。

`or` 表示满足任一条件就返回。返回时会自动退出 `with`，释放本次持锁。它不会因为 `return` 留下一把没有释放的锁。

**第 6～7 行：更高任期先处理**

如果响应里的任期比本地高，调用 `_follow(response_term)`，更新本地任期、清空旧任期的 `voted_for`、切换到 Follower、清空竞选票数、取消已知 Leader，并通知计时线程。

这个调用仍然在外层锁里，其他线程不能在“任期已升高但角色尚未降级”的中间插入。`_follow()` 没有再次写 `with`，不等于它在锁外运行。

为什么不先过滤 `sent_term != current_term`？例如本地已在任期 4，收到任期 2 发出的请求的迟到响应，但响应显示对方已在任期 6。它虽然不能算作本轮投票，仍携带了更新的任期信息，必须先按任期 6 降级。

**第 8～9 行：五个条件全部成立才计票**

| 条件 | 要排除的情况 |
| --- | --- |
| `flag == "voteGranted"` | 这是心跳响应，不是投票响应 |
| `accepted` | 对方拒绝授票 |
| `self.role == "candidate"` | 自己已退回 Follower，或已经是 Leader，不能再靠这条响应触发竞选当选 |
| `sent_term == self.current_term` | 请求属于旧一轮选举 |
| `self.current_term == response_term` | 对方响应的任期与当前竞选不一致 |

Python 的 `a == b == c` 表示 `a == b and b == c`。`and` 表示所有条件都要成立，并会在遇到第一个假条件时停止继续求值。

如果上一步刚因更高任期退回 Follower，这里的角色检查自然失败，所以“发现更高任期”和“按旧票当选”不会同时发生。

**第 10 行：`self._votes.add(peer_id)`**

将 n2 加入支持自己的节点集合。集合按成员 ID 去重，已存在 n2 时不会再加一票。集合本身不能替代锁：必须把“确认仍在当前竞选 → 加入票数 → 判断多数派 → 改为 Leader”整个步骤保护起来。

**第 11～12 行：检查多数派并当选**

`len(self._votes)` 是包含自投票在内的有效支持节点数，`self.majority` 按静态完整成员数计算。满足多数票时调用 `_become_leader()`。

它在同一临界区内设置 `role = "leader"`、`leader_id = self.node_id`、`_next_heartbeat = self.clock()`，然后 `notify_all()`。这次通知是让计时线程尽快重新判断并安排心跳，不是在回调里直接发送心跳。

如果 n2 和 n3 的同意票几乎同时到达，两个回调都可以先完成解析，但只有一个先取得锁。第一个让节点当选后，第二个取得锁时会发现角色已是 Leader，不会再次执行当选流程。

#### 对照几组输入，看这一段实际做什么

| 锁内当前状态 | 发出任期 / 响应 | 结果 |
| --- | --- | --- |
| Candidate，任期 3，自投一票，三节点 | 发出 3，响应 3，同意 | 加入该对端，达到两票则当选 |
| Candidate，任期 4 | 发出 3，响应 3，同意 | 清理占用，不计旧票 |
| Follower，任期 3 | 发出 3，响应 3，同意 | 清理占用，不重新竞选 |
| Candidate，任期 4 | 发出 2，响应 6，拒绝 | 先更新到任期 6 并退回 Follower，不计票 |
| Leader，任期 3 | 心跳响应 5，`success=false` | 更新到任期 5 并退回 Follower，心跳也能带回新任期 |
| 任意角色，正在停止 | 任意已完成结果 | 只清理占用，不改变选举状态 |
| 正在运行，但任务抛出异常 | 无有效 `response_term` | 清理占用，等待后续周期再尝试 |

### 6.13 两条实际执行时序：为什么 `RLock` 和回调有关？

#### 通常路径：任务完成时，计时线程已进入等待

```text
计时线程 raft-timer                      网络工作线程 raft-rpc_0
取得状态锁
把 n2 加入 _busy
submit(call, n2地址, 路径, 请求体) ──────→ 开始执行 HTTP 请求
拿到 Future，注册回调                    等待对端响应（不持状态锁）
继续为 n3 提交任务
Condition.wait() 释放锁                  请求结束，Future 保存响应字典
等待                                    执行回调，解析 Future.result()
                                        取得状态锁，清理 _busy、检查任期、计票
                                        若当选则 notify_all()，此时仍持锁
已被通知，但要等锁                       退出 with，释放状态锁
重新取得锁
发现已经是 Leader，尽快提交心跳
```

#### 快速路径：注册回调前，Future 已经结束

网络任务可能很快返回或很快连接失败，发生下面的顺序：

```text
计时线程已持有状态锁
    ↓
submit() 返回 Future；工作线程已经将它标记为完成
    ↓
计时线程调用 future.add_done_callback(...)
    ↓
回调立即在计时线程中执行
    ↓
_receive_response() 再进入 with self._condition
```

这时并不是“另一个线程想进来”，而是**同一个计时线程再次获取自己已经持有的锁**。`RLock` 允许重入，因此能够继续执行；不可重入的普通 `Lock` 在这条路径下可能让线程等自己释放锁而死锁。

退出回调里的 `with` 只释放内层这一次获取，外层 `_run()` 的锁仍然持有。计时线程会继续执行 `_broadcast()`，直到外层调用 `wait()` 才暂时完全释放状态锁。

如果回调在另一个工作线程中触发，则没有重入特权，仍需等待计时线程释放锁。`RLock` 允许的是同一个线程重复进入，不是允许多个线程同时进入。

同一线程的即时回调还说明：持锁不等于当前调用链不会修改状态。外部线程虽然不能插入，但当前线程自己调用回调仍可能改变角色，所以读代码时要把同步触发的回调也当作执行流程的一部分。

### 6.14 用自己的话记住这几个概念

| 概念 | 在这个项目里回答什么问题 |
| --- | --- |
| 状态锁 `RLock` | 多个线程怎样有序检查和修改同一个节点？ |
| 条件变量 `Condition` | 计时线程怎样等待，并在角色或时间变化后重新检查？ |
| 线程池 `ThreadPoolExecutor` | 谁来执行可能很慢的出站 HTTP 请求？ |
| `Future` | 怎样保存某一个异步任务的返回值、异常和完成状态？ |
| done callback | 任务结束后，怎样接着处理结果？ |
| 任期检查 | 回来的结果是否仍属于当前竞选，是否发现更新任期？ |
| `_votes` 集合 | 怎样保证一个成员只算一票？ |
| `_busy` 集合 | 怎样避免给同一个对端不断堆积请求？ |

可以这样复述整个过程：“计时线程在锁内决定要发什么，把任务交给线程池。工作线程在锁外做 HTTP，并把结果存入 Future。任务结束触发回调，回调取得同一把锁，先清理占用，再检查运行状态和任期，然后决定是否计票或降级。角色或截止时间改变时，条件变量通知计时线程重新判断。”

## 7. 各源码文件怎么读？

建议顺序：`config.py` → `protocol.py` → `__main__.py` → `node.py` → `server.py` → `transport.py`。

### config.py：知道“有哪些人”和“要等多久”

`Config` 是数据类，保存 `nodes` 和四个时间参数。`load()` 读取 JSON 并验证本节点存在、成员地址有效且不重复、时间参数为有限正数且满足约束。

默认心跳 100 ms、选举超时 500～1000 ms、出站 socket 超时 200 ms。随机超时降低多个节点同时竞选的概率；没有随机性时，它们容易反复同时超时、相互分票。

`frozen=True` 防止属性重新赋值，但不代表其中的字典天然不可变；项目约定加载后的配置和成员映射不再修改。动态修改集群成员需要另外的协议，本期不支持。

### protocol.py：定义请求字段，先检查再执行

`RequestVote`、`AppendEntries` 是协议数据类，字段名与 HTTP JSON 一致。`.parse()` 检查数据，再构造请求对象。算法代码因此不需要重复处理缺字段、字符串类型任期等输入错误。

`integer()` 用 `type(value) is int` 排除布尔值，因为在 Python 中 `bool` 是 `int` 的子类，但 JSON 的 `true` 不应被视为任期 1。

`log_is_up_to_date()` 比较 `(lastLogTerm, lastLogIndex)`，Python 元组按先后顺序比较。例如 `(3, 1)` 比 `(2, 100)` 更新，不能只比较索引。当前本地日志始终为空。

### node.py：真正决定角色和任期

| 函数 | 主要职责 |
| --- | --- |
| `start()` | 开始计时，启动后台状态循环 |
| `stop()` | 设置停止标志、唤醒并等待计时线程、关闭出站线程池 |
| `_reset_deadline()` | 随机设置下一次选举截止时间并通知计时线程 |
| `_set_term_vote()` | 在调用者持锁时共同更新内存任期和投票，不写磁盘 |
| `_follow()` | 必要时更新任期并清空旧票，退回 Follower，清除旧竞选计票 |
| `request_vote()` | 处理别人向自己申请投票 |
| `append_entries()` | 处理 Leader 心跳、更新 Leader 信息和选举时间 |
| `_start_election()` | 开始新任期，投自己、向其他节点请求投票 |
| `_become_leader()` | 当选并安排立即发送心跳 |
| `_broadcast()` | 为没有在途 RPC 的对端提交并行请求 |
| `_receive_response()` | 处理成功或失败的出站请求结果，更高任期降级，当前轮次计票 |
| `_run()` | 根据当前角色等待选举截止时间或下一次心跳时间 |
| `status()` | 在锁内生成状态快照 |
| `last_log()` | 日志元数据扩展位置，当前返回 `(0, 0)` |

容易忽略的规则：同任期心跳降级不清空该任期已投的票；更高任期才清空旧票。拒绝投票和旧任期心跳不刷新超时；否则无效请求也能让节点永远不选举。合法当前任期 Leader 的心跳即使前置日志匹配失败，也说明 Leader 正在通信，因此会刷新计时器，但返回 `success: false`。

### server.py：把 HTTP 请求交给节点

`ThreadingHTTPServer` 为请求创建处理线程。`GET /status` 返回状态，`GET /health` 表示本节点进程正在运行。

`do_POST()` 先检查请求体大小、解析 JSON、校验协议字段，再调用节点方法。非法字段返回 400，超过 64 KiB 返回 413；合法但旧任期的请求仍返回 HTTP 200，并在 `voteGranted` 或 `success` 中表达拒绝。

HTTP 200 的含义是请求被正常处理，不代表一定授票或认可心跳。`/health` 返回正常也不代表已拥有多数派。

### transport.py：给别的节点打电话

`HTTPTransport.call()` 使用标准库 `http.client` 发 POST，将字典编码成 JSON，读回结果并关闭连接。请求失败产生的异常保存在 Future 中，回调读取结果时捕获异常并清理占用；失败不会增加支持票，也没有单独的“反对票计数”。候选人下一次超时会重新竞选，Leader 则在后续心跳周期再次尝试。线程池、Future 与回调的逐步解释见 6.11～6.13。

### events.py：方便观察发生了什么

`emit()` 输出一行一个事件的 JSON 日志。观察 `election_started`、`vote_granted`、`became_leader` 和 `became_follower`，就能把运行现象对应到状态转换函数。

### __main__.py：组织整个程序的生命周期

Ctrl+C 处理函数只设置 `threading.Event`，不在信号处理函数里做复杂关闭。主循环发现事件后调用 `node.stop()`，等待后台任务结束，再停止 HTTP 服务、释放端口。

停止节点时先在锁内改运行标志，再在锁外 `join()`、关闭线程池；如果持着状态锁等待另一个也需要锁的线程退出，会造成死锁。

## 8. 答辩可以怎样回答？

**问：你的项目是 in-memory 吗？**

答：是。角色、任期、投票、已知 Leader、计票集合和计时器都保存在各进程的 `RaftNode` 对象里，没有数据库和状态文件。重启会丢失状态，所以当前版本用于演示，不提供完整 Raft 崩溃恢复的安全保证。

**问：节点状态改变时怎么同步线程？**

答：HTTP 请求线程、计时线程、RPC 回调线程都会访问同一节点对象。我用同一个 `Condition(RLock)` 保护角色、任期、投票和计时器，把条件判断与状态更新放在同一临界区。状态变化后通过条件变量唤醒计时线程；等待时释放锁。出站网络请求在线程池中执行，不持有状态锁，回调再加锁核对任期。

**问：能举一个必须加锁的例子吗？**

答：两个候选人可能同时请求同一节点投票。不加锁时，两个处理线程都可能看到“尚未投票”，然后分别返回同意。加锁后，第二个线程检查时会看到第一票已经投出，所以不会在同一任期投给不同候选人。这个保证只覆盖本次进程运行期间。

**问：为什么需要多数票？**

答：同一成员集合中的任意两个多数派至少有一个共同成员。如果每个成员在同一任期只投给一人，就不能同时支持两个候选人获得多数票。三节点两票、五节点三票。该论证依赖投票记录不丢失，内存版跨重启不满足这个前提。

**问：内存重启真的会影响安全性吗？**

答：会。例如任期 1 中 A 获得 A、B 两票当选；A 随后被网络隔离，仍认为自己是 Leader。B、C 都重启，忘记任期和旧投票，如果在收到 A 消息前又从任期 1 竞选，就可能选出另一个任期 1 的 Leader。正常网络下重启能够收敛，不等于所有崩溃、分区组合下都安全。

**问：Leader 不可达后，会主动退位吗？**

答：本项目没有 CheckQuorum。失联的旧 Leader 可能继续报告自己是 Leader；其他节点可在更高任期选出新 Leader。旧 Leader 恢复通信、发现更高任期后会退回 Follower。“同一任期最多一个当选 Leader”与“任何时刻所有节点都报告同一 Leader”是不同的说法，前者在本内存版中还要限定没有丢失投票记录的场景。

**问：为什么 Candidate 收到心跳要退回去？**

答：如果心跳任期至少等于当前任期，说明本轮或更新一轮已有 Leader，候选人应结束竞选。收到旧任期心跳则继续竞选。

**问：心跳是不是日志复制？**

答：Raft 共用 AppendEntries RPC，但本项目只支持空 `entries` 作为心跳。非空日志返回 400；没有实现日志写入、提交和状态机应用。

**问：你会怎么演示？**

答：启动三个节点，查询 `/status` 展示一个 Leader、两个 Follower；等待观察任期稳定；停止 Leader，展示其余节点选出更高任期的 Leader；再停止一个节点，说明失去多数派后不会产生新 Leader。线程同步可以通过代码中的临界区及同时投票请求的例子说明。

## 9. 建议的练习顺序

1. 打开三个终端按 README 启动，自己找出 Leader，不预设一定是 n1。
2. 从日志找到一次 `election_started`，对照 `_start_election()` 看哪些字段改变。
3. 找到一次 `vote_granted`，对照 `request_vote()` 用自己的话讲出所有授票条件。
4. 对照 `_receive_response()` 解释为什么“锁、任期检查、票数集合”三者都需要。
5. 停止 Leader 后，先预测剩余节点如何变化，再看日志验证。
6. 关闭全部节点重新启动，观察内存状态重新初始化，说明为什么完整 Raft 还需要持久化。

能独立解释一次投票请求从 HTTP 入口进入状态机、加锁修改、返回结果，再在候选人回调中计票的过程，就已经掌握了这个项目的主线。
