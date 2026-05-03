# Project2 RaftKV

Raft 是一个旨在易于理解的一致性算法。你可以在 [Raft 官网](https://raft.github.io/) 阅读关于 Raft 本身的资料，包括 Raft 的交互式可视化以及其他资源，如 [Raft 扩展论文](https://raft.github.io/raft.pdf)。

在本项目中，你将实现一个基于 Raft 的高可用 KV 服务器，这不仅需要你实现 Raft 算法，还需要将其投入实际使用，并带来更多挑战，例如使用 `badger` 管理 Raft 的持久化状态、为快照消息添加流控制等。

本项目包含 3 个部分：

- 实现基本的 Raft 算法
- 在 Raft 之上构建容错 KV 服务器
- 添加 Raft 日志 GC 和快照的支持

## Part A

### 代码结构

在这一部分中，你将实现基本的 Raft 算法。你需要实现的代码位于 `raft/` 目录下。在 `raft/` 中，有一些骨架代码和测试用例等待你完成。你在此实现的 Raft 算法与上层应用之间有一个精心设计的接口。此外，它使用逻辑时钟（此处称为 tick）来衡量选举超时和心跳超时，而不是物理时钟。也就是说，不要在 Raft 模块内部设置定时器，上层应用负责通过调用 `RawNode.Tick()` 来推进逻辑时钟。除此之外，消息的发送和接收以及其他操作都是异步处理的，也由上层应用决定何时实际执行这些操作（详见下文）。例如，Raft 不会阻塞等待任何请求消息的响应。

在实现之前，请先查看本部分的提示。同时，你应该大致浏览 proto 文件 `proto/proto/eraftpb.proto`。Raft 发送和接收的消息及相关结构体都定义在那里，你在实现时会用到它们。注意，与 Raft 论文不同，它将 Heartbeat 和 AppendEntries 拆分为不同的消息，以使逻辑更清晰。

本部分可以分为 3 个步骤：

- Leader 选举
- 日志复制
- Raw node 接口

### 实现 Raft 算法

`raft/raft.go` 中的 `raft.Raft` 提供了 Raft 算法的核心，包括消息处理、驱动逻辑时钟等。更多实现指南，请查看 `raft/doc.go`，其中包含概述设计以及各种 `MessageTypes` 的职责说明。

#### Leader 选举

要实现 Leader 选举，你可以从 `raft.Raft.tick()` 开始，该函数用于将内部逻辑时钟推进一个 tick，从而驱动选举超时或心跳超时。你现在不需要关心消息的发送和接收逻辑。如果需要发送消息，只需将其推入 `raft.Raft.msgs`，Raft 收到的所有消息将通过 `raft.Raft.Step()` 传递。测试代码会从 `raft.Raft.msgs` 获取消息，并通过 `raft.Raft.Step()` 传入响应消息。`raft.Raft.Step()` 是消息处理的入口，你需要处理 `MsgRequestVote`、`MsgHeartbeat` 等消息及其响应。同时，请实现测试桩函数并确保它们被正确调用，例如 `raft.Raft.becomeXXX`，该函数用于在 Raft 角色变化时更新 Raft 的内部状态。

你可以运行 `make project2aa` 来测试实现，在本部分末尾可以查看一些提示。

#### 日志复制

要实现日志复制，你可以从处理发送方和接收方的 `MsgAppend` 和 `MsgAppendResponse` 开始。查看 `raft/log.go` 中的 `raft.RaftLog`，这是一个帮助管理 Raft 日志的辅助结构体，在这里你还需要通过 `raft/storage.go` 中定义的 `Storage` 接口与上层应用交互，以获取持久化的数据，如日志条目和快照。

你可以运行 `make project2ab` 来测试实现，在本部分末尾可以查看一些提示。

### 实现 Raw Node 接口

`raft/rawnode.go` 中的 `raft.RawNode` 是我们与上层应用交互的接口，`raft.RawNode` 包含 `raft.Raft` 并提供一些包装函数，如 `RawNode.Tick()` 和 `RawNode.Step()`。它还提供了 `RawNode.Propose()` 让上层应用提议新的 Raft 日志。

另一个重要的结构体 `Ready` 也定义在此处。在处理消息或推进逻辑时钟时，`raft.Raft` 可能需要与上层应用交互，例如：

- 向其他 peer 发送消息
- 将日志条目保存到稳定存储
- 将 HardState（如 term、commit index 和 vote）保存到稳定存储
- 将已提交的日志条目应用到状态机
- 等等

但这些交互不会立即发生，而是被封装在 `Ready` 中，通过 `RawNode.Ready()` 返回给上层应用。由上层应用决定何时调用 `RawNode.Ready()` 并处理它。处理完返回的 `Ready` 后，上层应用还需要调用一些函数，如 `RawNode.Advance()`，来更新 `raft.Raft` 的内部状态，如 applied index、stabled log index 等。

你可以运行 `make project2ac` 来测试实现，运行 `make project2a` 来测试整个 Part A。

> 提示：
>
> - 你可以在 `raft.Raft`、`raft.RaftLog`、`raft.RawNode` 和 `eraftpb.proto` 的消息中添加你需要的任何状态
> - 测试假设首次启动 Raft 时 term 为 0
> - 测试假设新选举的 Leader 应该在其 term 内追加一个 noop 条目
> - 测试假设一旦 Leader 推进了其 commit index，将通过 `MessageType_MsgAppend` 消息广播 commit index
> - 测试不会为本地消息 `MessageType_MsgHup`、`MessageType_MsgBeat` 和 `MessageType_MsgPropose` 设置 term
> - Leader 和非 Leader 的日志条目追加方式有很大不同，来源、检查和处理都不同，请小心处理
> - 不要忘记不同 peer 之间的选举超时应该不同
> - `rawnode.go` 中的一些包装函数可以通过 `raft.Step(local message)` 来实现
> - 启动新的 Raft 时，从 `Storage` 获取最后的稳定状态来初始化 `raft.Raft` 和 `raft.RaftLog`

## Part B

在这一部分中，你将使用 Part A 中实现的 Raft 模块构建一个容错的键值存储服务。你的键值服务将是一个复制状态机，由多个使用 Raft 进行复制的键值服务器组成。只要大多数服务器存活且可以通信，你的键值服务就应该能够继续处理客户端请求，即使存在其他故障或网络分区。

在 project1 中你已经实现了一个独立 KV 服务器，所以你应该已经熟悉了 KV 服务器 API 和 `Storage` 接口。

在介绍代码之前，你需要先理解三个术语：`Store`、`Peer` 和 `Region`，它们定义在 `proto/proto/metapb.proto` 中。

- Store 代表一个 tinykv-server 实例
- Peer 代表运行在一个 Store 上的 Raft 节点
- Region 是 Peer 的集合，也称为 Raft group

![region](imgs/region.png)

为简化起见，在 project2 中，一个 Store 上只有一个 Peer，集群中只有一个 Region。所以你现在不需要考虑 Region 的范围。多个 Region 将在 project3 中进一步介绍。

### 代码结构

首先，你应该查看 `kv/storage/raft_storage/raft_server.go` 中的 `RaftStorage`，它也实现了 `Storage` 接口。与 `StandaloneStorage` 直接读写底层引擎不同，它先将每个写和读请求发送到 Raft，然后在 Raft 提交请求后才对底层引擎进行实际的读写操作。通过这种方式，它可以保持多个 Store 之间的一致性。

`RaftStorage` 创建一个 `Raftstore` 来驱动 Raft。当调用 `Reader` 或 `Write` 函数时，它实际上通过 channel（即 `raftWorker` 的 `raftCh`）向 raftstore 发送一个 `RaftCmdRequest`（定义在 `proto/proto/raft_cmdpb.proto` 中），该请求包含四种基本命令类型（Get/Put/Delete/Snap），并在 Raft 提交并应用该命令后返回响应。`Reader` 和 `Write` 函数的 `kvrpc.Context` 参数现在变得有用了，它携带了客户端视角的 Region 信息，并作为 `RaftCmdRequest` 的 header 传递。这些信息可能不正确或已过时，因此 raftstore 需要检查它们并决定是否提议该请求。

接下来是 TinyKV 的核心——raftstore。其结构有些复杂，请阅读 TiKV 参考文档以更好地理解其设计：

- <https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore>  (中文版)
- <https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore> (英文版)

raftstore 的入口是 `Raftstore`，参见 `kv/raftstore/raftstore.go`。它启动一些 worker 来异步处理特定任务，其中大部分现在还用不到，你可以忽略它们。你只需要关注 `raftWorker`（`kv/raftstore/raft_worker.go`）。

整个过程分为两部分：raft worker 轮询 `raftCh` 获取消息，包括驱动 Raft 模块的基本 tick 和需要作为 Raft 条目提议的 Raft 命令；它从 Raft 模块获取并处理 Ready，包括发送 Raft 消息、持久化状态、将已提交的条目应用到状态机。一旦应用完成，就将响应返回给客户端。

### 实现 Peer Storage

Peer Storage 是你在 Part A 中通过 `Storage` 接口与之交互的组件，但除了 Raft 日志之外，Peer Storage 还管理其他持久化的元数据，这对于重启后恢复一致性状态机非常重要。此外，在 `proto/proto/raft_serverpb.proto` 中定义了三个重要状态：

- RaftLocalState：用于存储当前 Raft 的 HardState 和最后的 Log Index。
- RaftApplyState：用于存储 Raft 应用的最后 Log Index 以及一些截断的 Log 信息。
- RegionLocalState：用于存储 Region 信息和该 Store 上对应 Peer 的状态。Normal 表示该 Peer 正常，Tombstone 表示该 Peer 已从 Region 中移除，无法加入 Raft Group。

这些状态存储在两个 badger 实例中：raftdb 和 kvdb：

- raftdb 存储 Raft 日志和 `RaftLocalState`
- kvdb 存储不同 column family 中的键值数据、`RegionLocalState` 和 `RaftApplyState`。你可以将 kvdb 视为 Raft 论文中提到的状态机

格式如下，`kv/raftstore/meta` 中提供了一些辅助函数，并使用 `writebatch.SetMeta()` 将它们写入 badger。

| Key              | KeyFormat                        | Value            | DB   |
| :--------------- | :------------------------------- | :--------------- | :--- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

> 你可能想知道为什么 TinyKV 需要两个 badger 实例。实际上，它可以只使用一个 badger 来存储 Raft 日志和状态机数据。分成两个实例只是为了与 TiKV 的设计保持一致。

这些元数据应该在 `PeerStorage` 中创建和更新。创建 PeerStorage 时，参见 `kv/raftstore/peer_storage.go`。它初始化该 Peer 的 RaftLocalState 和 RaftApplyState，或者在重启的情况下从底层引擎获取之前的值。注意，RAFT_INIT_LOG_TERM 和 RAFT_INIT_LOG_INDEX 的值都是 5（只要大于 1 即可），而不是 0。不设置为 0 的原因是为了与配置变更后被动创建的 peer 区分。你现在可能不太理解这一点，只需记住即可，详细内容将在 project3b 中实现配置变更时描述。

在这部分中你需要实现的代码只有一个函数：`PeerStorage.SaveReadyState`，该函数的作用是将 `raft.Ready` 中的数据保存到 badger，包括追加日志条目和保存 Raft HardState。

追加日志条目时，只需将 `raft.Ready.Entries` 中的所有日志条目保存到 raftdb，并删除之前追加的永远不会被提交的日志条目。同时，更新 peer storage 的 `RaftLocalState` 并保存到 raftdb。

保存 HardState 也很简单，只需更新 peer storage 的 `RaftLocalState.HardState` 并保存到 raftdb。

> 提示：
>
> - 使用 `WriteBatch` 一次性保存这些状态。
> - 参考 `peer_storage.go` 中的其他函数了解如何读写这些状态。
> - 设置环境变量 LOG_LEVEL=debug 可能有助于调试，另见所有可用的 [日志级别](../log/log.go)。

### 实现 Raft Ready 处理流程

在 project2 Part A 中，你已经构建了一个基于 tick 的 Raft 模块。现在你需要编写外部流程来驱动它。大部分代码已经在 `kv/raftstore/peer_msg_handler.go` 和 `kv/raftstore/peer.go` 中实现了。所以你需要学习这些代码并完成 `proposeRaftCommand` 和 `HandleRaftReady` 的逻辑。以下是对框架的一些解释。

Raft `RawNode` 已经使用 `PeerStorage` 创建并存储在 `peer` 中。在 raft worker 中，你可以看到它取出 `peer` 并用 `peerMsgHandler` 包装它。`peerMsgHandler` 主要有两个函数：一个是 `HandleMsg`，另一个是 `HandleRaftReady`。

`HandleMsg` 处理从 raftCh 接收的所有消息，包括调用 `RawNode.Tick()` 驱动 Raft 的 `MsgTypeTick`、封装客户端请求的 `MsgTypeRaftCmd`，以及 Raft peer 之间传输的消息 `MsgTypeRaftMessage`。所有消息类型定义在 `kv/raftstore/message/msg.go` 中。你可以查看详细信息，其中一些将在后续部分中使用。

消息处理完毕后，Raft 节点应该有一些状态更新。因此 `HandleRaftReady` 应该从 Raft 模块获取 Ready 并执行相应的操作，如持久化日志条目、应用已提交的条目以及通过网络向其他 peer 发送 Raft 消息。

用伪代码表示，raftstore 使用 Raft 的方式如下：

``` go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
  }
}
```

此后，读或写的整个流程如下：

- 客户端调用 RPC RawGet/RawPut/RawDelete/RawScan
- RPC handler 调用 `RaftStorage` 的相关方法
- `RaftStorage` 向 raftstore 发送 Raft 命令请求，并等待响应
- `RaftStore` 将 Raft 命令请求作为 Raft 日志提议
- Raft 模块追加日志，并通过 `PeerStorage` 持久化
- Raft 模块提交日志
- Raft worker 在处理 Raft ready 时执行 Raft 命令，并通过 callback 返回响应
- `RaftStorage` 从 callback 接收响应并返回给 RPC handler
- RPC handler 执行一些操作并将 RPC 响应返回给客户端

你应该运行 `make project2b` 来通过所有测试。整个测试运行一个模拟集群，包括多个带有模拟网络的 TinyKV 实例。它执行一些读写操作并检查返回值是否符合预期。

需要注意的是，错误处理是通过测试的重要部分。你可能已经注意到，`proto/proto/errorpb.proto` 中定义了一些错误，错误是 gRPC 响应的一个字段。此外，在 `kv/raftstore/util/error.go` 中定义了实现 `error` 接口的相应错误，因此你可以将它们作为函数的返回值。

这些错误主要与 Region 相关。因此它也是 `RaftCmdResponse` 的 `RaftResponseHeader` 的一个成员。在提议请求或应用命令时，可能会出现一些错误。如果是这样，你应该返回带有错误的 Raft 命令响应，然后错误将被进一步传递到 gRPC 响应。你可以在返回带错误的响应时，使用 `kv/raftstore/cmd_resp.go` 中提供的 `BindRespError` 将这些错误转换为 `errorpb.proto` 中定义的错误。

在此阶段，你可能需要考虑以下错误，其他错误将在 project3 中处理：

- ErrNotLeader：Raft 命令在 follower 上提议。使用它让客户端尝试其他 peer。
- ErrStaleCommand：可能是由于 Leader 变更导致一些日志未被提交，而被新 Leader 的日志覆盖。但客户端不知道这一点，仍在等待响应。所以你应该返回此错误让客户端知道并重试命令。

> 提示：
>
> - `PeerStorage` 实现了 Raft 模块的 `Storage` 接口，你应该使用提供的方法 `SaveReadyState()` 来持久化 Raft 相关状态。
> - 使用 `engine_util` 中的 `WriteBatch` 使多次写入原子化，例如，你需要确保在同一个 write batch 中应用已提交的条目并更新 applied index。
> - 使用 `Transport` 向其他 peer 发送 Raft 消息，它在 `GlobalContext` 中。
> - 如果服务器不是多数派的一部分且没有最新数据，则不应完成 get RPC。你可以直接将 get 操作放入 Raft 日志中，或者实现 Raft 论文第 8 节中描述的只读操作优化。
> - 应用日志条目时不要忘记更新并持久化 apply state。
> - 你可以像 TiKV 那样以异步方式应用已提交的 Raft 日志条目。这不是必须的，虽然对性能提升是一个很大的挑战。
> - 提议时记录命令的 callback，应用后返回该 callback。
> - 对于 snap 命令的响应，应该显式设置 badger Txn 到 callback。
> - 在 2A 之后，一些测试你可能需要运行多次才能发现 bug

## Part C

就你当前的代码而言，长时间运行的服务器永远记住完整的 Raft 日志是不现实的。相反，服务器会检查 Raft 日志的数量，并定期丢弃超过阈值的日志条目。

在这一部分中，你将在上述两部分实现的基础上实现快照（Snapshot）处理。一般来说，Snapshot 与 AppendEntries 类似，都是用于向 follower 复制数据的 Raft 消息，不同之处在于其大小——Snapshot 包含某一时刻的完整状态机数据，一次性构建和发送如此大的消息会消耗大量资源和时间，可能会阻塞其他 Raft 消息的处理。为缓解此问题，Snapshot 消息将使用独立的连接，并将数据分块传输。这就是 TinyKV 服务有 snapshot RPC API 的原因。如果你对发送和接收的细节感兴趣，可以查看 `snapRunner` 和参考文档 <https://pingcap.com/blog-cn/tikv-source-code-reading-10/>

### 代码结构

你需要修改的所有内容都基于 Part A 和 Part B 中编写的代码。

### 在 Raft 中实现

虽然我们需要对 Snapshot 消息进行一些不同的处理，但从 Raft 算法的角度来看应该没有区别。查看 proto 文件中 `eraftpb.Snapshot` 的定义，`eraftpb.Snapshot` 上的 `data` 字段并不代表实际的状态机数据，而是一些上层应用使用的元数据，你现在可以忽略。当 Leader 需要向 follower 发送 Snapshot 消息时，可以调用 `Storage.Snapshot()` 获取一个 `eraftpb.Snapshot`，然后像其他 Raft 消息一样发送快照消息。状态机数据实际如何构建和发送由 raftstore 实现，将在下一步中介绍。你可以假设一旦 `Storage.Snapshot()` 成功返回，Raft Leader 就可以安全地向 follower 发送快照消息，follower 应该调用 `handleSnapshot` 来处理它，该函数主要是从消息中的 `eraftpb.SnapshotMetadata` 恢复 Raft 的内部状态，如 term、commit index 和成员信息等，此后，快照处理的过程就完成了。

### 在 Raftstore 中实现

在这一步中，你需要了解 raftstore 的另外两个 worker——raftlog-gc worker 和 region worker。

Raftstore 会根据配置 `RaftLogGcCountLimit` 定期检查是否需要 GC 日志，参见 `onRaftGcLogTick()`。如果需要，它会提议一个 Raft 管理命令 `CompactLogRequest`，该命令像 project2 Part B 中实现的四种基本命令类型（Get/Put/Delete/Snap）一样封装在 `RaftCmdRequest` 中。然后你需要在 Raft 提交该管理命令时处理它。但与 Get/Put/Delete/Snap 命令读写状态机数据不同，`CompactLogRequest` 修改的是元数据，即更新 `RaftApplyState` 中的 `RaftTruncatedState`。之后，你应该通过 `ScheduleCompactLog` 向 raftlog-gc worker 调度一个任务。Raftlog-gc worker 将异步执行实际的日志删除工作。

然后由于日志压缩，Raft 模块可能需要发送快照。`PeerStorage` 实现了 `Storage.Snapshot()`。TinyKV 在 region worker 中生成快照和应用快照。调用 `Snapshot()` 时，它实际上向 region worker 发送一个 `RegionTaskGen` 任务。Region worker 的消息处理器位于 `kv/raftstore/runner/region-task.go`。它扫描底层引擎以生成快照，并通过 channel 发送快照元数据。在下一次 Raft 调用 `Snapshot` 时，它检查快照生成是否完成。如果完成，Raft 应该向其他 peer 发送快照消息，快照的发送和接收工作由 `kv/storage/raft_storage/snap_runner.go` 处理。你不需要深入了解细节，只需知道快照消息在接收后会由 `onRaftMsg` 处理。

然后快照将反映在下一次 Raft Ready 中，所以你需要修改 Raft Ready 处理流程来处理快照的情况。当你确定要应用快照时，可以更新 peer storage 的内存状态，如 `RaftLocalState`、`RaftApplyState` 和 `RegionLocalState`。同时，不要忘记将这些状态持久化到 kvdb 和 raftdb，并从 kvdb 和 raftdb 中删除过时的状态。此外，你还需要将 `PeerStorage.snapState` 更新为 `snap.SnapState_Applying`，并通过 `PeerStorage.regionSched` 向 region worker 发送 `runner.RegionTaskApply` 任务，并等待 region worker 完成。

你应该运行 `make project2c` 来通过所有测试。
