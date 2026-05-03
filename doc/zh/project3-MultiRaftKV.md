# Project3 MultiRaftKV

在 project2 中，你已经构建了一个基于 Raft 的高可用 kv 服务器，做得好！但这还不够，这样的 kv 服务器由单个 Raft group 支撑，无法无限扩展，而且每个写请求都要等到提交后逐个写入 badger，这是保证一致性的关键要求，但同时也扼杀了所有并发性。

![multiraft](imgs/multiraft.png)

在本项目中，你将实现一个带有均衡调度器的多 Raft kv 服务器，它由多个 Raft group 组成，每个 Raft group 负责一个单独的 key range，在这里被称为 Region，布局如上图所示。对单个 Region 的请求处理方式与之前相同，但多个 Region 可以并发处理请求，从而提升性能，同时也带来了一些新的挑战，比如如何将请求均衡到各个 Region 等。

本项目分为 3 个部分，包括：

1. 实现 Raft 算法的成员变更和领导权变更
2. 在 raftstore 上实现 ConfChange 和 Region Split
3. 引入调度器

## Part A

在本部分中，你将为基本 Raft 算法实现成员变更和领导权变更，这些功能是后续两个部分所必需的。成员变更，即 ConfChange，用于向 Raft group 添加或移除 Peer，这会改变 Raft group 的 quorum，所以需要小心处理。领导权变更，即 Leader Transfer，用于将领导权转移到另一个 Peer，这对于均衡非常有用。

### 代码说明

你需要修改的代码都集中在 `raft/raft.go` 和 `raft/rawnode.go` 中，同时可以参考 `proto/proto/eraft.proto` 中需要处理的新消息类型。ConfChange 和 Leader Transfer 都由上层应用触发，所以你可以从 `raft/rawnode.go` 开始。

### 实现 Leader Transfer

为了实现 Leader Transfer，我们引入两种新的消息类型：`MsgTransferLeader` 和 `MsgTimeoutNow`。要转移领导权，你需要首先在当前 Leader 上调用 `raft.Raft.Step` 并传入 `MsgTransferLeader` 消息，为了确保转移成功，当前 Leader 应该首先检查被转移者（即转移目标）的资格，比如：被转移者的日志是否是最新的等。如果被转移者不具备资格，当前 Leader 可以选择中止转移或者帮助被转移者，既然中止无益，让我们选择帮助被转移者。如果被转移者的日志不是最新的，当前 Leader 应该向被转移者发送 `MsgAppend` 消息并停止接受新的提案，以避免陷入循环。因此，如果被转移者具备资格（或者在当前 Leader 的帮助下变得具备资格），Leader 应该立即向被转移者发送 `MsgTimeoutNow` 消息，被转移者收到 `MsgTimeoutNow` 消息后应立即发起新的选举，无需等待选举超时，凭借更高的 term 和最新的日志，被转移者有很大机会击败当前 Leader 并成为新的 Leader。

### 实现 ConfChange

你在这里实现的 ConfChange 算法不是 Raft 扩展论文中提到的联合共识（Joint Consensus）算法，后者可以同时添加和/或移除任意多个 Peer，而这里的算法只能逐个添加或移除 Peer，这更简单也更容易推理。此外，ConfChange 从调用 Leader 的 `raft.RawNode.ProposeConfChange` 开始，它会提出一个 `pb.Entry.EntryType` 设置为 `EntryConfChange` 的条目，`pb.Entry.Data` 设置为输入的 `pb.ConfChange`。当类型为 `EntryConfChange` 的条目被提交后，你必须通过 `RawNode.ApplyConfChange` 并传入条目中的 `pb.ConfChange` 来应用它，然后才能通过 `raft.Raft.addNode` 和 `raft.Raft.removeNode` 根据 `pb.ConfChange` 向此 Raft 节点添加或移除 Peer。

> 提示：
>
> - `MsgTransferLeader` 消息是本地消息，不是来自网络的
> - 你需要将 `MsgTransferLeader` 消息的 `Message.from` 设置为被转移者（即转移目标）
> - 要立即发起新的选举，可以调用 `Raft.Step` 并传入 `MsgHup` 消息
> - 调用 `pb.ConfChange.Marshal` 获取 `pb.ConfChange` 的字节表示，并将其放入 `pb.Entry.Data`

## Part B

既然 Raft 模块已经支持了成员变更和领导权变更，在本部分中你需要让 TinyKV 基于 Part A 支持这些管理命令。如 `proto/proto/raft_cmdpb.proto` 所示，有四种类型的管理命令：

- CompactLog（已在 project2 part C 中实现）
- TransferLeader
- ChangePeer
- Split

`TransferLeader` 和 `ChangePeer` 是基于 Raft 的领导权变更和成员变更支持的命令。这些将作为均衡调度器的基本操作步骤。`Split` 将一个 Region 分成两个 Region，这是多 Raft 的基础。你将逐步实现它们。

### 代码说明

所有更改都基于 project2 的实现，所以你需要修改的代码都集中在 `kv/raftstore/peer_msg_handler.go` 和 `kv/raftstore/peer.go` 中。

### Propose Transfer Leader

这一步相当简单。作为一条 Raft 命令，`TransferLeader` 将作为 Raft 条目被提出。但 `TransferLeader` 实际上是一个不需要复制到其他 Peer 的操作，所以你只需调用 `RawNode` 的 `TransferLeader()` 方法，而不是为 `TransferLeader` 命令调用 `Propose()`。

### 在 raftstore 中实现 ConfChange

ConfChange 有两种不同类型，`AddNode` 和 `RemoveNode`。顾名思义，它们分别向 Region 添加 Peer 或从 Region 中移除 Peer。要实现 ConfChange，你需要先了解 `RegionEpoch` 的概念。`RegionEpoch` 是 `metapb.Region` 元信息的一部分。当 Region 添加或移除 Peer 或者进行 Split 时，Region 的 epoch 会发生变化。RegionEpoch 的 `conf_ver` 在 ConfChange 期间递增，而 `version` 在 Split 期间递增。它将用于保证在网络隔离情况下拥有最新的 Region 信息，避免一个 Region 中出现两个 Leader。

你需要让 raftstore 支持处理 ConfChange 命令。流程如下：

1. 通过 `ProposeConfChange` 提出 ConfChange 管理命令
2. 日志提交后，更改 `RegionLocalState`，包括 `Region` 中的 `RegionEpoch` 和 `Peers`
3. 调用 `raft.RawNode` 的 `ApplyConfChange()`

> 提示：
>
> - 对于执行 `AddNode`，新添加的 Peer 将由 Leader 的心跳创建，请查看 `storeWorker` 的 `maybeCreatePeer()`。此时，该 Peer 是未初始化的，我们不知道其 Region 的任何信息，因此用 0 来初始化其 `Log Term` 和 `Index`。Leader 随后会知道这个 Follower 没有数据（存在从 0 到 5 的日志缺口），它会直接向该 Follower 发送快照。
> - 对于执行 `RemoveNode`，你应该显式调用 `destroyPeer()` 来停止 Raft 模块。销毁逻辑已经为你提供。
> - 不要忘记更新 `GlobalContext` 中 `storeMeta` 的 Region 状态
> - 测试代码会多次调度同一个 ConfChange 命令，直到 ConfChange 被应用，所以你需要考虑如何忽略同一 ConfChange 的重复命令。

### 在 raftstore 中实现 Region Split

![raft_group](imgs/keyspace.png)

为了支持多 Raft，系统进行数据分片，使每个 Raft group 只存储一部分数据。Hash 和 Range 是常用的数据分片方式。TinyKV 使用 Range，主要原因是 Range 可以更好地聚合具有相同前缀的 key，便于 scan 等操作。此外，Range 在 Split 方面优于 Hash。通常，Split 只涉及元数据修改，不需要移动数据。

``` protobuf
message Region {
 uint64 id = 1;
 // Region key range [start_key, end_key).
 bytes start_key = 2;
 bytes end_key = 3;
 RegionEpoch region_epoch = 4;
 repeated Peer peers = 5
}
```

让我们重新看一下 Region 的定义，它包含 `start_key` 和 `end_key` 两个字段，用于表示 Region 负责的数据范围。因此，Split 是支持多 Raft 的关键步骤。在最开始，只有一个 Region，范围为 ["", "")。你可以将 key 空间视为一个环，所以 ["", "") 代表整个空间。随着数据的写入，Split 检查器会每隔 `cfg.SplitRegionCheckTickInterval` 检查 Region 大小，如果可能的话生成一个 Split key 将 Region 切成两部分，你可以查看 `kv/raftstore/runner/split_check.go` 中的逻辑。Split key 会被包装为 `MsgSplitRegion`，由 `onPrepareSplitRegion()` 处理。

为了确保新创建的 Region 和 Peer 的 ID 是唯一的，ID 由调度器分配。这部分已经提供，所以你不需要实现它。`onPrepareSplitRegion()` 实际上是为 pd worker 调度一个任务，向调度器请求 ID。在收到调度器的响应后，构造一个 Split 管理命令，参见 `kv/raftstore/runner/scheduler_task.go` 中的 `onAskSplit()`。

所以你的任务是实现处理 Split 管理命令的过程，就像 ConfChange 那样。提供的框架支持多 Raft，参见 `kv/raftstore/router.go`。当一个 Region 分裂成两个 Region 时，其中一个 Region 将继承分裂前的元数据，只需修改其 Range 和 RegionEpoch，而另一个则需要创建相关的元信息。

> 提示：
>
> - 新创建的 Region 对应的 Peer 应通过 `createPeer()` 创建，并注册到 `router.regions`。该 Region 的信息应插入到 `ctx.StoreMeta` 的 `regionRanges` 中。
> - 在网络隔离情况下的 Region Split，待应用的快照可能与现有 Region 的范围重叠。检查逻辑在 `kv/raftstore/peer_msg_handler.go` 的 `checkSnapshot()` 中。请在实现时记住这一点并注意处理这种情况。
> - 使用 `engine_util.ExceedEndKey()` 与 Region 的 end key 进行比较。因为当 end key 等于 "" 时，任何 key 都等于或大于 ""。
> - 还有更多错误需要考虑：`ErrRegionNotFound`、`ErrKeyNotInRegion`、`ErrEpochNotMatch`。

## Part C

如上所述，我们 kv 存储中的所有数据被分成多个 Region，每个 Region 包含多个副本。一个问题出现了：我们应该把每个副本放在哪里？如何为副本找到最佳位置？谁发送前面的 AddPeer 和 RemovePeer 命令？Scheduler（调度器）承担了这些职责。

为了做出明智的决策，Scheduler 需要了解整个集群的一些信息。它应该知道每个 Region 在哪里。它应该知道它们有多少 key。它应该知道它们有多大……为了获取相关信息，Scheduler 要求每个 Region 定期向其发送心跳请求。你可以在 `/proto/proto/schedulerpb.proto` 中找到心跳请求结构 `RegionHeartbeatRequest`。收到心跳后，Scheduler 将更新本地的 Region 信息。

同时，Scheduler 会定期检查 Region 信息，以发现 TinyKV 集群中是否存在不均衡。例如，如果某个 Store 包含了过多的 Region，就应该将其中的 Region 移到其他 Store。这些命令将作为对应 Region 心跳请求的响应被返回。

在本部分中，你需要为 Scheduler 实现上述两个功能。按照我们的指南和框架进行，不会太困难。

### 代码说明

你需要修改的代码都集中在 `scheduler/server/cluster.go` 和 `scheduler/server/schedulers/balance_region.go` 中。如上所述，当 Scheduler 收到 Region 心跳时，它会先更新本地的 Region 信息。然后检查该 Region 是否有待处理的命令。如果有，将作为响应返回。

你只需要实现 `processRegionHeartbeat` 函数，在该函数中 Scheduler 更新本地信息；以及 balance-region 调度器的 `Schedule` 函数，在该函数中 Scheduler 扫描 Store 并判断是否存在不均衡以及应该移动哪个 Region。

### 收集 Region 心跳

如你所见，`processRegionHeartbeat` 函数的唯一参数是一个 RegionInfo。它包含了发送此心跳的 Region 的信息。Scheduler 需要做的就是更新本地的 Region 记录。但是否每次心跳都需要更新这些记录呢？

当然不是！有两个原因。一是当 Region 没有变化时可以跳过更新。更重要的是 Scheduler 不能信任每一次心跳。具体来说，如果集群中某部分出现了网络分区，某些节点的信息可能是错误的。

例如，一些 Region 在 Split 后重新发起选举和分裂，但另一批被隔离的节点仍然通过心跳向 Scheduler 发送过时的信息。所以对于同一个 Region，两个节点都可能声称自己是 Leader，这意味着 Scheduler 不能同时信任它们。

哪个更可信？Scheduler 应该使用 `conf_ver` 和 `version` 来判断，即 `RegionEpoch`。Scheduler 应首先比较两个节点的 Region version 值。如果值相同，Scheduler 再比较配置变更版本的值。配置变更版本更大的节点一定拥有更新的信息。

简单来说，你可以按照以下方式组织检查流程：

1. 检查本地存储中是否存在相同 Id 的 Region。如果存在且心跳的 `conf_ver` 和 `version` 中至少有一个小于已有记录的，则该心跳 Region 是过时的

2. 如果不存在，扫描所有与之重叠的 Region。心跳的 `conf_ver` 和 `version` 应该大于或等于所有这些 Region 的对应值，否则该 Region 是过时的。

那么 Scheduler 如何判断是否可以跳过此次更新呢？我们可以列出一些简单的条件：

* 如果新信息的 `version` 或 `conf_ver` 大于原有信息，则不能跳过

* 如果 Leader 发生了变化，则不能跳过

* 如果新信息或原有信息存在 pending peer，则不能跳过

* 如果 ApproximateSize 发生了变化，则不能跳过

* ……

不用担心。你不需要找到一个严格的充分必要条件。冗余更新不会影响正确性。

如果 Scheduler 决定根据此心跳更新本地存储，需要更新两项内容：Region 树和 Store 状态。你可以使用 `RaftCluster.core.PutRegion` 来更新 Region 树，使用 `RaftCluster.core.UpdateStoreStatus` 来更新相关 Store 的状态（如 Leader 数量、Region 数量、Pending Peer 数量……）。

### 实现 Region 均衡调度器

Scheduler 中可以运行多种不同类型的调度器，例如 balance-region 调度器和 balance-leader 调度器。本学习材料将重点关注 balance-region 调度器。

每个调度器都应该实现 Scheduler 接口，你可以在 `/scheduler/server/schedule/scheduler.go` 中找到。Scheduler 将使用 `GetMinInterval` 的返回值作为默认间隔来周期性运行 `Schedule` 方法。如果返回 null（经过若干次重试后），Scheduler 将使用 `GetNextInterval` 来增加间隔。通过定义 `GetNextInterval`，你可以定义间隔如何增长。如果返回一个 Operator，Scheduler 将把这些 Operator 作为相关 Region 下一次心跳的响应进行分发。

Scheduler 接口的核心部分是 `Schedule` 方法。该方法的返回值是 `Operator`，包含多个步骤，如 `AddPeer` 和 `RemovePeer`。例如，`MovePeer` 可能包含 `AddPeer`、`transferLeader` 和 `RemovePeer`，这些你在之前的部分已经实现了。以下图中第一个 RaftGroup 为例。调度器试图将 Peer 从第三个 Store 移到第四个。首先，它应该为第四个 Store `AddPeer`。然后它检查第三个 Store 是否是 Leader，发现不是，所以不需要 `transferLeader`。然后它移除第三个 Store 中的 Peer。

你可以使用 `scheduler/server/schedule/operator` 包中的 `CreateMovePeerOperator` 函数来创建 `MovePeer` Operator。

![balance](imgs/balance1.png)

![balance](imgs/balance2.png)

在本部分中，你唯一需要实现的函数是 `scheduler/server/schedulers/balance_region.go` 中的 `Schedule` 方法。该调度器避免一个 Store 中有过多 Region。首先，Scheduler 将选择所有合适的 Store。然后根据它们的 Region 大小排序。接着 Scheduler 尝试从 Region 大小最大的 Store 中找到要移动的 Region。

调度器将尝试在该 Store 中找到最适合移动的 Region。首先，它会尝试选择一个 pending Region，因为 pending 可能意味着磁盘过载。如果没有 pending Region，它会尝试找一个 follower Region。如果仍然选不出 Region，它会尝试选择 leader Region。最终选出要移动的 Region，或者 Scheduler 会尝试下一个 Region 大小较小的 Store，直到所有 Store 都被尝试过。

在你选好一个要移动的 Region 后，Scheduler 将选择一个目标 Store。实际上，Scheduler 会选择 Region 大小最小的 Store。然后 Scheduler 会判断这次移动是否有价值，通过检查原 Store 和目标 Store 的 Region 大小差异。如果差异足够大，Scheduler 应该在目标 Store 上分配一个新的 Peer 并创建 MovePeer Operator。

你可能已经注意到，上面的流程只是一个粗略的过程。还有很多问题留待解决：

* 哪些 Store 是适合移动的？

简而言之，合适的 Store 应该是处于 up 状态且宕机时间不超过集群的 `MaxStoreDownTime`，你可以通过 `cluster.GetMaxStoreDownTime()` 获取。

* 如何选择 Region？

Scheduler 框架提供了三种方法来获取 Region。`GetPendingRegionsWithLock`、`GetFollowersWithLock` 和 `GetLeadersWithLock`。Scheduler 可以从中获取相关的 Region。然后你可以随机选择一个 Region。

* 如何判断操作是否有价值？

如果原 Store 和目标 Store 的 Region 大小差异太小，在我们把 Region 从原 Store 移到目标 Store 之后，Scheduler 下次可能又想移回来。所以我们必须确保差异大于 Region 近似大小的两倍，这保证了移动后目标 Store 的 Region 大小仍然小于原 Store。
