# Project1 StandaloneKV

在本项目中，你将构建一个支持列族（Column Family）的独立键值存储 [gRPC](https://grpc.io/docs/guides/) 服务。Standalone 意味着只有单节点，而不是分布式系统。[Column Family]( <https://en.wikipedia.org/wiki/Standard_column_family> )（下文简称为 CF）是一个类似于键命名空间的概念，即不同列族中相同键的值是不同的。你可以简单地将多个列族视为独立的小型数据库。它的使用是为了支持 project4 中的事务模型，届时你将会了解为什么 TinyKV 需要支持 CF。

该服务支持四种基本操作：Put/Delete/Get/Scan。它维护了一个简单的键值对数据库。键和值都是字符串。`Put` 替换数据库中指定 CF 下某个键的值，`Delete` 删除指定 CF 下某个键的值，`Get` 获取指定 CF 下某个键的当前值，`Scan` 获取指定 CF 下一系列键的当前值。

本项目可分为 2 个步骤，包括：

1. 实现独立存储引擎。
2. 实现 raw key/value 服务处理器。

### 代码结构

`gRPC` 服务器在 `kv/main.go` 中初始化，它包含一个 `tinykv.Server`，该 Server 提供名为 `TinyKv` 的 `gRPC` 服务。它由 [protocol-buffer]( https://developers.google.com/protocol-buffers ) 在 `proto/proto/tinykvpb.proto` 中定义，rpc 请求和响应的详细定义在 `proto/proto/kvrpcpb.proto` 中。

通常你不需要修改 proto 文件，因为所有必要的字段都已经为你定义好了。但如果你仍然需要修改，可以修改 proto 文件后运行 `make proto` 来更新 `proto/pkg/xxx/xxx.pb.go` 中相关的生成 Go 代码。

此外，`Server` 依赖于一个 `Storage` 接口，你需要为独立存储引擎实现该接口，代码位于 `kv/storage/standalone_storage/standalone_storage.go`。一旦在 `StandaloneStorage` 中实现了 `Storage` 接口，你就可以使用它为 `Server` 实现 raw key/value 服务。

#### 实现独立存储引擎

第一个任务是实现 [badger](https://github.com/dgraph-io/badger) 键值 API 的封装。gRPC 服务的 Server 依赖于一个 `Storage` 接口，该接口定义在 `kv/storage/storage.go` 中。在这里，独立存储引擎只是 badger 键值 API 的封装，它提供了两个方法：

``` go
type Storage interface {
    // Other stuffs
    Write(ctx *kvrpcpb.Context, batch []Modify) error
    Reader(ctx *kvrpcpb.Context) (StorageReader, error)
}
```

`Write` 应该提供一种方式来将一系列修改应用到内部状态上，在这种情况下，内部状态就是一个 badger 实例。

`Reader` 应该返回一个 `StorageReader`，它支持在快照上进行键值的点查和扫描操作。

你现在不需要考虑 `kvrpcpb.Context`，它将在后续项目中使用。

> 提示：
>
> - 你应该使用 [badger.Txn]( https://godoc.org/github.com/dgraph-io/badger#Txn ) 来实现 `Reader` 函数，因为 badger 提供的事务处理器可以提供键值的一致性快照。
> - Badger 本身不支持列族。engine_util 包（`kv/util/engine_util`）通过给键添加前缀来模拟列族。例如，属于特定列族 `cf` 的键 `key` 会被存储为 `${cf}_${key}`。它封装了 `badger` 以提供带 CF 的操作，还提供了许多有用的辅助函数。因此你应该通过 `engine_util` 提供的方法来进行所有读写操作。请阅读 `util/engine_util/doc.go` 了解更多。
> - TinyKV 使用的是原始 `badger` 的一个 fork 版本，其中包含一些修复，所以请使用 `github.com/Connor1996/badger` 而不是 `github.com/dgraph-io/badger`。
> - 不要忘记对 badger.Txn 调用 `Discard()`，并在 discard 之前关闭所有迭代器。

#### 实现服务处理器

本项目的最后一步是使用已实现的存储引擎来构建 raw key/value 服务处理器，包括 RawGet/RawScan/RawPut/RawDelete。处理器已经为你定义好了，你只需要在 `kv/server/raw_api.go` 中填写实现即可。完成后，记得运行 `make project1` 来通过测试套件。
