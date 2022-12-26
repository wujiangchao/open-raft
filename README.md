# Open-Raft
[![Gitter](https://badges.gitter.im/brokercap-Bifrost/Bifrost.svg)](https://gitter.im/brokercap-Bifrost/Bifrost?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
[![Build Status](https://travis-ci.org/brokercap/Bifrost.svg?branch=v1.7.x)](https://travis-ci.org/brokercap/Bifrost)
[![License](https://img.shields.io/github/license/jc3wish/Bifrost.svg)](https://opensource.org/licenses/apache2.0)

## a Java implementation of  raft protocol   
raft协议：https://raft.github.io/

#### Leader election
leader 选举：Term最大、log最完整

随机选举超时时间避免冲突

prevote的必要性？

每次新增节点为什么要重新选举？

新加入一个节点term值取什么？

#### Log Replication and Recovery
快照用途

1. JRaft新节点加入后，如何快速追上最新的数据
2. Raft 节点出现故障重新启动后如何高效恢复到最新的数据



主要参与组件：

Replicator: Leader节点通过复制器发起安装快照文件请求

NodeImpl：Follower节点接收安装请求

SnapshotExecutorImpl：注册新的快照文件下载任务，开始从 Leader 节点下载快照文件，并阻塞等待下载过程的完成

FSMCallerImpl：将快照数据透传给业务，并由业务决定如何在本地恢复快照所蕴含的状态数据

StateMachine：获取快照文件对应的元数据信息， 加载快照数据，并更新数据值


#### Snapshot and log Compaction

#### MemberShip Change


#### Transfer Leader

#### Fault Tolerance

#### Workaround when quorate peers are dead

#### Symmetric network partition tolerance

#### Asymmetric network parttion tolerance

#### ReadIndex
什么是线性一致读? 所谓线性一致读，一个简单的例子是在 t1 的时刻我们写入了一个值，那么在 t1 之后，
我们一定能读到这个值，不可能读到 t1 之前的旧值(想想 Java 中的 volatile 关键字，即线性一致读就是在分布式系统中实现 Java volatile 语义)。
简而言之是需要在分布式环境中实现 Java volatile 语义效果，即当 Client 向集群发起写操作的请求并且获得成功响应之后，该写操作的结果要对所有后来的读请求可见。
和 volatile 的区别在于 volatile 是实现线程之间的可见，而 Open-Raft 需要实现 Server 之间的可见。
![img.png](img.png)  
如上图 Client A、B、C、D 均符合线性一致读，其中 D 看起来是 Stale Read，其实并不是，D 请求横跨 3 个阶段，而 Read 可能发生在任意时刻，所以读到 1 或 2 都行。


#### Batch

#### Replication pipeline

#### Append log in parallel

#### Asynchronous

#### Fully concurrent replicaiton

#### LeaseRead
