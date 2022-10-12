package com.open.raft.core;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.open.raft.Status;
import com.open.raft.error.RaftError;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReplicatorOptions;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.util.Requires;
import com.open.raft.util.ThreadId;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @Description  Replicator for replicating log entry from leader to followers.
 * @Date 2022/10/11 9:37
 * @Author jack wu
 */
@ThreadSafe
public class Replicator  implements ThreadId.OnError {

    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);
    private final RaftClientService rpcService;


    private final ReplicatorOptions options;
    private final RaftOptions raftOptions;

    private volatile State state;



    /**
     * Replicator internal state
     *
     * @author dennis
     */
    public enum State {
        Created,
        Probe, // probe follower state
        Snapshot, // installing snapshot to follower
        Replicate, // replicate logs normally
        Destroyed // destroyed
    }


    public Replicator(ReplicatorOptions options, RaftOptions raftOptions) {
        this.options = options;
        this.raftOptions = raftOptions;
        setState(State.Created);
    }

    public static ThreadId start(final ReplicatorOptions opts, final RaftOptions raftOptions) {
        if (opts.getLogManager() == null || opts.getBallotBox() == null || opts.getNode() == null) {
            throw new IllegalArgumentException("Invalid ReplicatorOptions.");
        }
        final Replicator r = new Replicator(opts, raftOptions);
        // 建立与Follower的连接
        if (!r.rpcService.connect(opts.getPeerId().getEndpoint())) {
            LOG.error("Fail to init sending channel to {}.", opts.getPeerId());
            // Return and it will be retried later.
            return null;
        }

        // Register replicator metric set.
        final MetricRegistry metricRegistry = opts.getNode().getNodeMetrics().getMetricRegistry();
        if (metricRegistry != null) {
            try {
                if (!metricRegistry.getNames().contains(r.metricName)) {
                    metricRegistry.register(r.metricName, new ReplicatorMetricSet(opts, r));
                }
            } catch (final IllegalArgumentException e) {
                // ignore
            }
        }

        // Start replication
        r.id = new ThreadId(r, r);
        r.id.lock();

        //监听器ReplicatorStateListener.onCreated|onError|onDestroyed
        notifyReplicatorStatusListener(r, ReplicatorEvent.CREATED);
        LOG.info("Replicator={}@{} is started", r.id, r.options.getPeerId());
        r.catchUpClosure = null;
        r.lastRpcSendTimestamp = Utils.monotonicMs();
        //正式启动heartbeat timer
        r.startHeartbeatTimer(Utils.nowMs());
        // id.unlock in sendEmptyEntries
        //这里应该是为了把becomeLeader()->this.confCtx.flush更新的配置日志同步出去，并unlock
        r.sendProbeRequest();
        return r.id;
    }


    private void startHeartbeatTimer(final long startMs) {
        final long dueTime = startMs + this.options.getDynamicHeartBeatTimeoutMs();
        try {
            //心跳被作为一种超时异常处理。heartbeat为了不重复发送选择定时而非周期Timer，直到收到响应后再次计时发送。
            this.heartbeatTimer = this.timerManager.schedule(() -> onTimeout(this.id), dueTime - Utils.nowMs(),
                    TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOG.error("Fail to schedule heartbeat timer", e);
            onTimeout(this.id);
        }
    }

    private static void onTimeout(final ThreadId id) {
        if (id != null) {
            id.setError(RaftError.ETIMEDOUT.getNumber());
        } else {
            LOG.warn("Replicator id is null when timeout, maybe it's destroyed.");
        }
    }

    @Override
    public void onError(ThreadId id, Object data, int errorCode) {
        final Replicator r = (Replicator) data;
        if (errorCode == RaftError.ESTOP.getNumber()) {
            try {
                for (final Inflight inflight : r.inflights) {
                    if (inflight != r.rpcInFly) {
                        inflight.rpcFuture.cancel(true);
                    }
                }
                if (r.rpcInFly != null) {
                    r.rpcInFly.rpcFuture.cancel(true);
                    r.rpcInFly = null;
                }
                if (r.heartbeatInFly != null) {
                    r.heartbeatInFly.cancel(true);
                    r.heartbeatInFly = null;
                }
                if (r.timeoutNowInFly != null) {
                    r.timeoutNowInFly.cancel(true);
                    r.timeoutNowInFly = null;
                }
                if (r.heartbeatTimer != null) {
                    r.heartbeatTimer.cancel(true);
                    r.heartbeatTimer = null;
                }
                if (r.blockTimer != null) {
                    r.blockTimer.cancel(true);
                    r.blockTimer = null;
                }
                if (r.waitId >= 0) {
                    r.options.getLogManager().removeWaiter(r.waitId);
                }
                r.notifyOnCaughtUp(errorCode, true);
            } finally {
                r.destroy();
            }
        } else if (errorCode == RaftError.ETIMEDOUT.getNumber()) {
            RpcUtils.runInThread(() -> sendHeartbeat(id));
        } else {
            // noinspection ConstantConditions
            Requires.requireTrue(false, "Unknown error code for replicator: " + errorCode);
        }
    }

    private static void sendHeartbeat(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        // unlock in sendEmptyEntries
        r.sendEmptyEntries(true);
    }

    private void sendEmptyEntries(final boolean isHeartbeat) {
        sendEmptyEntries(isHeartbeat, null);
    }

    /**
     * Send probe or heartbeat request
     *
     * 首先会调用fillCommonFields方法，填写当前Replicator的配置信息到rb中；
     * 调用prepareEntry，根据当前的I和nextSendingIndex计算出当前的偏移量，然后去LogManager找到对应的LogEntry，
     * 再把LogEntry里面的属性设置到emb中，并把LogEntry里面的数据加入到RecyclableByteBufferList中；
     * 如果LogEntry里面没有新的数据，那么EntriesCount会为0，那么就返回；
     * 遍历byteBufList里面的数据，将数据添加到rb中，这样rb里面的数据就是前面是任期、类型、数据长度等信息，rb后面就是真正的数据；
     * 新建AppendEntriesRequest实例发送请求；
     * 添加 Inflight 到队列中。Leader 维护一个 queue，每发出一批 logEntry 就向 queue 中 添加一个代表这一批 logEntry 的 Inflight，
     * 这样当它知道某一批 logEntry 复制失败之后，就可以依赖 queue 中的 Inflight 把该批次 logEntry 以及后续的所有日志重新复制给 follower。
     * 既保证日志复制能够完成，又保证了复制日志的顺序不变
     *
     * @param isHeartbeat      if current entries is heartbeat
     * @param heartBeatClosure heartbeat callback
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void sendEmptyEntries(final boolean isHeartbeat,
                                  final RpcResponseClosure<RpcRequests.AppendEntriesResponse> heartBeatClosure) {
        final RpcRequests.AppendEntriesRequest.Builder rb = RpcRequests.AppendEntriesRequest.newBuilder();
        //将集群配置设置到rb中，例如Term，GroupId，ServerId等
        if (!fillCommonFields(rb, this.nextIndex - 1, isHeartbeat)) {
            // id is unlock in installSnapshot
            installSnapshot();
            if (isHeartbeat && heartBeatClosure != null) {
                RpcUtils.runClosureInThread(heartBeatClosure, new Status(RaftError.EAGAIN,
                        "Fail to send heartbeat to peer %s", this.options.getPeerId()));
            }
            return;
        }
        try {
            final long monotonicSendTimeMs = Utils.monotonicMs();
            //心跳包
            if (isHeartbeat) {
                final RpcRequests.AppendEntriesRequest request = rb.build();
                // Sending a heartbeat request
                this.heartbeatCounter++;
                RpcResponseClosure<RpcRequests.AppendEntriesResponse> heartbeatDone;
                // Prefer passed-in closure.
                if (heartBeatClosure != null) {
                    heartbeatDone = heartBeatClosure;
                } else {
                    heartbeatDone = new RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            onHeartbeatReturned(Replicator.this.id, status, request, getResponse(), monotonicSendTimeMs);
                        }
                    };
                }
                this.heartbeatInFly = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(), request,
                        this.options.getElectionTimeoutMs() / 2, heartbeatDone);
            } else {
                // probe  发送一个 Probe 类型的探针请求，目的是知道 Follower 已经拥有的的日志位置，以便于向 Follower 发送后续的日志。
                // No entries and has empty data means a probe request.
                // TODO(boyan) refactor, adds a new flag field?
                rb.setData(ByteString.EMPTY);
                final RpcRequests.AppendEntriesRequest request = rb.build();
                //statInfo这个类没看到哪里有用到，
                // Sending a probe request.
                //leader发送探针获取Follower的LastLogIndex
                this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
                //将lastLogIndex设置为比firstLogIndex小1
                this.statInfo.firstLogIndex = this.nextIndex;
                this.statInfo.lastLogIndex = this.nextIndex - 1;
                this.probeCounter++;
                //设置当前Replicator为发送探针
                setState(State.Probe);
                final int stateVersion = this.version;
                //返回reqSeq，并将reqSeq加一
                final int seq = getAndIncrementReqSeq();
                final Future<Message> rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(),
                        request, -1, new RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse>() {

                            @Override
                            public void run(final Status status) {
                                onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request,
                                        getResponse(), seq, stateVersion, monotonicSendTimeMs);
                            }

                        });
                //Inflight 是对批量发送出去的 logEntry 的一种抽象，他表示哪些 logEntry 已经被封装成日志复制 request 发送出去了
                //这里是将logEntry封装到Inflight中
                addInflight(RequestType.AppendEntries, this.nextIndex, 0, 0, seq, rpcFuture);
            }
            LOG.debug("Node {} send HeartbeatRequest to {} term {} lastCommittedIndex {}", this.options.getNode()
                    .getNodeId(), this.options.getPeerId(), this.options.getTerm(), rb.getCommittedIndex());
        } finally {
            unlockId();
        }
    }

    static void onHeartbeatReturned(final ThreadId id, final Status status, final AppendEntriesRequest request,
                                    final AppendEntriesResponse response, final long rpcSendTime) {
        if (id == null) {
            // replicator already was destroyed.
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }
        boolean doUnlock = true;
        try {
            final boolean isLogDebugEnabled = LOG.isDebugEnabled();
            StringBuilder sb = null;
            if (isLogDebugEnabled) {
                sb = new StringBuilder("Node ") //
                        .append(r.options.getGroupId()) //
                        .append(':') //
                        .append(r.options.getServerId()) //
                        .append(" received HeartbeatResponse from ") //
                        .append(r.options.getPeerId()) //
                        .append(" prevLogIndex=") //
                        .append(request.getPrevLogIndex()) //
                        .append(" prevLogTerm=") //
                        .append(request.getPrevLogTerm());
            }
            //网络通讯异常
            if (!status.isOk()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, sleep, status=") //
                            .append(status);
                    LOG.debug(sb.toString());
                }
                r.setState(State.Probe);
                notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
                if (++r.consecutiveErrorTimes % 10 == 0) {
                    LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}", r.options.getPeerId(),
                            r.consecutiveErrorTimes, status);
                }
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            r.consecutiveErrorTimes = 0;
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ") //
                            .append(response.getTerm()) //
                            .append(" expect term ") //
                            .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                final NodeImpl node = r.options.getNode();
                //新节点追赶上集群，以后成员变化会说到
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                r.destroy();
                //Leader不接受任期比自己大，increaseTermTo下线
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                return;
            }
            if (!response.getSuccess() && response.hasLastLogIndex()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, response term ") //
                            .append(response.getTerm()) //
                            .append(" lastLogIndex ") //
                            .append(response.getLastLogIndex());
                    LOG.debug(sb.toString());
                }
                LOG.warn("Heartbeat to peer {} failure, try to send a probe request.", r.options.getPeerId());
                doUnlock = false;
                //日志有异常，做AppendEntries的探测请求，对应上面Follower日志校验的逻辑
                r.sendProbeRequest();
                r.startHeartbeatTimer(startTimeMs);
                return;
            }
            if (isLogDebugEnabled) {
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            r.startHeartbeatTimer(startTimeMs);
        } finally {
            if (doUnlock) {
                id.unlock();
            }
        }
    }

    State getState() {
        return this.state;
    }

    void setState(final State state) {
        State oldState = this.state;
        this.state = state;

        if (oldState != state) {
            ReplicatorState newState = null;
            switch (state) {
                case Created:
                    newState = ReplicatorState.CREATED;
                    break;
                case Replicate:
                case Snapshot:
                    newState = ReplicatorState.ONLINE;
                    break;
                case Probe:
                    newState = ReplicatorState.OFFLINE;
                    break;
                case Destroyed:
                    newState = ReplicatorState.DESTROYED;
                    break;
            }

            if (newState != null) {
                notifyReplicatorStatusListener(this, ReplicatorEvent.STATE_CHANGED, null, newState);
            }
        }
    }

}
