package com.open.raft.core;

import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.open.raft.INode;
import com.open.raft.Status;
import com.open.raft.closure.CatchUpClosure;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.PeerId;
import com.open.raft.entity.RaftOutter;
import com.open.raft.error.RaftError;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReplicatorOptions;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.rpc.RpcUtils;
import com.open.raft.util.Requires;
import com.open.raft.util.ThreadId;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayDeque;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
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

    // Next sending log index
    private volatile long nextIndex;

    private final ReplicatorOptions options;
    private final RaftOptions raftOptions;
    protected ThreadId id;

    private volatile State state;

    private volatile long lastRpcSendTimestamp;
    private volatile long heartbeatCounter = 0;
    private volatile long probeCounter = 0;
    private volatile long appendEntriesCounter = 0;
    private volatile long installSnapshotCounter = 0;

    private ScheduledFuture<?> heartbeatTimer;
    private volatile SnapshotReader reader;
    private CatchUpClosure catchUpClosure;
    private final Scheduler timerManager;

    // Cached the latest RPC in-flight request.
    private Inflight rpcInFly;
    // Heartbeat RPC future
    private Future<Message> heartbeatInFly;
    // Timeout request RPC future
    private Future<Message> timeoutNowInFly;
    // In-flight RPC requests, FIFO queue
    private final ArrayDeque<Inflight> inflights = new ArrayDeque<>();


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

    enum ReplicatorEvent {
        CREATED, // created
        ERROR, // error
        DESTROYED,// destroyed
        STATE_CHANGED; // state changed.
    }


    public Replicator(ReplicatorOptions replicatorOptions, RaftOptions raftOptions) {
        this.options = replicatorOptions;
        this.timerManager = replicatorOptions.getTimerManager();
        this.raftOptions = raftOptions;
        this.rpcService = replicatorOptions.getRaftRpcService();
        // nextIndex从 1 开始
        this.nextIndex = this.options.getLogManager().getLastLogIndex() + 1;
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
        // Fixme 这里加锁不知意欲何为
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
        //startHeartbeatTimer  在线程中执行，所以 可能先发送 探针
        r.sendProbeRequest();
        return r.id;
    }


    private void startHeartbeatTimer(final long startMs) {
        //当前毫秒数
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

    static void onHeartbeatReturned(final ThreadId id, final Status status, final RpcRequests.AppendEntriesRequest request,
                                    final RpcRequests.AppendEntriesResponse response, final long rpcSendTime) {
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

    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users for none status.
     *
     * @param replicator replicator object
     * @param event      replicator's state listener event type
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event) {
        notifyReplicatorStatusListener(replicator, event, null);
    }

    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event,
                                                       final Status status) {
        notifyReplicatorStatusListener(replicator, event, status, null);
    }


    /**
     * Notify replicator event(such as created, error, destroyed) to replicatorStateListener which is implemented by users.
     *
     * @param replicator replicator object
     * @param event      replicator's state listener event type
     * @param status     replicator's error detailed status
     */
    private static void notifyReplicatorStatusListener(final Replicator replicator, final ReplicatorEvent event,
                                                       final Status status, final ReplicatorState newState) {
        final ReplicatorOptions replicatorOpts = Requires.requireNonNull(replicator.getOpts(), "replicatorOptions");
        final INode node = Requires.requireNonNull(replicatorOpts.getNode(), "node");
        final PeerId peer = Requires.requireNonNull(replicatorOpts.getPeerId(), "peer");

        final List<ReplicatorStateListener> listenerList = node.getReplicatorStatueListeners();
        for (int i = 0; i < listenerList.size(); i++) {
            final ReplicatorStateListener listener = listenerList.get(i);
            if (listener != null) {
                try {
                    switch (event) {
                        case CREATED:
                            RpcUtils.runInThread(() -> listener.onCreated(peer));
                            break;
                        case ERROR:
                            RpcUtils.runInThread(() -> listener.onError(peer, status));
                            break;
                        case DESTROYED:
                            RpcUtils.runInThread(() -> listener.onDestroyed(peer));
                            break;
                        case STATE_CHANGED:
                            RpcUtils.runInThread(() -> listener.stateChanged(peer, newState));
                        default:
                            break;
                    }
                } catch (final Exception e) {
                    LOG.error("Fail to notify ReplicatorStatusListener, listener={}, event={}.", listener, event);
                }
            }
        }
    }

    /**
     * 其实就是用来探测各种异常情况，或者探测当前节点的nextIndex。
     *
     * 什么时候发送探测消息？
     *
     * 节点刚成为leader（start）
     * 发送日志超时的时候，会发送探测消息。
     * 如果响应超时，如果jraft打开pipeline，会有一个pendingResponses阈值。如果响应队列数据大于这个值会调用该方法，并不会在响应超时的时候，无限loop。
     * 收到无效的日志请求。
     * 发送日志请求不成功
     */
    private void sendProbeRequest() {
        sendEmptyEntries(false);
    }

    private boolean fillCommonFields(final RpcRequests.AppendEntriesRequest.Builder rb, long prevLogIndex, final boolean isHeartbeat) {
        final long prevLogTerm = this.options.getLogManager().getTerm(prevLogIndex);
        if (prevLogTerm == 0 && prevLogIndex != 0) {

            if (!isHeartbeat) {
                Requires.requireTrue(prevLogIndex < this.options.getLogManager().getFirstLogIndex());
                LOG.debug("logIndex={} was compacted", prevLogIndex);
                return false;
            } else {
                // The log at prev_log_index has been compacted, which indicates
                // we is or is going to install snapshot to the follower. So we let
                // both prev_log_index and prev_log_term be 0 in the heartbeat
                // request so that follower would do nothing besides updating its
                // leader timestamp.
                prevLogIndex = 0;
            }
        }
        rb.setTerm(this.options.getTerm());
        rb.setGroupId(this.options.getGroupId());
        rb.setServerId(this.options.getServerId().toString());
        rb.setPeerId(this.options.getPeerId().toString());
        //注意prevLogIndex是nextIndex-1，表示当前的index
        rb.setPrevLogIndex(prevLogIndex);
        rb.setPrevLogTerm(prevLogTerm);
        rb.setCommittedIndex(this.options.getBallotBox().getLastCommittedIndex());
        return true;
    }

    /**
     * 校验用户是否设置配置参数类 NodeOptions 的“snapshotUri”属性，如果没有设置就不会开启快照，返回reader就为空
     * 是否可以返回一个获取快照的uri
     * 能否从获取从文件加载的元数据信息
     * 如果上面的校验都通过的话，那么就会发送一个InstallSnapshotRequest请求到Follower，交给InstallSnapshotRequestProcessor处理器处理，
     * 最后会跳转到NodeImpl的handleInstallSnapshot方法执行具体逻辑
     */
    void installSnapshot() {
        //正在安装快照
        if (getState() == State.Snapshot) {
            LOG.warn("Replicator {} is installing snapshot, ignore the new request.", this.options.getPeerId());
            unlockId();
            return;
        }
        boolean doUnlock = true;
        if (!this.rpcService.connect(this.options.getPeerId().getEndpoint())) {
            LOG.error("Fail to check install snapshot connection to peer={}, give up to send install snapshot request.", this.options.getPeerId().getEndpoint());
            block(Utils.nowMs(), RaftError.EHOSTDOWN.getNumber());
            return;
        }
        try {
            Requires.requireTrue(this.reader == null,
                    "Replicator %s already has a snapshot reader, current state is %s", this.options.getPeerId(),
                    getState());
            //初始化SnapshotReader
            this.reader = this.options.getSnapshotStorage().open();
            //如果快照存储功能没有开启，则设置错误信息并返回
            if (this.reader == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to open snapshot"));
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }

            //生成一个读uri连接，给其他节点读取快照
            final String uri = this.reader.generateURIForCopy();
            if (uri == null) {
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to generate uri for snapshot reader"));
                releaseReader();
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }

            //获取从文件加载的元数据信息
            final RaftOutter.SnapshotMeta meta = this.reader.load();
            if (meta == null) {
                final String snapshotPath = this.reader.getPath();
                final NodeImpl node = this.options.getNode();
                final RaftException error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT);
                error.setStatus(new Status(RaftError.EIO, "Fail to load meta from %s", snapshotPath));
                releaseReader();
                unlockId();
                doUnlock = false;
                node.onError(error);
                return;
            }

            //设置请求参数
            final InstallSnapshotRequest.Builder rb = InstallSnapshotRequest.newBuilder();
            rb.setTerm(this.options.getTerm());
            rb.setGroupId(this.options.getGroupId());
            rb.setServerId(this.options.getServerId().toString());
            rb.setPeerId(this.options.getPeerId().toString());
            rb.setMeta(meta);
            rb.setUri(uri);

            this.statInfo.runningState = RunningState.INSTALLING_SNAPSHOT;
            this.statInfo.lastLogIncluded = meta.getLastIncludedIndex();
            this.statInfo.lastTermIncluded = meta.getLastIncludedTerm();

            final InstallSnapshotRequest request = rb.build();
            setState(State.Snapshot);
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            final int seq = getAndIncrementReqSeq();
            //发起InstallSnapshotRequest请求
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(this.options.getPeerId().getEndpoint(),
                    request, new RpcResponseClosureAdapter<InstallSnapshotResponse>() {

                        @Override
                        public void run(final Status status) {
                            onRpcReturned(Replicator.this.id, RequestType.Snapshot, status, request, getResponse(), seq,
                                    stateVersion, monotonicSendTimeMs);
                        }
                    });
            addInflight(RequestType.Snapshot, this.nextIndex, 0, 0, seq, rpcFuture);
        } finally {
            if (doUnlock) {
                unlockId();
            }
        }
    }


}
