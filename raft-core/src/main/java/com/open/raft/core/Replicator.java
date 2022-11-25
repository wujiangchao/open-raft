package com.open.raft.core;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ZeroByteStringHelper;
import com.open.raft.INode;
import com.open.raft.Status;
import com.open.raft.closure.CatchUpClosure;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.PeerId;
import com.open.raft.entity.RaftOutter;
import com.open.raft.error.RaftError;
import com.open.raft.error.RaftException;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReplicatorOptions;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.rpc.RpcUtils;
import com.open.raft.storage.snapshot.SnapshotReader;
import com.open.raft.util.Recyclable;
import com.open.raft.util.RecycleUtil;
import com.open.raft.util.Requires;
import com.open.raft.util.ThreadId;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.PriorityQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Description Replicator for replicating log entry from leader to followers.
 * 每个非Leader节点都独享一个Replicator
 * @Date 2022/10/11 9:37
 * @Author jack wu
 */
@ThreadSafe
public class Replicator implements ThreadId.OnError {

    private static final Logger LOG = LoggerFactory.getLogger(Replicator.class);
    private final RaftClientService rpcService;

    // Next sending log index  下一个要发送的LogIndexId，Leader上任初始化为lastLogIndex + 1
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

    // 每次日志复制都把多个LogEntity封装进Inflight，一次发送
    // Cached the latest RPC in-flight request.这里记录最近要的一个
    private Inflight rpcInFly;
    // Heartbeat RPC future
    private Future<Message> heartbeatInFly;
    // Timeout request RPC future
    private Future<Message> timeoutNowInFly;
    // In-flight RPC requests, FIFO queue
    private final ArrayDeque<Inflight> inflights = new ArrayDeque<>();

    protected Stat statInfo = new Stat();

    //Raft不允许乱序日志复制，所以需要这两个字段限制某个inflight是否对应某个request和response
    // Request sequence
    private int reqSeq = 0;
    // Response sequence
    private int requiredNextSeq = 0;
    // Replicator state reset version
    private int version = 0;
    //连续错误次数
    private int consecutiveErrorTimes = 0;


    // Pending response queue;
    private final PriorityQueue<RpcResponse> pendingResponses = new PriorityQueue<>(50);

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
            //心跳时间间隔要远小于选举时间间隔，避免频繁选举超时，阻止fellow变为candidate,否则很快会进入新一轮的选举，系统很难收敛于稳定状态
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

    /**
     * Leader 还需向所有 followers 主动发送心跳维持领导地位(保持存在感)
     * 目的是让 leader 能够持续发送心跳来阻止 followers 触发选举
     *
     * @param id
     */
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
     * Send probe or heartbeat request ,send by leader
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
     * <p>
     * 什么时候发送探测消息？
     * <p>
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
        //
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
            final RpcRequests.InstallSnapshotRequest.Builder rb = RpcRequests.InstallSnapshotRequest.newBuilder();
            rb.setTerm(this.options.getTerm());
            rb.setGroupId(this.options.getGroupId());
            rb.setServerId(this.options.getServerId().toString());
            rb.setPeerId(this.options.getPeerId().toString());
            rb.setMeta(meta);
            rb.setUri(uri);

            this.statInfo.runningState = RunningState.INSTALLING_SNAPSHOT;
            this.statInfo.lastLogIncluded = meta.getLastIncludedIndex();
            this.statInfo.lastTermIncluded = meta.getLastIncludedTerm();

            final RpcRequests.InstallSnapshotRequest request = rb.build();
            setState(State.Snapshot);
            // noinspection NonAtomicOperationOnVolatileField
            this.installSnapshotCounter++;
            final long monotonicSendTimeMs = Utils.monotonicMs();
            final int stateVersion = this.version;
            final int seq = getAndIncrementReqSeq();
            //发起InstallSnapshotRequest请求
            final Future<Message> rpcFuture = this.rpcService.installSnapshot(this.options.getPeerId().getEndpoint(),
                    request, new RpcResponseClosureAdapter<RpcRequests.InstallSnapshotResponse>() {

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

    static class Stat {
        RunningState runningState;
        long firstLogIndex;
        long lastLogIncluded;
        long lastLogIndex;
        long lastTermIncluded;

        @Override
        public String toString() {
            return "<running=" + this.runningState + ", firstLogIndex=" + this.firstLogIndex + ", lastLogIncluded="
                    + this.lastLogIncluded + ", lastLogIndex=" + this.lastLogIndex + ", lastTermIncluded="
                    + this.lastTermIncluded + ">";
        }
    }

    enum RunningState {
        // idle
        IDLE,
        // blocking state
        BLOCKING,
        // appending log entries
        APPENDING_ENTRIES,
        // installing snapshot
        INSTALLING_SNAPSHOT
    }

    private int getAndIncrementReqSeq() {
        final int prev = this.reqSeq;
        this.reqSeq++;
        if (this.reqSeq < 0) {
            this.reqSeq = 0;
        }
        return prev;
    }

    /**
     * Leader收到Follower发过来的Response响应之后会调用Replicator的onRpcReturned方法
     * 检查版本号，因为每次resetInflights都会让version加一，所以检查一下是不是同一批的数据
     * 获取Replicator的pendingResponses队列，然后将当前响应的数据封装成RpcResponse实例加入到队列中
     * 校验队列里面的元素是否大于256，大于256则清空数据重新同步
     * 校验holdingQueue队列里面的seq最小的序列数据序列和当前的requiredNextSeq是否相同，不同的话如果是刚进入循环那么直接break退出循环
     * 获取inflights队列中第一个元素，如果seq没有对上，说明顺序乱了，重置状态
     * 调用onAppendEntriesReturned方法处理日志复制的response
     * 如果处理成功，那么则调用sendEntries继续发送复制日志到Followe
     *
     * @param id
     * @param reqType
     * @param status
     * @param request
     * @param response
     * @param seq
     * @param stateVersion
     * @param rpcSendTime
     */
    @SuppressWarnings("ContinueOrBreakFromFinallyBlock")
    static void onRpcReturned(final ThreadId id, final RequestType reqType, final Status status, final Message request,
                              final Message response, final int seq, final int stateVersion, final long rpcSendTime) {
        if (id == null) {
            return;
        }
        final long startTimeMs = Utils.nowMs();
        Replicator r;
        if ((r = (Replicator) id.lock()) == null) {
            return;
        }
        //检查版本号，因为每次resetInflights都会让version加一，所以检查一下
        if (stateVersion != r.version) {
            LOG.debug(
                    "Replicator {} ignored old version response {}, current version is {}, request is {}\n, and response is {}\n, status is {}.",
                    r, stateVersion, r.version, request, response, status);
            id.unlock();
            return;
        }
        //需要花点时间解释这个根据seq优先队列的用处
        //首先要知道raft强调日志必须顺序一致的，任何并发调用onRpcReturned都可能打乱复制顺序
        //假设现在this.reqSeq=3, requiredNextSeq=2，我们正在等待的reqSeq=2的响应由于种种原因还没到来
        //此时某次心跳onHeartbeatReturned触发了sendEmptyEntries(false)，将reqSeq改为4，也就说seq=3，而且该探测请求很快被响应且调用该方法
        //后来先到的response会被先hold到pendingResponses

        //使用优先队列按seq排序,最小的会在第一个
        final PriorityQueue<RpcResponse> holdingQueue = r.pendingResponses;
        //这里用一个优先队列是因为响应是异步的，seq小的可能响应比seq大慢
        holdingQueue.add(new RpcResponse(reqType, seq, status, request, response, rpcSendTime));
        //默认holdingQueue队列里面的数量不能超过256
        if (holdingQueue.size() > r.raftOptions.getMaxReplicatorInflightMsgs()) {
            LOG.warn("Too many pending responses {} for replicator {}, maxReplicatorInflightMsgs={}",
                    holdingQueue.size(), r.options.getPeerId(), r.raftOptions.getMaxReplicatorInflightMsgs());
            //重新发送探针
            //清空数据
            r.resetInflights();
            r.setState(State.Probe);
            r.sendProbeRequest();
            return;
        }

        boolean continueSendEntries = false;

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Replicator ")
                    .append(r)
                    .append(" is processing RPC responses, ");
        }
        try {
            int processed = 0;
            while (!holdingQueue.isEmpty()) {
                //取出holdingQueue里seq最小的数据
                final RpcResponse queuedPipelinedResponse = holdingQueue.peek();
                //如果Follower没有响应的话就会出现次序对不上的情况，那么就不往下走了
                // Sequence mismatch, waiting for next response.
                if (queuedPipelinedResponse.seq != r.requiredNextSeq) {
                    // 如果之前存在处理，则到此直接break循环
                    if (processed > 0) {
                        if (isLogDebugEnabled) {
                            sb.append("has processed ")
                                    .append(processed)
                                    .append(" responses, ");
                        }
                        break;
                    } else {
                        // Do not processed any responses, UNLOCK id and return.
                        continueSendEntries = false;
                        id.unlock();
                        return;
                    }
                }
                //走到这里说明seq对的上，那么就移除优先队列里面seq最小的数据
                holdingQueue.remove();
                processed++;
                //获取inflights队列里的第一个元素
                final Inflight inflight = r.pollInflight();
                //发起一个请求的时候会将inflight放入到队列中
                //如果为空，那么就忽略
                if (inflight == null) {
                    // The previous in-flight requests were cleared.
                    if (isLogDebugEnabled) {
                        sb.append("ignore response because request not found: ") //
                                .append(queuedPipelinedResponse) //
                                .append(",\n");
                    }
                    continue;
                }

                //seq没有对上，说明顺序乱了，重置状态
                if (inflight.seq != queuedPipelinedResponse.seq) {
                    // reset state
                    LOG.warn(
                            "Replicator {} response sequence out of order, expect {}, but it is {}, reset state to try again.",
                            r, inflight.seq, queuedPipelinedResponse.seq);
                    r.resetInflights();
                    r.setState(State.Probe);
                    continueSendEntries = false;
                    // 锁住节点，根据错误类别等待一段时间
                    r.block(Utils.nowMs(), RaftError.EREQUEST.getNumber());
                    return;
                }
                try {
                    switch (queuedPipelinedResponse.requestType) {
                        case AppendEntries:                        //处理日志复制的response
                            continueSendEntries = onAppendEntriesReturned(id, inflight, queuedPipelinedResponse.status,
                                    (RpcRequests.AppendEntriesRequest) queuedPipelinedResponse.request,
                                    (RpcRequests.AppendEntriesResponse) queuedPipelinedResponse.response, rpcSendTime, startTimeMs, r);
                            break;
                        case Snapshot:     //处理快照的response
                            continueSendEntries = onInstallSnapshotReturned(id, r, queuedPipelinedResponse.status,
                                    (RpcRequests.InstallSnapshotRequest) queuedPipelinedResponse.request,
                                    (RpcRequests.InstallSnapshotResponse) queuedPipelinedResponse.response);
                            break;
                    }
                } finally {
                    if (continueSendEntries) {
                        // Success, increase the response sequence.
                        r.getAndIncrementRequiredNextSeq();
                    } else {
                        // The id is already unlocked in onAppendEntriesReturned/onInstallSnapshotReturned, we SHOULD break out.
                        break;
                    }
                }
            }
        } finally {
            if (isLogDebugEnabled) {
                sb.append("after processed, continue to send entries: ") //
                        .append(continueSendEntries);
                LOG.debug(sb.toString());
            }
            if (continueSendEntries) {
                // unlock in sendEntries.
                r.sendEntries();
            }
        }
    }

    /**
     * Send as many requests as possible.
     */
    void sendEntries() {
        boolean doUnlock = true;
        try {
            long prevSendIndex = -1;
            while (true) {
                final long nextSendingIndex = getNextSendIndex();
                if (nextSendingIndex > prevSendIndex) {
                    if (sendEntries(nextSendingIndex)) {
                        prevSendIndex = nextSendingIndex;
                    } else {
                        doUnlock = false;
                        // id already unlock in sendEntries when it returns false.
                        break;
                    }
                } else {
                    break;
                }
            }
        } finally {
            if (doUnlock) {
                unlockId();
            }
        }
    }

    /**
     * Send log entries to follower, returns true when success, otherwise false and unlock the id.
     *
     * @param nextSendingIndex next sending index
     * @return send result.
     */
    private boolean sendEntries(final long nextSendingIndex) {
        final RpcRequests.AppendEntriesRequest.Builder rb = RpcRequests.AppendEntriesRequest.newBuilder();
        //填写当前Replicator的配置信息到rb中
        if (!fillCommonFields(rb, nextSendingIndex - 1, false)) {
            // unlock id in installSnapshot
            installSnapshot();
            return false;
        }

        ByteBufferCollector dataBuf = null;
        final int maxEntriesSize = this.raftOptions.getMaxEntriesSize();
        //这里使用了类似对象池的技术，避免重复创建对象
        final RecyclableByteBufferList byteBufList = RecyclableByteBufferList.newInstance();
        try {
            //循环遍历出所有的logEntry封装到byteBufList和emb中
            for (int i = 0; i < maxEntriesSize; i++) {
                final RaftOutter.EntryMeta.Builder emb = RaftOutter.EntryMeta.newBuilder();
                //nextSendingIndex代表下一个要发送的index，i代表偏移量
                if (!prepareEntry(nextSendingIndex, i, emb, byteBufList)) {
                    break;
                }
                rb.addEntries(emb.build());
            }
            //如果EntriesCount为0的话，说明LogManager里暂时没有新数据
            if (rb.getEntriesCount() == 0) {
                if (nextSendingIndex < this.options.getLogManager().getFirstLogIndex()) {
                    installSnapshot();
                    return false;
                }
                // _id is unlock in _wait_more
                waitMoreEntries(nextSendingIndex);
                return false;
            }

            //将byteBufList里面的数据放入到rb中
            if (byteBufList.getCapacity() > 0) {
                dataBuf = ByteBufferCollector.allocateByRecyclers(byteBufList.getCapacity());
                for (final ByteBuffer b : byteBufList) {
                    dataBuf.put(b);
                }
                final ByteBuffer buf = dataBuf.getBuffer();
                buf.flip();
                rb.setData(ZeroByteStringHelper.wrap(buf));
            }
        } finally {
            //回收一下byteBufList
            RecycleUtil.recycle(byteBufList);
        }

        final RpcRequests.AppendEntriesRequest request = rb.build();
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Node {} send AppendEntriesRequest to {} term {} lastCommittedIndex {} prevLogIndex {} prevLogTerm {} logIndex {} count {}",
                    this.options.getNode().getNodeId(), this.options.getPeerId(), this.options.getTerm(),
                    request.getCommittedIndex(), request.getPrevLogIndex(), request.getPrevLogTerm(), nextSendingIndex,
                    request.getEntriesCount());
        }
        this.statInfo.runningState = RunningState.APPENDING_ENTRIES;
        this.statInfo.firstLogIndex = rb.getPrevLogIndex() + 1;
        this.statInfo.lastLogIndex = rb.getPrevLogIndex() + rb.getEntriesCount();

        final Recyclable recyclable = dataBuf;
        final int v = this.version;
        final long monotonicSendTimeMs = Utils.monotonicMs();
        final int seq = getAndIncrementReqSeq();

        this.appendEntriesCounter++;
        Future<Message> rpcFuture = null;
        try {
            rpcFuture = this.rpcService.appendEntries(this.options.getPeerId().getEndpoint(), request, -1,
                    new RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse>() {

                        @Override
                        public void run(final Status status) {
                            //回收资源
                            RecycleUtil.recycle(recyclable); // TODO: recycle on send success, not response received.
                            onRpcReturned(Replicator.this.id, RequestType.AppendEntries, status, request, getResponse(),
                                    seq, v, monotonicSendTimeMs);
                        }
                    });
        } catch (final Throwable t) {
            RecycleUtil.recycle(recyclable);
            ThrowUtil.throwException(t);
        }
        //添加Inflight
        addInflight(RequestType.AppendEntries, nextSendingIndex, request.getEntriesCount(), request.getData().size(),
                seq, rpcFuture);
        return true;
    }


    /**
     * Adds a in-flight request
     *
     * @param reqType type of request
     * @param count   count if request
     * @param size    size in bytes
     */
    private void addInflight(final RequestType reqType, final long startIndex, final int count, final int size,
                             final int seq, final Future<Message> rpcInfly) {
        this.rpcInFly = new Inflight(reqType, startIndex, count, size, seq, rpcInfly);
        this.inflights.add(this.rpcInFly);
        this.nodeMetrics.recordSize(name(this.metricName, "replicate-inflights-count"), this.inflights.size());
    }


    // In-flight request type
    enum RequestType {
        Snapshot, // install snapshot
        AppendEntries // replicate logs
    }

    /**
     * Reset in-flight state.
     */
    void resetInflights() {
        this.version++;
        this.inflights.clear();
        this.pendingResponses.clear();
        final int rs = Math.max(this.reqSeq, this.requiredNextSeq);
        this.reqSeq = this.requiredNextSeq = rs;
        releaseReader();
    }


    /**
     * RPC response for AppendEntries/InstallSnapshot.
     *
     * @author dennis
     */
    static class RpcResponse implements Comparable<RpcResponse> {
        final Status status;
        final Message request;
        final Message response;
        final long rpcSendTime;
        final int seq;
        final RequestType requestType;

        public RpcResponse(final RequestType reqType, final int seq, final Status status, final Message request,
                           final Message response, final long rpcSendTime) {
            super();
            this.requestType = reqType;
            this.seq = seq;
            this.status = status;
            this.request = request;
            this.response = response;
            this.rpcSendTime = rpcSendTime;
        }

        @Override
        public String toString() {
            return "RpcResponse [status=" + this.status + ", request=" + this.request + ", response=" + this.response
                    + ", rpcSendTime=" + this.rpcSendTime + ", seq=" + this.seq + ", requestType=" + this.requestType
                    + "]";
        }

        /**
         * Sort by sequence.
         */
        @Override
        public int compareTo(final RpcResponse o) {
            return Integer.compare(this.seq, o.seq);
        }
    }

    /**
     * In-flight request.
     *
     * @author dennis
     */
    static class Inflight {
        // In-flight request count
        final int count;
        // Start log index
        final long startIndex;
        // Entries size in bytes
        final int size;
        // RPC future
        final Future<Message> rpcFuture;
        final RequestType requestType;
        // Request sequence.
        final int seq;

        public Inflight(final RequestType requestType, final long startIndex, final int count, final int size,
                        final int seq, final Future<Message> rpcFuture) {
            super();
            this.seq = seq;
            this.requestType = requestType;
            this.count = count;
            this.startIndex = startIndex;
            this.size = size;
            this.rpcFuture = rpcFuture;
        }

        @Override
        public String toString() {
            return "Inflight [count=" + this.count + ", startIndex=" + this.startIndex + ", size=" + this.size
                    + ", rpcFuture=" + this.rpcFuture + ", requestType=" + this.requestType + ", seq=" + this.seq + "]";
        }

        boolean isSendingLogEntries() {
            return this.requestType == RequestType.AppendEntries && this.count > 0;
        }
    }


    private Inflight pollInflight() {
        return this.inflights.poll();
    }

    /**
     * 校验数据序列有没有错
     * 进行度量和拼接日志操作
     * 判断一下返回的状态如果不是正常的，那么就通知监听器，进行重置操作并阻塞一定时间后再发送
     * 如果返回Success状态为false，那么校验一下任期，因为Leader 的切换，表明可能出现过一次网络分区，
     * 需要重新跟随新的 Leader；如果任期没有问题那么就进行重置操作,并根据Follower返回的最新的index来重新设值nextIndex
     * 如果各种校验都没有问题的话，那么进行日志提交确认，更新最新的日志提交位置索引
     *
     * @param id
     * @param inflight
     * @param status
     * @param request
     * @param response
     * @param rpcSendTime
     * @param startTimeMs
     * @param r
     * @return
     */
    private static boolean onAppendEntriesReturned(final ThreadId id, final Inflight inflight, final Status status,
                                                   final RpcRequests.AppendEntriesRequest request,
                                                   final RpcRequests.AppendEntriesResponse response, final long rpcSendTime,
                                                   final long startTimeMs, final Replicator r) {
        //校验数据序列有没有错
        if (inflight.startIndex != request.getPrevLogIndex() + 1) {
            LOG.warn(
                    "Replicator {} received invalid AppendEntriesResponse, in-flight startIndex={}, request prevLogIndex={}, reset the replicator state and probe again.",
                    r, inflight.startIndex, request.getPrevLogIndex());
            r.resetInflights();
            r.setState(State.Probe);
            // unlock id in sendEmptyEntries
            r.sendProbeRequest();
            return false;
        }
        // record metrics
        if (request.getEntriesCount() > 0) {
            r.nodeMetrics.recordLatency("replicate-entries", Utils.monotonicMs() - rpcSendTime);
            r.nodeMetrics.recordSize("replicate-entries-count", request.getEntriesCount());
            r.nodeMetrics.recordSize("replicate-entries-bytes", request.getData() != null ? request.getData().size()
                    : 0);
        }

        final boolean isLogDebugEnabled = LOG.isDebugEnabled();
        StringBuilder sb = null;
        if (isLogDebugEnabled) {
            sb = new StringBuilder("Node ") //
                    .append(r.options.getGroupId()) //
                    .append(':') //
                    .append(r.options.getServerId()) //
                    .append(" received AppendEntriesResponse from ") //
                    .append(r.options.getPeerId()) //
                    .append(" prevLogIndex=") //
                    .append(request.getPrevLogIndex()) //
                    .append(" prevLogTerm=") //
                    .append(request.getPrevLogTerm()) //
                    .append(" count=") //
                    .append(request.getEntriesCount());
        }

        //如果follower因为崩溃，RPC调用失败等原因没有收到成功响应
        //那么需要阻塞一段时间再进行调用
        if (!status.isOk()) {
            // If the follower crashes, any RPC to the follower fails immediately,
            // so we need to block the follower for a while instead of looping until
            // it comes back or be removed
            // dummy_id is unlock in block
            if (isLogDebugEnabled) {
                sb.append(" fail, sleep, status=") //
                        .append(status);
                LOG.debug(sb.toString());
            }
            //如果注册了Replicator状态监听器，那么通知所有监听器
            notifyReplicatorStatusListener(r, ReplicatorEvent.ERROR, status);
            if (++r.consecutiveErrorTimes % 10 == 0) {
                LOG.warn("Fail to issue RPC to {}, consecutiveErrorTimes={}, error={}", r.options.getPeerId(),
                        r.consecutiveErrorTimes, status);
            }
            r.resetInflights();
            r.setState(State.Probe);
            // unlock in in block
            r.block(startTimeMs, status.getCode());
            return false;
        }
        r.consecutiveErrorTimes = 0;
        //响应失败
        if (!response.getSuccess()) {
            // Target node  is busy, sleep for a while.
            if (response.getErrorResponse().getErrorCode() == RaftError.EBUSY.getNumber()) {
                if (isLogDebugEnabled) {
                    sb.append(" is busy, sleep, errorMsg='") //
                            .append(response.getErrorResponse().getErrorMsg()).append("'");
                    LOG.debug(sb.toString());
                }
                r.resetInflights();
                r.setState(State.Probe);
                // unlock in in block
                r.block(startTimeMs, status.getCode());
                return false;
            }
            // Leader 的切换，表明可能出现过一次网络分区，从新跟随新的 Leader
            if (response.getTerm() > r.options.getTerm()) {
                if (isLogDebugEnabled) {
                    sb.append(" fail, greater term ") //
                            .append(response.getTerm()) //
                            .append(" expect term ") //
                            .append(r.options.getTerm());
                    LOG.debug(sb.toString());
                }
                // 获取当前本节点的表示对象——NodeImpl
                final NodeImpl node = r.options.getNode();
                r.notifyOnCaughtUp(RaftError.EPERM.getNumber(), true);
                r.destroy();
                // 调整自己的 term 任期值
                node.increaseTermTo(response.getTerm(), new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Leader receives higher term heartbeat_response from peer:%s", r.options.getPeerId()));
                return false;
            }
            if (isLogDebugEnabled) {
                sb.append(" fail, find nextIndex remote lastLogIndex ").append(response.getLastLogIndex())
                        .append(" local nextIndex ").append(r.nextIndex);
                LOG.debug(sb.toString());
            }
            if (rpcSendTime > r.lastRpcSendTimestamp) {
                r.lastRpcSendTimestamp = rpcSendTime;
            }
            // Fail, reset the state to try again from nextIndex.
            r.resetInflights();
            //如果Follower最新的index小于下次要发送的index，那么设置为Follower响应的index
            // prev_log_index and prev_log_term doesn't match
            if (response.getLastLogIndex() + 1 < r.nextIndex) {
                LOG.debug("LastLogIndex at peer={} is {}", r.options.getPeerId(), response.getLastLogIndex());
                // The peer contains less logs than leader
                r.nextIndex = response.getLastLogIndex() + 1;
            } else {
                // The peer contains logs from old term which should be truncated,
                // decrease _last_log_at_peer by one to test the right index to keep
                if (r.nextIndex > 1) {
                    LOG.debug("logIndex={} dismatch", r.nextIndex);
                    r.nextIndex--;
                } else {
                    LOG.error("Peer={} declares that log at index=0 doesn't match, which is not supposed to happen",
                            r.options.getPeerId());
                }
            }
            //响应失败需要重新获取Follower的日志信息，用来重新同步
            // dummy_id is unlock in _send_heartbeat
            r.sendProbeRequest();
            return false;
        }
        if (isLogDebugEnabled) {
            sb.append(", success");
            LOG.debug(sb.toString());
        }
        // success
        //响应成功检查任期
        if (response.getTerm() != r.options.getTerm()) {
            r.resetInflights();
            r.setState(State.Probe);
            LOG.error("Fail, response term {} dismatch, expect term {}", response.getTerm(), r.options.getTerm());
            id.unlock();
            return false;
        }
        if (rpcSendTime > r.lastRpcSendTimestamp) {
            r.lastRpcSendTimestamp = rpcSendTime;
        }
        // 本次提交的日志数量
        final int entriesSize = request.getEntriesCount();
        if (entriesSize > 0) {
            if (r.options.getReplicatorType().isFollower()) {
                // Only commit index when the response is from follower.
                // append log 后投票
                r.options.getBallotBox().commitAt(r.nextIndex, r.nextIndex + entriesSize - 1, r.options.getPeerId());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Replicated logs in [{}, {}] to peer {}", r.nextIndex, r.nextIndex + entriesSize - 1,
                        r.options.getPeerId());
            }
        }

        r.setState(State.Replicate);
        r.blockTimer = null;
        r.nextIndex += entriesSize;
        r.hasSucceeded = true;
        r.notifyOnCaughtUp(RaftError.SUCCESS.getNumber(), false);
        // dummy_id is unlock in _send_entries
        if (r.timeoutNowIndex > 0 && r.timeoutNowIndex < r.nextIndex) {
            r.sendTimeoutNow(false, false);
        }
        return true;
    }

    private void notifyOnCaughtUp(final int code, final boolean beforeDestroy) {
        if (this.catchUpClosure == null) {
            return;
        }
        if (code != RaftError.ETIMEDOUT.getNumber()) {
            if (this.nextIndex - 1 + this.catchUpClosure.getMaxMargin() < this.options.getLogManager()
                    .getLastLogIndex()) {
                return;
            }
            if (this.catchUpClosure.isErrorWasSet()) {
                return;
            }
            this.catchUpClosure.setErrorWasSet(true);
            if (code != RaftError.SUCCESS.getNumber()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
            if (this.catchUpClosure.hasTimer()) {
                if (!beforeDestroy && !this.catchUpClosure.getTimer().cancel(true)) {
                    // There's running timer task, let timer task trigger
                    // on_caught_up to void ABA problem
                    return;
                }
            }
        } else {
            // timed out
            if (!this.catchUpClosure.isErrorWasSet()) {
                this.catchUpClosure.getStatus().setError(code, RaftError.describeCode(code));
            }
        }
        final CatchUpClosure savedClosure = this.catchUpClosure;
        this.catchUpClosure = null;
        RpcUtils.runClosureInThread(savedClosure, savedClosure.getStatus());
    }

    void destroy() {
        final ThreadId savedId = this.id;
        LOG.info("Replicator {} is going to quit", savedId);
        releaseReader();
        // Unregister replicator metric set
        if (this.nodeMetrics.isEnabled()) {
            this.nodeMetrics.getMetricRegistry() //
                    .removeMatching(MetricFilter.startsWith(this.metricName));
        }
        setState(State.Destroyed);
        notifyReplicatorStatusListener((Replicator) savedId.getData(), ReplicatorEvent.DESTROYED);
        savedId.unlockAndDestroy();
        this.id = null;
    }

    private int getAndIncrementRequiredNextSeq() {
        final int prev = this.requiredNextSeq;
        this.requiredNextSeq++;
        if (this.requiredNextSeq < 0) {
            this.requiredNextSeq = 0;
        }
        return prev;
    }

    private void unlockId() {
        if (this.id == null) {
            return;
        }
        this.id.unlock();
    }


    private void releaseReader() {
        if (this.reader != null) {
            Utils.closeQuietly(this.reader);
            this.reader = null;
        }
    }

    public static void waitForCaughtUp(final ThreadId id, final long maxMargin, final long dueTime,
                                       final CatchUpClosure done) {
        final Replicator r = (Replicator) id.lock();

        if (r == null) {
            RpcUtils.runClosureInThread(done, new Status(RaftError.EINVAL, "No such replicator"));
            return;
        }
        try {
            if (r.catchUpClosure != null) {
                LOG.error("Previous wait_for_caught_up is not over");
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Duplicated call"));
                return;
            }
            done.setMaxMargin(maxMargin);
            if (dueTime > 0) {
                done.setTimer(r.timerManager.schedule(() -> onCatchUpTimedOut(id), dueTime - Utils.nowMs(),
                        TimeUnit.MILLISECONDS));
            }
            r.catchUpClosure = done;
        } finally {
            id.unlock();
        }
    }


    private static void onCatchUpTimedOut(final ThreadId id) {
        final Replicator r = (Replicator) id.lock();
        if (r == null) {
            return;
        }
        try {
            r.notifyOnCaughtUp(RaftError.ETIMEDOUT.getNumber(), false);
        } finally {
            id.unlock();
        }
    }

    public static long getLastRpcSendTimestamp(final ThreadId id) {
        final Replicator r = (Replicator) id.getData();
        if (r == null) {
            return 0L;
        }
        return r.lastRpcSendTimestamp;
    }
}
