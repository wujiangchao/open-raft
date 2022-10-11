package com.open.raft.core;

import com.google.protobuf.Message;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.INode;
import com.open.raft.RaftServiceFactory;
import com.open.raft.Status;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.closure.ClosureQueueImpl;
import com.open.raft.core.done.OnPreVoteRpcDone;
import com.open.raft.core.done.OnRequestVoteRpcDone;
import com.open.raft.core.event.LogEntryEvent;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.entity.LogEntry;
import com.open.raft.entity.LogId;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.entity.Task;
import com.open.raft.error.OverloadException;
import com.open.raft.error.RaftError;
import com.open.raft.option.FSMCallerOptions;
import com.open.raft.option.LogManagerOptions;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReplicatorGroupOptions;
import com.open.raft.rpc.RaftServerService;
import com.open.raft.rpc.RpcRequestClosure;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.LogStorage;
import com.open.raft.storage.impl.LogManagerImpl;
import com.open.raft.util.Requires;
import com.open.raft.util.Utils;
import com.open.raft.util.concurrent.NodeReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.stream.Collectors;

/**
 * @Description 表示一个 raft 节点，可以提交 task，以及查询 raft group 信息，
 * 比如当前状态、当前 leader/term 等。
 * @Date 2022/9/22 18:51
 * @Author jack wu
 */
public class NodeImpl implements INode, RaftServerService {

    private static final Logger LOG = LoggerFactory.getLogger(NodeImpl.class);

    private volatile State state;
    private long currTerm;

    private volatile long lastLeaderTimestamp;

    /**
     * 当前节点的选举超时的次数
     */
    private volatile int electionTimeoutCounter;


    /**
     * Raft group and node options and identifier
     */
    private final String groupId;
    private NodeOptions options;

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }


    private RaftOptions raftOptions;
    private final PeerId serverId;

    private NodeId nodeId;
    private RaftServiceFactory serviceFactory;

    private PeerId leaderId = new PeerId();

    /**
     * Node's target leader election priority value
     */
    private volatile int targetPriority;


    /**
     * Internal states
     */
    private final ReadWriteLock readWriteLock = new NodeReadWriteLock(
            this);
    protected final Lock writeLock = this.readWriteLock
            .writeLock();
    protected final Lock readLock = this.readWriteLock
            .readLock();
    private volatile CountDownLatch shutdownLatch;


    private FSMCaller fsmCaller;
    private LogManager logManager;
    private LogStorage logStorage;
    private ClosureQueue closureQueue;
    private BallotBox ballotBox;
    private ReplicatorGroup replicatorGroup;

    /**
     * Disruptor to run node service
     */
    private Disruptor<LogEntryEvent> applyDisruptor;
    private RingBuffer<LogEntryEvent> applyQueue;

    public static final AtomicInteger GLOBAL_NUM_NODES = new AtomicInteger(
            0);

    public NodeImpl(String groupId, PeerId serverId) {
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        //一开始的设置为未初始化
        this.state = State.STATE_UNINITIALIZED;
        //设置新的任期为0
        this.currTerm = 0;
        //设置最新的时间戳
        updateLastLeaderTimestamp(Utils.monotonicMs());
//        this.confCtx = new ConfigurationCtx(this);
        final int num = GLOBAL_NUM_NODES.incrementAndGet();
        LOG.info("The number of active nodes increment to {}.", num);
    }

    @Override
    public boolean init(NodeOptions opts) {
        Requires.requireNonNull(opts, "Null node options");
        Requires.requireNonNull(opts.getRaftOptions(), "Null raft options");
        Requires.requireNonNull(opts.getServiceFactory(), "Null jraft service factory");

        this.serviceFactory = opts.getServiceFactory();
        this.options = opts;
        this.raftOptions = opts.getRaftOptions();
        //this.metrics = new NodeMetrics(opts.isEnableMetrics());
        this.serverId.setPriority(opts.getElectionPriority());
        this.electionTimeoutCounter = 0;


        //fsmCaller封装对业务 StateMachine 的状态转换的调用以及日志的写入等
        this.fsmCaller = new FSMCallerImpl(lastAppliedIndex, applyingIndex);

        //初始化日志存储功能
        if (!initLogStorage()) {
            LOG.error("Node {} initLogStorage failed.", getNodeId());
            return false;
        }
        //初始化元数据存储功能
        if (!initMetaStorage()) {
            LOG.error("Node {} initMetaStorage failed.", getNodeId());
            return false;
        }
        //对FSMCaller初始化
        if (!initFSMCaller(new LogId(0, 0))) {
            LOG.error("Node {} initFSMCaller failed.", getNodeId());
            return false;
        }



        // TODO RPC service and ReplicatorGroup is in cycle dependent, refactor it
        this.replicatorGroup = new ReplicatorGroupImpl();
        //收其他节点或者客户端发过来的请求，转交给对应服务处理
        this.rpcService = new DefaultRaftClientService(this.replicatorGroup);
        final ReplicatorGroupOptions rgOpts = new ReplicatorGroupOptions();
        rgOpts.setHeartbeatTimeoutMs(heartbeatTimeout(this.options.getElectionTimeoutMs()));
        rgOpts.setElectionTimeoutMs(this.options.getElectionTimeoutMs());
        rgOpts.setLogManager(this.logManager);
        rgOpts.setBallotBox(this.ballotBox);
        rgOpts.setNode(this);
        rgOpts.setRaftRpcClientService(this.rpcService);
        rgOpts.setSnapshotStorage(this.snapshotExecutor != null ? this.snapshotExecutor.getSnapshotStorage() : null);
        rgOpts.setRaftOptions(this.raftOptions);
        rgOpts.setTimerManager(this.timerManager);
        return false;
    }

    private boolean initLogStorage() {
        Requires.requireNonNull(this.fsmCaller, "Null fsm caller");
        this.logStorage = this.serviceFactory.createLogStorage(this.options.getLogUri(), this.raftOptions);
        this.logManager = new LogManagerImpl();
        final LogManagerOptions opts = new LogManagerOptions();
        opts.setLogEntryCodecFactory(this.serviceFactory.createLogEntryCodecFactory());
        opts.setLogStorage(this.logStorage);
        opts.setConfigurationManager(this.configManager);
        opts.setFsmCaller(this.fsmCaller);
        opts.setNodeMetrics(this.metrics);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        opts.setRaftOptions(this.raftOptions);
        return this.logManager.init(opts);
    }

    private boolean initFSMCaller(final LogId bootstrapId) {
        if (this.fsmCaller == null) {
            LOG.error("Fail to init fsm caller, null instance, bootstrapId={}.", bootstrapId);
            return false;
        }
        this.closureQueue = new ClosureQueueImpl();
        final FSMCallerOptions opts = new FSMCallerOptions();
        opts.setAfterShutdown(status -> afterShutdown());
        opts.setLogManager(this.logManager);
        opts.setFsm(this.options.getFsm());
        opts.setClosureQueue(this.closureQueue);
        opts.setNode(this);
        opts.setBootstrapId(bootstrapId);
        opts.setDisruptorBufferSize(this.raftOptions.getDisruptorBufferSize());
        return this.fsmCaller.init(opts);
    }

    /**
     * the handler of electionTimeout,called by electionTimeoutTimer
     * when elctionTimeOut
     */
    private void handleElectionTimeout() {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            doUnlock = false;
            if (this.state != State.STATE_FOLLOWER) {
                return;
            }
            //如果当前选举没有超时则说明此轮选举有效
            if (isCurrentLeaderValid()) {
                return;
            }
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT, "Lost connection from leader %s.",
                    this.leaderId));

            // Judge whether to launch a election.
            if (!allowLaunchElection()) {
                return;
            }

            doUnlock = false;
            //预投票 (pre-vote) 环节
            //候选者在发起投票之前，先发起预投票，
            //如果没有得到半数以上节点的反馈，则候选者就会识趣的放弃参选
            preVote();

        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 之所以要增加一个preVote的步骤，是为了解决系统中防止某个节点由于无法和leader同步，不断发起投票，抬升自己的Term，
     * 导致自己Term比Leader的Term还大，(更高的Term在Raft协议中代表更“新“的日志)然后迫使Leader放弃Leader身份，
     * 开始新一轮的选举。而preVote则强调节点必须获得半数以上的投票才能开始发起新一轮的选举
     * <p>
     * in writeLock
     */
    private void preVote() {
        long oldTerm;
        try {
            LOG.info("Node {} term {} start preVote.", getNodeId(), this.currTerm);
            //当前的节点不能再安装快照的时候进行选举
            if (this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn(
                        "Node {} term {} doesn't do preVote when installing snapshot as the configuration may be out of date.",
                        getNodeId(), this.currTerm);
                return;
            }
            //conf里面记录了集群节点的信息，如果当前的节点不包含在集群里说明是由问题的
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do preVote as it is not in conf <{}>.", getNodeId(), this.conf);
                return;
            }
            //设置一下当前的任期
            oldTerm = this.currTerm;

        } finally {
            this.writeLock.unlock();
        }

        //返回最新的log实体类
        final LogId lastLogId = this.logManager.getLastLogId(true);

        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            // pre_vote need defense ABA after unlock&writeLock
            //因为在上面没有重新加锁的间隙里可能会被别的线程改变了，所以这里校验一下
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when get lastLogId.", getNodeId(), this.currTerm);
                return;
            }
            //初始化预投票投票箱
            this.prevVoteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            for (final PeerId peer : this.conf.listPeers()) {
                //如果遍历的节点是当前节点就跳过
                if (peer.equals(this.serverId)) {
                    continue;
                }
                //如果遍历的节点因为宕机或者手动下线等原因连接不上也跳过
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                //设置一个回调的类
                final OnPreVoteRpcDone done = new OnPreVoteRpcDone(peer, this, term);

                //向被遍历到的这个节点发送一个预投票的请求
                done.request = RpcRequests.RequestVoteRequest.newBuilder() //
                        .setPreVote(true)
                        .setGroupId(this.groupId) //
                        .setServerId(this.serverId.toString()) //
                        .setPeerId(peer.toString()) //
                        // next term
                        .setTerm(this.currTerm + 1)
                        .setLastLogIndex(lastLogId.getIndex()) //
                        .setLastLogTerm(lastLogId.getTerm()) //
                        .build();
                //最后在发送成功收到响应之后会回调OnPreVoteRpcDone的run方法
                this.rpcService.preVote(peer.getEndpoint(), done.request, done);
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    private void resetLeaderId(final PeerId newLeaderId, final Status status) {
        if (newLeaderId.isEmpty()) {
            //这个判断表示如果当前节点是候选者或者是Follower，并且已经有leader了
            if (!this.leaderId.isEmpty() && this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                //向状态机装发布停止跟随该leader的事件
                this.fsmCaller.onStopFollowing(new LeaderChangeContext(this.leaderId.copy(), this.currTerm, status));
            }
            //把当前的leader设置为一个空值
            this.leaderId = PeerId.emptyPeer();
        } else {
            //如果当前节点没有leader
            if (this.leaderId == null || this.leaderId.isEmpty()) {
                //那么发布要跟随该leader的事件
                this.fsmCaller.onStartFollowing(new LeaderChangeContext(newLeaderId, this.currTerm, status));
            }
            this.leaderId = newLeaderId.copy();
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public PeerId getLeaderId() {
        return null;
    }

    @Override
    public NodeId getNodeId() {
        return null;
    }

    @Override
    public String getGroupId() {
        return null;
    }

    @Override
    public void apply(Task task) {
        //shutdown 方法中会用到 shutdownLatch，可利用此变量是否为null来判断当前是否在shutting down
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(task.getDone(), new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(task, "Null task");
        final LogEntry entry = new LogEntry();
        entry.setData(task.getData());

        //使用EventTranslator用于构建Event，主要是为了java8的lambda写法而产生的
        final EventTranslator<LogEntryEvent> translator = (event, sequence) -> {
            event.reset();
            event.done = task.getDone();
            event.entry = entry;
            event.expectedTerm = task.getExpectedTerm();
        };

        switch (this.options.getApplyTaskMode()) {
            case Blocking:
                //队列满了会阻塞
                this.applyQueue.publishEvent(translator);
                break;
            case NonBlocking:
            default:
                if (!this.applyQueue.tryPublishEvent(translator)) {
                    String errorMsg = "Node is busy, has too many tasks, queue is full and bufferSize=" + this.applyQueue.getBufferSize();
                    Utils.runClosureInThread(task.getDone(),
                            new Status(RaftError.EBUSY, errorMsg));
                    LOG.warn("Node {} applyQueue is overload.", getNodeId());
                    this.metrics.recordTimes("apply-task-overload-times", 1);
                    if (task.getDone() == null) {
                        throw new OverloadException(errorMsg);
                    }
                }
                break;
        }
    }

    /**
     * SOFAJRaft 在 Follower 本地维护了一个时间戳来记录收到 Leader
     * 上一次数据更新的时间 lastLeaderTimestamp,只有超过 election timeout 之后才允许接受预投票请求
     * 预防非对称网络分区带来的问题
     * 用当前时间和上次leader通信时间相减，如果小于ElectionTimeoutMs（默认1s），那么就没有超时，说明leader有效
     *
     * @return
     */
    private boolean isCurrentLeaderValid() {
        return Utils.monotonicMs() - this.lastLeaderTimestamp < this.options.getElectionTimeoutMs();
    }

    private void updateLastLeaderTimestamp(final long lastLeaderTimestamp) {
        this.lastLeaderTimestamp = lastLeaderTimestamp;
    }

    /**
     * Whether to allow for launching election or not by comparing node's priority with target
     * priority. And at the same time, if next leader is not elected until next election
     * timeout, it decays its local target priority exponentially.
     *
     * @return Whether current node will launch election or not.
     */
    private boolean allowLaunchElection() {

        // Priority 0 is a special value so that a node will never participate in election.
        if (this.serverId.isPriorityNotElected()) {
            LOG.warn("Node {} will never participate in election, because it's priority={}.", getNodeId(),
                    this.serverId.getPriority());
            return false;
        }

        // If this nodes disable priority election, then it can make a election.
        if (this.serverId.isPriorityDisabled()) {
            return true;
        }

        // If current node's priority < target_priority, it does not initiate leader,
        // election and waits for the next election timeout.
        if (this.serverId.getPriority() < this.targetPriority) {
            this.electionTimeoutCounter++;

            // If next leader is not elected until next election timeout, it
            // decays its local target priority exponentially.
            if (this.electionTimeoutCounter > 1) {
                decayTargetPriority();
                this.electionTimeoutCounter = 0;
            }

            if (this.electionTimeoutCounter == 1) {
                LOG.debug("Node {} does not initiate leader election and waits for the next election timeout.",
                        getNodeId());
                return false;
            }
        }

        return this.serverId.getPriority() >= this.targetPriority;
    }


    /**
     * Decay targetPriority value based on gap value.
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void decayTargetPriority() {
        // Default Gap value should be bigger than 10.
        final int decayPriorityGap = Math.max(this.options.getDecayPriorityGap(), 10);
        final int gap = Math.max(decayPriorityGap, (this.targetPriority / 5));

        final int prevTargetPriority = this.targetPriority;
        this.targetPriority = Math.max(ElectionPriority.MinValue, (this.targetPriority - gap));
        LOG.info("Node {} priority decay, from: {}, to: {}.", getNodeId(), prevTargetPriority, this.targetPriority);
    }

    /**
     * handle pre-vote request
     * 首先调用isActive，看一下当前节点是不是正常的节点，不是正常节点要返回Error信息
     * 将请求传过来的ServerId解析到candidateId实例中
     * 校验当前的节点如果有leader，并且leader有效的，那么就直接break，返回granted为false
     * 如果当前的任期大于请求的任期，那么调用checkReplicator检查自己是不是leader，如果是leader，
     * 那么将当前节点从failureReplicators移除，重新加入到replicatorMap中。然后直接break
     * 请求任期和当前任期相等的情况也要校验，只是不用break
     * 如果请求的日志比当前的最新的日志还要新，那么返回granted为true，代表授权成功
     *
     * @param request data of the pre vote
     * @return
     */
    @Override
    public Message handlePreVoteRequest(RpcRequests.RequestVoteRequest request) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }
            final PeerId candidateId = new PeerId();
            //发送过来的request请求携带的ServerId格式不能错
            if (!candidateId.parse(request.getServerId())) {
                LOG.warn("Node {} received PreVoteRequest from {} serverId bad format.", getNodeId(),
                        request.getServerId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.RequestVoteResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Parse candidateId failed: %s.", request.getServerId());
            }
            boolean granted = false;

            do {
                //节点不在集群中
                if (!this.conf.contains(candidateId)) {
                    LOG.warn("Node {} ignore PreVoteRequest from {} as it is not in conf <{}>.", getNodeId(),
                            request.getServerId(), this.conf);
                    break;
                }
                //已经有leader的情况
                if (this.leaderId != null && !this.leaderId.isEmpty() && isCurrentLeaderValid()) {
                    LOG.info(
                            "Node {} ignore PreVoteRequest from {}, term={}, currTerm={}, because the leader {}'s lease is still valid.",
                            getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, this.leaderId);
                    break;
                }
                //请求的任期小于当前的任期
                if (request.getTerm() < this.currTerm) {
                    LOG.info("Node {} ignore PreVoteRequest from {}, term={}, currTerm={}.", getNodeId(),
                            request.getServerId(), request.getTerm(), this.currTerm);
                    // A follower replicator may not be started when this node become leader, so we must check it.
                    //如果请求term小于当前term,当前节点刚刚选举成为leader时可能没有启动复制任务，校验复制任务
                    checkReplicator(candidateId);
                    break;
                }
                // A follower replicator may not be started when this node become leader, so we must check it.
                // check replicator state
                checkReplicator(candidateId);

                doUnlock = false;
                this.writeLock.unlock();
                //获取最新的日志
                final LogId lastLogId = this.logManager.getLastLogId(true);

                doUnlock = true;
                this.writeLock.lock();
                final LogId requestLastLogId = new LogId(request.getLastLogIndex(), request.getLastLogTerm());
                //比较当前节点的日志完整度和请求节点的日志完整度
                granted = requestLastLogId.compareTo(lastLogId) >= 0;
                LOG.info(
                        "Node {} received PreVoteRequest from {}, term={}, currTerm={}, granted={}, requestLastLogId={}, lastLogId={}.",
                        getNodeId(), request.getServerId(), request.getTerm(), this.currTerm, granted, requestLastLogId,
                        lastLogId);
            } while (false);
            return RpcRequests.RequestVoteResponse.newBuilder() //
                    .setTerm(this.currTerm) //
                    .setGranted(granted) //
                    .build();

        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
        return null;
    }

    @Override
    public Message handleRequestVoteRequest(RpcRequests.RequestVoteRequest request) {
        return null;
    }

    @Override
    public Message handleAppendEntriesRequest(RpcRequests.AppendEntriesRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public Message handleInstallSnapshot(RpcRequests.InstallSnapshotRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public Message handleTimeoutNowRequest(RpcRequests.TimeoutNowRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public void handleReadIndexRequest(RpcRequests.ReadIndexRequest request, RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {

    }

    @Override
    public Message handleInstallSnapshot(RpcRequests.InstallSnapshotRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public Message handleTimeoutNowRequest(RpcRequests.TimeoutNowRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public void handleReadIndexRequest(RpcRequests.ReadIndexRequest request, RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {

    }


    /**
     * 第一重试校验了当前的状态，如果不是FOLLOWER那么就不能发起选举。因为如果是leader节点，那么它不会选举，只能stepdown下台，
     * 把自己变成FOLLOWER后重新选举；如果是CANDIDATE，那么只能进行由FOLLOWER发起的投票，所以从功能上来说，只能FOLLOWER发起选举。
     * 从Raft 的设计上来说也只能由FOLLOWER来发起选举，所以这里进行了校验。
     * 第二重校验主要是校验发送请求时的任期和接受到响应时的任期还是不是一个，如果不是那么说明已经不是上次那轮的选举了，是一次失效的选举
     * 第三重校验是校验响应返回的任期是不是大于当前的任期，如果大于当前的任期，那么重置当前的leade
     *
     * @param peerId
     * @param term
     * @param response
     */
    public void handlePreVoteResponse(final PeerId peerId, final long term, final RpcRequests.RequestVoteResponse response) {
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            //只有follower才可以尝试发起选举
            if (this.state != State.STATE_FOLLOWER) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, state not in STATE_FOLLOWER but {}.",
                        getNodeId(), peerId, this.state);
                return;
            }
            if (term != this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term={}, currTerm={}.", getNodeId(),
                        peerId, term, this.currTerm);
                return;
            }
            //如果返回的任期大于当前的任期，那么这次请求也是无效的
            if (response.getTerm() > this.currTerm) {
                LOG.warn("Node {} received invalid PreVoteResponse from {}, term {}, expect={}.", getNodeId(), peerId,
                        response.getTerm(), this.currTerm);
                stepDown(response.getTerm(), false, new Status(RaftError.EHIGHERTERMRESPONSE,
                        "Raft node receives higher term pre_vote_response."));
                return;
            }
            LOG.info("Node {} received PreVoteResponse from {}, term={}, granted={}.", getNodeId(), peerId,
                    response.getTerm(), response.getGranted());
            // check granted quorum?
            if (response.getGranted()) {
                //校验完之后响应的节点会返回一个授权，如果授权通过的话则调用Ballot的grant方法，表示给当前的节点投一票
                this.prevVoteCtx.grant(peerId);
                //得到了半数以上的响应
                if (this.prevVoteCtx.isGranted()) {
                    doUnlock = false;
                    //进行选举
                    electSelf();
                }
            }
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 对当前的节点进行校验，如果当前节点不在集群里面则不进行选举
     * 因为是Follower发起的选举，所以大概是因为要进行正式选举了，把预选举定时器关掉
     * 清空leader再进行选举，注意这里会把votedId设置为当前节点，代表自己参选
     * 开始发起投票定时器，因为可能投票失败需要循环发起投票，voteTimer里面会根据当前的CANDIDATE状态调用electSelf进行选举
     * 调用init方法初始化投票箱，这里和prevVoteCtx是一样的
     * 遍历所有节点，然后向其他集群节点发送RequestVoteRequest请求，这里也是和preVote一样的，请求是被RequestVoteRequestProcessor处理器处理的。
     * 如果有超过半数以上的节点投票选中，那么就调用becomeLeader晋升为leader
     */
    // should be in writeLock
    private void electSelf() {
        long oldTerm;
        try {
            LOG.info("Node {} start vote and grant vote self, term={}.", getNodeId(), this.currTerm);
            //1. 如果当前节点不在集群里面则不进行选举
            if (!this.conf.contains(this.serverId)) {
                LOG.warn("Node {} can't do electSelf as it is not in {}.", getNodeId(), this.conf);
                return;
            }
            //2. 大概是因为要进行正式选举了，把预选举关掉
            if (this.state == State.STATE_FOLLOWER) {
                LOG.debug("Node {} stop election timer, term={}.", getNodeId(), this.currTerm);
                this.electionTimer.stop();
            }

            //3. 清空leader
            resetLeaderId(PeerId.emptyPeer(), new Status(RaftError.ERAFTTIMEDOUT,
                    "A follower's leader_id is reset to NULL as it begins to request_vote."));
            this.state = State.STATE_CANDIDATE;
            this.currTerm++;

            this.votedId = this.serverId.copy();
            LOG.debug("Node {} start vote timer, term={} .", getNodeId(), this.currTerm);
            //4. 开始发起投票定时器，因为可能投票失败需要循环发起投票
            this.voteTimer.start();
            //5. 初始化投票箱
            this.voteCtx.init(this.conf.getConf(), this.conf.isStable() ? null : this.conf.getOldConf());
            oldTerm = this.currTerm;
        } finally {
            this.writeLock.unlock();
        }

        final LogId lastLogId = this.logManager.getLastLogId(true);

        this.writeLock.lock();
        try {
            // vote need defense ABA after unlock&writeLock
            if (oldTerm != this.currTerm) {
                LOG.warn("Node {} raise term {} when getLastLogId.", getNodeId(), this.currTerm);
                return;
            }
            //6. 遍历所有节点
            for (final PeerId peer : this.conf.listPeers()) {
                if (peer.equals(this.serverId)) {
                    continue;
                }
                if (!this.rpcService.connect(peer.getEndpoint())) {
                    LOG.warn("Node {} channel init failed, address={}.", getNodeId(), peer.getEndpoint());
                    continue;
                }
                final OnRequestVoteRpcDone done = new OnRequestVoteRpcDone(peer, this.currTerm, this);
                done.request = RpcRequests.RequestVoteRequest.newBuilder() //
                        .setPreVote(false) // It's not a pre-vote request.
                        .setGroupId(this.groupId) //
                        .setServerId(this.serverId.toString()) //
                        .setPeerId(peer.toString()) //
                        .setTerm(this.currTerm) //
                        .setLastLogIndex(lastLogId.getIndex()) //
                        .setLastLogTerm(lastLogId.getTerm()) //
                        .build();
                this.rpcService.requestVote(peer.getEndpoint(), done.request, done);
            }

            this.metaStorage.setTermAndVotedFor(this.currTerm, this.serverId);
            this.voteCtx.grant(this.serverId);
            if (this.voteCtx.isGranted()) {
                //7. 投票成功，那么就晋升为leader
                becomeLeader();
            }
        } finally {
            this.writeLock.unlock();
        }

    }

    private void becomeLeader() {
        Requires.requireTrue(this.state == State.STATE_CANDIDATE, "Illegal state: " + this.state);
        LOG.info("Node {} become leader of group, term={}, conf={}, oldConf={}.", getNodeId(), this.currTerm,
                this.conf.getConf(), this.conf.getOldConf());
        // cancel candidate vote timer
        //晋升leader之后就会把选举的定时器关闭了
        stopVoteTimer();
        this.state = State.STATE_LEADER;
        this.leaderId = this.serverId.copy();
        //复制集群中设置新的任期
        this.replicatorGroup.resetTerm(this.currTerm);
        // Start follower's replicators
        //当节点成为leader后，会启动所有follower和learner的replicator。其实是通过addReplicator方法实现的。
        for (final PeerId peer : this.conf.listPeers()) {
            if (peer.equals(this.serverId)) {
                continue;
            }
            LOG.debug("Node {} add a replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            //如果成为leader，那么需要把自己的日志信息复制到其他节点
            if (!this.replicatorGroup.addReplicator(peer)) {
                LOG.error("Fail to add a replicator, peer={}.", peer);
            }
        }

        // Start learner's replicators
        for (final PeerId peer : this.conf.listLearners()) {
            LOG.debug("Node {} add a learner replicator, term={}, peer={}.", getNodeId(), this.currTerm, peer);
            if (!this.replicatorGroup.addReplicator(peer, ReplicatorType.Learner)) {
                LOG.error("Fail to add a learner replicator, peer={}.", peer);
            }
        }

        // init commit manager
        this.ballotBox.resetPendingIndex(this.logManager.getLastLogIndex() + 1);
        // Register _conf_ctx to reject configuration changing before the first log
        // is committed.
        if (this.confCtx.isBusy()) {
            throw new IllegalStateException();
        }
        this.confCtx.flush(this.conf.getConf(), this.conf.getOldConf());
        //如果是leader了，那么就要定时的检查不是有资格胜任
        this.stepDownTimer.start();
    }

    private void checkReplicator(final PeerId candidateId) {
        if (this.state == State.STATE_LEADER) {
            this.replicatorGroup.checkReplicator(candidateId, false);
        }
    }

    public void executeApplyingTasks(final List<LogEntryEvent> tasks) {
        this.writeLock.lock();
        try {
            final int size = tasks.size();
            //如果当前节点的状态已经不是leader,直接执行task.done.run()
            if (this.state != State.STATE_LEADER) {
                final Status st = new Status();
                if (this.state != State.STATE_TRANSFERRING) {
                    st.setError(RaftError.EPERM, "Is not leader.");
                } else {
                    st.setError(RaftError.EBUSY, "Is transferring leadership.");
                }
                LOG.debug("Node {} can't apply, status={}.", getNodeId(), st);
                final List<Closure> dones = tasks.stream().map(ele -> ele.done)
                        .filter(Objects::nonNull).collect(Collectors.toList());
                Utils.runInThread(() -> {
                    for (final Closure done : dones) {
                        // Closure.run(错误状态）返回
                        done.run(st);
                    }
                });
                return;
            }
            final List<LogEntry> entries = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final LogEntryEvent task = tasks.get(i);
                //FIXME 此分支何时触发 暂且未知
                if (task.expectedTerm != -1 && task.expectedTerm != this.currTerm) {
                    LOG.debug("Node {} can't apply task whose expectedTerm={} doesn't match currTerm={}.", getNodeId(),
                            task.expectedTerm, this.currTerm);
                    if (task.done != null) {
                        final Status st = new Status(RaftError.EPERM, "expected_term=%d doesn't match current_term=%d",
                                task.expectedTerm, this.currTerm);
                        Utils.runClosureInThread(task.done, st);
                        task.reset();
                    }
                    continue;
                }
                if (!this.ballotBox.appendPendingTask(this.conf.getConf(),
                        this.conf.isStable() ? null : this.conf.getOldConf(), task.done)) {
                    Utils.runClosureInThread(task.done, new Status(RaftError.EINTERNAL, "Fail to append task."));
                    task.reset();
                    continue;
                }
                // set task entry info before adding to list.
                task.entry.getId().setTerm(this.currTerm);
                task.entry.setType(EnumOutter.EntryType.ENTRY_TYPE_DATA);
                entries.add(task.entry);
                task.reset();
            }
            //落盘后调用LeaderStableClosure，给自己投一票
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries));
            // update conf.first
            checkAndSetConfiguration(true);
        } finally {
            this.writeLock.unlock();
        }
    }
}
