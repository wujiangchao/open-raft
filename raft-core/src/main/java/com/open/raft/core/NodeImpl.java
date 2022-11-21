package com.open.raft.core;

import com.google.protobuf.Message;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.INode;
import com.open.raft.JRaftServiceFactory;
import com.open.raft.ReadOnlyService;
import com.open.raft.Status;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.closure.ClosureQueueImpl;
import com.open.raft.closure.LeaderStableClosure;
import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.closure.ReadIndexHeartbeatResponseClosure;
import com.open.raft.conf.Configuration;
import com.open.raft.conf.ConfigurationEntry;
import com.open.raft.conf.ConfigurationManager;
import com.open.raft.core.done.ConfigurationChangeDone;
import com.open.raft.core.done.OnPreVoteRpcDone;
import com.open.raft.core.done.OnRequestVoteRpcDone;
import com.open.raft.core.event.LogEntryEvent;
import com.open.raft.entity.Ballot;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.entity.LogEntry;
import com.open.raft.entity.LogId;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.entity.RaftOutter;
import com.open.raft.entity.Task;
import com.open.raft.error.OverloadException;
import com.open.raft.error.RaftError;
import com.open.raft.error.RaftException;
import com.open.raft.option.FSMCallerOptions;
import com.open.raft.option.LogManagerOptions;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RaftMetaStorageOptions;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReadOnlyOption;
import com.open.raft.option.ReadOnlyServiceOptions;
import com.open.raft.option.ReplicatorGroupOptions;
import com.open.raft.option.SnapshotExecutorOptions;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.rpc.RaftServerService;
import com.open.raft.rpc.RpcRequestClosure;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.rpc.impl.core.DefaultRaftClientService;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.LogStorage;
import com.open.raft.storage.RaftMetaStorage;
import com.open.raft.storage.impl.LogManagerImpl;
import com.open.raft.storage.snapshot.SnapshotExecutor;
import com.open.raft.storage.snapshot.SnapshotExecutorImpl;
import com.open.raft.util.RaftUtils;
import com.open.raft.util.RepeatedTimer;
import com.open.raft.util.Requires;
import com.open.raft.util.RpcFactoryHelper;
import com.open.raft.util.Utils;
import com.open.raft.util.concurrent.NodeReadWriteLock;
import com.open.raft.util.timer.RaftTimerFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
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

    public final static RaftTimerFactory TIMER_FACTORY = RaftUtils
            .raftTimerFactory();

    /**
     * Raft group and node options and identifier
     */
    private final String groupId;
    private NodeOptions options;

    private RaftOptions raftOptions;
    private final PeerId serverId;

    private NodeId nodeId;
    private JRaftServiceFactory serviceFactory;
    private PeerId leaderId = new PeerId();
    private PeerId votedId;

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
    private ConfigurationEntry conf;


    private FSMCaller fsmCaller;
    private SnapshotExecutor snapshotExecutor;
    private LogManager logManager;
    private ConfigurationManager configManager;
    private LogStorage logStorage;
    private RaftMetaStorage metaStorage;
    private ClosureQueue closureQueue;
    private BallotBox ballotBox;
    private ReplicatorGroup replicatorGroup;
    private RaftClientService rpcService;
    private final ConfigurationCtx confCtx;
    private ReadOnlyService readOnlyService;


    /**
     * Timers
     */
    private Scheduler timerManager;
    private RepeatedTimer electionTimer;
    private RepeatedTimer voteTimer;
    private RepeatedTimer stepDownTimer;
    private RepeatedTimer snapshotTimer;

    /**
     * Disruptor to run node service
     */
    private Disruptor<LogEntryEvent> applyDisruptor;
    private RingBuffer<LogEntryEvent> applyQueue;

    public static final AtomicInteger GLOBAL_NUM_NODES = new AtomicInteger(
            0);

    private final Ballot voteCtx = new Ballot();
    private final Ballot prevVoteCtx = new Ballot();

    /**
     * Metrics
     */
    private NodeMetrics metrics;

    public NodeImpl(String groupId, PeerId serverId) {
        this.groupId = groupId;
        this.serverId = serverId != null ? serverId.copy() : null;
        //一开始的设置为未初始化
        this.state = State.STATE_UNINITIALIZED;
        //设置新的任期为0
        this.currTerm = 0;
        //设置最新的时间戳
        updateLastLeaderTimestamp(Utils.monotonicMs());
        this.confCtx = new ConfigurationCtx(this);
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

        this.timerManager = TIMER_FACTORY.getRaftScheduler(this.options.isSharedTimerPool(),
                this.options.getTimerPoolSize(), "JRaft-Node-ScheduleThreadPool");

        /**
         * voteTimer是用来控制选举的，如果选举超时，当前的节点又是候选者角色，那么就会发起选举。
         *  electionTimer是预投票计时器。候选者在发起投票之前，先发起预投票，如果没有得到半数以上节点的反馈，则候选者就会识趣的放弃参选。
         *  stepDownTimer定时检查是否需要重新选举leader。当前的leader可能出现它的Follower可能并没有整个集群的1/2却还没有下台的情况，那么这个时候会定期的检查看leader的Follower是否有那么多，没有那么多的话会强制让leader下台。
         *  snapshotTimer快照计时器。这个计时器会每隔1小时触发一次生成一个快照。
         *
         * 这些计时器的具体实现现在暂时不表，等到要讲具体功能的时候再进行梳理。
         *
         * 这些计时器有一个共同的特点就是会根据不同的计时器返回一个在一定范围内随机的时间。返回一个随机的时间可以防止多个节点在同一时间内同时发起投票选举从而降低选举失败的概率。
         */

        // Init timers
        final String suffix = getNodeId().toString();
        String name = "JRaft-VoteTimer-" + suffix;
        //用来控制选举
        this.voteTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(), TIMER_FACTORY.getVoteTimer(
                this.options.isSharedVoteTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleVoteTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                return randomTimeout(timeoutMs);
            }
        };

        //设置预投票计时器
        //当leader在规定的一段时间内没有与 Follower 舰船进行通信时，
        // Follower 就可以认为leader已经不能正常担任旗舰的职责，则 Follower 可以去尝试接替leader的角色。
        // 这段通信超时被称为 Election Timeout
        //候选者在发起投票之前，先发起预投票
        name = "JRaft-ElectionTimer-" + suffix;
        this.electionTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs(),
                TIMER_FACTORY.getElectionTimer(this.options.isSharedElectionTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleElectionTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                //在一定范围内返回一个随机的时间戳
                //为了避免同时发起选举而导致失败
                return randomTimeout(timeoutMs);
            }
        };
        //用来控制leader下台
        //定时检查是否需要重新选举leader
        name = "JRaft-StepDownTimer-" + suffix;
        this.stepDownTimer = new RepeatedTimer(name, this.options.getElectionTimeoutMs() >> 1,
                TIMER_FACTORY.getStepDownTimer(this.options.isSharedStepDownTimer(), name)) {

            @Override
            protected void onTrigger() {
                handleStepDownTimeout();
            }
        };
        name = "JRaft-SnapshotTimer-" + suffix;
        this.snapshotTimer = new RepeatedTimer(name, this.options.getSnapshotIntervalSecs() * 1000,
                TIMER_FACTORY.getSnapshotTimer(this.options.isSharedSnapshotTimer(), name)) {

            private volatile boolean firstSchedule = true;

            @Override
            protected void onTrigger() {
                handleSnapshotTimeout();
            }

            @Override
            protected int adjustTimeout(final int timeoutMs) {
                if (!this.firstSchedule) {
                    return timeoutMs;
                }

                // Randomize the first snapshot trigger timeout
                this.firstSchedule = false;
                if (timeoutMs > 0) {
                    int half = timeoutMs / 2;
                    return half + ThreadLocalRandom.current().nextInt(half);
                } else {
                    return timeoutMs;
                }
            }
        };


        //fsmCaller封装对业务 StateMachine 的状态转换的调用以及日志的写入等
        this.fsmCaller = new FSMCallerImpl();

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


        //初始化快照存储功能
        if (!initSnapshotStorage()) {
            LOG.error("Node {} initSnapshotStorage failed.", getNodeId());
            return false;
        }
        //校验日志文件索引的一致性
        //检查快照index是否落在first & last log index之内
        final Status st = this.logManager.checkConsistency();
        if (!st.isOk()) {
            LOG.error("Node {} is initialized with inconsistent log, status={}.", getNodeId(), st);
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
        //用来调度heartbeat
        rgOpts.setTimerManager(this.timerManager);
        this.replicatorGroup.init(new NodeId(this.groupId, this.serverId), rgOpts);

        //只读服务，包括readindex也是在这里被调用
        this.readOnlyService = new ReadOnlyServiceImpl();
        final ReadOnlyServiceOptions rosOpts = new ReadOnlyServiceOptions();
        rosOpts.setFsmCaller(this.fsmCaller);
        rosOpts.setNode(this);
        rosOpts.setRaftOptions(this.raftOptions);

        //只读服务初始化
        if (!this.readOnlyService.init(rosOpts)) {
            LOG.error("Fail to init readOnlyService.");
            return false;
        }

        return false;
    }

    private int heartbeatTimeout(final int electionTimeout) {
        return Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
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

    private boolean initSnapshotStorage() {
        if (StringUtils.isEmpty(this.options.getSnapshotUri())) {
            LOG.warn("Do not set snapshot uri, ignore initSnapshotStorage.");
            return true;
        }
        this.snapshotExecutor = new SnapshotExecutorImpl();
        final SnapshotExecutorOptions opts = new SnapshotExecutorOptions();
        opts.setUri(this.options.getSnapshotUri());
        opts.setFsmCaller(this.fsmCaller);
        opts.setNode(this);
        opts.setLogManager(this.logManager);
        opts.setAddr(this.serverId != null ? this.serverId.getEndpoint() : null);
        opts.setInitTerm(this.currTerm);
        opts.setFilterBeforeCopyRemote(this.options.isFilterBeforeCopyRemote());
        // get snapshot throttle
        opts.setSnapshotThrottle(this.options.getSnapshotThrottle());
        return this.snapshotExecutor.init(opts);
    }

    private void afterShutdown() {
        List<Closure> savedDoneList = null;
        this.writeLock.lock();
        try {
            if (!this.shutdownContinuations.isEmpty()) {
                savedDoneList = new ArrayList<>(this.shutdownContinuations);
            }
            if (this.logStorage != null) {
                this.logStorage.shutdown();
            }
            this.state = State.STATE_SHUTDOWN;
        } finally {
            this.writeLock.unlock();
        }
        if (savedDoneList != null) {
            for (final Closure closure : savedDoneList) {
                Utils.runClosureInThread(closure);
            }
        }
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

    private void handleSnapshotTimeout() {
        this.writeLock.lock();
        try {
            if (!this.state.isActive()) {
                return;
            }
        } finally {
            this.writeLock.unlock();
        }
        // do_snapshot in another thread to avoid blocking the timer thread.
        Utils.runInThread(() -> doSnapshot(null));
    }

    private void doSnapshot(final Closure done) {
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.doSnapshot(done);
        } else {
            if (done != null) {
                final Status status = new Status(RaftError.EINVAL, "Snapshot is not supported");
                Utils.runClosureInThread(done, status);
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
        shutdown(null);
    }

    @Override
    public void shutdown(Closure done) {
        List<RepeatedTimer> timers = null;
        this.writeLock.lock();
        try {
            LOG.info("Node {} shutdown, currTerm={} state={}.", getNodeId(), this.currTerm, this.state);
            if (this.state.compareTo(State.STATE_SHUTTING) < 0) {
                NodeManager.getInstance().remove(this);
                // If it is leader, set the wakeup_a_candidate with true;
                // If it is follower, call on_stop_following in step_down
                if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                    stepDown(this.currTerm, this.state == State.STATE_LEADER,
                            new Status(RaftError.ESHUTDOWN, "Raft node is going to quit."));
                }
                this.state = State.STATE_SHUTTING;
                // Stop all timers
                timers = stopAllTimers();
                if (this.readOnlyService != null) {
                    this.readOnlyService.shutdown();
                }
                if (this.logManager != null) {
                    this.logManager.shutdown();
                }
                if (this.metaStorage != null) {
                    this.metaStorage.shutdown();
                }
                if (this.snapshotExecutor != null) {
                    this.snapshotExecutor.shutdown();
                }
                if (this.wakingCandidate != null) {
                    Replicator.stop(this.wakingCandidate);
                }
                if (this.fsmCaller != null) {
                    this.fsmCaller.shutdown();
                }
                if (this.rpcService != null) {
                    this.rpcService.shutdown();
                }
                if (this.applyQueue != null) {
                    final CountDownLatch latch = new CountDownLatch(1);
                    this.shutdownLatch = latch;
                    Utils.runInThread(
                            () -> this.applyQueue.publishEvent((event, sequence) -> event.shutdownLatch = latch));
                } else {
                    final int num = GLOBAL_NUM_NODES.decrementAndGet();
                    LOG.info("The number of active nodes decrement to {}.", num);
                }
                if (this.timerManager != null) {
                    this.timerManager.shutdown();
                }
            }

            if (this.state != State.STATE_SHUTDOWN) {
                if (done != null) {
                    this.shutdownContinuations.add(done);
                    done = null;
                }
                return;
            }
        } finally {
            this.writeLock.unlock();

            // Destroy all timers out of lock
            if (timers != null) {
                destroyAllTimers(timers);
            }
            // Call join() asynchronously
            final Closure shutdownHook = done;
            Utils.runInThread(() -> {
                try {
                    join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    // This node is down, it's ok to invoke done right now. Don't invoke this
                    // in place to avoid the dead writeLock issue when done.Run() is going to acquire
                    // a writeLock which is already held by the caller
                    if (shutdownHook != null) {
                        shutdownHook.run(Status.OK());
                    }
                }
            });

        }
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

    @Override
    public NodeOptions getOptions() {
        return options;
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

    /**
     * 完整信息: (currentTerm, logEntries[], prevTerm, prevLogIndex, commitTerm, commitLogIndex)
     * currentTerm, logEntries[]：日志信息，为了效率，日志通常为多条
     * prevTerm, prevLogIndex：日志有效性检查
     * commitTerm, commitLogIndex：最新的提交日志位点(commitIndex)
     * <p>
     * 校验当前的Node节点是否还处于活跃状态，如果不是的话，那么直接返回一个error的response
     * 校验请求的serverId的格式是否正确，不正确则返回一个error的response
     * 校验请求的任期是否小于当前的任期，如果是那么返回一个AppendEntriesResponse类型的response
     * 调用checkStepDown方法检测当前节点的任期，以及状态，是否有leader等
     * 如果请求的serverId和当前节点的leaderId是不是同一个，用来校验是不是leader发起的请求，如果不是返回一个AppendEntriesResponse
     * <p>
     * 校验是否正在生成快照
     * 获取请求的Index在当前节点中对应的LogEntry的任期是不是和请求传入的任期相同，不同的话则返回AppendEntriesResponse
     * 如果传入的entriesCount为零，那么leader发送的可能是心跳或者发送的是sendEmptyEntry，返回AppendEntriesResponse，
     * 并将当前任期和最新index封装返回请求的数据不为空，那么遍历所有的数据
     * 实例化一个logEntry，并且将数据和属性设置到logEntry实例中，最后将logEntry放入到entries集合中
     * 调用logManager将数据批量提交日志写入 RocksDB
     *
     * @param request data of the entries to append
     * @param done    callback
     * @return
     */
    @Override
    public Message handleAppendEntriesRequest(RpcRequests.AppendEntriesRequest request, RpcRequestClosure done) {
        boolean doUnlock = true;
        final long startMs = Utils.monotonicMs();
        this.writeLock.lock();
        //获取entryLog个数
        final int entriesCount = request.getEntriesCount();
        boolean success = false;
        try {
            //校验当前节点是否活跃
            if (!this.state.isActive()) {
                LOG.warn("Node {} is not in active state, currTerm={}.", getNodeId(), this.currTerm);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s is not in active state, state %s.", getNodeId(), this.state.name());
            }

            final PeerId serverId = new PeerId();
            if (!serverId.parse(request.getServerId())) {
                LOG.warn("Node {} received AppendEntriesRequest from {} serverId bad format.", getNodeId(),
                        request.getServerId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Parse serverId failed: %s.", request.getServerId());
            }

            // Check stale term
            //校验任期 (request 由leader发出)
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale AppendEntriesRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getServerId(), request.getTerm(), this.currTerm);
                return RpcRequests.AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(this.currTerm) //
                        .build();
            }

            // Check term and state to step down
            // 当前节点如果不是Follower节点的话要执行StepDown操作
            // 检查heartbeat是否来自新上任Leader，如果是，则调用stepDown并重新设置new leader
            checkStepDown(request.getTerm(), serverId);
            //这说明请求的节点不是当前节点的leader
            if (!serverId.equals(this.leaderId)) {
                //在成员变化时有可能出现两个同样任期的Leader，只需要term+1就可让两个leader下线，重新选举
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                        "More than one leader in the same term."));
                return RpcRequests.AppendEntriesResponse.newBuilder() //
                        .setSuccess(false) //
                        .setTerm(request.getTerm() + 1) //
                        .build();
            }

            //心跳成功更新时间
            updateLastLeaderTimestamp(Utils.monotonicMs());
            //校验是否正在生成快照，安装或加载快照会让follower阻塞日志复制，防止快照覆盖新的commit
            if (entriesCount > 0 && this.snapshotExecutor != null && this.snapshotExecutor.isInstallingSnapshot()) {
                LOG.warn("Node {} received AppendEntriesRequest while installing snapshot.", getNodeId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.AppendEntriesResponse.getDefaultInstance(), RaftError.EBUSY,
                                "Node %s:%s is installing snapshot.", this.groupId, this.serverId);
            }

            /*
             * 这里证明follower日志落后于Leader
             * 因为走到这里只有request.getTerm() = this.currTerm
             * 所以localPrevLogTerm <= this.currTerm
             * 如果prevLogIndex > lastLogIndex, 说明localPrevLogTerm=0，RocksDB未把日志刷盘，机器挂了，丢失最近一部分数据
             * 如果prevLogIndex < lastLogIndex，说明localPrevLogTerm!=0 && localPrevLogTerm < prevLogTerm，日志属于过期Leader，需要保证强一致性，每行日志的term&logIndex必须一致
             * 第二种情况，会在长期网络分区后出现
             */
            //传入的是发起请求节点的nextIndex-1
            final long prevLogIndex = request.getPrevLogIndex();
            final long prevLogTerm = request.getPrevLogTerm();
            final long localPrevLogTerm = this.logManager.getTerm(prevLogIndex);
            //发起请求的节点prevLogIndex对应的任期和当前节点的index所对应的任期不匹配
            if (localPrevLogTerm != prevLogTerm) {
                final long lastLogIndex = this.logManager.getLastLogIndex();

                LOG.warn(
                        "Node {} reject term_unmatched AppendEntriesRequest from {}, term={}, prevLogIndex={}, prevLogTerm={}, localPrevLogTerm={}, lastLogIndex={}, entriesSize={}.",
                        getNodeId(), request.getServerId(), request.getTerm(), prevLogIndex, prevLogTerm, localPrevLogTerm,
                        lastLogIndex, entriesCount);

                return RpcRequests.AppendEntriesResponse.newBuilder()
                        .setSuccess(false)
                        .setTerm(this.currTerm)
                        .setLastLogIndex(lastLogIndex)
                        .build();
            }
            //响应心跳或者发送的是sendEmptyEntry
            if (entriesCount == 0) {
                // heartbeat or probe request
                final RpcRequests.AppendEntriesResponse.Builder respBuilder = RpcRequests.AppendEntriesResponse.newBuilder() //
                        .setSuccess(true) //
                        .setTerm(this.currTerm) //
                        //  返回当前节点的最新的index
                        .setLastLogIndex(this.logManager.getLastLogIndex());
                doUnlock = false;
                this.writeLock.unlock();
                // see the comments at FollowerStableClosure#run()
                // 前面一切正常了，再更新lastCommittedIndex，后面的日志同步会用到。
                this.ballotBox.setLastCommittedIndex(Math.min(request.getCommittedIndex(), prevLogIndex));
                return respBuilder.build();
            }
            // fast checking if log manager is overloaded
            if (!this.logManager.hasAvailableCapacityToAppendEntries(1)) {
                LOG.warn("Node {} received AppendEntriesRequest but log manager is busy.", getNodeId());
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(AppendEntriesResponse.getDefaultInstance(), RaftError.EBUSY,
                                "Node %s:%s log manager is busy.", this.groupId, this.serverId);
            }

            // Parse request
            long index = prevLogIndex;
            //获取所有数据
            final List<LogEntry> entries = new ArrayList<>(entriesCount);
            ByteBuffer allData = null;
            if (request.hasData()) {
                allData = request.getData().asReadOnlyByteBuffer();
            }

            final List<RaftOutter.EntryMeta> entriesList = request.getEntriesList();
            for (int i = 0; i < entriesCount; i++) {
                index++;
                final RaftOutter.EntryMeta entry = entriesList.get(i);
                //给logEntry属性设值     将数据填充到logEntry
                final LogEntry logEntry = logEntryFromMeta(index, allData, entry);

                if (logEntry != null) {
                    // Validate checksum
                    if (this.raftOptions.isEnableLogEntryChecksum() && logEntry.isCorrupted()) {
                        long realChecksum = logEntry.checksum();
                        LOG.error(
                                "Corrupted log entry received from leader, index={}, term={}, expectedChecksum={}, realChecksum={}",
                                logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                realChecksum);
                        return RpcFactoryHelper //
                                .responseFactory() //
                                .newResponse(RpcRequests.AppendEntriesResponse.getDefaultInstance(), RaftError.EINVAL,
                                        "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d",
                                        logEntry.getId().getIndex(), logEntry.getId().getTerm(), logEntry.getChecksum(),
                                        realChecksum);
                    }
                    entries.add(logEntry);
                }
            }

            //存储日志，并回调返回response
            final FollowerStableClosure closure = new FollowerStableClosure(request, AppendEntriesResponse.newBuilder()
                    .setTerm(this.currTerm), this, done, this.currTerm);
            this.logManager.appendEntries(entries, closure);
            // update configuration after _log_manager updated its memory status
            checkAndSetConfiguration(true);
            success = true;
            return null;
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
            final long processLatency = Utils.monotonicMs() - startMs;
            if (entriesCount == 0) {
                this.metrics.recordLatency("handle-heartbeat-requests", processLatency);
            } else {
                this.metrics.recordLatency("handle-append-entries", processLatency);
            }
            if (success) {
                // Don't stats heartbeat requests.
                this.metrics.recordSize("handle-append-entries-count", entriesCount);
            }
        }
    }

    @Override
    public Message handleInstallSnapshot(RpcRequests.InstallSnapshotRequest request, RpcRequestClosure done) {
        // 如果快照安装执行器不存在，则抛出异常不支持快照操作
        if (this.snapshotExecutor == null) {
            return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RpcRequests.InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL, "Not supported snapshot");
        }
        // 根据请求携带的 serverId 序列化 PeerId
        final PeerId serverId = new PeerId();
        if (!serverId.parse(request.getServerId())) {
            LOG.warn("Node {} ignore InstallSnapshotRequest from {} bad server id.", getNodeId(), request.getServerId());
            return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(RpcRequests.InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                            "Parse serverId failed: %s", request.getServerId());
        }

        this.writeLock.lock();

        try{
            if (!this.state.isActive()) {
                LOG.warn("Node {} ignore InstallSnapshotRequest as it is not in active state {}.", getNodeId(),
                        this.state);
                return RpcFactoryHelper //
                        .responseFactory() //
                        .newResponse(RpcRequests.InstallSnapshotResponse.getDefaultInstance(), RaftError.EINVAL,
                                "Node %s:%s is not in active state, state %s.", this.groupId, this.serverId, this.state.name());
            }

            // 判断 request 携带的 term 比当前节点的 trem小，比较 term 的合法性
            if (request.getTerm() < this.currTerm) {
                LOG.warn("Node {} ignore stale InstallSnapshotRequest from {}, term={}, currTerm={}.", getNodeId(),
                        request.getPeerId(), request.getTerm(), this.currTerm);
                return RpcRequests.InstallSnapshotResponse.newBuilder() //
                        .setTerm(this.currTerm) //
                        .setSuccess(false) //
                        .build();
            }

            //当前节点如果不是Follower节点的话要执行StepDown操作
            checkStepDown(request.getTerm(), serverId);

            //这说明请求的节点不是当前节点的leader
            if (!serverId.equals(this.leaderId)) {
                LOG.error("Another peer {} declares that it is the leader at term {} which was occupied by leader {}.",
                        serverId, this.currTerm, this.leaderId);
                // Increase the term by 1 and make both leaders step down to minimize the
                // loss of split brain
                stepDown(request.getTerm() + 1, false, new Status(RaftError.ELEADERCONFLICT,
                        "More than one leader in the same term."));
                return RpcRequests.InstallSnapshotResponse.newBuilder() //
                        .setTerm(request.getTerm() + 1) //
                        .setSuccess(false) //
                        .build();
            }
        }finally {
            this.writeLock.unlock();
        }
        final long startMs = Utils.monotonicMs();

        try {
                if (LOG.isInfoEnabled()) {
                    LOG.info(
                            "Node {} received InstallSnapshotRequest from {}, lastIncludedLogIndex={}, lastIncludedLogTerm={}, lastLogId={}.",
                            getNodeId(), request.getServerId(), request.getMeta().getLastIncludedIndex(), request.getMeta()
                                    .getLastIncludedTerm(), this.logManager.getLastLogId(false));
                }

                // 执行快照安装
                this.snapshotExecutor.installSnapshot(request, RpcRequests.InstallSnapshotResponse.newBuilder(), done);
                return null;
        }finally {
            this.metrics.recordLatency("install-snapshot", Utils.monotonicMs() - startMs);
        }
    }

    @Override
    public Message handleTimeoutNowRequest(RpcRequests.TimeoutNowRequest request, RpcRequestClosure done) {
        return null;
    }

    @Override
    public void handleReadIndexRequest(RpcRequests.ReadIndexRequest request, RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {
        final long startMs = Utils.monotonicMs();
        this.readLock.lock();
        try {
            switch (this.state) {
                case STATE_LEADER:
                    readLeader(request, RpcRequests.ReadIndexResponse.newBuilder(), done);
                    break;
                case STATE_FOLLOWER:
                    readFollower(request, done);
                    break;
                case STATE_TRANSFERRING:
                    // 当前正在发生选主过程，无法满足线性读的要求
                    done.run(new Status(RaftError.EBUSY, "Is transferring leadership."));
                    break;
                default:
                    done.run(new Status(RaftError.EPERM, "Invalid state for readIndex: %s.", this.state));
                    break;
            }
        } finally {
            this.readLock.unlock();
            this.metrics.recordLatency("handle-read-index", Utils.monotonicMs() - startMs);
            this.metrics.recordSize("handle-read-index-entries", request.getEntriesCount());
        }
    }

    private void readLeader(final RpcRequests.ReadIndexRequest request, final RpcRequests.ReadIndexResponse.Builder respBuilder,
                            final RpcResponseClosure<RpcRequests.ReadIndexResponse> closure) {
        final int quorum = getQuorum();
        //检查当前 Raft 集群节点数量，如果集群只有一个 Peer 节点直接获取投票箱 BallotBox 最新提交索引 lastCommittedIndex
        // 即 Leader 节点当前 Log 的 commitIndex 构建 ReadIndexClosure 响应
        if (quorum <= 1) {
            // Only one peer, fast path.
            respBuilder.setSuccess(true) //
                    .setIndex(this.ballotBox.getLastCommittedIndex());
            closure.setResponse(respBuilder.build());
            closure.run(Status.OK());
            return;
        }

        final long lastCommittedIndex = this.ballotBox.getLastCommittedIndex();
        if (this.logManager.getTerm(lastCommittedIndex) != this.currTerm) {
            // Reject read only request when this leader has not committed any log entry at its term
            closure
                    .run(new Status(
                            RaftError.EAGAIN,
                            "ReadIndex request rejected because leader has not committed any log entry at its term, logIndex=%d, currTerm=%d.",
                            lastCommittedIndex, this.currTerm));
            return;
        }
        respBuilder.setIndex(lastCommittedIndex);

        // 如果是从 Follower 节点或者 Learner 节点发起的读请求，则需要判断他们在不在当前的Raft Conf里面
        if (request.getPeerId() != null) {
            // request from follower or learner, check if the follower/learner is in current conf.
            final PeerId peer = new PeerId();
            peer.parse(request.getServerId());
            if (!this.conf.contains(peer) && !this.conf.containsLearner(peer)) {
                closure.run(new Status(RaftError.EPERM, "Peer %s is not in current configuration: %s.", peer, this.conf));
                return;
            }
        }

        //默认 case ReadOnlySafe  时钟漂移的存在所以不用lease
        ReadOnlyOption readOnlyOpt = this.raftOptions.getReadOnlyOptions();

        switch (readOnlyOpt) {
            case ReadOnlySafe:
                final List<PeerId> peers = this.conf.getConf().getPeers();
                Requires.requireTrue(peers != null && !peers.isEmpty(), "Empty peers");
                final ReadIndexHeartbeatResponseClosure heartbeatDone = new ReadIndexHeartbeatResponseClosure(
                        respBuilder, closure, quorum, peers.size());
                // Send heartbeat requests to followers
                // 线性安全读的模式，需要向所有的成员发送心跳信息
                for (final PeerId peer : peers) {
                    if (peer.equals(this.serverId)) {
                        continue;
                    }
                    // 调用 Replicator#sendHeartbeat(rid, closure) 方法向 Followers 节点发送
                    // Heartbeat 心跳请求， 发送心跳成功执行 ReadIndexHeartbeatResponseClosure
                    // 心跳响应回调

                    //sendHeartbeat 后ballotBox会更新lastCommittedIndex
                    this.replicatorGroup.sendHeartbeat(peer, heartbeatDone);
                }
                break;
            case ReadOnlyLeaseBased:
                // Responses to followers and local node.
                respBuilder.setSuccess(true);
                closure.setResponse(respBuilder.build());
                closure.run(Status.OK());
                break;
        }
    }


    private void readFollower(final RpcRequests.ReadIndexRequest request, final RpcResponseClosure<RpcRequests.ReadIndexResponse> closure) {
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            closure.run(new Status(RaftError.EPERM, "No leader at term %d.", this.currTerm));
            return;
        }
        // send request to leader.
        final RpcRequests.ReadIndexRequest newRequest = RpcRequests.ReadIndexRequest.newBuilder()
                .mergeFrom(request)
                .setPeerId(this.leaderId.toString())
                .build();
        //向 Leader 发送 ReadIndex 请求，Leader 节点调用 readIndex(requestContext, done) 方法启动可线性化只读查询请求
        //closure 是 ReadIndexResponseClosure
        this.rpcService.readIndex(this.leaderId.getEndpoint(), newRequest, -1, closure);
    }


    private int getQuorum() {
        final Configuration c = this.conf.getConf();
        if (c.isEmpty()) {
            return 0;
        }
        return c.getPeers().size() / 2 + 1;
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
                        // 以counter的例子来说 done就是 CounterClosure
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
            this.logManager.appendEntries(entries, new LeaderStableClosure(entries, this));
            // update conf.first
            checkAndSetConfiguration(true);
        } finally {
            this.writeLock.unlock();
        }
    }

    // in writeLock
    private void checkStepDown(final long requestTerm, final PeerId serverId) {
        final Status status = new Status();
        if (requestTerm > this.currTerm) {
            status.setError(RaftError.ENEWLEADER, "Raft node receives message from new leader with higher term.");
            stepDown(requestTerm, false, status);
        } else if (this.state != State.STATE_FOLLOWER) {
            status.setError(RaftError.ENEWLEADER, "Candidate receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        } else if (this.leaderId.isEmpty()) {
            status.setError(RaftError.ENEWLEADER, "Follower receives message from new leader with the same term.");
            stepDown(requestTerm, false, status);
        }
        // save current leader
        if (this.leaderId == null || this.leaderId.isEmpty()) {
            resetLeaderId(serverId, status);
        }
    }

    private void stopVoteTimer() {
        if (this.voteTimer != null) {
            this.voteTimer.stop();
        }
    }

    public void unsafeApplyConfiguration(final Configuration newConf, final Configuration oldConf,
                                         final boolean leaderStart) {
        Requires.requireTrue(this.confCtx.isBusy(), "ConfigurationContext is not busy");
        final LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
        entry.setId(new LogId(0, this.currTerm));
        entry.setPeers(newConf.listPeers());
        entry.setLearners(newConf.listLearners());
        if (oldConf != null) {
            entry.setOldPeers(oldConf.listPeers());
            entry.setOldLearners(oldConf.listLearners());
        }
        final ConfigurationChangeDone configurationChangeDone = new ConfigurationChangeDone(this.currTerm, leaderStart, this);
        // Use the new_conf to deal the quorum of this very log
        if (!this.ballotBox.appendPendingTask(newConf, oldConf, configurationChangeDone)) {
            Utils.runClosureInThread(configurationChangeDone, new Status(RaftError.EINTERNAL, "Fail to append task."));
            return;
        }
        final List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        this.logManager.appendEntries(entries, new LeaderStableClosure(entries, this));
        checkAndSetConfiguration(false);
    }

    public void onConfigurationChangeDone(final long term) {
        this.writeLock.lock();
        try {
            if (term != this.currTerm || this.state.compareTo(State.STATE_TRANSFERRING) > 0) {
                LOG.warn("Node {} process onConfigurationChangeDone at term {} while state={}, currTerm={}.",
                        getNodeId(), term, this.state, this.currTerm);
                return;
            }
            this.confCtx.nextStage();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    @Override
    public void readIndex(byte[] requestContext, ReadIndexClosure done) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(done, new Status(RaftError.ENODESHUTDOWN, "Node is shutting down."));
            throw new IllegalStateException("Node is shutting down");
        }
        Requires.requireNonNull(done, "Null closure");
        this.readOnlyService.addRequest(requestContext, done);
    }


    // called when leader receive greater term in AppendEntriesResponse
    void increaseTermTo(final long newTerm, final Status status) {
        this.writeLock.lock();
        try {
            if (newTerm < this.currTerm) {
                return;
            }
            stepDown(newTerm, false, status);
        } finally {
            this.writeLock.unlock();
        }
    }


    /**
     * 如果当前的节点是个候选人（STATE_CANDIDATE），那么这个时候会让它暂时不要投票
     * 如果当前的节点状态是（STATE_TRANSFERRING）表示正在转交leader或是leader（STATE_LEADER），
     * 那么就需要把当前节点的stepDownTimer这个定时器给关闭
     * 如果当前是leader（STATE_LEADER），那么就需要告诉状态机leader下台了，可以在状态机中对下台的动作做处理
     * 重置当前节点的leader，把当前节点的state状态设置为Follower，重置confCtx上下文
     * 停止当前的快照生成，设置新的任期，让所有的复制节点停止工作
     * 启动electionTimer
     * 调用stopVoteTimer和stopStepDownTimer方法里面主要是调用相应的RepeatedTimer的stop方法，
     * 在stop方法里面会将stopped状态设置为ture，并将timeout设置为取消，并将这个timeout加入到cancelledTimeouts集合中去
     *
     * @param term
     * @param wakeupCandidate
     * @param status
     */
    // should be in writeLock
    private void stepDown(final long term, final boolean wakeupCandidate, final Status status) {
        LOG.debug("Node {} stepDown, term={}, newTerm={}, wakeupCandidate={}.", getNodeId(), this.currTerm, term,
                wakeupCandidate);
        if (!this.state.isActive()) {
            return;
        }
        if (this.state == State.STATE_CANDIDATE) {
            //如果是候选者，那么停止选举
            stopVoteTimer();
        } else if (this.state.compareTo(State.STATE_TRANSFERRING) <= 0) {
            //如果当前状态是leader或TRANSFERRING
            //让启动的stepDownTimer停止运作
            stopStepDownTimer();
            //清空选票箱中的内容
            this.ballotBox.clearPendingTasks();
            // signal fsm leader stop immediately
            if (this.state == State.STATE_LEADER) {
                //发送leader下台的事件给其他Follower
                onLeaderStop(status);
            }
        }
        // reset leader_id
        resetLeaderId(PeerId.emptyPeer(), status);

        // soft state in memory
        this.state = State.STATE_FOLLOWER;
        this.confCtx.reset();
        updateLastLeaderTimestamp(Utils.monotonicMs());
        if (this.snapshotExecutor != null) {
            this.snapshotExecutor.interruptDownloadingSnapshots(term);
        }

        //设置任期为大的那个
        // meta state
        if (term > this.currTerm) {
            this.currTerm = term;
            this.votedId = PeerId.emptyPeer();
            //重设元数据信息保存到文件中
            this.metaStorage.setTermAndVotedFor(term, this.votedId);
        }

        if (wakeupCandidate) {
            this.wakingCandidate = this.replicatorGroup.stopAllAndFindTheNextCandidate(this.conf);
            if (this.wakingCandidate != null) {
                Replicator.sendTimeoutNowAndStop(this.wakingCandidate, this.options.getElectionTimeoutMs());
            }
        } else {
            //把replicatorGroup里面的所有replicator标记为stop
            this.replicatorGroup.stopAll();
        }

        //leader转移的时候会用到
        if (this.stopTransferArg != null) {
            if (this.transferTimer != null) {
                this.transferTimer.cancel(true);
            }
            // There is at most one StopTransferTimer at the same term, it's safe to
            // mark stopTransferArg to NULL
            this.stopTransferArg = null;
        }
        // Learner node will not trigger the election timer.
        if (!isLearner()) {
            this.electionTimer.restart();
        } else {
            LOG.info("Node {} is a learner, election timer is not started.", this.nodeId);
        }
    }

    private boolean initMetaStorage() {
        this.metaStorage = this.serviceFactory.createRaftMetaStorage(this.options.getRaftMetaUri(), this.raftOptions);
        RaftMetaStorageOptions opts = new RaftMetaStorageOptions();
        opts.setNode(this);
        if (!this.metaStorage.init(opts)) {
            LOG.error("Node {} init meta storage failed, uri={}.", this.serverId, this.options.getRaftMetaUri());
            return false;
        }
        this.currTerm = this.metaStorage.getTerm();
        this.votedId = this.metaStorage.getVotedFor().copy();
        return true;
    }

    /**
     * 节点发生错误时
     *
     * @param error
     */
    public void onError(final RaftException error) {
        LOG.warn("Node {} got error: {}.", getNodeId(), error);
        if (this.fsmCaller != null) {
            // onError of fsmCaller is guaranteed to be executed once.
            this.fsmCaller.onError(error);
        }
        if (this.readOnlyService != null) {
            this.readOnlyService.setError(error);
        }
        this.writeLock.lock();
        try {
            // If it is leader, need to wake up a new one;
            // If it is follower, also step down to call on_stop_following.
            if (this.state.compareTo(State.STATE_FOLLOWER) <= 0) {
                stepDown(this.currTerm, this.state == State.STATE_LEADER, new Status(RaftError.EBADNODE,
                        "Raft node(leader or candidate) is in error."));
            }
            if (this.state.compareTo(State.STATE_ERROR) < 0) {
                this.state = State.STATE_ERROR;
            }
        } finally {
            this.writeLock.unlock();
        }
    }
}
