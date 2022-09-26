package com.open.raft.core;

import com.open.raft.FSMCaller;
import com.open.raft.INode;
import com.open.raft.RaftServiceFactory;
import com.open.raft.Status;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.closure.ClosureQueueImpl;
import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.entity.LogId;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.error.RaftError;
import com.open.raft.option.FSMCallerOptions;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RaftOptions;
import com.open.raft.util.Requires;
import com.open.raft.util.Utils;
import com.open.raft.util.concurrent.NodeReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @Description 表示一个 raft 节点，可以提交 task，以及查询 raft group 信息，
 * 比如当前状态、当前 leader/term 等。
 * @Date 2022/9/22 18:51
 * @Author jack wu
 */
public class NodeImpl implements INode {

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


    private FSMCaller fsmCaller;
    private ClosureQueue closureQueue;


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
//        final int num = GLOBAL_NUM_NODES.incrementAndGet();
//        LOG.info("The number of active nodes increment to {}.", num);
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
        return false;
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

    private void preVote() {

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
}
