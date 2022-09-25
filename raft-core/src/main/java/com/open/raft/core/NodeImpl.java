package com.open.raft.core;

import com.open.raft.INode;
import com.open.raft.RaftServiceFactory;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RaftOptions;
import com.open.raft.util.Requires;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        return false;
    }


    /**
     * the handler of electionTimeout,called by electionTimeoutTimer
     * when elctionTimeOut
     */
    private void handleElectionTimeout() {

    }

    private void preVote() {

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

}
