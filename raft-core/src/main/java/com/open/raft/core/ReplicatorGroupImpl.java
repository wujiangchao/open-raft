package com.open.raft.core;

import com.open.raft.closure.CatchUpClosure;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReplicatorGroupOptions;
import com.open.raft.option.ReplicatorOptions;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.util.Requires;
import com.open.raft.util.ThreadId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Description TODO
 * @Date 2022/10/11 9:09
 * @Author jack wu
 */
public class ReplicatorGroupImpl implements ReplicatorGroup {

    private static final Logger LOG = LoggerFactory
            .getLogger(ReplicatorGroupImpl.class);

    // <peerId, replicatorId>
    private final ConcurrentMap<PeerId, ThreadId> replicatorMap = new ConcurrentHashMap<>();

    /**
     * common replicator options
     */
    private ReplicatorOptions commonOptions;
    private int dynamicTimeoutMs = -1;
    private int electionTimeoutMs = -1;
    private RaftOptions raftOptions;
    private final Map<PeerId, ReplicatorType> failureReplicators = new ConcurrentHashMap<>();

    @Override
    public boolean init(NodeId nodeId, ReplicatorGroupOptions opts) {
        // 值为 Math.max(electionTimeout / this.raftOptions.getElectionHeartbeatFactor(), 10);
        this.dynamicTimeoutMs = opts.getHeartbeatTimeoutMs();

        this.electionTimeoutMs = opts.getElectionTimeoutMs();

        this.raftOptions = opts.getRaftOptions();
        this.commonOptions = new ReplicatorOptions();
        this.commonOptions.setDynamicHeartBeatTimeoutMs(this.dynamicTimeoutMs);
        this.commonOptions.setElectionTimeoutMs(this.electionTimeoutMs);
        this.commonOptions.setRaftRpcService(opts.getRaftRpcClientService());
        this.commonOptions.setLogManager(opts.getLogManager());
        this.commonOptions.setBallotBox(opts.getBallotBox());
        this.commonOptions.setNode(opts.getNode());
        // term 从零开始
        this.commonOptions.setTerm(0);
        this.commonOptions.setGroupId(nodeId.getGroupId());
        this.commonOptions.setServerId(nodeId.getPeerId());
        this.commonOptions.setSnapshotStorage(opts.getSnapshotStorage());
        this.commonOptions.setTimerManager(opts.getTimerManager());
        return true;
    }

    /**
     * addReplicator里面主要是做了两件事：1. 将要加入的节点从failureReplicators集合里移除；
     * 2. 将要加入的节点放入到replicatorMap集合中去。
     *
     * @param peer           target peer
     * @param replicatorType replicator type
     * @param sync           synchronous
     * @return
     */
    @Override
    public boolean addReplicator(PeerId peer, ReplicatorType replicatorType, boolean sync) {
        Requires.requireTrue(this.commonOptions.getTerm() != 0);
        this.failureReplicators.remove(peer);
        if (this.replicatorMap.containsKey(peer)) {
            return true;
        }
        //赋值一个新的ReplicatorOptions
        final ReplicatorOptions opts = this.commonOptions == null ? new ReplicatorOptions() : this.commonOptions.copy();
        opts.setReplicatorType(replicatorType);
        //新的ReplicatorOptions添加这个PeerId
        opts.setPeerId(peer);
        if (!sync) {
            final RaftClientService client = opts.getRaftRpcService();
            if (client != null && !client.checkConnection(peer.getEndpoint(), true)) {
                LOG.error("Fail to check replicator connection to peer={}, replicatorType={}.", peer, replicatorType);
                this.failureReplicators.put(peer, replicatorType);
                return false;
            }
        }
        final ThreadId rid = Replicator.start(opts, this.raftOptions);
        //将返回的ThreadId 加入到replicatorMap，失败加入到failureReplicator
        if (rid == null) {
            LOG.error("Fail to start replicator to peer={}, replicatorType={}.", peer, replicatorType);
            this.failureReplicators.put(peer, replicatorType);
            return false;
        }
        return this.replicatorMap.put(peer, rid) == null;
    }

    @Override
    public void sendHeartbeat(PeerId peer, RpcResponseClosure<RpcRequests.AppendEntriesResponse> closure) {

    }

    @Override
    public ThreadId getReplicator(PeerId peer) {
        return null;
    }

    @Override
    public void checkReplicator(PeerId peer, boolean lockNode) {

    }

    @Override
    public boolean resetTerm(long newTerm) {
        if (newTerm <= this.commonOptions.getTerm()) {
            return false;
        }
        this.commonOptions.setTerm(newTerm);
        return true;
    }

    @Override
    public boolean waitCaughtUp(PeerId peer, long maxMargin, long dueTime, CatchUpClosure done) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return false;
        }

        Replicator.waitForCaughtUp(rid, maxMargin, dueTime, done);
        return true;
    }

    @Override
    public long getLastRpcSendTimestamp(PeerId peer) {
        final ThreadId rid = this.replicatorMap.get(peer);
        if (rid == null) {
            return 0L;
        }
        return Replicator.getLastRpcSendTimestamp(rid);
    }
}
