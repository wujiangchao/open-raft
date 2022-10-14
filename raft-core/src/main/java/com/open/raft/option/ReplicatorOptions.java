package com.open.raft.option;

import com.open.raft.core.BallotBox;
import com.open.raft.core.NodeImpl;
import com.open.raft.core.ReplicatorType;
import com.open.raft.core.Scheduler;
import com.open.raft.entity.PeerId;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.SnapshotStorage;
import com.open.raft.util.Copiable;

/**
 * @Description TODO
 * @Date 2022/10/11 9:26
 * @Author jack wu
 */
public class ReplicatorOptions implements Copiable<ReplicatorOptions> {

    private int dynamicHeartBeatTimeoutMs;
    private int electionTimeoutMs;
    private String groupId;
    private PeerId serverId;
    private PeerId peerId;
    private LogManager logManager;
    private BallotBox ballotBox;
    private NodeImpl node;
    private long term;
    private SnapshotStorage snapshotStorage;
    private RaftClientService raftRpcService;
    private Scheduler timerManager;
    private ReplicatorType replicatorType;

    public ReplicatorOptions() {
        super();
    }


    public int getDynamicHeartBeatTimeoutMs() {
        return this.dynamicHeartBeatTimeoutMs;
    }

    public void setDynamicHeartBeatTimeoutMs(final int dynamicHeartBeatTimeoutMs) {
        this.dynamicHeartBeatTimeoutMs = dynamicHeartBeatTimeoutMs;
    }

    @Override
    public ReplicatorOptions copy() {
        return null;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    //fellow or leaner
    public ReplicatorType getReplicatorType() {
        return replicatorType;
    }

    public void setReplicatorType(ReplicatorType replicatorType) {
        this.replicatorType = replicatorType;
    }

    public PeerId getPeerId() {
        return peerId;
    }

    public void setPeerId(PeerId peerId) {
        this.peerId = peerId;
    }

    public RaftClientService getRaftRpcService() {
        return raftRpcService;
    }

    public void setRaftRpcService(RaftClientService raftRpcService) {
        this.raftRpcService = raftRpcService;
    }

    public int getElectionTimeoutMs() {
        return electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public PeerId getServerId() {
        return serverId;
    }

    public void setServerId(PeerId serverId) {
        this.serverId = serverId;
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public BallotBox getBallotBox() {
        return ballotBox;
    }

    public void setBallotBox(BallotBox ballotBox) {
        this.ballotBox = ballotBox;
    }

    public NodeImpl getNode() {
        return node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public SnapshotStorage getSnapshotStorage() {
        return snapshotStorage;
    }

    public void setSnapshotStorage(SnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    public Scheduler getTimerManager() {
        return timerManager;
    }

    public void setTimerManager(Scheduler timerManager) {
        this.timerManager = timerManager;
    }
}
