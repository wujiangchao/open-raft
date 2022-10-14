package com.open.raft.option;

import com.open.raft.core.BallotBox;
import com.open.raft.core.NodeImpl;
import com.open.raft.core.Scheduler;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.SnapshotStorage;

/**
 * @Description TODO
 * @Date 2022/10/11 9:14
 * @Author jack wu
 */
public class ReplicatorGroupOptions {
    private int               heartbeatTimeoutMs;
    private int               electionTimeoutMs;
    private LogManager logManager;
    private BallotBox         ballotBox;
    //当前结点
    private NodeImpl node;
    private SnapshotStorage   snapshotStorage;
    private RaftClientService raftRpcClientService;
    private RaftOptions       raftOptions;
    private Scheduler timerManager;

    public Scheduler getTimerManager() {
        return this.timerManager;
    }

    public void setTimerManager(Scheduler timerManager) {
        this.timerManager = timerManager;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public RaftClientService getRaftRpcClientService() {
        return this.raftRpcClientService;
    }

    public void setRaftRpcClientService(RaftClientService raftRpcService) {
        this.raftRpcClientService = raftRpcService;
    }

    public int getHeartbeatTimeoutMs() {
        return this.heartbeatTimeoutMs;
    }

    public void setHeartbeatTimeoutMs(int heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public int getElectionTimeoutMs() {
        return this.electionTimeoutMs;
    }

    public void setElectionTimeoutMs(int electionTimeoutMs) {
        this.electionTimeoutMs = electionTimeoutMs;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public BallotBox getBallotBox() {
        return this.ballotBox;
    }

    public void setBallotBox(BallotBox ballotBox) {
        this.ballotBox = ballotBox;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public SnapshotStorage getSnapshotStorage() {
        return this.snapshotStorage;
    }

    public void setSnapshotStorage(SnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public String toString() {
        return "ReplicatorGroupOptions{" + "heartbeatTimeoutMs=" + heartbeatTimeoutMs + ", electionTimeoutMs="
                + electionTimeoutMs + ", logManager=" + logManager + ", ballotBox=" + ballotBox + ", node=" + node
                + ", snapshotStorage=" + snapshotStorage + ", raftRpcClientService=" + raftRpcClientService
                + ", raftOptions=" + raftOptions + ", timerManager=" + timerManager + '}';
    }
}
