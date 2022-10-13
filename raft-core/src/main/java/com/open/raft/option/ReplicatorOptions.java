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
}
