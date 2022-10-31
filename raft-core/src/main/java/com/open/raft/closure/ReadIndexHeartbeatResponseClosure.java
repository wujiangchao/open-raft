package com.open.raft.closure;

import com.open.raft.Status;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.rpc.RpcResponseClosureAdapter;

/**
 * @Description TODO
 * @Date 2022/10/31 10:32
 * @Author jack wu
 */
public class ReadIndexHeartbeatResponseClosure extends RpcResponseClosureAdapter<RpcRequests.AppendEntriesResponse> {
    final RpcRequests.ReadIndexResponse.Builder respBuilder;
    final RpcResponseClosure<RpcRequests.ReadIndexResponse> closure;
    final int quorum;
    final int failPeersThreshold;
    int ackSuccess;
    int ackFailures;
    boolean isDone;

    public ReadIndexHeartbeatResponseClosure(RpcRequests.ReadIndexResponse.Builder rb,
                                             RpcResponseClosure<RpcRequests.ReadIndexResponse> closure, int quorum,
                                             final int peersCount) {
        super();
        this.closure = closure;
        this.respBuilder = rb;
        this.quorum = quorum;
        this.failPeersThreshold = peersCount % 2 == 0 ? (quorum - 1) : quorum;
        this.ackSuccess = 0;
        this.ackFailures = 0;
        this.isDone = false;
    }

    @Override
    public synchronized void run(Status status) {
        if (this.isDone) {
            return;
        }
        if (status.isOk() && getResponse().getSuccess()) {
            this.ackSuccess++;
        } else {
            this.ackFailures++;
        }
        // Include leader self vote yes.
        if (this.ackSuccess + 1 >= this.quorum) {
            this.respBuilder.setSuccess(true);
            this.closure.setResponse(this.respBuilder.build());
            this.closure.run(Status.OK());
            this.isDone = true;
        } else if (this.ackFailures >= this.failPeersThreshold) {
            this.respBuilder.setSuccess(false);
            this.closure.setResponse(this.respBuilder.build());
            this.closure.run(Status.OK());
            this.isDone = true;
        }
    }
}
