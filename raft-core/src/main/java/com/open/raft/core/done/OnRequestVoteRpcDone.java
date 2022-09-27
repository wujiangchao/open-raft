package com.open.raft.core.done;

import com.open.raft.Status;
import com.open.raft.core.NodeImpl;
import com.open.raft.entity.PeerId;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.util.Utils;

/**
 * @Description TODO
 * @Date 2022/9/27 17:21
 * @Author jack wu
 */
public class OnRequestVoteRpcDone extends RpcResponseClosureAdapter<RpcRequests.RequestVoteResponse> {
    final long startMs;
    final PeerId peer;
    final long term;
    final NodeImpl node;
    public RpcRequests.RequestVoteRequest request;

    public OnRequestVoteRpcDone(final PeerId peer, final long term, final NodeImpl node) {
        super();
        this.startMs = Utils.monotonicMs();
        this.peer = peer;
        this.term = term;
        this.node = node;
    }

    @Override
    public void run(final Status status) {
        NodeImpl.this.metrics.recordLatency("request-vote", Utils.monotonicMs() - this.startMs);
        if (!status.isOk()) {
            LOG.warn("Node {} RequestVote to {} error: {}.", this.node.getNodeId(), this.peer, status);
        } else {
            this.node.handleRequestVoteResponse(this.peer, this.term, getResponse());
        }
    }
}
