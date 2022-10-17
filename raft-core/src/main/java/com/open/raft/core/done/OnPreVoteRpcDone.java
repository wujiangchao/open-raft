package com.open.raft.core.done;

import com.open.raft.Status;
import com.open.raft.core.NodeImpl;
import com.open.raft.entity.PeerId;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosureAdapter;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/9/27 14:48
 * @Author jack wu
 */
public class OnPreVoteRpcDone extends RpcResponseClosureAdapter<RpcRequests.RequestVoteResponse> {

    private static final Logger LOG = LoggerFactory.getLogger(OnPreVoteRpcDone.class);


    final long startMs;
    final PeerId peer;
    final NodeImpl node;
    final long term;
    public RpcRequests.RequestVoteRequest request;

    public OnPreVoteRpcDone(final PeerId peer, final NodeImpl node, long term) {
        super();
        this.term = term;
        this.startMs = Utils.monotonicMs();
        this.peer = peer;
        this.node = node;
    }

    @Override
    public void run(Status status) {
        node.metrics.recordLatency("pre-vote", Utils.monotonicMs() - this.startMs);
        if (!status.isOk()) {
            LOG.warn("Node {} PreVote to {} error: {}.", node.getNodeId(), this.peer, status);
        } else {
            // 在这个方法中如果收到正常的响应，那么会调用handlePreVoteResponse方法处理响应
            node.handlePreVoteResponse(this.peer, term, getResponse());
        }
    }
}
