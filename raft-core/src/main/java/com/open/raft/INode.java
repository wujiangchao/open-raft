package com.open.raft;

import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.entity.Task;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RaftOptions;

/**
 * @Description define raft node
 * @Date 2022/9/22 17:57
 * @Author jack wu
 */
public interface INode extends Lifecycle<NodeOptions> {

    /**
     * Get the leader peer id for redirect, null if absent.
     */
    PeerId getLeaderId();

    /**
     * Get current node id.
     */
    NodeId getNodeId();

    /**
     * Get the raft group id.
     */
    String getGroupId();

    /**
     * [Thread-safe and wait-free]
     *
     * Apply task to the replicated-state-machine
     *
     * About the ownership:
     * |task.data|: for the performance consideration, we will take away the
     *               content. If you want keep the content, copy it before call
     *               this function
     * |task.done|: If the data is successfully committed to the raft group. We
     *              will pass the ownership to #{@link StateMachine#onApply(Iterator)}.
     *              Otherwise we will specify the error and call it.
     *
     * @param task task to apply
     */
    void apply(final Task task);

    /**
     * Get the node options.
     */
    NodeOptions getOptions();

    /**
     * Get the raft options
     */
    RaftOptions getRaftOptions();

    /**
     * [Thread-safe and wait-free]
     *
     * Starts a linearizable read-only query request with request context(optional,
     * such as request id etc.) and closure.  The closure will be called when the
     * request is completed, and user can read data from state machine if the result
     * status is OK.
     *
     * @param requestContext the context of request
     * @param done           callback
     *
     * @since 0.0.3
     */
    void readIndex(final byte[] requestContext, final ReadIndexClosure done);


}
