package com.open.raft.core;

import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.option.ReplicatorGroupOptions;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.util.ThreadId;

/**
 * @Description 用于单个 raft group 管理所有的 replicator，必要的权限检查和派发
 * @Date 2022/9/27 16:36
 * @Author jack wu
 */
public interface ReplicatorGroup {

    /**
     * Init the replicator group.
     *
     * @param nodeId node id
     * @param opts   options of replicator group
     * @return true if init success
     */
    boolean init(final NodeId nodeId, final ReplicatorGroupOptions opts);

    /**
     * Adds a replicator for follower({@link ReplicatorType#Follower}).
     * @see #addReplicator(PeerId, ReplicatorType)
     *
     * @param peer target peer
     * @return true on success
     */
    default boolean addReplicator(final PeerId peer) {
        return addReplicator(peer, ReplicatorType.Follower);
    }

    /**
     * Add a replicator attached with |peer|
     * will be a notification when the replicator catches up according to the
     * arguments.
     * NOTE: when calling this function, the replicators starts to work
     * immediately, and might call Node#stepDown which might have race with
     * the caller, you should deal with this situation.
     *
     * @param peer           target peer
     * @param replicatorType replicator type
     * @return true on success
     */
    default boolean addReplicator(final PeerId peer, ReplicatorType replicatorType) {
        return addReplicator(peer, replicatorType, true);
    }

    /**
     * Try to add a replicator attached with |peer|
     * will be a notification when the replicator catches up according to the
     * arguments.
     * NOTE: when calling this function, the replicators starts to work
     * immediately, and might call Node#stepDown which might have race with
     * the caller, you should deal with this situation.
     *
     * @param peer           target peer
     * @param replicatorType replicator type
     * @param sync           synchronous
     * @return true on success
     */
    boolean addReplicator(final PeerId peer, ReplicatorType replicatorType, boolean sync);

    /**
     * Send heartbeat to a peer.
     *
     * @param peer    target peer
     * @param closure callback
     */
    void sendHeartbeat(final PeerId peer, final RpcResponseClosure<RpcRequests.AppendEntriesResponse> closure);

    /**
     * Get replicator id by peer, null if not found.
     *
     * @param peer peer of replicator
     * @return the replicator id
     */
    ThreadId getReplicator(final PeerId peer);

    /**
     * Check replicator state, if it's not started, start it;
     * if it is blocked, unblock it. It should be called by leader.
     *
     * @param peer     peer of replicator
     * @param lockNode if lock with node
     */
    void checkReplicator(final PeerId peer, final boolean lockNode);

}
