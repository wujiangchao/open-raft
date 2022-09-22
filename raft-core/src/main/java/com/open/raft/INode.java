package com.open.raft;

import com.open.raft.entity.NodeId;
import com.open.raft.entity.PeerId;
import com.open.raft.option.NodeOptions;

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

}
