
package com.open.raft.option;

import com.open.raft.FSMCaller;
import com.open.raft.core.NodeImpl;

/**
 * Read-Only service options.
 *
 */
public class ReadOnlyServiceOptions {

    private RaftOptions raftOptions;
    private NodeImpl node;
    private FSMCaller fsmCaller;

    public NodeImpl getNode() {
        return node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public FSMCaller getFsmCaller() {
        return fsmCaller;
    }

    public void setFsmCaller(FSMCaller fsm) {
        this.fsmCaller = fsm;
    }
}
