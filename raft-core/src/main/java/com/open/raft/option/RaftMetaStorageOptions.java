
package com.open.raft.option;

import com.open.raft.core.NodeImpl;

/**
 * Raft meta storage options
 * @author dennis
 *
 */
public class RaftMetaStorageOptions {
    private NodeImpl node;

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }
}
