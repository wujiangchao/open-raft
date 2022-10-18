package com.open.raft.closure;

import com.open.raft.Status;
import com.open.raft.core.NodeImpl;
import com.open.raft.core.ReplicatorGroupImpl;
import com.open.raft.entity.LogEntry;
import com.open.raft.storage.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @Description TODO
 * @Date 2022/10/17 9:50
 * @Author jack wu
 */
public class LeaderStableClosure extends LogManager.StableClosure {

    private static final Logger LOG = LoggerFactory
            .getLogger(LeaderStableClosure.class);

    NodeImpl node;

    public LeaderStableClosure(final List<LogEntry> entries, NodeImpl node) {
        super(entries);
        this.node = node;
    }

    @Override
    public void run(final Status status) {
        if (status.isOk()) {
            node.ballotBox.commitAt(this.firstLogIndex, this.firstLogIndex + this.nEntries - 1,
                    node.serverId);
        } else {
            LOG.error("Node {} append [{}, {}] failed, status={}.", node.getNodeId(), this.firstLogIndex,
                    this.firstLogIndex + this.nEntries - 1, status);
        }
    }
}
