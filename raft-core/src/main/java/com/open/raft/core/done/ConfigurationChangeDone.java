package com.open.raft.core.done;

import com.open.raft.Closure;
import com.open.raft.Status;
import com.open.raft.core.ConfigurationCtx;
import com.open.raft.core.NodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/10/17 9:32
 * @Author jack wu
 */
public class ConfigurationChangeDone implements Closure {

    private static final Logger LOG = LoggerFactory
            .getLogger(ConfigurationCtx.class);

    private final long term;
    private final boolean leaderStart;
    private final NodeImpl node;

    public ConfigurationChangeDone(final long term, final boolean leaderStart, NodeImpl node) {
        super();
        this.term = term;
        this.leaderStart = leaderStart;
        this.node = node;
    }

    @Override
    public void run(final Status status) {
        if (status.isOk()) {
            node.onConfigurationChangeDone(this.term);
            if (this.leaderStart) {
                node.getOptions().getFsm().onLeaderStart(this.term);
            }
        } else {
            LOG.error("Fail to run ConfigurationChangeDone, status: {}.", status);
        }
    }
}
