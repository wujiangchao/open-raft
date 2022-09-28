package com.open.raft.core;

import com.open.raft.FSMCaller;
import com.open.raft.Lifecycle;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.option.BallotBoxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description TODO
 * @Date 2022/9/28 7:47
 * @Author jack wu
 */
public class BallotBox implements Lifecycle<BallotBoxOptions> {
    private static final Logger LOG                = LoggerFactory.getLogger(BallotBox.class);

    private FSMCaller waiter;
    private ClosureQueue closureQueue;
    @Override
    public boolean init(BallotBoxOptions opts) {
        if (opts.getWaiter() == null || opts.getClosureQueue() == null) {
            LOG.error("waiter or closure queue is null.");
            return false;
        }
        this.waiter = opts.getWaiter();
        this.closureQueue = opts.getClosureQueue();
        return true;
    }

    @Override
    public void shutdown() {

    }
}
