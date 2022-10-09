package com.open.raft.core;

import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.Lifecycle;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.entity.Ballot;
import com.open.raft.option.BallotBoxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Description 投票箱
 * @Date 2022/9/28 7:47
 * @Author jack wu
 */
public class BallotBox implements Lifecycle<BallotBoxOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(BallotBox.class);

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

    /**
     * Called by leader, otherwise the behavior is undefined
     * Store application context before replication.
     *
     * @param conf    current configuration
     * @param oldConf old configuration
     * @param done    callback
     * @return returns true on success
     */
    public boolean appendPendingTask(final Configuration conf, final Configuration oldConf, final Closure done) {
        // 每个写入Task都生成一个Ballot ，并放入pendingMetaQueue，后续其他
        final Ballot bl = new Ballot();
        if (!bl.init(conf, oldConf)) {
            LOG.error("Fail to init ballot.");
            return false;
        }
        final long stamp = this.stampedLock.writeLock();
        try {
            if (this.pendingIndex <= 0) {
                LOG.error("Fail to appendingTask, pendingIndex={}.", this.pendingIndex);
                return false;
            }
            this.pendingMetaQueue.add(bl);
            this.closureQueue.appendPendingClosure(done);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }


    @Override
    public void shutdown() {

    }
}
