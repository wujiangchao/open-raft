package com.open.raft.core;

import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.Lifecycle;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.conf.Configuration;
import com.open.raft.entity.Ballot;
import com.open.raft.option.BallotBoxOptions;
import com.open.raft.util.SegmentList;
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
    private final SegmentList<Ballot> pendingMetaQueue   = new SegmentList<>(false);


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
        // 每个写入Task都生成一个Ballot ，并放入pendingMetaQueue，
        // 还记得上面选主投票的时候，用的也是这个来标记选举是否过半，这里也是一样，
        // 后续其他节点复制Leader该Task的数据的时候，会对应更新Leader中对应该Task的Ballot 投票信息，
        // 通过该Ballot 能够判断集群是否有过半节点已经完成了该Task的写入。同时也将Task写入集群过半节点成功之后的回调入口Closure
        // 保存在closureQueue中，当其他节点写入Task成功更新对应Task的Ballot 的时候，会判断是否过半节点写入成功，
        // 如果成功则会回调对应Task的Closure的run方法。
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
