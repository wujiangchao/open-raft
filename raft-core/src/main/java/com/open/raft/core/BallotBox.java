package com.open.raft.core;

import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.Lifecycle;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.conf.Configuration;
import com.open.raft.entity.Ballot;
import com.open.raft.entity.PeerId;
import com.open.raft.option.BallotBoxOptions;
import com.open.raft.util.SegmentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.StampedLock;

/**
 * @Description 投票箱
 * @Date 2022/9/28 7:47
 * @Author jack wu
 */
public class BallotBox implements Lifecycle<BallotBoxOptions> {
    private static final Logger LOG = LoggerFactory.getLogger(BallotBox.class);

    private long lastCommittedIndex = 0;
    /**
     * pendingIndex为上一次阻塞的偏移。他为lastCommittedIndex + 1。没有真正commit的ballot都会在pendingMetaQueue中存在，
     * 每次响应成功都会调用bl.grant方法。最后根据bl.isGranted结果断定是否更新lastCommittedIndex
     */
    private long pendingIndex;

    /**
     * 不可重入 的读写锁
     */
    private StampedLock stampedLock = new StampedLock();
    private FSMCaller waiter;
    /**
     * 存放的是task.done
     */
    private ClosureQueue closureQueue;
    private final SegmentList<Ballot> pendingMetaQueue = new SegmentList<>(false);


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

    /**
     * Called by leader, otherwise the behavior is undefined
     * Set logs in [first_log_index, last_log_index] are stable at |peer|.
     * <p>
     * 这里可以看到，就是每个节点写入成功后，调用Leader节点的.ballotBox.commitAt，
     * 更新对应写入数据的投票信息，如果bl.isGranted，即完成了过半节点的写入，那么会调用this.waiter.onCommitted逻辑，
     * 这里最终会调用到StateMachineAdapter.onApply方法。
     */
    public boolean commitAt(final long firstLogIndex, final long lastLogIndex, final PeerId peer) {
        // TODO  use lock-free algorithm here?
        final long stamp = this.stampedLock.writeLock();
        long lastCommittedIndex = 0;
        try {
            if (this.pendingIndex == 0) {
                return false;
            }
            if (lastLogIndex < this.pendingIndex) {
                return true;
            }

            if (lastLogIndex >= this.pendingIndex + this.pendingMetaQueue.size()) {
                throw new ArrayIndexOutOfBoundsException();
            }

            final long startAt = Math.max(this.pendingIndex, firstLogIndex);
            Ballot.PosHint hint = new Ballot.PosHint();
            for (long logIndex = startAt; logIndex <= lastLogIndex; logIndex++) {
                final Ballot bl = this.pendingMetaQueue.get((int) (logIndex - this.pendingIndex));
                hint = bl.grant(peer, hint);
                if (bl.isGranted()) {
                    lastCommittedIndex = logIndex;
                }
            }
            if (lastCommittedIndex == 0) {
                return true;
            }
            // When removing a peer off the raft group which contains even number of
            // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
            // this case, the log after removal may be committed before some previous
            // logs, since we use the new configuration to deal the quorum of the
            // removal request, we think it's safe to commit all the uncommitted
            // previous logs, which is not well proved right now
            this.pendingMetaQueue.removeFromFirst((int) (lastCommittedIndex - this.pendingIndex) + 1);
            LOG.debug("Committed log fromIndex={}, toIndex={}.", this.pendingIndex, lastCommittedIndex);
            this.pendingIndex = lastCommittedIndex + 1;
            this.lastCommittedIndex = lastCommittedIndex;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
        //最后调用this.waiter.onCommitted执行状态机提交操作。
        this.waiter.onCommitted(lastCommittedIndex);
        return true;
    }


    /**
     * Called when a candidate becomes the new leader, otherwise the behavior is
     * undefined.
     * According the the raft algorithm, the logs from previous terms can't be
     * committed until a log at the new term becomes committed, so
     * |newPendingIndex| should be |last_log_index| + 1.
     * <p>
     * 在新的任期不能提交上个任期没有提交的日志
     * https://zhuanlan.zhihu.com/p/517969401
     * https://www.modb.pro/db/150555
     *
     * @param newPendingIndex pending index of new leader
     * @return returns true if reset success
     */
    public boolean resetPendingIndex(final long newPendingIndex) {
        final long stamp = this.stampedLock.writeLock();
        try {
            //初始化情况下 pendingIndex = 0  pendingMetaQueue is Empty
            if (!(this.pendingIndex == 0 && this.pendingMetaQueue.isEmpty())) {
                LOG.error("resetPendingIndex fail, pendingIndex={}, pendingMetaQueueSize={}.", this.pendingIndex,
                        this.pendingMetaQueue.size());
                return false;
            }
            //首次选举  newPendingIndex =1  lastCommittedIndex=0
            if (newPendingIndex <= this.lastCommittedIndex) {
                LOG.error("resetPendingIndex fail, newPendingIndex={}, lastCommittedIndex={}.", newPendingIndex,
                        this.lastCommittedIndex);
                return false;
            }
            this.pendingIndex = newPendingIndex;
            this.closureQueue.resetFirstIndex(newPendingIndex);
            return true;
        } finally {
            this.stampedLock.unlockWrite(stamp);
        }
    }


    public long getLastCommittedIndex() {
        return lastCommittedIndex;
    }

    public void setLastCommittedIndex(long lastCommittedIndex) {
        this.lastCommittedIndex = lastCommittedIndex;
    }
}
