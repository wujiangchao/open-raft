package com.open.raft.storage.snapshot;

import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.Status;
import com.open.raft.core.NodeImpl;
import com.open.raft.error.RaftError;
import com.open.raft.option.SnapshotExecutorOptions;
import com.open.raft.rpc.RpcRequestClosure;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.SnapshotStorage;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description TODO
 * @Date 2022/11/7 22:41
 * @Author jack wu
 */
public class SnapshotExecutorImpl implements SnapshotExecutor {
    private static final Logger LOG = LoggerFactory
            .getLogger(SnapshotExecutorImpl.class);

    private final Lock lock = new ReentrantLock();

    private long lastSnapshotTerm;
    private long lastSnapshotIndex;
    private long term;
    private volatile boolean savingSnapshot;
    private volatile boolean loadingSnapshot;
    private volatile boolean stopped;

    private FSMCaller fsmCaller;
    private NodeImpl node;
    private LogManager logManager;
    private final AtomicReference<DownloadingSnapshot> downloadingSnapshot = new AtomicReference<>(null);

    @Override
    public NodeImpl getNode() {
        return null;
    }

    @Override
    public void doSnapshot(Closure done) {
        boolean doUnlock = true;
        this.lock.lock();
        try {
            if (this.stopped) {
                Utils.runClosureInThread(done, new Status(RaftError.EPERM, "Is stopped."));
                return;
            }
            if (this.downloadingSnapshot.get() != null) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is loading another snapshot."));
                return;
            }

            if (this.savingSnapshot) {
                Utils.runClosureInThread(done, new Status(RaftError.EBUSY, "Is saving another snapshot."));
                return;
            }

            //当前业务状态机已经提交的 Index 索引是否等于 Snapshot 最后保存的日志 Index 索引
            //如果两个值相等则表示，业务数据没有新增，无需再生成一次没有意义的 Snapshot
            if (this.fsmCaller.getLastAppliedIndex() == this.lastSnapshotIndex) {
                // There might be false positive as the getLastAppliedIndex() is being
                // updated. But it's fine since we will do next snapshot saving in a
                // predictable time.
                doUnlock = false;
                this.lock.unlock();
                this.logManager.clearBufferedLogs();
                Utils.runClosureInThread(done);
                return;
            }

            //snapshotLogIndexMargin default 0
            final long distance = this.fsmCaller.getLastAppliedIndex() - this.lastSnapshotIndex;
            if (distance < this.node.getOptions().getSnapshotLogIndexMargin()) {
                // If state machine's lastAppliedIndex value minus lastSnapshotIndex value is
                // less than snapshotLogIndexMargin value, then directly return.
                if (this.node != null) {
                    LOG.debug(
                            "Node {} snapshotLogIndexMargin={}, distance={}, so ignore this time of snapshot by snapshotLogIndexMargin setting.",
                            this.node.getNodeId(), distance, this.node.getOptions().getSnapshotLogIndexMargin());
                }
                doUnlock = false;
                this.lock.unlock();
                Utils.runClosureInThread(done);
                return;
            }

            //创建一个快照存储器，用来写数据
            final SnapshotWriter writer = this.snapshotStorage.create();
            if (writer == null) {
                Utils.runClosureInThread(done, new Status(RaftError.EIO, "Fail to create writer."));
                reportError(RaftError.EIO.getNumber(), "Fail to create snapshot writer.");
                return;
            }
            this.savingSnapshot = true;
            //封装了回调方法和快照存储器
            final SaveSnapshotDone saveSnapshotDone = new SaveSnapshotDone(writer, done, null);
            //交给状态机来保存快照
            if (!this.fsmCaller.onSnapshotSave(saveSnapshotDone)) {
                Utils.runClosureInThread(done, new Status(RaftError.EHOSTDOWN, "The raft node is down."));
                return;
            }
            this.runningJobs.incrementAndGet();
        } finally {
            if (doUnlock) {
                this.lock.unlock();
            }
        }


    }

    @Override
    public void installSnapshot(RpcRequests.InstallSnapshotRequest request, RpcRequests.InstallSnapshotResponse.Builder response, RpcRequestClosure done) {

    }

    @Override
    public void interruptDownloadingSnapshots(long newTerm) {

    }

    @Override
    public boolean isInstallingSnapshot() {
        return false;
    }

    @Override
    public SnapshotStorage getSnapshotStorage() {
        return null;
    }

    @Override
    public void join() throws InterruptedException {

    }

    @Override
    public boolean init(SnapshotExecutorOptions opts) {
        return false;
    }

    @Override
    public void shutdown() {

    }
}
