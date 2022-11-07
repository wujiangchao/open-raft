package com.open.raft.storage.snapshot;

import com.open.raft.Closure;
import com.open.raft.core.NodeImpl;
import com.open.raft.option.SnapshotExecutorOptions;
import com.open.raft.rpc.RpcRequestClosure;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.storage.SnapshotStorage;

/**
 * @Description TODO
 * @Date 2022/11/7 22:41
 * @Author jack wu
 */
public class SnapshotExecutorImpl implements SnapshotExecutor{
    @Override
    public NodeImpl getNode() {
        return null;
    }

    @Override
    public void doSnapshot(Closure done) {

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
