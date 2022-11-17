package com.open.raft.storage.snapshot.local;

import com.open.raft.storage.SnapshotStorage;
import com.open.raft.storage.snapshot.SnapshotReader;
import com.open.raft.storage.snapshot.SnapshotWriter;

/**
 * @Description TODO
 * @Date 2022/11/17 10:26
 * @Author jack wu
 */
public class LocalSnapshotStorage implements SnapshotStorage {
    @Override
    public boolean init(Void opts) {
        return false;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean setFilterBeforeCopyRemote() {
        return false;
    }

    @Override
    public SnapshotWriter create() {
        return null;
    }

    @Override
    public SnapshotReader open() {
        return null;
    }

    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        return null;
    }

    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        return null;
    }

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {

    }
}
