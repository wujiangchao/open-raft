package com.open.raft.storage.snapshot;

import com.open.raft.Status;

/**
 * @Description TODO
 * @Date 2022/11/21 11:06
 * @Author jack wu
 */
public abstract  class SnapshotCopier extends Status implements Cloneable {
    /**
     * Cancel the copy job.
     */
    public abstract void cancel();

    /**
     * Block the thread until this copy job finishes, or some error occurs.
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    public abstract void join() throws InterruptedException;

    /**
     * Start the copy job.
     */
    public abstract void start();

    /**
     * Get the the SnapshotReader which represents the copied Snapshot
     */
    public abstract SnapshotReader getReader();
}
