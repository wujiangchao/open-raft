package com.open.raft;


import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.error.RaftException;
import com.open.raft.option.ReadOnlyServiceOptions;

/**
 * The read-only query service.
 *
 * @author dennis
 *
 */
public interface ReadOnlyService extends Lifecycle<ReadOnlyServiceOptions> {

    /**
     * Adds a ReadIndex request.
     *
     * @param reqCtx    request context of readIndex
     * @param closure   callback
     */
    void addRequest(final byte[] reqCtx, final ReadIndexClosure closure);

    /**
     * Waits for service shutdown.
     *
     * @throws InterruptedException if the current thread is interrupted
     *         while waiting
     */
    void join() throws InterruptedException;

    /**
     * Called when the node is turned into error state.
     * @param error error with raft info
     */
    void setError(final RaftException error);

}
