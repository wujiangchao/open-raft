
package com.open.raft.storage.snapshot;

import com.open.raft.Closure;
import com.open.raft.Lifecycle;
import com.open.raft.core.NodeImpl;
import com.open.raft.option.SnapshotExecutorOptions;
import com.open.raft.rpc.RpcRequestClosure;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.storage.SnapshotStorage;

/**
 * Executing Snapshot related stuff.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-22 2:27:02 PM
 */
public interface SnapshotExecutor extends Lifecycle<SnapshotExecutorOptions>, Describer {

    /**
     * Return the owner NodeImpl
     */
    NodeImpl getNode();

    /**
     * Start to snapshot StateMachine, and |done| is called after the
     * execution finishes or fails.
     *
     * @param done snapshot callback
     */
    void doSnapshot(final Closure done);

    /**
     * Install snapshot according to the very RPC from leader
     * After the installing succeeds (StateMachine is reset with the snapshot)
     * or fails, done will be called to respond
     * Errors:
     *  - Term mismatches: which happens interrupt_downloading_snapshot was 
     *    called before install_snapshot, indicating that this RPC was issued by
     *    the old leader.
     *  - Interrupted: happens when interrupt_downloading_snapshot is called or
     *    a new RPC with the same or newer snapshot arrives
     * - Busy: the state machine is saving or loading snapshot
     */
    void installSnapshot(final RpcRequests.InstallSnapshotRequest request, final RpcRequests.InstallSnapshotResponse.Builder response,
                         final RpcRequestClosure done);

    /**
     * Interrupt the downloading if possible.
     * This is called when the term of node increased to |new_term|, which
     * happens when receiving RPC from new peer. In this case, it's hard to
     * determine whether to keep downloading snapshot as the new leader
     * possibly contains the missing logs and is going to send AppendEntries. To
     * make things simplicity and leader changing during snapshot installing is 
     * very rare. So we interrupt snapshot downloading when leader changes, and
     * let the new leader decide whether to install a new snapshot or continue 
     * appending log entries.
     * 
     * NOTE: we can't interrupt the snapshot installing which has finished
     *  downloading and is reseting the State Machine.
     *
     * @param newTerm new term num
     */
    void interruptDownloadingSnapshots(final long newTerm);

    /**
     * Returns true if this is currently installing a snapshot, either
     * downloading or loading.
     */
    boolean isInstallingSnapshot();

    /**
     * Returns the backing snapshot storage
     */
    SnapshotStorage getSnapshotStorage();

    /**
     * Block the current thread until all the running job finishes (including failure)
     */
    void join() throws InterruptedException;
}
