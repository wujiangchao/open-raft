
package com.open.raft.storage.snapshot;

import com.open.raft.Lifecycle;
import com.open.raft.entity.RaftOutter;

import java.io.Closeable;

/**
 * Snapshot reader.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 4:53:40 PM
 */
public abstract class SnapshotReader extends Snapshot implements Closeable, Lifecycle<Void> {

    /**
     * Load the snapshot metadata.
     */
    public abstract RaftOutter.SnapshotMeta load();

    /**
     * Generate uri for other peers to copy this snapshot.
     * Return an empty string if some error has occur.
     */
    public abstract String generateURIForCopy();
}
