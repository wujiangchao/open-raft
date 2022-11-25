
package com.open.raft.storage;


import com.open.raft.Lifecycle;
import com.open.raft.entity.PeerId;
import com.open.raft.option.RaftMetaStorageOptions;

/**
 * Raft metadata storage service.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 3:54:21 PM
 */
public interface RaftMetaStorage extends Lifecycle<RaftMetaStorageOptions> {

    /**
     * Set current term.
     */
    boolean setTerm(final long term);

    /**
     * Get current term.
     */
    long getTerm();

    /**
     * Set voted for information.
     */
    boolean setVotedFor(final PeerId peerId);

    /**
     * Get voted for information.
     */
    PeerId getVotedFor();

    /**
     * Set term and voted for information.
     */
    boolean setTermAndVotedFor(final long term, final PeerId peerId);
}
