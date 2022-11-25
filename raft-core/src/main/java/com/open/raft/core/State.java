
package com.open.raft.core;

/**
 * Node state
 *
 * @author jack.wu
 */
public enum State {
    // It's a leader
    STATE_LEADER,
    // It's transferring leadership
    STATE_TRANSFERRING,
    //  It's a candidate
    STATE_CANDIDATE,
    // It's a follower
    STATE_FOLLOWER,
    // It's in error
    STATE_ERROR,
    // It's uninitialized
    STATE_UNINITIALIZED,
    // It's shutting down
    STATE_SHUTTING,
    // It's shutdown already
    STATE_SHUTDOWN,
    // State end
    STATE_END;

    public boolean isActive() {
        return this.ordinal() < STATE_ERROR.ordinal();
    }
}
