
package com.open.raft.closure;


import com.open.raft.Closure;

/**
 * Closure for task applying.
 * @author jack wu
 */
public interface TaskClosure extends Closure {

    /**
     * Called when task is committed to majority peers of the
     * RAFT group but before it is applied to state machine.
     * 
     * <strong>Note: user implementation should not block
     * this method and throw any exceptions.</strong>
     */
    void onCommitted();
}
