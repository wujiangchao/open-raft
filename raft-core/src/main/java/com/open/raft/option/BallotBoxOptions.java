
package com.open.raft.option;


import com.open.raft.FSMCaller;
import com.open.raft.closure.ClosureQueue;

/**
 * Ballot box options.
 *
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Apr-04 2:58:36 PM
 */
public class BallotBoxOptions {

    private FSMCaller waiter;
    private ClosureQueue closureQueue;

    public FSMCaller getWaiter() {
        return this.waiter;
    }

    public void setWaiter(FSMCaller waiter) {
        this.waiter = waiter;
    }

    public ClosureQueue getClosureQueue() {
        return this.closureQueue;
    }

    public void setClosureQueue(ClosureQueue closureQueue) {
        this.closureQueue = closureQueue;
    }
}
