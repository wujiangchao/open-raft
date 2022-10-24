
package com.open.raft.entity;

import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.util.Bytes;

/**
 * ReadIndex state
 *
 * @author jack.wu
 */
public class ReadIndexState {

    /**
     * The committed log index
     */
    private long index = -1;
    /**
     * User request context
     */
    private final Bytes requestContext;
    /**
     * User ReadIndex closure
     */
    private final ReadIndexClosure done;
    /**
     * Request start timestamp
     */
    private final long startTimeMs;

    public ReadIndexState(Bytes requestContext, ReadIndexClosure done, long startTimeMs) {
        super();
        this.requestContext = requestContext;
        this.done = done;
        this.startTimeMs = startTimeMs;
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public Bytes getRequestContext() {
        return requestContext;
    }

    public ReadIndexClosure getDone() {
        return done;
    }

}
