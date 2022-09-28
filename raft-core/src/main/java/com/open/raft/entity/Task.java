package com.open.raft.entity;

import com.open.raft.Closure;
import com.open.raft.util.Requires;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @Description TODO
 * @Date 2022/9/28 9:17
 * @Author jack wu
 */
public class Task implements Serializable {
    private static final long serialVersionUID = 2971309899898274575L;

    /** Associated  task data*/
    private ByteBuffer data             = LogEntry.EMPTY_DATA;
    /** task closure, called when the data is successfully committed to the raft group or failures happen.*/
    private Closure           done;
    /** Reject this task if expectedTerm doesn't match the current term of this Node if the value is not -1, default is -1.*/
    private long              expectedTerm     = -1;

    public Task() {
        super();
    }

    /**
     * Creates a task with data/done.
     */
    public Task(final ByteBuffer data, final Closure done) {
        super();
        this.data = data;
        this.done = done;
    }

    /**
     * Creates a task with data/done/expectedTerm.
     */
    public Task(final ByteBuffer data, final Closure done, final long expectedTerm) {
        super();
        this.data = data;
        this.done = done;
        this.expectedTerm = expectedTerm;
    }

    public ByteBuffer getData() {
        return this.data;
    }

    public void setData(final ByteBuffer data) {
        Requires.requireNonNull(data, "data should not be null, you can use LogEntry.EMPTY_DATA instead.");
        this.data = data;
    }

    public Closure getDone() {
        return this.done;
    }

    public void setDone(final Closure done) {
        this.done = done;
    }

    public long getExpectedTerm() {
        return this.expectedTerm;
    }

    public void setExpectedTerm(final long expectedTerm) {
        this.expectedTerm = expectedTerm;
    }
}
