package com.open.raft.core.event;

import com.open.raft.Closure;
import com.open.raft.entity.LogEntry;

import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Date 2022/9/28 10:24
 * @Author jack wu
 */
public class LogEntryEvent {
    public LogEntry entry;
    public Closure done;
    public long expectedTerm;
    CountDownLatch shutdownLatch;

    public void reset() {
        this.entry = null;
        this.done = null;
        this.expectedTerm = 0;
        this.shutdownLatch = null;
    }
}
