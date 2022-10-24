package com.open.raft.core.event;

import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.util.Bytes;

import java.util.concurrent.CountDownLatch;

/**
 * @Description TODO
 * @Date 2022/10/24 9:16
 * @Author jack wu
 */
public class ReadIndexEvent {
    public Bytes requestContext;
    public ReadIndexClosure done;
    public CountDownLatch shutdownLatch;
    public long             startTime;

    public void reset() {
        this.requestContext = null;
        this.done = null;
        this.shutdownLatch = null;
        this.startTime = 0L;
    }
}
