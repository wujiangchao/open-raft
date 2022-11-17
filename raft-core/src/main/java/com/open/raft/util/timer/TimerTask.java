package com.open.raft.util.timer;

import io.netty.util.Timeout;

/**
 * @Description TODO
 * @Date 2022/11/17 11:14
 * @Author jack wu
 */
public interface TimerTask {
    /**
     * Executed after the delay specified with
     * Timer#newTimeout(TimerTask, long, TimeUnit).
     *
     * @param timeout a handle which is associated with this task
     */
    void run(final Timeout timeout) throws Exception;
}
