package com.open.raft.util;

import com.open.raft.util.timer.RaftTimerFactory;

/**
 * @Description
 * @Date 2022/11/17 14:01
 * @Author jack wu
 */
public class RaftUtils {
    private final static RaftTimerFactory TIMER_FACTORY = JRaftServiceLoader.load(RaftTimerFactory.class)
            .first();

    /**
     * Get raft timer factory.
     *
     * @return {@link RaftTimerFactory}
     */
    public static RaftTimerFactory raftTimerFactory() {
        return TIMER_FACTORY;
    }

}
