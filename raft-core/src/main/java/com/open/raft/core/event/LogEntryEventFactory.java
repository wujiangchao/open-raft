package com.open.raft.core.event;

import com.lmax.disruptor.EventFactory;

/**
 * @Description TODO
 * @Date 2022/9/28 10:25
 * @Author jack wu
 */
public class LogEntryEventFactory implements EventFactory<LogEntryEvent> {
    @Override
    public LogEntryEvent newInstance() {
        return new LogEntryEvent();
    }
}
