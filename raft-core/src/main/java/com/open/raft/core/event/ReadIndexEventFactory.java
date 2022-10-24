package com.open.raft.core.event;

import com.lmax.disruptor.EventFactory;

/**
 * @Description TODO
 * @Date 2022/10/24 9:18
 * @Author jack wu
 */
public class ReadIndexEventFactory implements EventFactory<ReadIndexEvent> {
    @Override
    public ReadIndexEvent newInstance() {
        return new ReadIndexEvent();
    }
}
