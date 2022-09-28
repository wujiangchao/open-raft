package com.open.raft.core.event;

import com.lmax.disruptor.EventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Date 2022/9/28 10:27
 * @Author jack wu
 */
public class LogEntryEventHandler implements EventHandler<LogEntryEvent> {
    // task list for batch
    private final List<LogEntryEvent> tasks = new ArrayList<>(NodeImpl.this.raftOptions.getApplyBatch());

    @Override
    public void onEvent(LogEntryEvent logEntryEvent, long sequnce, boolean endOfBatch) throws Exception {

    }
}
