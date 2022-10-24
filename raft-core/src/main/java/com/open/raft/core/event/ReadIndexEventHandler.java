package com.open.raft.core.event;

import com.lmax.disruptor.EventHandler;
import com.open.raft.core.ReadOnlyServiceImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Date 2022/10/24 9:18
 * @Author jack wu
 */
public class ReadIndexEventHandler implements EventHandler<ReadIndexEvent> {
    // task list for batch
    private final List<ReadIndexEvent> events = new ArrayList<>(
            ReadOnlyServiceImpl.this.raftOptions.getApplyBatch());

    @Override
    public void onEvent(final ReadIndexEvent newEvent, final long sequence, final boolean endOfBatch)
            throws Exception {
        if (newEvent.shutdownLatch != null) {
            executeReadIndexEvents(this.events);
            reset();
            newEvent.shutdownLatch.countDown();
            return;
        }

        this.events.add(newEvent);
        if (this.events.size() >= ReadOnlyServiceImpl.this.raftOptions.getApplyBatch() || endOfBatch) {
            executeReadIndexEvents(this.events);
            reset();
        }
    }
    private void reset() {
        for (final ReadIndexEvent event : this.events) {
            event.reset();
        }
        this.events.clear();
    }
}
