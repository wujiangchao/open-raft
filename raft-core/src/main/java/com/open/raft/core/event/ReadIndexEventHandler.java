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
    private final List<ReadIndexEvent> events;
    private ReadOnlyServiceImpl readOnlyService;

    public ReadIndexEventHandler(ReadOnlyServiceImpl readOnlyService) {
        this.readOnlyService = readOnlyService;
        events = new ArrayList<>(
                readOnlyService.getRaftOptions().getApplyBatch());

    }

    @Override
    public void onEvent(final ReadIndexEvent newEvent, final long sequence, final boolean endOfBatch)
            throws Exception {
        if (newEvent.shutdownLatch != null) {
            readOnlyService.executeReadIndexEvents(this.events);
            reset();
            newEvent.shutdownLatch.countDown();
            return;
        }

        this.events.add(newEvent);
        if (this.events.size() >= readOnlyService.getRaftOptions().getApplyBatch() || endOfBatch) {
            readOnlyService.executeReadIndexEvents(this.events);
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
