package com.open.raft.core.event;

import com.lmax.disruptor.EventHandler;
import com.open.raft.core.NodeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description handler the logEntryEvent
 * @Date 2022/9/28 10:27
 * @Author jack wu
 */
public class LogEntryEventHandler implements EventHandler<LogEntryEvent> {

    private  final Logger LOG = LoggerFactory.getLogger(LogEntryEventHandler.class);

    private final List<LogEntryEvent> tasks;

    private final NodeImpl node;

    public LogEntryEventHandler(NodeImpl node) {
        this.node = node;
        this.tasks = new ArrayList<>(node.getRaftOptions().getApplyBatch());
    }

    @Override
    public void onEvent(final LogEntryEvent event, final long sequence, final boolean endOfBatch)
            throws Exception {
        if (event.shutdownLatch != null) {
            if (!this.tasks.isEmpty()) {
                node.executeApplyingTasks(this.tasks);
                reset();
            }
            final int num = NodeImpl.GLOBAL_NUM_NODES.decrementAndGet();
            LOG.info("The number of active nodes decrement to {}.", num);
            event.shutdownLatch.countDown();
            return;
        }

        this.tasks.add(event);
        if (this.tasks.size() >= node.getRaftOptions().getApplyBatch() || endOfBatch) {
            node.executeApplyingTasks(this.tasks);
            reset();
        }
    }

    private void reset() {
        for (final LogEntryEvent task : this.tasks) {
            task.reset();
        }
        this.tasks.clear();
    }
}
