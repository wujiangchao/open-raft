package com.open.raft.util.concurrent;

import com.open.raft.INode;
import com.open.raft.core.NodeImpl;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

  public class NodeReadWriteLock extends LongHeldDetectingReadWriteLock {
      private static final Logger LOG = LoggerFactory.getLogger(NodeImpl.class);


      static final long MAX_BLOCKING_MS_TO_REPORT = SystemPropertyUtil.getLong(
            "jraft.node.detecting.lock.max_blocking_ms_to_report", -1);

    private final INode node;

    public NodeReadWriteLock(final INode node) {
        super(MAX_BLOCKING_MS_TO_REPORT, TimeUnit.MILLISECONDS);
        this.node = node;
    }

    @Override
    public void report(final AcquireMode acquireMode, final Thread heldThread,
                       final Collection<Thread> queuedThreads, final long blockedNanos) {
        final long blockedMs = TimeUnit.NANOSECONDS.toMillis(blockedNanos);
        LOG.warn(
                "Raft-Node-Lock report: currentThread={}, acquireMode={}, heldThread={}, queuedThreads={}, blockedMs={}.",
                Thread.currentThread(), acquireMode, heldThread, queuedThreads, blockedMs);

//        final NodeMetrics metrics = this.node.getNodeMetrics();
//        if (metrics != null) {
//            metrics.recordLatency("node-lock-blocked", blockedMs);
//        }
    }
}