package com.open.raft.core;

import com.alipay.remoting.NamedThreadFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.open.raft.FSMCaller;
import com.open.raft.ReadOnlyService;
import com.open.raft.Status;
import com.open.raft.closure.ReadIndexClosure;
import com.open.raft.core.event.ReadIndexEvent;
import com.open.raft.core.event.ReadIndexEventFactory;
import com.open.raft.core.event.ReadIndexEventHandler;
import com.open.raft.error.OverloadException;
import com.open.raft.error.RaftError;
import com.open.raft.error.RaftException;
import com.open.raft.option.RaftOptions;
import com.open.raft.option.ReadOnlyServiceOptions;
import com.open.raft.util.Bytes;
import com.open.raft.util.DisruptorBuilder;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Description TODO
 * @Date 2022/10/24 9:05
 * @Author jack wu
 */
public class ReadOnlyServiceImpl implements ReadOnlyService , FSMCaller.LastAppliedLogIndexListener {
    private static final Logger LOG                 = LoggerFactory.getLogger(ReadOnlyServiceImpl.class);
    /** Disruptor to run readonly service. */
    private Disruptor<ReadIndexEvent> readIndexDisruptor;
    private RingBuffer<ReadIndexEvent> readIndexQueue;
    private RaftOptions raftOptions;
    private NodeImpl                                   node;
    private final Lock lock                = new ReentrantLock();
    private FSMCaller                                  fsmCaller;
    private volatile CountDownLatch shutdownLatch;

    private ScheduledExecutorService scheduledExecutorService;

    private NodeMetrics                                nodeMetrics;

    private volatile RaftException                     error;

    @Override
    public boolean init(final ReadOnlyServiceOptions opts) {
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.fsmCaller = opts.getFsmCaller();
        this.raftOptions = opts.getRaftOptions();

        this.scheduledExecutorService = Executors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("ReadOnlyService-PendingNotify-Scanner", true));
        this.readIndexDisruptor = DisruptorBuilder.<ReadIndexEvent> newInstance()
                .setEventFactory(new ReadIndexEventFactory())
                .setRingBufferSize(this.raftOptions.getDisruptorBufferSize())
                .setThreadFactory(new NamedThreadFactory("Raft-ReadOnlyService-Disruptor-", true))
                .setWaitStrategy(new BlockingWaitStrategy())
                .setProducerType(ProducerType.MULTI)
                .build();
        this.readIndexDisruptor.handleEventsWith(new ReadIndexEventHandler());
        this.readIndexDisruptor
                .setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.readIndexQueue = this.readIndexDisruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry() //
                    .register("jraft-read-only-service-disruptor", new DisruptorMetricSet(this.readIndexQueue));
        }
        // listen on lastAppliedLogIndex change events.
        this.fsmCaller.addLastAppliedLogIndexListener(this);

        // start scanner
        this.scheduledExecutorService.scheduleAtFixedRate(() -> onApplied(this.fsmCaller.getLastAppliedIndex()),
                this.raftOptions.getMaxElectionDelayMs(), this.raftOptions.getMaxElectionDelayMs(), TimeUnit.MILLISECONDS);
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        this.shutdownLatch = new CountDownLatch(1);
        Utils.runInThread( //
                () -> this.readIndexQueue.publishEvent((event, sequence) -> event.shutdownLatch = this.shutdownLatch));
        this.scheduledExecutorService.shutdown();
    }


    @Override
    public void addRequest(byte[] reqCtx, ReadIndexClosure closure) {
        if (this.shutdownLatch != null) {
            Utils.runClosureInThread(closure, new Status(RaftError.EHOSTDOWN, "Was stopped"));
            throw new IllegalStateException("Service already shutdown.");
        }

        try {
            EventTranslator<ReadIndexEvent> translator = (event, sequence) -> {
                event.done = closure;
                event.requestContext = new Bytes(reqCtx);
                event.startTime = Utils.monotonicMs();
            };

            switch (this.node.getOptions().getApplyTaskMode()) {
                case Blocking:
                    this.readIndexQueue.publishEvent(translator);
                    break;
                case NonBlocking:
                default:
                    if (!this.readIndexQueue.tryPublishEvent(translator)) {
                        final String errorMsg = "Node is busy, has too many read-index requests, queue is full and bufferSize=" + this.readIndexQueue.getBufferSize();
                        Utils.runClosureInThread(closure,
                                new Status(RaftError.EBUSY, errorMsg));
                        this.nodeMetrics.recordTimes("read-index-overload-times", 1);
                        LOG.warn("Node {} ReadOnlyServiceImpl readIndexQueue is overload.", this.node.getNodeId());
                        if (closure == null) {
                            throw new OverloadException(errorMsg);
                        }
                    }
                    break;
            }
        } catch (final Exception e) {
            Utils.runClosureInThread(closure, new Status(RaftError.EPERM, "Node is down."));
        }
    }

    @Override
    public void join() throws InterruptedException {

    }

    @Override
    public void setError(RaftException error) {

    }

    @Override
    public void onApplied(long lastAppliedLogIndex) {

    }
}
