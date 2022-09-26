package com.open.raft.core;

import com.alipay.remoting.NamedThreadFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.open.raft.Closure;
import com.open.raft.FSMCaller;
import com.open.raft.StateMachine;
import com.open.raft.Status;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.LeaderChangeContext;
import com.open.raft.option.FSMCallerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.LogManager;

/**
 * @Description TODO
 * @Date 2022/9/26 12:37
 * @Author jack wu
 */
public class FSMCallerImpl implements FSMCaller {

    private static final Logger LOG = LoggerFactory.getLogger(FSMCallerImpl.class);


    private LogManager logManager;
    private StateMachine fsm;
    private ClosureQueue closureQueue;
    private final AtomicLong lastAppliedIndex;
    private long                                                    lastAppliedTerm;
    private Closure afterShutdown;
    private NodeImpl                                                node;
    private volatile TaskType                                       currTask;
    private final AtomicLong                                        applyingIndex;
    private volatile RaftException                                  error;
    private Disruptor<ApplyTask> disruptor;
    private RingBuffer<ApplyTask> taskQueue;
    private volatile CountDownLatch shutdownLatch;
    private NodeMetrics                                             nodeMetrics;
    private final CopyOnWriteArrayList<LastAppliedLogIndexListener> lastAppliedLogIndexListeners = new CopyOnWriteArrayList<>();


    /**
     * Apply task for disruptor.
     *
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 11:12:35 AM
     */
    private static class ApplyTask {
        TaskType            type;
        // union fields
        long                committedIndex;
        long                term;
        Status status;
        LeaderChangeContext leaderChangeCtx;
        Closure             done;
        CountDownLatch      shutdownLatch;

        public void reset() {
            this.type = null;
            this.committedIndex = 0;
            this.term = 0;
            this.status = null;
            this.leaderChangeCtx = null;
            this.done = null;
            this.shutdownLatch = null;
        }
    }

    private static class ApplyTaskFactory implements EventFactory<ApplyTask> {

        @Override
        public ApplyTask newInstance() {
            return new ApplyTask();
        }
    }

    @Override
    public boolean onStopFollowing(LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.STOP_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    @Override
    public boolean init(FSMCallerOptions opts) {
        this.logManager = opts.getLogManager();
        this.fsm = opts.getFsm();
        this.closureQueue = opts.getClosureQueue();
        this.afterShutdown = opts.getAfterShutdown();
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.lastAppliedIndex.set(opts.getBootstrapId().getIndex());
        notifyLastAppliedIndexUpdated(this.lastAppliedIndex.get());
        this.lastAppliedTerm = opts.getBootstrapId().getTerm();
        this.disruptor = DisruptorBuilder.<ApplyTask> newInstance() //
                .setEventFactory(new ApplyTaskFactory()) //
                .setRingBufferSize(opts.getDisruptorBufferSize()) //
                .setThreadFactory(new NamedThreadFactory("JRaft-FSMCaller-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.disruptor.handleEventsWith(new ApplyTaskHandler());
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.taskQueue = this.disruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry().register("jraft-fsm-caller-disruptor",
                    new DisruptorMetricSet(this.taskQueue));
        }
        this.error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_NONE);
        LOG.info("Starts FSMCaller successfully.");
        return true;
    }

    private boolean enqueueTask(final EventTranslator<ApplyTask> tpl) {
        if (this.shutdownLatch != null) {
            // Shutting down
            LOG.warn("FSMCaller is stopped, can not apply new task.");
            return false;
        }
        //使用Disruptor发布事件 taskQueue是在FSMCallerImpl的init方法中被初始化的
        // 在taskQueue中发布了一个任务之后会交给ApplyTaskHandler进行处理
        this.taskQueue.publishEvent(tpl);
        return true;
    }

    @Override
    public void shutdown() {

    }
}
