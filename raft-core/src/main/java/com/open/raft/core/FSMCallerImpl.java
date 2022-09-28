package com.open.raft.core;

import com.alipay.remoting.NamedThreadFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
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
    private long lastAppliedTerm;
    private Closure afterShutdown;
    private NodeImpl node;
    private volatile TaskType currTask;
    private final AtomicLong applyingIndex;
    private volatile RaftException error;
    private Disruptor<ApplyTask> disruptor;
    private RingBuffer<ApplyTask> taskQueue;
    private volatile CountDownLatch shutdownLatch;
    private NodeMetrics nodeMetrics;
    private final CopyOnWriteArrayList<LastAppliedLogIndexListener> lastAppliedLogIndexListeners = new CopyOnWriteArrayList<>();

    public FSMCallerImpl() {
        this.currTask = TaskType.IDLE;
        this.lastAppliedIndex = new AtomicLong(0);
        this.applyingIndex = new AtomicLong(0);
    }


    /**
     * Task type
     * 2018-Apr-03 11:12:25 AM
     */
    private enum TaskType {
        IDLE, //
        COMMITTED, //
        SNAPSHOT_SAVE, //
        SNAPSHOT_LOAD, //
        LEADER_STOP, //
        LEADER_START, //
        START_FOLLOWING, //
        STOP_FOLLOWING, //
        SHUTDOWN, //
        FLUSH, //
        ERROR;

        private String metricName;

        public String metricName() {
            if (this.metricName == null) {
                this.metricName = "fsm-" + name().toLowerCase().replaceAll("_", "-");
            }
            return this.metricName;
        }
    }


    /**
     * Apply task for disruptor.
     * <p>
     * 2018-Apr-03 11:12:35 AM
     */
    private static class ApplyTask {
        TaskType type;
        // union fields
        long committedIndex;
        long term;
        Status status;
        LeaderChangeContext leaderChangeCtx;
        Closure done;
        CountDownLatch shutdownLatch;

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

    private class ApplyTaskHandler implements EventHandler<ApplyTask> {
        // max committed index in current batch, reset to -1 every batch
        private long maxCommittedIndex = -1;

        /**
         * @param event      事件处理器正处理的事件
         * @param sequence   当前处理的Event所对应的序号
         * @param endOfBatch 表示RingBuffer中，在该事件之后，是否还有事件，即true就表示无事件了，消费者可以退出了，false表示后面还有事件，需要继续消费
         * @throws Exception
         */
        @Override
        public void onEvent(final ApplyTask event, final long sequence, final boolean endOfBatch) throws Exception {
            this.maxCommittedIndex = runApplyTask(event, this.maxCommittedIndex, endOfBatch);
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
        this.disruptor = DisruptorBuilder.<ApplyTask>newInstance() //
                .setEventFactory(new ApplyTaskFactory()) //
                .setRingBufferSize(opts.getDisruptorBufferSize()) //
                .setThreadFactory(new NamedThreadFactory("Raft-FSMCaller-Disruptor-", true)) //
                .setProducerType(ProducerType.MULTI) //
                .setWaitStrategy(new BlockingWaitStrategy()) //
                .build();
        this.disruptor.handleEventsWith(new ApplyTaskHandler());
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.taskQueue = this.disruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry().register("raft-fsm-caller-disruptor",
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
