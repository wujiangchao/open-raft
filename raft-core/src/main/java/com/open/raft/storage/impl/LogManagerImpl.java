package com.open.raft.storage.impl;

import com.alipay.remoting.NamedThreadFactory;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.open.raft.Status;
import com.open.raft.conf.Configuration;
import com.open.raft.conf.ConfigurationEntry;
import com.open.raft.entity.EnumOutter;
import com.open.raft.entity.LogEntry;
import com.open.raft.entity.LogId;
import com.open.raft.error.RaftError;
import com.open.raft.option.LogManagerOptions;
import com.open.raft.option.LogStorageOptions;
import com.open.raft.option.RaftOptions;
import com.open.raft.storage.LogManager;
import com.open.raft.storage.LogStorage;
import com.open.raft.util.Requires;
import com.open.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Description TODO
 * @Date 2022/10/10 9:46
 * @Author jack wu
 */
public class LogManagerImpl implements LogManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(LogManagerImpl.class);

    private LogStorage logStorage;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock writeLock = this.lock.writeLock();
    private final Lock readLock = this.lock.readLock();
    private volatile boolean stopped;
    private volatile boolean hasError;
    private volatile long firstLogIndex;
    private volatile long lastLogIndex;

    private Disruptor<StableClosureEvent> disruptor;
    private RingBuffer<StableClosureEvent> diskQueue;
    private RaftOptions raftOptions;

    private LogId                                            diskId                = new LogId(0, 0);



    private enum EventType {
        OTHER, // other event type.
        RESET, // reset
        TRUNCATE_PREFIX, // truncate log from prefix
        TRUNCATE_SUFFIX, // truncate log from suffix
        SHUTDOWN, //
        LAST_LOG_ID // get last log id
    }

    private static class StableClosureEvent {
        StableClosure done;
        EventType     type;

        void reset() {
            this.done = null;
            this.type = null;
        }
    }

    private static class StableClosureEventFactory implements EventFactory<StableClosureEvent> {

        @Override
        public StableClosureEvent newInstance() {
            return new StableClosureEvent();
        }
    }

    @Override
    public boolean init(LogManagerOptions opts) {
        this.writeLock.lock();
        try {
            if (opts.getLogStorage() == null) {
                LOG.error("Fail to init log manager, log storage is null");
                return false;
            }
            this.raftOptions = opts.getRaftOptions();
            this.nodeMetrics = opts.getNodeMetrics();
            this.logStorage = opts.getLogStorage();
            this.configManager = opts.getConfigurationManager();

            LogStorageOptions lsOpts = new LogStorageOptions();
            lsOpts.setConfigurationManager(this.configManager);
            lsOpts.setLogEntryCodecFactory(opts.getLogEntryCodecFactory());

            if (!this.logStorage.init(lsOpts)) {
                LOG.error("Fail to init logStorage");
                return false;
            }
            // 默认是 1
            this.firstLogIndex = this.logStorage.getFirstLogIndex();
            // 默认是 0
            this.lastLogIndex = this.logStorage.getLastLogIndex();
            // [index =0 ,term = 0]
            this.diskId = new LogId(this.lastLogIndex, getTermFromLogStorage(this.lastLogIndex));
            this.fsmCaller = opts.getFsmCaller();
            this.disruptor = DisruptorBuilder.<StableClosureEvent> newInstance() //
                    .setEventFactory(new StableClosureEventFactory()) //
                    .setRingBufferSize(opts.getDisruptorBufferSize()) //
                    .setThreadFactory(new NamedThreadFactory("JRaft-LogManager-Disruptor-", true)) //
                    .setProducerType(ProducerType.MULTI) //
                    /*
                     *  Use timeout strategy in log manager. If timeout happens, it will called reportError to halt the node.
                     */
                    .setWaitStrategy(new TimeoutBlockingWaitStrategy(
                            this.raftOptions.getDisruptorPublishEventWaitTimeoutSecs(), TimeUnit.SECONDS)) //
                    .build();
            this.disruptor.handleEventsWith(new StableClosureEventHandler());
            this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(this.getClass().getSimpleName(),
                    (event, ex) -> reportError(-1, "LogManager handle event error")));
            this.diskQueue = this.disruptor.start();
            if (this.nodeMetrics.getMetricRegistry() != null) {
                this.nodeMetrics.getMetricRegistry().register("jraft-log-manager-disruptor",
                        new DisruptorMetricSet(this.diskQueue));
            }
        } finally {
            this.writeLock.unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public LogEntry getEntry(long index) {
        return null;
    }

    @Override
    public long getTerm(long index) {
        return 0;
    }

    @Override
    public long getFirstLogIndex() {
        return 0;
    }

    @Override
    public long getLastLogIndex() {
        return 0;
    }

    @Override
    public long getLastLogIndex(boolean isFlush) {
        return 0;
    }

    @Override
    public LogId getLastLogId(boolean isFlush) {
        return null;
    }

    @Override
    public void appendEntries(List<LogEntry> entries, StableClosure done) {
        assert (done != null);

        Requires.requireNonNull(done, "done");
        if (this.hasError) {
            entries.clear();
            Utils.runClosureInThread(done, new Status(RaftError.EIO, "Corrupted LogStorage"));
            return;
        }
        boolean doUnlock = true;
        this.writeLock.lock();
        try {
            if (!entries.isEmpty() && !checkAndResolveConflict(entries, done, this.writeLock)) {
                // If checkAndResolveConflict returns false, the done will be called in it.
                entries.clear();
                return;
            }
            for (int i = 0; i < entries.size(); i++) {
                final LogEntry entry = entries.get(i);
                // Set checksum after checkAndResolveConflict
                if (this.raftOptions.isEnableLogEntryChecksum()) {
                    entry.setChecksum(entry.checksum());
                }
                if (entry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                    Configuration oldConf = new Configuration();
                    if (entry.getOldPeers() != null) {
                        oldConf = new Configuration(entry.getOldPeers(), entry.getOldLearners());
                    }
                    final ConfigurationEntry conf = new ConfigurationEntry(entry.getId(),
                            new Configuration(entry.getPeers(), entry.getLearners()), oldConf);
                    this.configManager.add(conf);
                }
            }
            if (!entries.isEmpty()) {
                done.setFirstLogIndex(entries.get(0).getId().getIndex());
                this.logsInMemory.addAll(entries);
            }
            done.setEntries(entries);

            doUnlock = false;
            if (!wakeupAllWaiter(this.writeLock)) {
                notifyLastLogIndexListeners();
            }

            // publish event out of lock
            this.diskQueue.publishEvent((event, sequence) -> {
                event.reset();
                event.type = EventType.OTHER;
                event.done = done;
            });
        } finally {
            if (doUnlock) {
                this.writeLock.unlock();
            }
        }
    }

    /**
     * 检查Node节点，解决日志冲突。
     * 配置管理器：缓存配置变更
     * LogsInMemory缓存日志Entries
     *
     * @param entries
     * @param done
     * @param lock
     * @return
     */
    private boolean checkAndResolveConflict(final List<LogEntry> entries, final StableClosure done, final Lock lock) {
        final LogEntry firstLogEntry = ArrayDeque.peekFirst(entries);
        if (firstLogEntry.getId().getIndex() == 0) {
            // Node is currently the leader and |entries| are from the user who
            // don't know the correct indexes the logs should assign to. So we have
            // to assign indexes to the appending entries
            for (int i = 0; i < entries.size(); i++) {
                entries.get(i).getId().setIndex(++this.lastLogIndex);
            }
            return true;
        } else {
            // Node is currently a follower and |entries| are from the leader. We
            // should check and resolve the conflicts between the local logs and
            // |entries|
            if (firstLogEntry.getId().getIndex() > this.lastLogIndex + 1) {
                Utils.runClosureInThread(done, new Status(RaftError.EINVAL,
                        "There's gap between first_index=%d and last_log_index=%d", firstLogEntry.getId().getIndex(),
                        this.lastLogIndex));
                return false;
            }
            final long appliedIndex = this.appliedId.getIndex();
            final LogEntry lastLogEntry = ArrayDeque.peekLast(entries);
            if (lastLogEntry.getId().getIndex() <= appliedIndex) {
                LOG.warn(
                        "Received entries of which the lastLog={} is not greater than appliedIndex={}, return immediately with nothing changed.",
                        lastLogEntry.getId().getIndex(), appliedIndex);
                // Replicate old logs before appliedIndex should be considered successfully, response OK.
                Utils.runClosureInThread(done);
                return false;
            }
            if (firstLogEntry.getId().getIndex() == this.lastLogIndex + 1) {
                // fast path
                this.lastLogIndex = lastLogEntry.getId().getIndex();
            } else {
                // Appending entries overlap the local ones. We should find if there
                // is a conflicting index from which we should truncate the local
                // ones.
                int conflictingIndex = 0;
                for (; conflictingIndex < entries.size(); conflictingIndex++) {
                    if (unsafeGetTerm(entries.get(conflictingIndex).getId().getIndex()) != entries
                            .get(conflictingIndex).getId().getTerm()) {
                        break;
                    }
                }
                if (conflictingIndex != entries.size()) {
                    if (entries.get(conflictingIndex).getId().getIndex() <= this.lastLogIndex) {
                        // Truncate all the conflicting entries to make local logs
                        // consensus with the leader.
                        unsafeTruncateSuffix(entries.get(conflictingIndex).getId().getIndex() - 1, lock);
                    }
                    this.lastLogIndex = lastLogEntry.getId().getIndex();
                } // else this is a duplicated AppendEntriesRequest, we have
                // nothing to do besides releasing all the entries
                if (conflictingIndex > 0) {
                    // Remove duplication
                    entries.subList(0, conflictingIndex).clear();
                }
            }
            return true;
        }
    }

    private void notifyLastLogIndexListeners() {
        for (int i = 0; i < this.lastLogIndexListeners.size(); i++) {
            final LastLogIndexListener listener = this.lastLogIndexListeners.get(i);
            if (listener != null) {
                try {
                    listener.onLastLogIndexChanged(this.lastLogIndex);
                } catch (final Exception e) {
                    LOG.error("Fail to notify LastLogIndexListener, listener={}, index={}", listener, this.lastLogIndex);
                }
            }
        }
    }

    private boolean wakeupAllWaiter(final Lock lock) {
        if (this.waitMap.isEmpty()) {
            lock.unlock();
            return false;
        }
        final List<WaitMeta> wms = new ArrayList<>(this.waitMap.values());
        final int errCode = this.stopped ? RaftError.ESTOP.getNumber() : RaftError.SUCCESS.getNumber();
        this.waitMap.clear();
        lock.unlock();

        final int waiterCount = wms.size();
        for (int i = 0; i < waiterCount; i++) {
            final WaitMeta wm = wms.get(i);
            wm.errorCode = errCode;
            Utils.runInThread(() -> runOnNewLog(wm));
        }
        return true;
    }

    private long getTermFromLogStorage(final long index) {
        final LogEntry entry = this.logStorage.getEntry(index);
        if (entry != null) {
            if (this.raftOptions.isEnableLogEntryChecksum() && entry.isCorrupted()) {
                // Report error to node and throw exception.
                final String msg = String.format(
                        "The log entry is corrupted, index=%d, term=%d, expectedChecksum=%d, realChecksum=%d", entry
                                .getId().getIndex(), entry.getId().getTerm(), entry.getChecksum(), entry.checksum());
                reportError(RaftError.EIO.getNumber(), msg);
                throw new LogEntryCorruptedException(msg);
            }

            return entry.getId().getTerm();
        }
        return 0;
    }
}
