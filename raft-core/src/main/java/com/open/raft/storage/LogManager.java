package com.open.raft.storage;

import com.open.raft.Closure;
import com.open.raft.Lifecycle;
import com.open.raft.entity.LogEntry;
import com.open.raft.entity.LogId;
import com.open.raft.option.LogManagerOptions;

import java.util.List;

/**
 * @Description 负责调用底层日志存储 LogStorage，针对日志存储调用进行缓存、批量提交、必要的检查和优化
 * @Date 2022/9/26 17:56
 * @Author jack wu
 */
public interface LogManager extends Lifecycle<LogManagerOptions> {

    abstract class StableClosure implements Closure {

        protected long           firstLogIndex = 0;
        protected List<LogEntry> entries;
        protected int            nEntries;

        public StableClosure() {
            // NO-OP
        }

        public long getFirstLogIndex() {
            return this.firstLogIndex;
        }

        public void setFirstLogIndex(final long firstLogIndex) {
            this.firstLogIndex = firstLogIndex;
        }

        public List<LogEntry> getEntries() {
            return this.entries;
        }

        public void setEntries(final List<LogEntry> entries) {
            this.entries = entries;
            if (entries != null) {
                this.nEntries = entries.size();
            } else {
                this.nEntries = 0;
            }
        }

        public StableClosure(final List<LogEntry> entries) {
            super();
            setEntries(entries);
        }

    }


    /**
     * Get the log entry at index.
     *
     * @param index the index of log entry
     * @return the log entry with {@code index}
     */
    LogEntry getEntry(final long index);

    /**
     * Get the log term at index.
     *
     * @param index the index of log entry
     * @return the term of log entry
     */
    long getTerm(final long index);

    /**
     * Get the first log index of log
     */
    long getFirstLogIndex();

    /**
     * Get the last log index of log
     */
    long getLastLogIndex();

    /**
     * Get the last log index of log
     *
     * @param isFlush whether to flush from disk.
     */
    long getLastLogIndex(final boolean isFlush);

    /**
     * Return the id the last log.
     *
     * @param isFlush whether to flush all pending task.
     */
    LogId getLastLogId(final boolean isFlush);

    /**
     * Append log entry vector and wait until it's stable (NOT COMMITTED!)
     *
     * @param entries log entries
     * @param done    callback
     */
    void appendEntries(final List<LogEntry> entries, StableClosure done);
}
