package com.open.raft.storage;

import com.open.raft.Lifecycle;
import com.open.raft.entity.LogEntry;
import com.open.raft.entity.LogId;
import com.open.raft.option.LogManagerOptions;

/**
 * @Description 负责调用底层日志存储 LogStorage，针对日志存储调用进行缓存、批量提交、必要的检查和优化
 * @Date 2022/9/26 17:56
 * @Author jack wu
 */
public interface LogManager extends Lifecycle<LogManagerOptions> {

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
}
