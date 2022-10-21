package com.open.raft.storage;

import com.open.raft.Lifecycle;
import com.open.raft.entity.LogEntry;
import com.open.raft.option.LogStorageOptions;

import java.util.List;

/**
 * @Description LogStorage 是日志存储实现，默认实现基于 RocksDB 存储，
 *              通过 LogStorage 接口扩展自定义日志存储实现；
 * @Date 2022/9/27 7:17
 * @Author jack wu
 */
public interface LogStorage extends Lifecycle<LogStorageOptions> {

    /**
     * Returns first log index in log.
     */
    long getFirstLogIndex();

    /**
     * Returns last log index in log.
     */
    long getLastLogIndex();

    /**
     * Get logEntry by index.
     */
    LogEntry getEntry(final long index);

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     * @deprecated
     */
    @Deprecated
    long getTerm(final long index);

    /**
     * Append entries to log.
     */
    boolean appendEntry(final LogEntry entry);

    /**
     * Append entries to log, return append success number.
     */
    int appendEntries(final List<LogEntry> entries);

}
