
package com.open.raft;


import com.open.raft.entity.codec.LogEntryCodecFactory;
import com.open.raft.option.RaftOptions;
import com.open.raft.storage.LogStorage;
import com.open.raft.storage.RaftMetaStorage;
import com.open.raft.storage.SnapshotStorage;

/**
 * Abstract factory to create services for SOFAJRaft.
 *   这个工厂主要用于创建各种存储实现类。这里通过SPI机制暴露给也业务方实现。当然raft有个默认的实现。DefaultJRaftServiceFactory
 * @author boyan(boyan@antfin.com)
 * @since  1.2.6
 */
public interface JRaftServiceFactory {

    /**
     * Creates a raft log storage.
     * @param uri  The log storage uri from {@link NodeOptions#getSnapshotUri()}
     * @param raftOptions  the raft options.
     * @return storage to store raft log entries.
     */
    LogStorage createLogStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a raft snapshot storage
     * @param uri  The snapshot storage uri from {@link NodeOptions#getSnapshotUri()}
     * @param raftOptions  the raft options.
     * @return storage to store state machine snapshot.
     */
    SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a raft meta storage.
     * @param uri  The meta storage uri from {@link NodeOptions#getRaftMetaUri()}
     * @param raftOptions  the raft options.
     * @return meta storage to store raft meta info.
     */
    RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions);

    /**
     * Creates a log entry codec factory.
     * @return a codec factory to create encoder/decoder for raft log entry.
     */
    LogEntryCodecFactory createLogEntryCodecFactory();
}
