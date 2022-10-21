
package com.open.raft.core;

import com.open.raft.entity.codec.LogEntryCodecFactory;
import com.open.raft.entity.codec.v2.LogEntryV2CodecFactory;
import com.open.raft.option.RaftOptions;
import com.open.raft.storage.LogStorage;
import com.open.raft.storage.RaftMetaStorage;
import com.open.raft.storage.SnapshotStorage;
import com.open.raft.util.Requires;
import com.open.raft.util.SPI;
import org.apache.commons.lang.StringUtils;

/**
 * The default factory for JRaft services.
 *
 */
@SPI
public class DefaultJRaftServiceFactory implements JRaftServiceFactory {

    public static DefaultJRaftServiceFactory newInstance() {
        return new DefaultJRaftServiceFactory();
    }

    @Override
    public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(StringUtils.isNotBlank(uri), "Blank log storage uri.");
        return new RocksDBLogStorage(uri, raftOptions);
    }

    @Override
    public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank snapshot storage uri.");
        return new LocalSnapshotStorage(uri, raftOptions);
    }

    @Override
    public RaftMetaStorage createRaftMetaStorage(final String uri, final RaftOptions raftOptions) {
        Requires.requireTrue(!StringUtils.isBlank(uri), "Blank raft meta storage uri.");
        return new LocalRaftMetaStorage(uri, raftOptions);
    }

    @Override
    public LogEntryCodecFactory createLogEntryCodecFactory() {
        return LogEntryV2CodecFactory.getInstance();
    }
}
