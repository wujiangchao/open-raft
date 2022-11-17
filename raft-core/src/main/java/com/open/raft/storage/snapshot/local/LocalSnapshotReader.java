package com.open.raft.storage.snapshot.local;

import com.google.protobuf.Message;
import com.open.raft.entity.RaftOutter;
import com.open.raft.option.RaftOptions;
import com.open.raft.storage.snapshot.SnapshotReader;
import com.open.raft.storage.snapshot.SnapshotThrottle;
import com.open.raft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * @Description snapshotReader本地文件系统实现
 * @Date 2022/11/17 10:01
 * @Author jack wu
 */
public class LocalSnapshotReader extends SnapshotReader {

    private static final Logger LOG = LoggerFactory.getLogger(LocalSnapshotReader.class);

    /**
     * Generated reader id
     */
    private long readerId;
    /**
     * remote peer addr
     */
    private final Endpoint addr;
    private final LocalSnapshotMetaTable metaTable;
    private final String path;
    private final LocalSnapshotStorage snapshotStorage;
    private final SnapshotThrottle snapshotThrottle;

    public LocalSnapshotReader(LocalSnapshotStorage snapshotStorage, SnapshotThrottle snapshotThrottle, Endpoint addr,
                               RaftOptions raftOptions, String path) {
        super();
        this.snapshotStorage = snapshotStorage;
        this.snapshotThrottle = snapshotThrottle;
        this.addr = addr;
        this.path = path;
        this.readerId = 0;
        this.metaTable = new LocalSnapshotMetaTable(raftOptions);
    }

    @Override
    public boolean init(Void opts) {
        return false;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public String getPath() {
        return null;
    }

    @Override
    public Set<String> listFiles() {
        return null;
    }

    @Override
    public Message getFileMeta(String fileName) {
        return null;
    }

    @Override
    public RaftOutter.SnapshotMeta load() {
        return null;
    }

    @Override
    public String generateURIForCopy() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
