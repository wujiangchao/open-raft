package com.open.raft.storage.snapshot;

import com.google.protobuf.Message;
import com.open.raft.Status;

import java.util.Set;

/**
 * @Description TODO
 * @Date 2022/11/16 18:02
 * @Author jack wu
 */
public abstract class Snapshot extends Status {
    /**
     * Snapshot metadata file name.
     */
    public static final String JRAFT_SNAPSHOT_META_FILE   = "__raft_snapshot_meta";
    /**
     * Snapshot file prefix.
     */
    public static final String JRAFT_SNAPSHOT_PREFIX      = "snapshot_";
    /** Snapshot uri scheme for remote peer */
    public static final String REMOTE_SNAPSHOT_URI_SCHEME = "remote://";

    /**
     * Get the path of the Snapshot
     */
    public abstract String getPath();

    /**
     * List all the existing files in the Snapshot currently
     */
    public abstract Set<String> listFiles();

    /**
     * Get file meta by fileName.
     */
    public abstract Message getFileMeta(final String fileName);
}
