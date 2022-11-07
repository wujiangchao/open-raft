package com.open.raft.option;

import com.open.raft.FSMCaller;
import com.open.raft.core.NodeImpl;
import com.open.raft.storage.LogManager;
import com.open.raft.util.Endpoint;

/**
 * @Description TODO
 * @Date 2022/11/7 22:43
 * @Author jack wu
 */
public class SnapshotExecutorOptions {
    // URI of SnapshotStorage
    private String           uri;
    private FSMCaller fsmCaller;
    private NodeImpl node;
    private LogManager logManager;
    private long             initTerm;
    private Endpoint addr;
    private boolean          filterBeforeCopyRemote;
    private SnapshotThrottle snapshotThrottle;

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public FSMCaller getFsmCaller() {
        return fsmCaller;
    }

    public void setFsmCaller(FSMCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public NodeImpl getNode() {
        return node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public LogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public long getInitTerm() {
        return initTerm;
    }

    public void setInitTerm(long initTerm) {
        this.initTerm = initTerm;
    }

    public Endpoint getAddr() {
        return addr;
    }

    public void setAddr(Endpoint addr) {
        this.addr = addr;
    }

    public boolean isFilterBeforeCopyRemote() {
        return filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }

    public SnapshotThrottle getSnapshotThrottle() {
        return snapshotThrottle;
    }

    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }
}
