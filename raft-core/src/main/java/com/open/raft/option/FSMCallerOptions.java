package com.open.raft.option;

import com.open.raft.Closure;
import com.open.raft.StateMachine;
import com.open.raft.closure.ClosureQueue;
import com.open.raft.core.NodeImpl;
import com.open.raft.entity.LogId;
import com.open.raft.storage.LogManager;


/**
 * @Description TODO
 * @Date 2022/9/26 11:41
 * @Author jack wu
 */
public class FSMCallerOptions {
    private LogManager logManager;
    private StateMachine fsm;
    private Closure afterShutdown;
    private LogId bootstrapId;
    private ClosureQueue closureQueue;
    private NodeImpl node;
    /**
     * disruptor buffer size.
     */
    private int          disruptorBufferSize = 1024;

    public LogManager getLogManager() {
        return logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public StateMachine getFsm() {
        return fsm;
    }

    public void setFsm(StateMachine fsm) {
        this.fsm = fsm;
    }

    public Closure getAfterShutdown() {
        return afterShutdown;
    }

    public void setAfterShutdown(Closure afterShutdown) {
        this.afterShutdown = afterShutdown;
    }

    public LogId getBootstrapId() {
        return bootstrapId;
    }

    public void setBootstrapId(LogId bootstrapId) {
        this.bootstrapId = bootstrapId;
    }

    public ClosureQueue getClosureQueue() {
        return closureQueue;
    }

    public void setClosureQueue(ClosureQueue closureQueue) {
        this.closureQueue = closureQueue;
    }

    public NodeImpl getNode() {
        return node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }
}
