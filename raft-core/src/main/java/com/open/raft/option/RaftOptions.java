package com.open.raft.option;

import com.open.raft.util.Copiable;

/**
 * @Description 用于设置跟性能和数据可靠性相关的参数
 * @Date 2022/9/22 18:57
 * @Author jack wu
 */
public class RaftOptions implements Copiable<RaftOptions> {

    /**
     * Maximum number of tasks that can be applied in a batch
     *  (Disruptor handler batch size)
     */
    private int applyBatch = 32;

    /**
     * Raft election:heartbeat timeout factor
     */
    private int electionHeartbeatFactor = 10;

    /**
     * The maximum replicator pipeline in-flight requests/responses, only valid when enable replicator pipeline.
     */
    private int maxReplicatorInflightMsgs = 256;

    /**
     * Internal disruptor buffers size for Node/FSMCaller/LogManager etc.
     */
    private int disruptorBufferSize = 16384;

    /**
     * When true, validate log entry checksum when transferring the log entry from disk or network, default is false.
     * If true, it would hurt the performance of JRAft but gain the data safety.
     *
     */
    private boolean enableLogEntryChecksum = false;



    @Override
    public RaftOptions copy() {
        return null;
    }

    public int getApplyBatch() {
        return applyBatch;
    }

    public void setApplyBatch(int applyBatch) {
        this.applyBatch = applyBatch;
    }

    public int getElectionHeartbeatFactor() {
        return electionHeartbeatFactor;
    }

    public void setElectionHeartbeatFactor(int electionHeartbeatFactor) {
        this.electionHeartbeatFactor = electionHeartbeatFactor;
    }

    public int getMaxReplicatorInflightMsgs() {
        return maxReplicatorInflightMsgs;
    }

    public void setMaxReplicatorInflightMsgs(int maxReplicatorInflightMsgs) {
        this.maxReplicatorInflightMsgs = maxReplicatorInflightMsgs;
    }

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public boolean isEnableLogEntryChecksum() {
        return enableLogEntryChecksum;
    }

    public void setEnableLogEntryChecksum(boolean enableLogEntryChecksum) {
        this.enableLogEntryChecksum = enableLogEntryChecksum;
    }
}
