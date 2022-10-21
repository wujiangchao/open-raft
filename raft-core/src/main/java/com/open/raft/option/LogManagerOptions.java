package com.open.raft.option;

import com.open.raft.FSMCaller;
import com.open.raft.conf.ConfigurationManager;
import com.open.raft.core.NodeMetrics;
import com.open.raft.entity.codec.LogEntryCodecFactory;
import com.open.raft.entity.codec.v2.LogEntryV2CodecFactory;
import com.open.raft.storage.LogStorage;

/**
 * @Description TODO
 * @Date 2022/9/26 17:58
 * @Author jack wu
 */
public class LogManagerOptions {
    private LogStorage logStorage;
    private ConfigurationManager configurationManager;
    private FSMCaller fsmCaller;
    private int                  disruptorBufferSize  = 1024;
    private RaftOptions          raftOptions;
    private NodeMetrics nodeMetrics;
    private LogEntryCodecFactory logEntryCodecFactory = LogEntryV2CodecFactory.getInstance();

    public LogStorage getLogStorage() {
        return logStorage;
    }

    public void setLogStorage(LogStorage logStorage) {
        this.logStorage = logStorage;
    }

    public ConfigurationManager getConfigurationManager() {
        return configurationManager;
    }

    public void setConfigurationManager(ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public FSMCaller getFsmCaller() {
        return fsmCaller;
    }

    public void setFsmCaller(FSMCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public int getDisruptorBufferSize() {
        return disruptorBufferSize;
    }

    public void setDisruptorBufferSize(int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public NodeMetrics getNodeMetrics() {
        return nodeMetrics;
    }

    public void setNodeMetrics(NodeMetrics nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    public LogEntryCodecFactory getLogEntryCodecFactory() {
        return logEntryCodecFactory;
    }

    public void setLogEntryCodecFactory(LogEntryCodecFactory logEntryCodecFactory) {
        this.logEntryCodecFactory = logEntryCodecFactory;
    }
}
