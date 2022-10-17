package com.open.raft.entity;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @Description TODO
 * @Date 2022/9/28 10:35
 * @Author jack wu
 */
public class LogEntry implements Checksum{
    public static final ByteBuffer EMPTY_DATA = ByteBuffer.wrap(new byte[0]);

    /** entry type */
    private EnumOutter.EntryType   type;
    /** log id with index/term */
    private LogId                  id         = new LogId(0, 0);

    /** log entry current peers */
    private List<PeerId> peers;
    /** log entry old peers */
    private List<PeerId>           oldPeers;
    /** log entry current learners */
    private List<PeerId>           learners;
    /** log entry old learners */
    private List<PeerId>           oldLearners;
    /** entry data */
    private ByteBuffer             data       = EMPTY_DATA;

    public LogEntry() {
        super();
    }

    public LogEntry(final EnumOutter.EntryType type) {
        super();
        this.type = type;
    }


    @Override
    public long checksum() {
        return 0;
    }


    public EnumOutter.EntryType getType() {
        return type;
    }

    public void setType(EnumOutter.EntryType type) {
        this.type = type;
    }

    public LogId getId() {
        return id;
    }

    public void setId(LogId id) {
        this.id = id;
    }

    public static ByteBuffer getEmptyData() {
        return EMPTY_DATA;
    }

    public List<PeerId> getPeers() {
        return peers;
    }

    public void setPeers(List<PeerId> peers) {
        this.peers = peers;
    }

    public List<PeerId> getOldPeers() {
        return oldPeers;
    }

    public void setOldPeers(List<PeerId> oldPeers) {
        this.oldPeers = oldPeers;
    }

    public List<PeerId> getLearners() {
        return learners;
    }

    public void setLearners(List<PeerId> learners) {
        this.learners = learners;
    }

    public List<PeerId> getOldLearners() {
        return oldLearners;
    }

    public void setOldLearners(List<PeerId> oldLearners) {
        this.oldLearners = oldLearners;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }
}
