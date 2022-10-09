package com.open.raft.entity;

import java.nio.ByteBuffer;

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
}
