
package com.open.raft.entity.codec.v2;

import com.open.raft.entity.codec.LogEntryCodecFactory;
import com.open.raft.entity.codec.LogEntryDecoder;
import com.open.raft.entity.codec.LogEntryEncoder;

/**
 * V2(Now) log entry codec implementation, header format:
 *
 *   0  1     2    3  4  5
 *  +-+-+-+-+-+-+-+-++-+-+-+
 *  |Magic|Version|Reserved|
 *  +-+-+-+-+-+-+-+-++-+-+-+
 *
 * @author boyan(boyan@antfin.com)
 * @since 1.2.6
 *
 */
public class LogEntryV2CodecFactory implements LogEntryCodecFactory {

    private static final LogEntryV2CodecFactory INSTANCE = new LogEntryV2CodecFactory();

    public static LogEntryV2CodecFactory getInstance() {
        return INSTANCE;
    }

    // BB-8 and R2D2 are good friends.
    public static final byte[] MAGIC_BYTES = new byte[] { (byte) 0xBB, (byte) 0xD2 };
    // Codec version
    public static final byte   VERSION     = 1;

    public static final byte[] RESERVED    = new byte[3];

    public static final int    HEADER_SIZE = MAGIC_BYTES.length + 1 + RESERVED.length;

    @Override
    public LogEntryEncoder encoder() {
        return V2Encoder.INSTANCE;
    }

    @Override
    public LogEntryDecoder decoder() {
        return AutoDetectDecoder.INSTANCE;
    }

    private LogEntryV2CodecFactory() {
    }
}
