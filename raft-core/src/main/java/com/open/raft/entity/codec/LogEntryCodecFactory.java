
package com.open.raft.entity.codec;

/**
 * Log entry codec factory to create encoder/decoder for LogEntry.
 *
 */
public interface LogEntryCodecFactory {
    /**
     * Returns a log entry encoder.
     * @return encoder instance
     */
    LogEntryEncoder encoder();

    /**
     * Returns a log entry decoder.
     * @return encoder instance
     */
    LogEntryDecoder decoder();
}
