package com.open.raft.error;

/**
 *
 */
public class JRaftException extends RuntimeException {

    private static final long serialVersionUID = 0L;

    public JRaftException() {
    }

    public JRaftException(String message) {
        super(message);
    }

    public JRaftException(String message, Throwable cause) {
        super(message, cause);
    }

    public JRaftException(Throwable cause) {
        super(cause);
    }

    public JRaftException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
