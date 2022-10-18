package com.open.raft.error;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Date 2022/9/26 9:24
 * @Author jack wu
 */
public enum RaftError {

    /**
     * Unknown error
     */
    UNKNOWN(-1),

    /**
     * Success, no error.
     */
    SUCCESS(0),

    /**
     * Permission issue
     */
    EPERM(1008),

    ENODESHUTDOWN(10006),

    /**
     * Server is in busy state
     */
    EBUSY(1009),

    /**
     * Timed out
     */
    ETIMEDOUT(1010),

    /**
     * Internal exception
     */
    EINTERNAL(1004),

    /**
     * <pre>
     * Receive Higher Term Requests
     * </pre>
     * <p>
     * <code>EHIGHERTERMREQUEST = 10007;</code>
     */
    EHIGHERTERMREQUEST(10007),

    /**
     * <pre>
     * Receive Higher Term Response
     * </pre>
     * <p>
     * <code>EHIGHERTERMRESPONSE = 10008;</code>
     */
    EHIGHERTERMRESPONSE(10008),


    /**
     * All Kinds of Timeout(Including Election_timeout, Timeout_now, Stepdown_timeout)
     */
    ERAFTTIMEDOUT(10001);




    private static final Map<Integer, RaftError> RAFT_ERROR_MAP = new HashMap<>();

    static {
        for (final RaftError error : RaftError.values()) {
            RAFT_ERROR_MAP.put(error.getNumber(), error);
        }
    }

    public final int getNumber() {
        return this.value;
    }

    public static RaftError forNumber(final int value) {
        return RAFT_ERROR_MAP.getOrDefault(value, UNKNOWN);
    }

    public static String describeCode(final int code) {
        RaftError e = forNumber(code);
        return e != null ? e.name() : "<Unknown:" + code + ">";
    }

    private final int value;

    RaftError(final int value) {
        this.value = value;
    }
}
