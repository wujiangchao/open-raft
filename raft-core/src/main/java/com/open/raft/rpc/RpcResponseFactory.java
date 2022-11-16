package com.open.raft.rpc;

import com.google.protobuf.Message;
import com.open.raft.Status;
import com.open.raft.error.RaftError;

/**
 * @Description TODO
 * @Date 2022/11/16 14:47
 * @Author jack wu
 */
public interface RpcResponseFactory {
    /**
     * This is a convention that if a {@link Message} contains an {@link RpcRequests.ErrorResponse} field,
     * it can only be in position 99.
     */
    int ERROR_RESPONSE_NUM = 99;

    /**
     * Creates a RPC response from status, return OK response
     * when status is null.
     *
     * @param parent parent message
     * @param st     status with response
     * @return a response instance
     */
    default Message newResponse(final Message parent, final Status st) {
        if (st == null) {
            return newResponse(parent, 0, "OK");
        }
        return newResponse(parent, st.getCode(), st.getErrorMsg());
    }

    /**
     * Creates an error response with parameters.
     *
     * @param parent parent message
     * @param error  error with raft info
     * @param fmt    message with format string
     * @param args   arguments referenced by the format specifiers in the format string
     * @return a response instance
     */
    default Message newResponse(final Message parent, final RaftError error, final String fmt, final Object... args) {
        return newResponse(parent, error.getNumber(), fmt, args);
    }

    /**
     * Creates an error response with parameters.
     *
     * @param parent parent message
     * @param code   error code with raft info
     * @param fmt    message with format string
     * @param args   arguments referenced by the format specifiers in the format string
     * @return a response instance
     */
    default Message newResponse(final Message parent, final int code, final String fmt, final Object... args) {
        final RpcRequests.ErrorResponse.Builder eBuilder = RpcRequests.ErrorResponse.newBuilder();
        eBuilder.setErrorCode(code);
        if (fmt != null) {
            eBuilder.setErrorMsg(String.format(fmt, args));
        }
        return eBuilder.build();
    }
}
