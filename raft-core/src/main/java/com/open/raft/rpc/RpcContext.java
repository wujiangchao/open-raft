package com.open.raft.rpc;

/**
 * @Description TODO
 * @Date 2022/11/7 23:37
 * @Author jack wu
 */
public interface RpcContext {
    /**
     * Send a response back.
     *
     * @param responseObj the response object
     */
    void sendResponse(final Object responseObj);

    /**
     * Get current connection.
     *
     * @return current connection
     */
    Connection getConnection();

    /**
     * Get the remote address.
     *
     * @return remote address
     */
    String getRemoteAddress();
}
