package com.open.raft.rpc;

/**
 * @Description TODO
 * @Date 2022/11/15 8:37
 * @Author jack wu
 */
public interface RaftRpcFactory {

    RpcResponseFactory DEFAULT = new RpcResponseFactory() {};

    default RpcResponseFactory getRpcResponseFactory() {
        return DEFAULT;
    }
}
