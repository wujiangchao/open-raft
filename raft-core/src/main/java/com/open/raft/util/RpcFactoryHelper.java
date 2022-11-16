
package com.open.raft.util;


import com.open.raft.rpc.RaftRpcFactory;
import com.open.raft.rpc.RpcResponseFactory;

/**
 * @auth jack.wu
 */
public class RpcFactoryHelper {

    private static final RaftRpcFactory RPC_FACTORY = JRaftServiceLoader.load(RaftRpcFactory.class)
            .first();

    public static RaftRpcFactory rpcFactory() {
        return RPC_FACTORY;
    }

    public static RpcResponseFactory responseFactory() {
        return RPC_FACTORY.getRpcResponseFactory();
    }
}
