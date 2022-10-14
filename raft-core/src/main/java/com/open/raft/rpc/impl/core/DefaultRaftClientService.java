
package com.open.raft.rpc.impl.core;


import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
import com.google.protobuf.Message;
import com.open.raft.Closure;
import com.open.raft.core.ReplicatorGroup;
import com.open.raft.error.RaftError;
import com.open.raft.option.NodeOptions;
import com.open.raft.option.RpcOptions;
import com.open.raft.rpc.RaftClientService;
import com.open.raft.rpc.RpcRequests;
import com.open.raft.rpc.RpcResponseClosure;
import com.open.raft.util.Endpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * Raft rpc service based bolt.
 *
 * @author boyan (boyan@alibaba-inc.com)
 * @author jiachun.fjc
 */
public class DefaultRaftClientService extends AbstractClientService implements RaftClientService {

    private static final FixedThreadsExecutorGroup  APPEND_ENTRIES_EXECUTORS = DefaultFixedThreadsExecutorGroupFactory.INSTANCE
                                                                                 .newExecutorGroup(
                                                                                     Utils.APPEND_ENTRIES_THREADS_SEND,
                                                                                     "Append-Entries-Thread-Send",
                                                                                     Utils.MAX_APPEND_ENTRIES_TASKS_PER_THREAD,
                                                                                     true);

    private final ConcurrentMap<Endpoint, Executor> appendEntriesExecutorMap = new ConcurrentHashMap<>();

    // cached node options
    private NodeOptions nodeOptions;
    private final ReplicatorGroup rgGroup;

    @Override
    protected void configRpcClient(final RpcClient rpcClient) {
        rpcClient.registerConnectEventListener(this.rgGroup);
    }

    public DefaultRaftClientService(final ReplicatorGroup rgGroup) {
        this.rgGroup = rgGroup;
    }

    @Override
    public synchronized boolean init(final RpcOptions rpcOptions) {
        final boolean ret = super.init(rpcOptions);
        if (ret) {
            this.nodeOptions = (NodeOptions) rpcOptions;
        }
        return ret;
    }

    @Override
    public Future<Message> preVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                   final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> requestVote(final Endpoint endpoint, final RpcRequests.RequestVoteRequest request,
                                       final RpcResponseClosure<RpcRequests.RequestVoteResponse> done) {
        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, this.rpcExecutor);
        }

        return invokeWithDone(endpoint, request, done, this.nodeOptions.getElectionTimeoutMs());
    }

    @Override
    public Future<Message> appendEntries(final Endpoint endpoint, final RpcRequests.AppendEntriesRequest request,
                                         final int timeoutMs, final RpcResponseClosure<RpcRequests.AppendEntriesResponse> done) {
        final Executor executor = this.appendEntriesExecutorMap.computeIfAbsent(endpoint, k -> APPEND_ENTRIES_EXECUTORS.next());

        if (!checkConnection(endpoint, true)) {
            return onConnectionFail(endpoint, request, done, executor);
        }

        return invokeWithDone(endpoint, request, done, timeoutMs, executor);
    }

    @Override
    public Future<Message> getFile(final Endpoint endpoint, final RpcRequests.GetFileRequest request, final int timeoutMs,
                                   final RpcResponseClosure<RpcRequests.GetFileResponse> done) {
        // open checksum
        final InvokeContext ctx = new InvokeContext();
        ctx.put(InvokeContext.CRC_SWITCH, true);
        return invokeWithDone(endpoint, request, ctx, done, timeoutMs);
    }

    @Override
    public Future<Message> installSnapshot(final Endpoint endpoint, final RpcRequests.InstallSnapshotRequest request,
                                           final RpcResponseClosure<RpcRequests.InstallSnapshotResponse> done) {
        return invokeWithDone(endpoint, request, done, this.rpcOptions.getRpcInstallSnapshotTimeout());
    }

    @Override
    public Future<Message> timeoutNow(final Endpoint endpoint, final RpcRequests.TimeoutNowRequest request, final int timeoutMs,
                                      final RpcResponseClosure<RpcRequests.TimeoutNowResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    @Override
    public Future<Message> readIndex(final Endpoint endpoint, final RpcRequests.ReadIndexRequest request, final int timeoutMs,
                                     final RpcResponseClosure<RpcRequests.ReadIndexResponse> done) {
        return invokeWithDone(endpoint, request, done, timeoutMs);
    }

    // fail-fast when no connection
    private Future<Message> onConnectionFail(final Endpoint endpoint, final Message request, Closure done, final Executor executor) {
        final FutureImpl<Message> future = new FutureImpl<>();
        executor.execute(() -> {
            final String fmt = "Check connection[%s] fail and try to create new one";
            if (done != null) {
                try {
                    done.run(new Status(RaftError.EINTERNAL, fmt, endpoint));
                } catch (final Throwable t) {
                    LOG.error("Fail to run RpcResponseClosure, the request is {}.", request, t);
                }
            }
            if (!future.isDone()) {
                future.failure(new RemotingException(String.format(fmt, endpoint)));
            }
        });
        return future;
    }
}
